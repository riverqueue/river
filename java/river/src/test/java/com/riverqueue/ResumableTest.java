package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class ResumableTest {
  private static final JobType<String> TYPE = JobType.of("checkpoint_test", String.class);
  @RegisterExtension final TestDatabase databases = new TestDatabase();
  @TempDir Path directory;

  @ParameterizedTest
  @CsvSource({
    "false,false,false",
    "false,false,true",
    "false,true,false",
    "false,true,true",
    "true,false,false",
    "true,false,true",
    "true,true,false",
    "true,true,true"
  })
  @SuppressWarnings("try") // Resources intentionally scope background activity.
  void checkpointCommitsWithApplicationWrites(
      boolean priorCheckpoint, boolean rollback, boolean withCursor) throws Exception {
    var client = new Client(databases.open(directory.resolve("river.db")));
    var events = new LinkedBlockingQueue<Workers.Event>();
    var errors = new LinkedBlockingQueue<Throwable>();
    var preparations = new AtomicInteger();
    var steps = new LinkedBlockingQueue<Integer>();
    try (var workers =
            client
                .workers()
                .queue("default", 1)
                .pollOnly(true)
                .leadership(false)
                .retryPolicy(job -> Duration.ofDays(1))
                .stopTimeout(Duration.ofSeconds(5))
                .errorHandler(errors::add)
                .add(
                    TYPE,
                    context -> {
                      if (priorCheckpoint)
                        context.stepWithCursor(
                            "initial",
                            cursor -> {
                              assertEquals(1, context.job().attempt());
                              context.transaction(connection -> context.checkpoint(connection, 1));
                            });
                      context.step(
                          "prepare", () -> assertEquals(1, preparations.incrementAndGet()));
                      WorkContext.CursorStep action =
                          cursor -> {
                            steps.add(context.job().attempt());
                            if (context.job().attempt() > 1) {
                              if (withCursor && !rollback)
                                assertEquals(2, cursor.path("offset").asInt());
                              else assertTrue(cursor.isMissingNode());
                              return;
                            }
                            context.output("deferred");
                            if (withCursor) context.cursor(Map.of("offset", 1));
                            context.transaction(
                                connection -> {
                                  client.insert(
                                      connection,
                                      TYPE,
                                      "application write",
                                      InsertOptions.builder().queue("other").build());
                                  var updated =
                                      withCursor
                                          ? context.checkpoint(connection, Map.of("offset", 2))
                                          : context.checkpoint(connection);
                                  assertEquals(Job.State.RUNNING, updated.state());
                                  assertEquals(
                                      "process",
                                      updated
                                          .metadata()
                                          .path(Protocol.METADATA_RESUMABLE_STEP)
                                          .asString());
                                  assertFalse(updated.metadata().has(Protocol.METADATA_OUTPUT));
                                  if (rollback) throw new IllegalStateException("rollback");
                                  return null;
                                });
                            throw new IllegalStateException("retry after commit");
                          };
                      if (withCursor) context.stepWithCursor("process", action);
                      else context.step("process", () -> action.run(Json.object().path("missing")));
                    })
                .start();
        var subscription = workers.subscribe(events::add)) {
      var inserted =
          client
              .insert(
                  TYPE,
                  "work",
                  InsertOptions.builder()
                      .metadata(Map.of("unrelated", Map.of("keep", true)))
                      .build())
              .job();
      var failed = events.poll(5, TimeUnit.SECONDS);
      assertNotNull(failed);
      assertEquals(Workers.EventKind.JOB_FAILED, failed.kind());
      var saved = client.get(inserted.id());
      assertEquals(
          rollback ? "prepare" : "process",
          saved.metadata().path(Protocol.METADATA_RESUMABLE_STEP).asString());
      assertEquals("deferred", saved.metadata().path(Protocol.METADATA_OUTPUT).asString());
      assertTrue(saved.metadata().path("unrelated").path("keep").asBoolean());
      assertEquals(rollback ? 1 : 2, client.list(JobQuery.all()).jobs().size());
      if (rollback) {
        var cursor = saved.metadata().path(Protocol.METADATA_RESUMABLE_CURSOR);
        assertTrue(cursor.isMissingNode() || cursor.isNull());
      }
      client.retry(inserted.id());
      var completed = events.poll(5, TimeUnit.SECONDS);
      assertNotNull(completed);
      assertEquals(Workers.EventKind.JOB_COMPLETED, completed.kind());
      assertEquals(1, preparations.get());
      assertEquals(
          rollback || withCursor ? java.util.List.of(1, 2) : java.util.List.of(1),
          java.util.List.copyOf(steps));
    }
    assertTrue(errors.isEmpty(), () -> "Unexpected worker errors: " + errors);
  }

  @Test
  void checkpointRejectsInvalidTransactionsAndStaleAttempts() throws Exception {
    var client = new Client(databases.open(directory.resolve("river.db")));
    var inserted = client.insert(TYPE, "work").job();
    client.transaction(
        connection -> {
          try (var statement =
              Sql.prepare(
                  connection,
                  "UPDATE "
                      + client.database().prefix()
                      + "river_job SET state='running',attempt=1,attempted_at=? WHERE id=?",
                  client.database().timestamp(Instant.now()),
                  inserted.id())) {
            statement.executeUpdate();
          }
          return null;
        });
    var job = client.get(inserted.id(), TYPE);
    var context = new WorkContext<>(client, job);
    try (var connection = client.database().connection()) {
      assertThrows(IllegalStateException.class, () -> context.checkpoint(connection));
      context.step(
          "process",
          () -> {
            assertThrows(IllegalArgumentException.class, () -> context.checkpoint(connection));
            connection.setAutoCommit(false);
            client.complete(connection, job);
            assertEquals(
                RiverException.Code.REJECTED,
                assertThrows(RiverException.class, () -> context.checkpoint(connection)).code());
            assertEquals(Job.State.COMPLETED, client.get(connection, job.id()).state());
            connection.rollback();
            try (var statement =
                Sql.prepare(
                    connection,
                    "UPDATE " + client.database().prefix() + "river_job SET attempt=2 WHERE id=?",
                    job.id())) {
              statement.executeUpdate();
            }
            assertThrows(RiverException.class, () -> context.checkpoint(connection));
            connection.rollback();
          });
    }
    assertFalse(client.get(job.id()).metadata().has(Protocol.METADATA_RESUMABLE_STEP));
  }

  @ParameterizedTest
  @CsvSource({"false,false", "true,false", "false,true"})
  void checkpointRetainsOnlyCommittedProgress(boolean rollback, boolean finishStep)
      throws Exception {
    var client = new Client(databases.open(directory.resolve("river.db")));
    var inserted = client.insert(TYPE, "work").job();
    client.transaction(
        connection -> {
          try (var statement =
              Sql.prepare(
                  connection,
                  "UPDATE "
                      + client.database().prefix()
                      + "river_job SET state='running',attempt=1,attempted_at=? WHERE id=?",
                  client.database().timestamp(Instant.now()),
                  inserted.id())) {
            statement.executeUpdate();
          }
          return null;
        });
    var context = new WorkContext<>(client, client.get(inserted.id(), TYPE));
    var failure = new IllegalStateException("retry");
    WorkContext.Step work =
        () ->
            context.stepWithCursor(
                "process",
                cursor -> {
                  context.transaction(connection -> context.checkpoint(connection, 1));
                  context.transaction(
                      connection -> {
                        context.checkpoint(connection, 2);
                        if (rollback) throw failure;
                        return null;
                      });
                  if (!finishStep) throw failure;
                });
    if (finishStep) work.run();
    else assertSame(failure, assertThrows(IllegalStateException.class, work::run));
    context.finish(failure);
    if (finishStep) {
      assertEquals(
          "process", context.metadataUpdates().path(Protocol.METADATA_RESUMABLE_STEP).asString());
      assertTrue(context.metadataUpdates().path(Protocol.METADATA_RESUMABLE_CURSOR).isNull());
    } else assertTrue(context.metadataUpdates().isEmpty(), "Checkpoint must not be buffered again");
    assertEquals(
        rollback ? 1 : 2,
        client
            .get(inserted.id())
            .metadata()
            .path(Protocol.METADATA_RESUMABLE_CURSOR)
            .path("process")
            .asInt());
  }

  @ParameterizedTest
  @ValueSource(strings = {"plain", "cursor", "null", "first", "first_null"})
  @SuppressWarnings("try") // Resources intentionally scope background activity.
  void cursorDoesNotCompleteFailedSteps(String mode) throws Exception {
    var client = new Client(databases.open(directory.resolve("river.db")));
    var events = new LinkedBlockingQueue<Workers.Event>();
    var errors = new LinkedBlockingQueue<Throwable>();
    var prepares = new AtomicInteger();
    var calls = new AtomicInteger();
    boolean first = mode.startsWith("first");
    boolean nullCursor = mode.contains("null");
    try (var workers =
            client
                .workers()
                .queue("default", 1)
                .pollOnly(true)
                .leadership(false)
                .retryPolicy(job -> Duration.ofDays(1))
                .stopTimeout(Duration.ofSeconds(5))
                .errorHandler(errors::add)
                .add(
                    TYPE,
                    context -> {
                      if (!first)
                        context.step("prepare", () -> assertEquals(1, prepares.incrementAndGet()));
                      WorkContext.CursorStep action =
                          cursor -> {
                            calls.incrementAndGet();
                            if (context.job().attempt() == 1) {
                              context.cursor(nullCursor ? null : Map.of("offset", 1));
                              throw new IllegalStateException("retry");
                            }
                            if (!mode.equals("plain")) {
                              if (nullCursor) assertTrue(cursor.isNull());
                              else assertEquals(1, cursor.path("offset").asInt());
                            }
                          };
                      if (mode.equals("plain"))
                        context.step("process", () -> action.run(Json.tree(null)));
                      else context.stepWithCursor("process", action);
                    })
                .start();
        var subscription = workers.subscribe(events::add)) {
      var job = client.insert(TYPE, "args").job();
      var failed = events.poll(5, TimeUnit.SECONDS);
      assertNotNull(failed);
      assertEquals(Workers.EventKind.JOB_FAILED, failed.kind());
      var metadata = client.get(job.id()).metadata();
      assertEquals(
          first ? "" : "prepare", metadata.path(Protocol.METADATA_RESUMABLE_STEP).asString(""));
      assertTrue(metadata.path(Protocol.METADATA_RESUMABLE_CURSOR).has("process"));
      client.retry(job.id());
      var completed = events.poll(5, TimeUnit.SECONDS);
      assertNotNull(completed);
      assertEquals(Workers.EventKind.JOB_COMPLETED, completed.kind());
      assertEquals(2, calls.get());
      assertEquals(first ? 0 : 1, prepares.get());
    }
    assertTrue(errors.isEmpty(), () -> "Unexpected worker errors: " + errors);
  }

  @Test
  void cursorInFirstStepIsBufferedAndNestedStepsAreRejected() throws Exception {
    var client = new Client(databases.open(directory.resolve("river.db")));
    var context = new WorkContext<>(client, client.insert(TYPE, "work").job());
    var failure = new IllegalStateException("retry");
    assertSame(
        failure,
        assertThrows(
            IllegalStateException.class,
            () ->
                context.stepWithCursor(
                    "first",
                    cursor -> {
                      assertThrows(
                          IllegalStateException.class, () -> context.step("nested", () -> {}));
                      context.cursor(Map.of("offset", 3));
                      throw failure;
                    })));
    assertSame(failure, context.finish(failure));
    assertFalse(context.metadataUpdates().has(Protocol.METADATA_RESUMABLE_STEP));
    assertEquals(
        3,
        context
            .metadataUpdates()
            .path(Protocol.METADATA_RESUMABLE_CURSOR)
            .path("first")
            .path("offset")
            .asInt());
  }

  @Test
  @SuppressWarnings("try") // Resources intentionally scope background activity.
  void cursorMapsReplaceCompletedStepsAndPreserveNullValues() throws Exception {
    var client = new Client(databases.open(directory.resolve("river.db")));
    var events = new LinkedBlockingQueue<Workers.Event>();
    var errors = new LinkedBlockingQueue<Throwable>();
    var firstCalls = new AtomicInteger();
    try (var workers =
            client
                .workers()
                .queue("default", 1)
                .pollOnly(true)
                .leadership(false)
                .retryPolicy(job -> Duration.ofDays(1))
                .stopTimeout(Duration.ofSeconds(5))
                .errorHandler(errors::add)
                .add(
                    TYPE,
                    context -> {
                      context.stepWithCursor(
                          "first",
                          cursor -> {
                            firstCalls.incrementAndGet();
                            if (context.job().attempt() == 1) {
                              context.cursor(1);
                              throw new IllegalStateException("retry first");
                            }
                            assertEquals(1, cursor.asInt());
                          });
                      context.stepWithCursor(
                          "second",
                          cursor -> {
                            if (context.job().attempt() == 2) {
                              context.cursor(
                                  Json.parse("{\"offset\":2,\"nested\":{\"value\":null}}"));
                              throw new IllegalStateException("retry second");
                            }
                            assertEquals(2, cursor.path("offset").asInt());
                            assertTrue(cursor.path("nested").has("value"));
                            assertTrue(cursor.path("nested").path("value").isNull());
                          });
                    })
                .start();
        var subscription = workers.subscribe(events::add)) {
      var job = client.insert(TYPE, "args").job();
      for (int attempt = 1; attempt <= 3; attempt++) {
        var event = events.poll(5, TimeUnit.SECONDS);
        assertNotNull(event);
        assertEquals(
            attempt == 3 ? Workers.EventKind.JOB_COMPLETED : Workers.EventKind.JOB_FAILED,
            event.kind());
        if (attempt == 2) {
          var cursors = client.get(job.id()).metadata().path(Protocol.METADATA_RESUMABLE_CURSOR);
          assertFalse(cursors.has("first"));
          assertTrue(cursors.has("second"));
        }
        if (attempt < 3) client.retry(job.id());
      }
      assertEquals(2, firstCalls.get());
    }
    assertTrue(errors.isEmpty(), () -> "Unexpected worker errors: " + errors);
  }
}
