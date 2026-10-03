package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.sql.Connection;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class ClientTest {
  private static final JobType<Email> EMAIL = JobType.of("email", Email.class);
  private static final Instant NOW = Instant.parse("2026-01-02T03:04:05Z");
  @TempDir Path directory;
  @RegisterExtension final TestDatabase databases = new TestDatabase();

  private Client database() {
    var database = databases.open(directory.resolve("river.db"));
    return new Client(database, Clock.fixed(NOW, ZoneOffset.UTC));
  }

  @Test
  void applicationTransactionOwnsCommitAndRollback() throws Exception {
    var river = database();
    try (var connection = river.database().connection()) {
      connection.setAutoCommit(false);
      long rolledBack = river.insert(connection, EMAIL, new Email("one@example.com")).job().id();
      assertEquals(rolledBack, river.get(connection, rolledBack).id());
      assertThrows(RiverException.class, () -> river.get(rolledBack));
      connection.rollback();
      assertThrows(RiverException.class, () -> river.get(rolledBack));
      long committed = river.insert(connection, EMAIL, new Email("two@example.com")).job().id();
      connection.commit();
      assertEquals(committed, river.get(committed).id());
      assertFalse(connection.isClosed());
      assertFalse(connection.getAutoCommit());
    }
  }

  @Test
  void crossKindUniqueConflictsPreserveTheIncumbent() {
    var client = database();
    var firstType = JobType.of("original_kind", tools.jackson.databind.JsonNode.class);
    var secondType = JobType.of("attempted_kind", tools.jackson.databind.JsonNode.class);
    var unique = Unique.none().perQueue().excludeKind(true);
    var original =
        client.insert(
            firstType,
            Json.parse("{\"original\":true}"),
            InsertOptions.builder()
                .unique(unique)
                .metadata(java.util.Map.of("source", "original"))
                .priority(1)
                .tags("original")
                .build());
    var duplicate =
        client.insert(
            secondType,
            Json.parse("{\"attempted\":true}"),
            InsertOptions.builder()
                .unique(unique)
                .metadata(java.util.Map.of("source", "attempted"))
                .priority(4)
                .tags("attempted")
                .build());
    assertFalse(original.uniqueSkippedAsDuplicate());
    assertTrue(duplicate.uniqueSkippedAsDuplicate());
    assertEquals(original.job(), duplicate.job());
    assertEquals(original.job(), client.get(original.job().id()));
    assertEquals(1, client.list(JobQuery.all()).jobs().size());
  }

  @ParameterizedTest
  @ValueSource(strings = {"single", "bulk", "mixed"})
  void crossKindUniqueConflictsWithDifferentArgumentSchemas(String operation) {
    record Existing(String value) {}
    record Requested(List<Integer> value) {}
    var client = database();
    var existingType = JobType.of("existing_schema", Existing.class);
    var requestedType = JobType.of("requested_schema", Requested.class);
    var options =
        InsertOptions.builder().unique(Unique.none().perQueue().excludeKind(true)).build();
    var original = client.insert(existingType, new Existing("original"), options).job();
    var args = new Requested(List.of(1, 2));
    var duplicate =
        switch (operation) {
          case "single" -> client.insert(requestedType, args, options);
          case "bulk" -> client.insertMany(requestedType, List.of(args), options).getFirst();
          default -> client.insertMany(List.of(requestedType.submission(args, options))).getFirst();
        };
    assertTrue(duplicate.uniqueSkippedAsDuplicate());
    assertEquals(original.id(), duplicate.job().id());
    assertEquals(original.kind(), duplicate.job().kind());
    assertEquals(Json.tree(new Existing("original")), Json.tree(duplicate.job().args()));
    assertEquals(1, client.list(JobQuery.all()).jobs().size());
  }

  @ParameterizedTest
  @ValueSource(strings = {"single", "bulk", "mixed"})
  void duplicateResultsPreserveUnfamiliarArgumentFields(String operation) {
    var client = database();
    var options = InsertOptions.builder().unique(Unique.none().perQueue()).build();
    var args = Json.parse("{\"address\":\"original@example.com\",\"peer_field\":{\"version\":2}}");
    var original =
        client
            .insert(JobType.of("email", tools.jackson.databind.JsonNode.class), args, options)
            .job();
    var attempted = new Email("attempted@example.com");
    var duplicate =
        switch (operation) {
          case "single" -> client.insert(EMAIL, attempted, options);
          case "bulk" -> client.insertMany(EMAIL, List.of(attempted), options).getFirst();
          default -> client.insertMany(List.of(EMAIL.submission(attempted, options))).getFirst();
        };
    assertTrue(duplicate.uniqueSkippedAsDuplicate());
    assertEquals(original, duplicate.job());
    assertEquals(args, duplicate.job().args());
  }

  @ParameterizedTest
  @EnumSource(
      value = Job.State.class,
      names = {"AVAILABLE", "PENDING", "RETRYABLE", "SCHEDULED"})
  void delayedCompletionPreservesARescheduledJob(Job.State state) {
    var client = database();
    var database = client.database();
    var inserted = client.insert(EMAIL, new Email("delayed@example.com")).job();
    var claimed =
        client.transaction(
            connection -> {
              try (var statement =
                      Sql.prepare(
                          connection,
                          Sql.query(database, "claim"),
                          database.timestamp(NOW),
                          "old-worker",
                          "default",
                          database.timestamp(NOW),
                          1);
                  var rows = statement.executeQuery()) {
                assertTrue(rows.next());
                return client.read(rows);
              }
            });
    client.transaction(
        connection -> {
          // Simulate rescue or an operator rescheduling the attempt before it acknowledges.
          try (var statement =
              Sql.prepare(
                  connection,
                  "UPDATE "
                      + database.prefix()
                      + "river_job SET state='"
                      + state.value()
                      + "', scheduled_at=? WHERE id=?",
                  database.timestamp(NOW.plusSeconds(3600)),
                  inserted.id())) {
            assertEquals(1, statement.executeUpdate());
          }
          return null;
        });
    var before = client.get(inserted.id());
    var completed = client.transaction(connection -> client.complete(connection, claimed));
    assertEquals(before, completed);
    assertEquals(before, client.get(inserted.id()));
  }

  @Test
  void failedBatchRollsBackWithoutAbortingApplicationTransaction() throws Exception {
    var river = database();
    var options = InsertOptions.builder().unique(Unique.args()).build();
    try (var connection = river.database().connection()) {
      connection.setAutoCommit(false);
      assertThrows(
          RiverException.class,
          () ->
              river.insertMany(
                  connection,
                  List.of(
                      EMAIL.submission(new Email("duplicate@example.com"), options),
                      EMAIL.submission(new Email("duplicate@example.com"), options))));
      var kept = river.insert(connection, EMAIL, new Email("kept@example.com"));
      connection.commit();
      assertEquals(
          List.of(kept.job().id()),
          river.list(JobQuery.all()).jobs().stream().map(Job::id).toList());
    }
  }

  @Test
  void failingHookRollsBackItsApplicationWrites() {
    var base = database();
    base.transaction(
        c -> {
          try (var statement = c.createStatement()) {
            statement.execute("CREATE TABLE " + base.database().prefix() + "audit (message TEXT)");
          }
          return null;
        });
    var river =
        base.withExtension(
            new Extension() {
              @Override
              public void beforeInsert(
                  Connection connection, JobType<?> type, Object args, InsertOptions options) {
                try (var statement = connection.createStatement()) {
                  statement.executeUpdate(
                      "INSERT INTO " + base.database().prefix() + "audit VALUES ('attempt')");
                } catch (java.sql.SQLException e) {
                  throw new IllegalStateException(e);
                }
                throw new IllegalStateException("reject");
              }
            });
    assertThrows(
        IllegalStateException.class, () -> river.insert(EMAIL, new Email("one@example.com")));
    assertThrows(
        IllegalStateException.class,
        () -> river.insertMany(EMAIL, List.of(new Email("two@example.com"))));
    assertEquals(
        Integer.valueOf(0),
        base.transaction(
            c -> {
              try (var statement = c.createStatement();
                  var rows =
                      statement.executeQuery(
                          "SELECT count(*) FROM " + base.database().prefix() + "audit")) {
                rows.next();
                return rows.getInt(1);
              }
            }));
  }

  @Test
  void fetchOnlyKnownKindsLeavesPeerJobsAvailable() throws Exception {
    var river = database();
    var peer = JobType.of("peer_only", Email.class);
    var unknown = river.insert(peer, new Email("peer@example.com")).job();
    var worked = new CountDownLatch(1);
    try (var workers =
        river
            .workers()
            .queue("default", 4)
            .leadership(false)
            .pollOnly(true)
            .fetchOnlyKnownKinds(true)
            .add(EMAIL, context -> worked.countDown())
            .start()) {
      river.insert(EMAIL, new Email("java@example.com"));
      assertTrue(worked.await(10, TimeUnit.SECONDS));
    }
    assertEquals(Job.State.AVAILABLE, river.get(unknown.id()).state());
    assertEquals(0, river.get(unknown.id()).attempt());
  }

  @Test
  void removingQueueDrainsItsCommittedClaims() throws Exception {
    var river = database();
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var removed = new CountDownLatch(1);
    var calls = new java.util.concurrent.atomic.AtomicInteger();
    var first = river.insert(EMAIL, new Email("first@example.com")).job();
    var second = river.insert(EMAIL, new Email("second@example.com")).job();
    try (var workers =
        river
            .workers()
            .queue("default", 1)
            .leadership(false)
            .pollOnly(true)
            .add(
                EMAIL,
                context -> {
                  calls.incrementAndGet();
                  entered.countDown();
                  release.await();
                })
            .start()) {
      assertTrue(entered.await(10, TimeUnit.SECONDS));
      var removal =
          CompletableFuture.runAsync(
              () -> {
                workers.removeQueue("default");
                removed.countDown();
              });
      try {
        assertFalse(removed.await(100, TimeUnit.MILLISECONDS), "Removal must await the active job");
      } finally {
        release.countDown();
      }
      removal.get(10, TimeUnit.SECONDS);
      assertEquals(1, calls.get());
      assertEquals(Job.State.COMPLETED, river.get(first.id()).state());
      assertEquals(Job.State.AVAILABLE, river.get(second.id()).state());
    }
  }

  @Test
  void workerCompletionCommitsApplicationChangesWithJob() throws Exception {
    var river = database();
    river.transaction(
        c -> {
          try (var statement = c.createStatement()) {
            statement.execute(
                "CREATE TABLE " + river.database().prefix() + "deliveries (email TEXT)");
          }
          return null;
        });
    var release = new CountDownLatch(1);
    var done = new CompletableFuture<Workers.Event>();
    var failed = new CompletableFuture<Throwable>();
    try (var workers =
        river
            .workers()
            .queue("default", 1)
            .leadership(false)
            .pollInterval(Duration.ofMillis(10))
            .errorHandler(failed::complete)
            .add(
                EMAIL,
                context -> {
                  release.await();
                  context.transaction(
                      c -> {
                        try (var statement =
                            c.prepareStatement(
                                "INSERT INTO "
                                    + river.database().prefix()
                                    + "deliveries VALUES (?)")) {
                          statement.setString(1, context.args().address());
                          statement.executeUpdate();
                        }
                        context.output("delivered");
                        return context.complete(c);
                      });
                })
            .start()) {
      workers.subscribe(
          event -> {
            if (event.kind() == Workers.EventKind.JOB_COMPLETED) done.complete(event);
          });
      var inserted = river.insert(EMAIL, new Email("one@example.com"));
      release.countDown();
      var completed = done.get(10, TimeUnit.SECONDS);
      assertEquals(inserted.job().id(), completed.job().id());
      assertEquals(Job.State.COMPLETED, river.get(inserted.job().id()).state());
      assertEquals(
          "delivered", river.get(inserted.job().id()).metadata().path("output").asString());
      assertEquals(
          Integer.valueOf(1),
          river.transaction(
              c -> {
                try (var statement = c.createStatement();
                    var rows =
                        statement.executeQuery(
                            "SELECT count(*) FROM " + river.database().prefix() + "deliveries")) {
                  rows.next();
                  return rows.getInt(1);
                }
              }));
      assertFalse(failed.isDone(), () -> String.valueOf(failed.getNow(null)));
    }
  }

  @Test
  void defaultListingUsesStableIdOrder() {
    var river = database();
    var first =
        river
            .insert(
                EMAIL,
                new Email("later@example.com"),
                InsertOptions.builder().scheduledAt(NOW.plusSeconds(3600)).build())
            .job();
    var second = river.insert(EMAIL, new Email("now@example.com")).job();
    var page = river.list(JobQuery.all().limit(1));
    assertEquals(first.id(), page.jobs().getFirst().id());
    assertEquals(
        second.id(),
        river
            .list(JobQuery.builder().after(page.cursor()).limit(1).build())
            .jobs()
            .getFirst()
            .id());
    assertEquals(
        second.id(),
        river
            .list(JobQuery.builder().order(JobQuery.Order.TIME).limit(1).build())
            .jobs()
            .getFirst()
            .id());
  }

  @Test
  void typedGetAndMixedBatchRetainArgumentKinds() throws Exception {
    var river = database();
    var report = JobType.of("report", Report.class);
    var results =
        river.insertMany(
            List.of(
                EMAIL.submission(new Email("one@example.com")),
                report.submission(
                    new Report(42), InsertOptions.builder().queue("reports").build())));
    Job<Email> email = river.get(results.get(0).job().id(), EMAIL);
    Job<Report> typedReport = river.get(results.get(1).job().id(), report);
    assertEquals(new Email("one@example.com"), email.args());
    assertEquals(new Report(42), typedReport.args());
    assertEquals("reports", typedReport.queue());
    assertThrows(IllegalArgumentException.class, () -> river.get(email.id(), report));
    try (var connection = river.database().connection()) {
      connection.setAutoCommit(false);
      assertThrows(IllegalArgumentException.class, () -> river.complete(connection, email));
      var inserted =
          river.insertMany(
              connection,
              EMAIL,
              List.of(new Email("rolled-back@example.com")),
              InsertOptions.builder().queue("mail").build());
      assertEquals("mail", inserted.getFirst().job().queue());
      connection.rollback();
      assertThrows(RiverException.class, () -> river.get(inserted.getFirst().job().id()));
    }
  }

  @Test
  void checkedHookFailureRollsBackInsideCallerTransaction() throws Exception {
    var base = database();
    var river =
        base.withExtension(
            new Extension() {
              @Override
              public void beforeInsert(
                  Connection connection, JobType<?> type, Object args, InsertOptions options)
                  throws java.sql.SQLException {
                try (var statement = connection.createStatement()) {
                  statement.execute(
                      "CREATE TABLE " + base.database().prefix() + "hook_write (id INTEGER)");
                }
                throw new java.sql.SQLException("hook rejected");
              }
            });
    try (var connection = river.database().connection()) {
      connection.setAutoCommit(false);
      var failure =
          assertThrows(
              RiverException.class,
              () ->
                  river.insertMany(
                      connection, List.of(EMAIL.submission(new Email("rejected@example.com")))));
      assertEquals(RiverException.Code.DATABASE, failure.code());
      assertInstanceOf(java.sql.SQLException.class, failure.getCause());
      try (var rows =
          connection
              .getMetaData()
              .getTables(
                  null,
                  river.database().schema().isEmpty() ? null : river.database().schema(),
                  "hook_write",
                  null)) {
        assertFalse(rows.next());
      }
      var kept = base.insert(connection, EMAIL, new Email("kept@example.com")).job();
      connection.commit();
      assertEquals(
          List.of(kept.id()), base.list(JobQuery.all()).jobs().stream().map(Job::id).toList());
    }
  }

  @Test
  void sqliteQueueNotificationFailureRollsBackQueueMutation() throws Exception {
    var river =
        new Client(
            TestDatabase.sqlite(directory.resolve("river.db")), Clock.fixed(NOW, ZoneOffset.UTC));
    try (var workers =
            river
                .workers()
                .queue("default", 1)
                .leadership(false)
                .pollOnly(true)
                .add(EMAIL, ignored -> {})
                .start();
        var connection = river.database().connection()) {
      connection.setAutoCommit(false);
      try (var statement = connection.createStatement()) {
        statement.execute(
            "CREATE TRIGGER reject_control BEFORE INSERT ON river_notification "
                + "WHEN NEW.topic = 'river_control' BEGIN SELECT RAISE(ABORT, 'reject control'); END");
      }
      assertThrows(RiverException.class, () -> river.queues().pause(connection, "default"));
      assertNull(river.queues().get(connection, "default").pausedAt());
      assertThrows(
          RiverException.class,
          () -> river.queues().update(connection, "default", java.util.Map.of("changed", true)));
      assertFalse(river.queues().get(connection, "default").metadata().has("changed"));
      var kept = river.insert(connection, EMAIL, new Email("kept@example.com")).job();
      connection.commit();
      assertEquals(kept.id(), river.get(kept.id()).id());
    }
  }

  @Test
  void committedCompletionWinsOverSubsequentHandlerFailure() throws Exception {
    var river = database();
    var event = new CompletableFuture<Workers.Event>();
    var failedEvents = new java.util.concurrent.atomic.AtomicInteger();
    try (var workers =
            river
                .workers()
                .queue("default", 1)
                .leadership(false)
                .pollOnly(true)
                .add(
                    EMAIL,
                    context -> {
                      Job<Email> completed = context.transaction(context::complete);
                      assertEquals(context.args(), completed.args());
                      assertEquals(Job.State.COMPLETED, completed.state());
                      throw new IllegalStateException("failure after committed completion");
                    })
                .start();
        var subscription = workers.subscribe(event::complete, Workers.EventKind.JOB_COMPLETED);
        var failures =
            workers.subscribe(
                ignored -> failedEvents.incrementAndGet(), Workers.EventKind.JOB_FAILED)) {
      var inserted = river.insert(EMAIL, new Email("one@example.com"));
      assertEquals(inserted.job().id(), event.get(10, TimeUnit.SECONDS).job().id());
    }
    assertEquals(0, failedEvents.get());
  }

  @Test
  void retryPolicyReceivesFailedAttemptSnapshot() throws Exception {
    var river = database();
    var observed = new CompletableFuture<Job<?>>();
    var failed = new CompletableFuture<Workers.Event>();
    try (var workers =
            river
                .workers()
                .queue("default", 1)
                .leadership(false)
                .pollOnly(true)
                .retryPolicy(
                    job -> {
                      observed.complete(job);
                      return Duration.ofDays(1);
                    })
                .add(
                    EMAIL,
                    ignored -> {
                      throw new IllegalStateException("retry me");
                    })
                .start();
        var subscription = workers.subscribe(failed::complete, Workers.EventKind.JOB_FAILED)) {
      var inserted =
          river.insert(
              EMAIL,
              new Email("retry@example.com"),
              InsertOptions.builder().metadata(java.util.Map.of("policy", "slow")).build());
      var job = observed.get(10, TimeUnit.SECONDS);
      assertEquals(inserted.job().id(), job.id());
      assertEquals("email", job.kind());
      assertEquals(1, job.attempt());
      assertTrue(job.errors().isEmpty());
      assertEquals("slow", job.metadata().path("policy").asString());
      assertEquals(Job.State.RETRYABLE, failed.get(10, TimeUnit.SECONDS).job().state());
    }
  }

  @Test
  void jobKindLengthMatchesDatabaseConstraint() throws Exception {
    var client = database();
    var type = JobType.of("k".repeat(127), Email.class);
    String oversized = "k".repeat(128);
    assertThrows(IllegalArgumentException.class, () -> JobType.of(oversized, Email.class));
    var completed = new CompletableFuture<Workers.Event>();
    var worked = new CompletableFuture<Job<Email>>();
    try (var workers =
            client
                .workers()
                .queue("default", 1)
                .leadership(false)
                .pollOnly(true)
                .add(type, context -> worked.complete(context.job()))
                .start();
        var subscription =
            workers.subscribe(completed::complete, Workers.EventKind.JOB_COMPLETED)) {
      var args = new Email("long-kind@example.com");
      var inserted = client.insert(type, args).job();
      assertEquals(type.kind(), inserted.kind());
      assertEquals(args, client.get(inserted.id(), type).args());
      assertEquals(type.kind(), worked.get(10, TimeUnit.SECONDS).kind());
      assertEquals(args, worked.get().args());
      assertEquals(inserted.id(), completed.get(10, TimeUnit.SECONDS).job().id());
      var error =
          assertThrows(
              RiverException.class,
              () ->
                  client.transaction(
                      connection -> {
                        try (var statement =
                            Sql.prepare(
                                connection,
                                "UPDATE "
                                    + client.database().prefix()
                                    + "river_job SET kind=? WHERE id=?",
                                oversized,
                                inserted.id())) {
                          statement.executeUpdate();
                        }
                        return null;
                      }));
      assertEquals(RiverException.Code.DATABASE, error.code());
      assertTrue(error.getMessage().contains("kind_length"));
    }
  }

  @Test
  void stopAfterTimeoutStillWaitsForActiveWorker() throws Exception {
    var acknowledging = new CountDownLatch(1);
    var releaseAcknowledgement = new CountDownLatch(1);
    var river =
        database()
            .withPlugin(
                new Plugin() {
                  @Override
                  public void afterAttempt(
                      Connection connection,
                      Client.Driver driver,
                      Job<tools.jackson.databind.JsonNode> job)
                      throws InterruptedException {
                    acknowledging.countDown();
                    releaseAcknowledgement.await();
                  }
                });
    var entered = new CompletableFuture<WorkContext<Email>>();
    var release = new CountDownLatch(1);
    var workers =
        river
            .workers()
            .queue("default", 1)
            .leadership(false)
            .pollOnly(true)
            .stopTimeout(Duration.ofMillis(100))
            .stuckThreshold(Duration.ofMinutes(1))
            .add(
                EMAIL,
                context -> {
                  entered.complete(context);
                  release.await();
                })
            .start();
    try {
      river.insert(EMAIL, new Email("one@example.com"));
      var context = entered.get(10, TimeUnit.SECONDS);
      assertThrows(RiverException.class, workers::stop);
      assertNull(context.cancellation(), "Graceful stop timeout must not cancel the attempt");
      assertThrows(RiverException.class, workers::stop);
      assertNull(context.cancellation(), "Graceful stop timeout must not cancel the attempt");
      release.countDown();
      assertTrue(acknowledging.await(10, TimeUnit.SECONDS));
      // Returning from the handler does not finish stopping until its completion commits.
      assertThrows(RiverException.class, workers::stop);
    } finally {
      release.countDown();
      releaseAcknowledgement.countDown();
      // The deliberately short deadline also applies to cleanup. Keep waiting for durable
      // completion and background services without requiring CI to finish them within 100 ms.
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
      while (true) {
        try {
          workers.stopAndCancel();
          break;
        } catch (RiverException error) {
          if (!error.getMessage().equals("Workers have not stopped within the stop deadline")
              || System.nanoTime() >= deadline) throw error;
        }
      }
    }
    assertEquals(Job.State.COMPLETED, river.list(JobQuery.all()).jobs().getFirst().state());
  }

  @Test
  void uniqueByFieldsSelectsJsonNamesWithoutSplittingDots() {
    var river = database();
    var type =
        JobType.of("field_unique", tools.jackson.databind.JsonNode.class)
            .uniqueBy("account.id")
            .withDefaults(InsertOptions.builder().unique(Unique.args()).build());
    var first = river.insert(type, Json.parse("{\"account.id\":1,\"account\":{\"id\":2}}"));
    var duplicate = river.insert(type, Json.parse("{\"account.id\":1,\"account\":{\"id\":3}}"));
    var different = river.insert(type, Json.parse("{\"account.id\":2,\"account\":{\"id\":2}}"));
    assertTrue(duplicate.uniqueSkippedAsDuplicate());
    assertEquals(first.job().id(), duplicate.job().id());
    assertFalse(different.uniqueSkippedAsDuplicate());
    assertNotEquals(first.job().id(), different.job().id());
  }

  record Report(int accountId) {}

  record Email(String address) {}
}
