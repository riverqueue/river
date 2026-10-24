package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.sql.Connection;
import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneOffset;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.JsonNode;

class RetryTest {
  private static final Instant NOW = Instant.parse("2026-01-02T03:04:05Z");
  private static final JobType<String> TYPE = JobType.of("retry_test", String.class);
  @RegisterExtension final TestDatabase databases = new TestDatabase();
  @TempDir Path directory;

  @Test
  void retryClearsCancellation() {
    var database = databases.open(directory.resolve("river.db"));
    var client = new Client(database);
    var inserted = client.insert(TYPE, "retry cancelled").job();
    var cancelled = client.cancel(inserted.id());
    assertTrue(cancelled.metadata().has("cancel_attempted_at"));
    var retried = client.retry(inserted.id());
    assertEquals(Job.State.AVAILABLE, retried.state());
    assertFalse(retried.metadata().has("cancel_attempted_at"));
    assertFalse(client.get(inserted.id()).metadata().has("cancel_attempted_at"));
  }

  @Test
  void retryClearsCancellationWithoutReorderingAvailableJob() {
    var database = databases.open(directory.resolve("river.db"));
    var client = new Client(database, Clock.fixed(NOW, ZoneOffset.UTC));
    var inserted =
        client
            .insert(
                TYPE,
                "already available",
                InsertOptions.builder()
                    .metadata(Map.of("cancel_attempted_at", NOW.toString(), "preserved", true))
                    .scheduledAt(NOW.minusSeconds(3600))
                    .build())
            .job();
    client.transaction(
        connection -> {
          try (var statement =
              Sql.prepare(
                  connection,
                  "UPDATE "
                      + database.prefix()
                      + "river_job SET state='available', attempt=max_attempts WHERE id=?",
                  inserted.id())) {
            assertEquals(1, statement.executeUpdate());
          }
          return null;
        });
    var retried = client.retry(inserted.id());
    assertEquals(inserted.scheduledAt(), retried.scheduledAt());
    assertEquals(inserted.maxAttempts(), retried.maxAttempts());
    var expectedMetadata = (tools.jackson.databind.node.ObjectNode) inserted.metadata().deepCopy();
    expectedMetadata.remove("cancel_attempted_at");
    assertEquals(expectedMetadata, retried.metadata());
    assertEquals(retried, client.get(inserted.id()));
  }

  @ParameterizedTest
  @EnumSource(Job.State.class)
  void retryNotifiesOnlyWhenTheRowChanges(Job.State state) {
    var database = databases.open(directory.resolve("river.db"));
    var client = new Client(database, Clock.fixed(NOW, ZoneOffset.UTC));
    var inserted = client.insert(TYPE, "retry").job();
    client.transaction(
        connection -> {
          try (var statement =
              Sql.prepare(
                  connection,
                  "UPDATE "
                      + database.prefix()
                      + "river_job SET state='"
                      + state.value()
                      + "', attempt=1, max_attempts=1, finalized_at=?, scheduled_at=? WHERE id=?",
                  database.timestamp(state.isFinalized() ? NOW : null),
                  database.timestamp(
                      state == Job.State.AVAILABLE ? NOW.minusSeconds(1) : NOW.plusSeconds(3600)),
                  inserted.id())) {
            assertEquals(1, statement.executeUpdate());
          }
          return null;
        });
    var before = client.get(inserted.id());
    var notifications = new AtomicInteger();
    client.onInsertCommit(notifications::incrementAndGet);
    var retried = client.retry(inserted.id());
    if (state == Job.State.AVAILABLE || state == Job.State.RUNNING) {
      assertEquals(before, retried);
      assertEquals(0, notifications.get());
    } else {
      assertEquals(Job.State.AVAILABLE, retried.state());
      assertEquals(NOW, retried.scheduledAt());
      assertNull(retried.finalizedAt());
      assertEquals(2, retried.maxAttempts());
      assertEquals(1, notifications.get());
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {"null", "42", "false", "\"cancel_attempted_at\"", "[\"cancel_attempted_at\"]"})
  void retryPreservesNonObjectMetadata(String metadata) {
    var database = databases.open(directory.resolve("river.db"));
    var client = new Client(database);
    var inserted = client.insert(TYPE, "retry metadata").job();
    client.transaction(
        connection -> {
          try (var statement =
              Sql.prepare(
                  connection,
                  "UPDATE "
                      + database.prefix()
                      + "river_job SET state='retryable', metadata="
                      + (database.dialect() == Database.Dialect.POSTGRES
                          ? "CAST(? AS jsonb)"
                          : "jsonb(?)")
                      + " WHERE id=?",
                  metadata,
                  inserted.id())) {
            assertEquals(1, statement.executeUpdate());
          }
          return null;
        });

    assertEquals(Job.State.AVAILABLE, client.retry(inserted.id()).state());
    assertEquals(Json.parse(metadata), client.get(inserted.id()).metadata());
  }

  @ParameterizedTest
  @ValueSource(strings = {"local", "peer", "transaction"})
  @SuppressWarnings("try") // Resources intentionally scope background activity.
  void retryWakesWorkersAfterCommit(String mode) throws Exception {
    var database = databases.open(directory.resolve("river.db"));
    var warmed = new CompletableFuture<Void>();
    var idle = new CompletableFuture<Void>();
    var client =
        new Client(database)
            .withPilot(
                new Pilot() {
                  @Override
                  public void afterAttempt(
                      Connection connection, Client.Driver driver, Job<JsonNode> original) {
                    if (original.args().asString().equals("warmup")) warmed.complete(null);
                  }

                  @Override
                  public List<Client.Decoded> claim(
                      Connection connection,
                      Client.Driver driver,
                      Claim claim,
                      Client.Transaction<List<Client.Decoded>> next)
                      throws Exception {
                    var rows = next.run(connection);
                    if (warmed.isDone() && rows.isEmpty()) idle.complete(null);
                    return rows;
                  }
                });
    var inserted =
        client
            .insert(
                TYPE,
                "retry",
                InsertOptions.builder()
                    .queue("urgent")
                    .scheduledAt(Instant.now().plusSeconds(3600))
                    .build())
            .job();
    client.insert(TYPE, "warmup", InsertOptions.builder().queue("urgent").build());
    var listening = new CompletableFuture<Void>();
    if (mode.equals("local")) listening.complete(null);
    var worked = new CompletableFuture<Job<String>>();
    var errors = new LinkedBlockingQueue<Throwable>();
    try (var workers =
        client.toBuilder()
            .queue("urgent", 1)
            .leadership(false)
            .pollOnly(mode.equals("local"))
            .pollInterval(Duration.ofHours(1))
            .serviceInterval(Duration.ofHours(1))
            .stopTimeout(Duration.ofSeconds(5))
            .observe(
                event -> {
                  if (event.equals("listen_ready")) listening.complete(null);
                })
            .errorHandler(errors::add)
            .worker(
                TYPE,
                context -> {
                  listening.get(5, TimeUnit.SECONDS);
                  if (context.args().equals("retry")) worked.complete(context.job());
                })
            .build()
            .start()) {
      // Completing the warmup drains startup wake-ups before testing retry's notification.
      idle.get(5, TimeUnit.SECONDS);
      var actor = mode.equals("local") ? workers.client() : new Client(database);
      if (mode.equals("transaction")) {
        try (var connection = database.connection()) {
          connection.setAutoCommit(false);
          assertEquals(Job.State.AVAILABLE, actor.retry(connection, inserted.id()).state());
          assertEquals(Job.State.SCHEDULED, client.get(inserted.id()).state());
          assertFalse(worked.isDone());
          connection.rollback();
          assertEquals(Job.State.SCHEDULED, client.get(inserted.id()).state());
          actor.retry(connection, inserted.id());
          connection.commit();
        }
      } else actor.retry(inserted.id());
      assertEquals(inserted.id(), worked.get(5, TimeUnit.SECONDS).id());
    }
    assertTrue(errors.isEmpty(), () -> "Unexpected worker errors: " + errors);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rolledBackRetriesPreserveEarlierCommittedNotifications(boolean earlierInsert) {
    var client = new Client(databases.open(directory.resolve("river.db")));
    var inserted =
        client
            .insert(
                TYPE,
                "retry",
                InsertOptions.builder().scheduledAt(Instant.now().plusSeconds(3600)).build())
            .job();
    var notifications = new AtomicInteger();
    client.onInsertCommit(notifications::incrementAndGet);
    client.transaction(
        connection -> {
          if (earlierInsert) client.insert(connection, TYPE, "kept");
          assertThrows(
              IllegalStateException.class,
              () ->
                  client.transaction(
                      connection,
                      nested -> {
                        client.retry(nested, inserted.id());
                        throw new IllegalStateException("roll back retry");
                      }));
          assertEquals(inserted, client.get(connection, inserted.id()));
          assertEquals(0, notifications.get(), "Callbacks must wait for the outer commit");
          return null;
        });
    assertEquals(inserted, client.get(inserted.id()));
    assertEquals(earlierInsert ? 1 : 0, notifications.get());
  }

  @Test
  void sqliteNotificationFailureRollsBackTheRetry() {
    var database = TestDatabase.sqlite(directory.resolve("river.db"));
    var client = new Client(database);
    var inserted =
        client
            .insert(
                TYPE,
                "retry",
                InsertOptions.builder().scheduledAt(Instant.now().plusSeconds(3600)).build())
            .job();
    var notifications = new AtomicInteger();
    client.onInsertCommit(notifications::incrementAndGet);
    client.transaction(
        connection -> {
          try (var statement = connection.createStatement()) {
            statement.execute(
                "CREATE TRIGGER reject_insert BEFORE INSERT ON river_notification "
                    + "WHEN NEW.topic = 'river_insert' BEGIN SELECT RAISE(ABORT, 'reject insert'); END");
          }
          assertThrows(RiverException.class, () -> client.retry(connection, inserted.id()));
          assertEquals(inserted, client.get(connection, inserted.id()));
          client.output(connection, inserted.id(), "outer transaction remains usable");
          return null;
        });
    var job = client.get(inserted.id());
    assertEquals(Job.State.SCHEDULED, job.state());
    assertEquals(
        "outer transaction remains usable",
        job.metadata().path(Protocol.METADATA_OUTPUT).asString());
    assertEquals(0, notifications.get());
  }

  @Test
  void sqliteRetryQueryPreservesMalformedMetadata() {
    var database = TestDatabase.sqlite(directory.resolve("river.db"));
    var client = new Client(database);
    var inserted = client.insert(TYPE, "retry malformed metadata").job();
    client.transaction(
        connection -> {
          try (var statement =
              Sql.prepare(
                  connection,
                  "UPDATE river_job SET state='retryable', metadata='[not json' WHERE id=?",
                  inserted.id())) {
            assertEquals(1, statement.executeUpdate());
          }
          // The public retry validates the locked row first. Exercise the SQL
          // directly so cleanup cannot turn a decode error into a SQL error.
          try (var statement =
                  Sql.prepare(
                      connection,
                      Sql.query(database, "retry"),
                      database.timestamp(NOW),
                      inserted.id());
              var rows = statement.executeQuery()) {
            assertTrue(rows.next());
            assertEquals("[not json", rows.getString("metadata"));
          }
          return null;
        });
    var error = assertThrows(IllegalArgumentException.class, () -> client.get(inserted.id()));
    assertTrue(error.getMessage().contains("metadata"));
  }
}
