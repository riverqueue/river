package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.JsonNode;

class RescueTest {
  private static final JobType<String> TYPE = JobType.of("rescue_test", String.class);
  @RegisterExtension final TestDatabase databases = new TestDatabase();
  @TempDir Path directory;

  @ParameterizedTest
  @ValueSource(strings = {"default", "disabled", "error", "override", "throw", "zero"})
  void pilotReceivesWorkerTimeoutCallback(String mode) throws Exception {
    var request = new CompletableFuture<Pilot.Rescue>();
    var errors = new LinkedBlockingQueue<Throwable>();
    var client =
        new Client(databases.open(directory.resolve("river.db")))
            .withPilot(
                new Pilot() {
                  @Override
                  public boolean rescue(Client client, Rescue rescue) {
                    request.complete(rescue);
                    return true;
                  }
                });
    var job = client.insert(TYPE, mode).job();
    try (var workers =
        client
            .workers()
            .queue("other", 1)
            .pollOnly(true)
            .jobTimeout(Duration.ofMinutes(5))
            .stopTimeout(Duration.ofSeconds(5))
            .errorHandler(errors::add)
            .add(
                TYPE,
                new Workers.Handler<>() {
                  @Override
                  public Duration timeout(Job<String> job) {
                    assertEquals(mode, job.args());
                    return switch (mode) {
                      case "disabled" -> Duration.ofNanos(-1);
                      case "error" -> throw new AssertionError("timeout error");
                      case "override" -> Duration.ofMinutes(2);
                      case "throw" -> throw new IllegalStateException("bad timeout");
                      case "zero" -> Duration.ZERO;
                      default -> null;
                    };
                  }

                  @Override
                  public void work(WorkContext<String> context) {
                    fail("Wrong queue");
                  }
                })
            .start()) {
      var rescue = request.get(5, TimeUnit.SECONDS);
      var expected =
          switch (mode) {
            case "disabled" -> Duration.ofNanos(-1);
            case "error", "throw" -> Duration.ZERO;
            case "override" -> Duration.ofMinutes(2);
            default -> Duration.ofMinutes(5);
          };
      assertEquals(expected, rescue.timeout().apply(job));
    }
    if (mode.equals("error")) assertInstanceOf(AssertionError.class, errors.remove());
    if (mode.equals("throw")) assertInstanceOf(IllegalStateException.class, errors.remove());
    assertTrue(errors.isEmpty(), () -> "Unexpected worker errors: " + errors);
  }

  @ParameterizedTest
  @ValueSource(strings = {"decode", "retry", "timeout"})
  void rescueCallbacksReleaseDatabaseLocksAndRejectStaleAttempts(String mode) throws Exception {
    var entered = new CompletableFuture<Void>();
    var release = new CompletableFuture<Void>();
    var maintained = new CompletableFuture<Void>();
    var errors = new LinkedBlockingQueue<Throwable>();
    var database = databases.open(directory.resolve("river.db"));
    var client =
        new Client(database)
            .withPilot(
                new Pilot() {
                  @Override
                  public JsonNode decode(Job<JsonNode> job) {
                    if (mode.equals("decode")) {
                      entered.complete(null);
                      release.join();
                    }
                    return job.args();
                  }

                  @Override
                  public boolean clean(Client client, Instant now) {
                    maintained.complete(null);
                    return true;
                  }
                });
    var job = client.insert(TYPE, "stuck").job();
    client.transaction(
        connection -> {
          try (var statement =
              Sql.prepare(
                  connection,
                  "UPDATE "
                      + database.prefix()
                      + "river_job SET state='running',attempt=1,attempted_at=? WHERE id=?",
                  database.timestamp(Instant.EPOCH),
                  job.id())) {
            statement.executeUpdate();
          }
          return null;
        });
    var workers =
        client
            .workers()
            .queue("other", 1)
            .pollOnly(true)
            .serviceInterval(Duration.ofHours(1))
            .stopTimeout(Duration.ofSeconds(5))
            .errorHandler(errors::add)
            .retryPolicy(
                ignored -> {
                  if (mode.equals("retry")) {
                    entered.complete(null);
                    release.join();
                  }
                  return Duration.ofDays(1);
                })
            .add(
                TYPE,
                new Workers.Handler<>() {
                  @Override
                  public Duration timeout(Job<String> job) {
                    if (mode.equals("timeout")) {
                      entered.complete(null);
                      release.join();
                    }
                    return Duration.ofNanos(1);
                  }

                  @Override
                  public void work(WorkContext<String> context) {
                    fail("Wrong queue");
                  }
                })
            .start();
    try {
      entered.get(5, TimeUnit.SECONDS);
      var write =
          CompletableFuture.runAsync(
              () ->
                  client.transaction(
                      connection -> {
                        try (var statement =
                            Sql.prepare(
                                connection,
                                "UPDATE "
                                    + database.prefix()
                                    + "river_job SET attempt=2,attempted_at=? WHERE id=?",
                                database.timestamp(Instant.now()),
                                job.id())) {
                          statement.executeUpdate();
                        }
                        return null;
                      }));
      write.get(2, TimeUnit.SECONDS);
      release.complete(null);
      maintained.get(5, TimeUnit.SECONDS);
      var updated = client.get(job.id());
      assertEquals(Job.State.RUNNING, updated.state());
      assertEquals(2, updated.attempt());
      assertTrue(updated.errors().isEmpty());
    } finally {
      release.complete(null);
      workers.stop();
    }
    assertTrue(errors.isEmpty(), () -> "Unexpected worker errors: " + errors);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void rescueHonorsWorkerTimeouts(boolean defaultDisabled) throws Exception {
    var maintained = new CompletableFuture<Void>();
    var errors = new LinkedBlockingQueue<Throwable>();
    var client =
        new Client(databases.open(directory.resolve("river.db")))
            .withPilot(
                new Pilot() {
                  @Override
                  public boolean clean(Client client, Instant now) {
                    maintained.complete(null);
                    return true;
                  }
                });
    var jobs = new java.util.LinkedHashMap<String, Long>();
    for (var args :
        List.of("cancelled", "default", "disabled", "error", "invalid", "long", "short", "zero")) {
      var job =
          client
              .insert(
                  TYPE,
                  args,
                  InsertOptions.builder()
                      .maxAttempts(1)
                      .metadata(
                          args.equals("cancelled")
                              ? java.util.Map.of("cancel_attempted_at", Instant.now().toString())
                              : java.util.Map.of())
                      .build())
              .job();
      jobs.put(args, job.id());
      client.transaction(
          connection -> {
            try (var statement =
                Sql.prepare(
                    connection,
                    "UPDATE "
                        + client.database().prefix()
                        + "river_job SET state='running',attempt=1,attempted_at=? WHERE id=?",
                    client.database().timestamp(Instant.now().minus(Duration.ofHours(2))),
                    job.id())) {
              statement.executeUpdate();
            }
            return null;
          });
    }
    try (var workers =
        client
            .workers()
            .queue("other", 1)
            .pollOnly(true)
            .jobTimeout(Duration.ofNanos(defaultDisabled ? -1 : 1))
            .rescueAfter(Duration.ofHours(1))
            .stopTimeout(Duration.ofSeconds(5))
            .errorHandler(errors::add)
            .add(
                TYPE,
                new Workers.Handler<>() {
                  @Override
                  public Duration timeout(Job<String> job) {
                    return switch (job.args()) {
                      case "cancelled", "disabled" -> Duration.ofNanos(-1);
                      case "error" -> throw new AssertionError("timeout error");
                      case "invalid" -> throw new IllegalStateException("bad timeout");
                      case "zero" -> Duration.ZERO;
                      case "long" -> Duration.ofHours(3);
                      case "short" -> Duration.ofMinutes(1);
                      default -> null;
                    };
                  }

                  @Override
                  public void work(WorkContext<String> context) {
                    fail("Wrong queue");
                  }
                })
            .start()) {
      maintained.get(5, TimeUnit.SECONDS);
      for (var entry : jobs.entrySet()) {
        var expected =
            switch (entry.getKey()) {
              case "cancelled" -> Job.State.CANCELLED;
              case "default", "zero" -> defaultDisabled ? Job.State.RUNNING : Job.State.DISCARDED;
              case "disabled", "long" -> Job.State.RUNNING;
              default -> Job.State.DISCARDED;
            };
        assertEquals(expected, client.get(entry.getValue()).state(), entry.getKey());
      }
    }
    assertEquals("timeout error", errors.remove().getMessage());
    assertEquals("bad timeout", errors.remove().getMessage());
    assertTrue(errors.isEmpty(), () -> "Unexpected worker errors: " + errors);
  }

  @ParameterizedTest
  @ValueSource(strings = {"local", "peer", "peer_claim_retry", "peer_notification_retry"})
  void shortRescuedRetriesWakeFetchingBeforeMaintenance(String mode) throws Exception {
    boolean peer = !mode.equals("local");
    var database = databases.open(directory.resolve("river.db"));
    var maintained = new CompletableFuture<Void>();
    var failNotification = new AtomicBoolean(mode.equals("peer_notification_retry"));
    var failClaim = new AtomicBoolean(mode.equals("peer_claim_retry"));
    var client =
        new Client(failingNotifications(database, maintained, failNotification))
            .withPilot(
                new Pilot() {
                  @Override
                  public boolean clean(Client client, Instant now) {
                    maintained.complete(null);
                    return true;
                  }
                });
    var inserted = client.insert(TYPE, "abandoned").job();
    client.transaction(
        connection -> {
          try (var statement =
              Sql.prepare(
                  connection,
                  "UPDATE "
                      + database.prefix()
                      + "river_job SET state='running', attempt=1, attempted_at=? WHERE id=?",
                  database.timestamp(Instant.EPOCH),
                  inserted.id())) {
            assertEquals(1, statement.executeUpdate());
          }
          return null;
        });
    var worked = new CompletableFuture<Job<String>>();
    var errors = new LinkedBlockingQueue<Throwable>();
    var listening = new CompletableFuture<Void>();
    var fetched = new CompletableFuture<Void>();
    var peerClient =
        new Client(database)
            .withPilot(
                new Pilot() {
                  @Override
                  public List<Client.Decoded> claim(
                      Connection connection,
                      Client.Driver driver,
                      Claim claim,
                      Client.Transaction<List<Client.Decoded>> next)
                      throws Exception {
                    listening.get(5, TimeUnit.SECONDS);
                    var rows = next.run(connection);
                    if (!rows.isEmpty() && failClaim.compareAndSet(true, false))
                      throw new SQLException("claim temporarily unavailable");
                    fetched.complete(null);
                    return rows;
                  }
                });
    try (var follower =
        peer
            ? peerClient
                .workers()
                .queue("default", 1)
                .leadership(false)
                .pollInterval(Duration.ofHours(1))
                .serviceInterval(Duration.ofHours(1))
                .stopTimeout(Duration.ofSeconds(5))
                .errorHandler(errors::add)
                .observe(
                    event -> {
                      if (event.equals("listen_ready")) listening.complete(null);
                    })
                .add(TYPE, context -> worked.complete(context.job()))
                .start()
            : null) {
      if (peer) fetched.get(5, TimeUnit.SECONDS);
      try (var workers =
          client
              .workers()
              .queue(peer ? "other" : "default", 1)
              .pollOnly(true)
              .pollInterval(Duration.ofHours(1))
              .serviceInterval(Duration.ofHours(1))
              .stopTimeout(Duration.ofSeconds(5))
              .retryPolicy(job -> Duration.ofSeconds(1))
              .errorHandler(errors::add)
              .add(TYPE, context -> worked.complete(context.job()))
              .start()) {
        maintained.get(5, TimeUnit.SECONDS);
        var rescued = client.get(inserted.id());
        assertEquals(Job.State.AVAILABLE, rescued.state());
        var attempt = worked.get(5, TimeUnit.SECONDS);
        assertEquals(inserted.id(), attempt.id());
        assertEquals(2, attempt.attempt());
        assertFalse(attempt.attemptedAt().isBefore(rescued.scheduledAt()));
        assertEquals(1, attempt.metadata().path(Protocol.METADATA_RESCUE_COUNT).asInt());
        assertEquals(1, attempt.errors().size());
      }
    }
    assertFalse(failNotification.get());
    assertFalse(failClaim.get());
    if (mode.equals("peer_claim_retry")) {
      var error = errors.poll();
      assertNotNull(error);
      assertTrue(error.getMessage().contains("claim temporarily unavailable"));
    }
    if (mode.equals("peer_notification_retry")) {
      var error = errors.poll();
      assertNotNull(error);
      assertTrue(error.getMessage().contains("notification temporarily unavailable"));
    }
    assertTrue(errors.isEmpty(), () -> "Unexpected worker errors: " + errors);
  }

  private static Database failingNotifications(
      Database database, CompletableFuture<Void> maintained, AtomicBoolean fail) {
    if (!fail.get()) return database;
    var source =
        (DataSource)
            Proxy.newProxyInstance(
                DataSource.class.getClassLoader(),
                new Class<?>[] {DataSource.class},
                (proxy, method, args) -> {
                  if (!method.getName().equals("getConnection"))
                    throw new UnsupportedOperationException();
                  var connection = database.connection();
                  return Proxy.newProxyInstance(
                      Connection.class.getClassLoader(),
                      new Class<?>[] {Connection.class},
                      (ignored, operation, arguments) -> {
                        if (operation.getName().equals("prepareStatement")
                            && arguments[0].equals(Sql.query(database, "notify"))
                            && maintained.isDone()
                            && fail.compareAndSet(true, false))
                          throw new SQLException("notification temporarily unavailable");
                        try {
                          return operation.invoke(connection, arguments);
                        } catch (InvocationTargetException error) {
                          throw error.getCause();
                        }
                      });
                });
    return new Database(source, database.dialect()).withSchema(database.schema());
  }
}
