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

class RescueTest {
  private static final JobType<String> TYPE = JobType.of("rescue_test", String.class);
  @RegisterExtension final TestDatabase databases = new TestDatabase();
  @TempDir Path directory;

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
            .withPlugin(
                new Plugin() {
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
            .withPlugin(
                new Plugin() {
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
