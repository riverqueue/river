package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class LeadershipTest {
  private static final JobType<String> TYPE = JobType.of("leadership_test", String.class);
  @TempDir Path directory;
  @RegisterExtension final TestDatabase databases = new TestDatabase();

  private static Database failLeadershipCommit(Database database, AtomicBoolean fail) {
    var source =
        (DataSource)
            Proxy.newProxyInstance(
                DataSource.class.getClassLoader(),
                new Class<?>[] {DataSource.class},
                (proxy, method, args) -> {
                  if (!method.getName().equals("getConnection"))
                    throw new UnsupportedOperationException();
                  var connection = database.connection();
                  var election = new AtomicBoolean();
                  return Proxy.newProxyInstance(
                      Connection.class.getClassLoader(),
                      new Class<?>[] {Connection.class},
                      (ignored, operation, arguments) -> {
                        if (operation.getName().equals("prepareStatement")
                            && ((String) arguments[0]).contains("river_leader")) election.set(true);
                        if (operation.getName().equals("commit")
                            && election.get()
                            && fail.compareAndSet(true, false))
                          throw new SQLException("leadership commit failed");
                        try {
                          return operation.invoke(connection, arguments);
                        } catch (InvocationTargetException error) {
                          throw error.getCause();
                        }
                      });
                });
    return new Database(source, database.dialect()).withSchema(database.schema());
  }

  @Test
  void failedElectionCommitDoesNotPublishLeadership() throws Exception {
    var database = databases.open(directory.resolve("river.db"));
    var failCommit = new AtomicBoolean(true);
    var errors = new LinkedBlockingQueue<Throwable>();
    var started = new CountDownLatch(1);
    var reported = new CompletableFuture<Void>();
    var client =
        new Client(failLeadershipCommit(database, failCommit))
            .withExtension(
                new Extension() {
                  @Override
                  public void periodicStarted() {
                    started.countDown();
                  }
                });
    try (var workers =
        client
            .workers()
            .queue("default", 1)
            .pollOnly(true)
            .serviceInterval(Duration.ofHours(1))
            .stopTimeout(Duration.ofSeconds(5))
            .errorHandler(
                error -> {
                  errors.add(error);
                  reported.join();
                })
            .add(TYPE, context -> {})
            .start()) {
      try {
        assertNotNull(errors.poll(5, TimeUnit.SECONDS));
        assertFalse(failCommit.get());
        assertFalse(workers.isLeader());
        assertEquals(1, started.getCount());
        assertEquals(0, leaderCount(new Client(database)));
      } finally {
        reported.complete(null);
      }
    }
  }

  private static int leaderCount(Client client) {
    return client.transaction(
        connection -> {
          try (var statement = connection.createStatement();
              var rows =
                  statement.executeQuery(
                      "SELECT count(*) FROM " + client.database().prefix() + "river_leader")) {
            rows.next();
            return rows.getInt(1);
          }
        });
  }

  private static Lease lease(Client client) {
    return client.transaction(
        connection -> {
          try (var statement = connection.createStatement();
              var rows =
                  statement.executeQuery(
                      "SELECT elected_at, expires_at FROM "
                          + client.database().prefix()
                          + "river_leader")) {
            assertTrue(rows.next());
            return new Lease(
                Database.instant(rows.getString(1)), Database.instant(rows.getString(2)));
          }
        });
  }

  @ParameterizedTest
  @ValueSource(longs = {10, 3600, 86399})
  void leaseCoversTheRenewalInterval(long seconds) throws Exception {
    var client = new Client(databases.open(directory.resolve("river.db")));
    var database = client.database();
    var checked = new LinkedBlockingQueue<String>();
    var errors = new LinkedBlockingQueue<Throwable>();
    var ready = new CountDownLatch(1);
    var interval = Duration.ofSeconds(seconds);
    try (var workers =
        client
            .workers()
            .queue("default", 1)
            .serviceInterval(interval)
            .stopTimeout(Duration.ofSeconds(5))
            .errorHandler(errors::add)
            .observe(
                event -> {
                  if (event.equals("listen_ready")) ready.countDown();
                  if (event.equals("leadership_check")) checked.add(event);
                })
            .add(TYPE, context -> {})
            .start()) {
      assertTrue(ready.await(5, TimeUnit.SECONDS));
      assertNotNull(checked.poll(5, TimeUnit.SECONDS));
      assertTrue(workers.isLeader());
      var original = lease(client);
      assertEquals(
          interval.plusSeconds(10), Duration.between(original.electedAt(), original.expiresAt()));
      assertFalse(
          workers.isLeader(System.nanoTime() + interval.plusSeconds(10).toNanos()),
          "A lease must not be trusted indefinitely when renewal stalls");

      client.transaction(
          connection -> {
            // Shorten the persisted lease to distinguish renewal from the original acquisition.
            try (var statement =
                Sql.prepare(
                    connection,
                    "UPDATE " + database.prefix() + "river_leader SET expires_at=?",
                    database.timestamp(Instant.now().plusSeconds(5)))) {
              assertEquals(1, statement.executeUpdate());
            }
            client.notify(connection, Protocol.resigned("peer"));
            return null;
          });
      assertNotNull(checked.poll(5, TimeUnit.SECONDS));
      var renewed = lease(client);
      assertEquals(original.electedAt(), renewed.electedAt());
      assertFalse(renewed.expiresAt().isBefore(original.expiresAt()));
      assertTrue(workers.isLeader());
    }
    assertTrue(errors.isEmpty(), () -> "Unexpected worker errors: " + errors);
  }

  @Test
  void reacquisitionStartsANewTerm() throws Exception {
    var client = new Client(databases.open(directory.resolve("river.db")));
    var starts = new LinkedBlockingQueue<Boolean>();
    var errors = new LinkedBlockingQueue<Throwable>();
    client =
        client.withExtension(
            new Extension() {
              @Override
              public void periodicStarted() {
                starts.add(true);
              }
            });
    try (var workers =
        client
            .workers()
            .queue("default", 1)
            .pollOnly(true)
            .serviceInterval(Duration.ofMillis(20))
            .stopTimeout(Duration.ofSeconds(5))
            .errorHandler(errors::add)
            .add(TYPE, context -> {})
            .start()) {
      assertNotNull(starts.poll(5, TimeUnit.SECONDS));
      var original = lease(client);
      var database = client.database();
      client.transaction(
          connection -> {
            try (var statement =
                Sql.prepare(connection, "DELETE FROM " + database.prefix() + "river_leader")) {
              assertEquals(1, statement.executeUpdate());
            }
            return null;
          });
      assertNotNull(
          starts.poll(5, TimeUnit.SECONDS), "A newly acquired term must rerun leadership hooks");
      assertNotEquals(original.electedAt(), lease(client).electedAt());
      assertTrue(workers.isLeader());
    }
    assertTrue(errors.isEmpty(), () -> "Unexpected worker errors: " + errors);
  }

  @Test
  void resignationWakesPeersOnBothDatabases() throws Exception {
    var client = new Client(databases.open(directory.resolve("river.db")));
    var ready = new CountDownLatch(2);
    var elected = new CountDownLatch(1);
    var firstCheck = new CountDownLatch(1);
    var secondCheck = new CountDownLatch(1);
    var errors = new LinkedBlockingQueue<Throwable>();
    var first =
        client
            .workers()
            .queue("default", 1)
            .id("first")
            .serviceInterval(Duration.ofHours(1))
            .stopTimeout(Duration.ofSeconds(5))
            .errorHandler(errors::add)
            .observe(
                event -> {
                  if (event.equals("listen_ready")) ready.countDown();
                  if (event.equals("leadership_check")) firstCheck.countDown();
                })
            .add(TYPE, context -> {})
            .start();
    try (first) {
      assertTrue(firstCheck.await(5, TimeUnit.SECONDS));
      var follower =
          client.withExtension(
              new Extension() {
                @Override
                public void periodicStarted() {
                  elected.countDown();
                }
              });
      try (var second =
          follower
              .workers()
              .queue("default", 1)
              .id("second")
              .serviceInterval(Duration.ofHours(1))
              .stopTimeout(Duration.ofSeconds(5))
              .errorHandler(errors::add)
              .observe(
                  event -> {
                    if (event.equals("listen_ready")) ready.countDown();
                    if (event.equals("leadership_check")) secondCheck.countDown();
                  })
              .add(TYPE, context -> {})
              .start()) {
        assertTrue(ready.await(5, TimeUnit.SECONDS));
        assertTrue(secondCheck.await(5, TimeUnit.SECONDS));
        assertTrue(first.isLeader());
        assertFalse(second.isLeader());
        client.requestResign();
        assertTrue(
            elected.await(5, TimeUnit.SECONDS), "Resignation must wake the follower immediately");
        assertFalse(first.isLeader());
        assertTrue(second.isLeader());
      }
    }
    assertTrue(errors.isEmpty(), () -> "Unexpected worker errors: " + errors);
  }

  @Test
  void resignedTermDoesNotContinueMaintenance() throws Exception {
    var base = new Client(databases.open(directory.resolve("river.db")));
    var entered = new CountDownLatch(1);
    var ready = new CountDownLatch(1);
    var release = new CompletableFuture<Void>();
    var rescues = new AtomicInteger();
    var errors = new LinkedBlockingQueue<Throwable>();
    var blocked =
        base.withPlugin(
            new Plugin() {
              @Override
              public void maintain(Client client, Instant now) {
                entered.countDown();
                release.join();
              }

              @Override
              public boolean rescue(Client client, Rescue request) {
                rescues.incrementAndGet();
                return true;
              }
            });
    var first =
        blocked
            .workers()
            .queue("default", 1)
            .id("old-leader")
            .serviceInterval(Duration.ofHours(1))
            .stopTimeout(Duration.ofSeconds(5))
            .errorHandler(errors::add)
            .observe(
                event -> {
                  if (event.equals("listen_ready")) ready.countDown();
                })
            .add(TYPE, context -> {})
            .start();
    try {
      assertTrue(ready.await(5, TimeUnit.SECONDS));
      assertTrue(entered.await(5, TimeUnit.SECONDS));
      var elected = new CountDownLatch(1);
      var peer =
          base.withExtension(
              new Extension() {
                @Override
                public void periodicStarted() {
                  elected.countDown();
                }
              });
      try (var second =
          peer.workers()
              .queue("default", 1)
              .id("new-leader")
              .pollOnly(true)
              .serviceInterval(Duration.ofMillis(20))
              .stopTimeout(Duration.ofSeconds(5))
              .errorHandler(errors::add)
              .add(TYPE, context -> {})
              .start()) {
        base.requestResign();
        assertTrue(elected.await(5, TimeUnit.SECONDS));
        assertFalse(first.isLeader());
        assertTrue(second.isLeader());
        release.complete(null);
        first.stop();
        assertEquals(0, rescues.get(), "A resigned term must not start another maintenance stage");
      }
    } finally {
      release.complete(null);
      first.stop();
    }
    assertTrue(errors.isEmpty(), () -> "Unexpected worker errors: " + errors);
  }

  @Test
  void startupHooksAreRetriedAfterFailure() throws Exception {
    var attempts = new AtomicInteger();
    var started = new CountDownLatch(1);
    var errors = new LinkedBlockingQueue<Throwable>();
    var client =
        new Client(databases.open(directory.resolve("river.db")))
            .withExtension(
                new Extension() {
                  @Override
                  public void periodicStarted() {
                    if (attempts.incrementAndGet() == 1)
                      throw new IllegalStateException("startup failed");
                    started.countDown();
                  }
                });
    try (var workers =
        client
            .workers()
            .queue("default", 1)
            .pollOnly(true)
            .serviceInterval(Duration.ofMillis(20))
            .stopTimeout(Duration.ofSeconds(5))
            .errorHandler(errors::add)
            .add(TYPE, context -> {})
            .start()) {
      assertTrue(started.await(5, TimeUnit.SECONDS));
      assertTrue(workers.isLeader());
      assertEquals(2, attempts.get());
      assertEquals("startup failed", errors.remove().getMessage());
      assertTrue(errors.isEmpty());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"heartbeat", "resign"})
  void stopRetriesFailedFinalization(String failure) throws Exception {
    var database = databases.open(directory.resolve("river.db"));
    var failCommit = new AtomicBoolean();
    var failHeartbeat = new AtomicBoolean(failure.equals("heartbeat"));
    var stoppingThread = Thread.currentThread();
    var checked = new CountDownLatch(1);
    var client =
        new Client(failLeadershipCommit(database, failCommit))
            .withPlugin(
                new Plugin() {
                  @Override
                  public void producer(
                      Connection connection, Client.Driver driver, Producer producer)
                      throws SQLException {
                    if (producer.paused()
                        && Thread.currentThread() == stoppingThread
                        && failHeartbeat.compareAndSet(true, false))
                      throw new SQLException("final heartbeat failed");
                  }
                });
    var workers =
        client
            .workers()
            .queue("default", 1)
            .pollOnly(true)
            .serviceInterval(Duration.ofHours(1))
            .stopTimeout(Duration.ofSeconds(5))
            .observe(
                event -> {
                  if (event.equals("leadership_check")) checked.countDown();
                })
            .add(TYPE, context -> {})
            .start();
    try {
      assertTrue(checked.await(5, TimeUnit.SECONDS));
      failCommit.set(failure.equals("resign"));
      assertThrows(RiverException.class, workers::stop);
      workers.stop();
      assertFalse(workers.isLeader());
      assertEquals(
          0, leaderCount(new Client(database)), "Repeated stop must finish releasing leadership");
    } finally {
      workers.stop();
    }
  }

  private record Lease(Instant electedAt, Instant expiresAt) {}
}
