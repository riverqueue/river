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
import org.junit.jupiter.api.io.TempDir;

class ClientTest {
  private static final JobType<Email> EMAIL = JobType.of("email", Email.class);
  private static final Instant NOW = Instant.parse("2026-01-02T03:04:05Z");
  @TempDir Path directory;

  private Client database() {
    var database = Database.connect("jdbc:sqlite:" + directory.resolve("river.db"));
    new Migrator(database).migrate();
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
            statement.execute("CREATE TABLE audit (message TEXT)");
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
                  statement.executeUpdate("INSERT INTO audit VALUES ('attempt')");
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
                  var rows = statement.executeQuery("SELECT count(*) FROM audit")) {
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
            .handle(EMAIL, context -> worked.countDown())
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
            .handle(
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
            statement.execute("CREATE TABLE deliveries (email TEXT)");
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
            .handle(
                EMAIL,
                context -> {
                  release.await();
                  context.transaction(
                      c -> {
                        try (var statement =
                            c.prepareStatement("INSERT INTO deliveries VALUES (?)")) {
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
                    var rows = statement.executeQuery("SELECT count(*) FROM deliveries")) {
                  rows.next();
                  return rows.getInt(1);
                }
              }));
      assertFalse(failed.isDone(), () -> String.valueOf(failed.getNow(null)));
    }
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
                  statement.execute("CREATE TABLE hook_write (id INTEGER)");
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
      try (var statement = connection.createStatement();
          var rows =
              statement.executeQuery(
                  "SELECT count(*) FROM sqlite_master WHERE name = 'hook_write'")) {
        assertTrue(rows.next());
        assertEquals(0, rows.getInt(1));
      }
      var kept = base.insert(connection, EMAIL, new Email("kept@example.com")).job();
      connection.commit();
      assertEquals(
          List.of(kept.id()), base.list(JobQuery.all()).jobs().stream().map(Job::id).toList());
    }
  }

  @Test
  void queueNotificationFailureRollsBackQueueMutation() throws Exception {
    var river = database();
    try (var workers =
            river
                .workers()
                .queue("default", 1)
                .leadership(false)
                .pollOnly(true)
                .handle(EMAIL, _ -> {})
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
                .handle(
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
            workers.subscribe(_ -> failedEvents.incrementAndGet(), Workers.EventKind.JOB_FAILED)) {
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
                .handle(
                    EMAIL,
                    _ -> {
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
  void stopAfterTimeoutStillWaitsForActiveWorker() throws Exception {
    var river = database();
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
            .handle(
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
    } finally {
      release.countDown();
      // A fresh stop call must wait for completion even after an earlier timeout.
      workers.stopAndCancel();
    }
    assertEquals(Job.State.COMPLETED, river.list(JobQuery.all()).jobs().getFirst().state());
  }

  record Report(int accountId) {}

  record Email(String address) {}
}
