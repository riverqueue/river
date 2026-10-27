package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;
import java.util.Optional;
import java.util.TimeZone;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.Isolated;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import tools.jackson.databind.JsonNode;

@Isolated("Changes the JVM default time zone for periodic runtime tests")
class SchedulingTest {
  private static final JobType<String> TYPE = JobType.of("periodic_test", String.class);
  @RegisterExtension final TestDatabase databases = new TestDatabase();
  @TempDir Path directory;

  @Test
  @SuppressWarnings("try") // Resources intentionally scope background activity.
  void anonymousPeriodicJobsOmitTheIdMetadata() throws Exception {
    var client = new Client(databases.open(directory.resolve("river.db")));
    var worked = new CompletableFuture<Job<String>>();
    var errors = new LinkedBlockingQueue<Throwable>();
    try (var workers =
        client.toBuilder()
            .queue("default", 1)
            .pollOnly(true)
            .stopTimeout(Duration.ofSeconds(5))
            .errorHandler(errors::add)
            .worker(TYPE, context -> worked.complete(context.job()))
            .periodic(
                new PeriodicJob<>(
                    null,
                    Schedule.every(Duration.ofDays(1)),
                    TYPE,
                    "args",
                    InsertOptions.defaults(),
                    true))
            .build()
            .start()) {
      var job = worked.get(5, TimeUnit.SECONDS);
      assertTrue(job.metadata().path("periodic").asBoolean());
      assertFalse(job.metadata().has(Protocol.METADATA_PERIODIC_JOB_ID));
    }
    assertTrue(errors.isEmpty(), () -> "Unexpected worker errors: " + errors);
  }

  @Test
  void dynamicPeriodicJobsRequireLeadershipAndValidIds() throws Exception {
    var client = new Client(databases.open(directory.resolve("river.db")));
    try (var workers =
        client.toBuilder()
            .queue("default", 1)
            .pollOnly(true)
            .leadership(false)
            .worker(TYPE, context -> {})
            .build()
            .start()) {
      assertThrows(
          IllegalStateException.class,
          () ->
              workers
                  .periodicJobs()
                  .add(
                      "dynamic",
                      Schedule.every(Duration.ofDays(1)),
                      TYPE,
                      "args",
                      InsertOptions.defaults(),
                      true));
    }
    try (var workers =
        client.toBuilder()
            .queue("default", 1)
            .pollOnly(true)
            .worker(TYPE, context -> {})
            .build()
            .start()) {
      for (var id : java.util.List.of("", "invalid id", "x".repeat(128)))
        assertThrows(
            IllegalArgumentException.class,
            () ->
                workers
                    .periodicJobs()
                    .add(
                        id,
                        Schedule.every(Duration.ofDays(1)),
                        TYPE,
                        "args",
                        InsertOptions.defaults(),
                        true));
    }
  }

  @Test
  void dynamicPeriodicJobsWaitForLeadership() throws Exception {
    var checks = new LinkedBlockingQueue<String>();
    var errors = new LinkedBlockingQueue<Throwable>();
    var worked = new LinkedBlockingQueue<String>();
    var client = new Client(databases.open(directory.resolve("river.db")));
    var builder =
        client.toBuilder()
            .queue("default", 1)
            .pollOnly(true)
            .serviceInterval(Duration.ofMillis(20))
            .stopTimeout(Duration.ofSeconds(5))
            .errorHandler(errors::add)
            .worker(TYPE, context -> worked.add(context.args()));
    var first = builder.observe(checks::add).build().start();
    try {
      assertEquals("leadership_check", checks.poll(5, TimeUnit.SECONDS));
      assertTrue(first.isLeader());
      try (var second = builder.build().start()) {
        second
            .periodicJobs()
            .add(
                "dynamic",
                Schedule.every(Duration.ofDays(1)),
                TYPE,
                "args",
                InsertOptions.defaults(),
                true);
        // The leader's builder snapshot and the follower's local registration are independent.
        assertNull(worked.poll(100, TimeUnit.MILLISECONDS));
        assertFalse(second.isLeader());
        first.stop();
        assertEquals("args", worked.poll(5, TimeUnit.SECONDS));
        assertTrue(second.isLeader());
      }
    } finally {
      first.stop();
    }
    assertTrue(errors.isEmpty(), () -> "Unexpected worker errors: " + errors);
  }

  @Test
  void dynamicPeriodicJobsWakeMaintenanceAndCanBeReplaced() throws Exception {
    var entered = new CompletableFuture<Void>();
    var release = new CompletableFuture<Void>();
    var worked = new LinkedBlockingQueue<String>();
    var errors = new LinkedBlockingQueue<Throwable>();
    var client =
        new Client(databases.open(directory.resolve("river.db")))
            .withPilot(
                new Pilot() {
                  @Override
                  public void maintain(Client client, Instant now) {
                    if (entered.complete(null)) release.join();
                  }
                });
    var workers =
        client.toBuilder()
            .queue("default", 1)
            .pollOnly(true)
            .serviceInterval(Duration.ofHours(1))
            .stopTimeout(Duration.ofSeconds(5))
            .errorHandler(errors::add)
            .worker(TYPE, context -> worked.add(context.args()))
            .build()
            .start();
    try {
      entered.get(5, TimeUnit.SECONDS);
      workers
          .periodicJobs()
          .add(
              "dynamic",
              Schedule.every(Duration.ofDays(1)),
              TYPE,
              "first",
              InsertOptions.defaults(),
              true);
      assertThrows(
          IllegalArgumentException.class,
          () ->
              workers
                  .periodicJobs()
                  .add(
                      "dynamic",
                      Schedule.every(Duration.ofDays(1)),
                      TYPE,
                      "duplicate",
                      InsertOptions.defaults(),
                      true));
      release.complete(null);
      assertEquals("first", worked.poll(5, TimeUnit.SECONDS));
      assertTrue(workers.periodicJobs().removeById("dynamic"));
      assertFalse(workers.periodicJobs().removeById("dynamic"));
      workers
          .periodicJobs()
          .add(
              "dynamic",
              Schedule.every(Duration.ofDays(1)),
              TYPE,
              "replacement",
              InsertOptions.defaults(),
              true);
      assertEquals("replacement", worked.poll(5, TimeUnit.SECONDS));
      assertTrue(workers.periodicJobs().removeById("dynamic"));
    } finally {
      release.complete(null);
      workers.stop();
    }
    var stopped =
        workers
            .periodicJobs()
            .add(
                "stopped",
                Schedule.every(Duration.ofDays(1)),
                TYPE,
                "args",
                InsertOptions.defaults(),
                true);
    assertTrue(workers.periodicJobs().remove(stopped));
    workers.periodicJobs().clear();
    assertTrue(worked.isEmpty());
    assertTrue(errors.isEmpty(), () -> "Unexpected worker errors: " + errors);
  }

  @ParameterizedTest
  @ValueSource(strings = {"initial schedule", "following schedule", "insertion"})
  @SuppressWarnings("try") // Resources intentionally scope background activity.
  void failedPeriodicJobDoesNotBlockMaintenanceOrOtherJobs(String failurePoint) throws Exception {
    var errors = new LinkedBlockingQueue<Throwable>();
    var worked = new LinkedBlockingQueue<String>();
    var client =
        new Client(databases.open(directory.resolve("river.db")))
            .withPilot(
                new Pilot() {
                  @Override
                  public void afterInsert(
                      Connection connection, Client.Driver driver, Job<JsonNode> job) {
                    if (job.args().asString().equals("broken"))
                      throw new IllegalStateException("periodic failed");
                  }
                });
    client.insert(
        TYPE,
        "scheduled",
        InsertOptions.builder().scheduledAt(Instant.now().minusSeconds(1)).build());
    try (var workers =
        client.toBuilder()
            .queue("default", 1)
            .pollOnly(true)
            .serviceInterval(Duration.ofMillis(20))
            .stopTimeout(Duration.ofSeconds(5))
            .errorHandler(errors::add)
            .worker(TYPE, context -> worked.add(context.args()))
            .periodic(
                "broken",
                after -> {
                  if (!failurePoint.equals("insertion"))
                    throw new IllegalStateException("periodic failed");
                  return Optional.of(after.plusDays(1));
                },
                TYPE,
                "broken",
                InsertOptions.defaults(),
                !failurePoint.equals("initial schedule"))
            .periodic(
                "healthy",
                Schedule.every(Duration.ofDays(1)),
                TYPE,
                "periodic",
                InsertOptions.defaults(),
                true)
            .build()
            .start()) {
      var first = worked.poll(5, TimeUnit.SECONDS);
      var second = worked.poll(5, TimeUnit.SECONDS);
      assertNotNull(first);
      assertNotNull(second);
      assertEquals(java.util.Set.of("periodic", "scheduled"), java.util.Set.of(first, second));
      assertFalse(errors.isEmpty());
      assertTrue(errors.stream().allMatch(error -> "periodic failed".equals(error.getMessage())));
    }
  }

  @Test
  @SuppressWarnings("try") // Resources intentionally scope background activity.
  void failedScheduleDoesNotInsertTheOccurrenceTwice() throws Exception {
    var calls = new AtomicInteger();
    var maintained = new CompletableFuture<Void>();
    var errors = new LinkedBlockingQueue<Throwable>();
    var client =
        new Client(databases.open(directory.resolve("river.db")))
            .withPilot(
                new Pilot() {
                  @Override
                  public void maintain(Client client, Instant now) {
                    if (calls.get() >= 3) maintained.complete(null);
                  }
                });
    Schedule schedule =
        after ->
            switch (calls.incrementAndGet()) {
              case 1 -> Optional.of(after.plusNanos(1));
              case 2 -> throw new IllegalStateException("schedule failed");
              default -> Optional.of(after.plusDays(1));
            };
    try (var workers =
        client.toBuilder()
            .queue("default", 1)
            .pollOnly(true)
            .serviceInterval(Duration.ofMillis(20))
            .stopTimeout(Duration.ofSeconds(5))
            .errorHandler(errors::add)
            .worker(TYPE, context -> {})
            .periodic(
                "retry",
                schedule,
                TYPE,
                "args",
                InsertOptions.builder().queue("periodic").build(),
                false)
            .build()
            .start()) {
      maintained.get(5, TimeUnit.SECONDS);
      assertEquals(1, client.list(JobQuery.all()).jobs().size());
      assertEquals("schedule failed", errors.remove().getMessage());
      assertTrue(errors.isEmpty());
    }
  }

  @Test
  @SuppressWarnings("try") // Resources intentionally scope background activity.
  void periodicSchedulesKeepTheirPlannedCadence() throws Exception {
    var inserted = new AtomicReference<Job<JsonNode>>();
    var client =
        new Client(databases.open(directory.resolve("river.db")))
            .withPilot(
                new Pilot() {
                  @Override
                  public void afterInsert(
                      Connection connection, Client.Driver driver, Job<JsonNode> job) {
                    inserted.set(job);
                  }
                });
    var planned = new AtomicReference<OffsetDateTime>();
    var nextFrom = new CompletableFuture<OffsetDateTime>();
    var worked = new CompletableFuture<Job<String>>();
    var errors = new LinkedBlockingQueue<Throwable>();
    Schedule schedule =
        after -> {
          if (planned.compareAndSet(null, after.plusNanos(1))) return Optional.of(planned.get());
          nextFrom.complete(after);
          return Optional.of(after.plusDays(1));
        };
    try (var workers =
        client.toBuilder()
            .queue("default", 1)
            .pollOnly(true)
            .serviceInterval(Duration.ofMillis(20))
            .stopTimeout(Duration.ofSeconds(5))
            .errorHandler(errors::add)
            .worker(TYPE, context -> worked.complete(context.job()))
            .periodic("cadence", schedule, TYPE, "args", InsertOptions.defaults(), false)
            .build()
            .start()) {
      var actual = nextFrom.get(5, TimeUnit.SECONDS);
      assertEquals(planned.get(), actual);
      var job = worked.get(5, TimeUnit.SECONDS);
      assertEquals(Job.State.AVAILABLE, inserted.get().state());
      assertEquals(
          client.database().timestamp(planned.get().toInstant()),
          client.database().timestamp(job.scheduledAt()),
          "Periodic rows must retain the occurrence's planned timestamp");
    }
    assertTrue(errors.isEmpty(), () -> "Unexpected worker errors: " + errors);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @SuppressWarnings("try") // Resources intentionally scope background activity.
  void periodicSchedulesUseTheSystemTimeZone(boolean runOnStart) throws Exception {
    var original = TimeZone.getDefault();
    var zone = ZoneId.of("Asia/Kolkata");
    var calls = new LinkedBlockingQueue<ZonedDateTime>();
    var errors = new LinkedBlockingQueue<Throwable>();
    var count = new AtomicInteger();
    Schedule schedule =
        new Schedule() {
          @Override
          public Optional<OffsetDateTime> next(OffsetDateTime after) {
            return next(after.toZonedDateTime()).map(ZonedDateTime::toOffsetDateTime);
          }

          @Override
          public Optional<ZonedDateTime> next(ZonedDateTime after) {
            calls.add(after);
            return Optional.of(
                count.incrementAndGet() == 1 && !runOnStart
                    ? after.plusNanos(1)
                    : after.plusDays(1));
          }
        };
    try {
      TimeZone.setDefault(TimeZone.getTimeZone(zone));
      var client = new Client(databases.open(directory.resolve("river.db")));
      try (var workers =
          client.toBuilder()
              .queue("default", 1)
              .pollOnly(true)
              .serviceInterval(Duration.ofMillis(20))
              .stopTimeout(Duration.ofSeconds(5))
              .errorHandler(errors::add)
              .worker(TYPE, context -> {})
              .periodic("local-zone", schedule, TYPE, "args", InsertOptions.defaults(), runOnStart)
              .build()
              .start()) {
        for (int i = 0; i < (runOnStart ? 1 : 2); i++) {
          var reference = calls.poll(5, TimeUnit.SECONDS);
          assertNotNull(reference);
          assertEquals(zone, reference.getZone());
        }
      }
    } finally {
      TimeZone.setDefault(original);
    }
    assertTrue(errors.isEmpty(), () -> "Unexpected worker errors: " + errors);
  }

  @ParameterizedTest
  @ValueSource(strings = {"handle", "id", "handles", "ids", "clear"})
  void removedPeriodicJobIsNotInsertedAfterItsScheduleReturns(String removal) throws Exception {
    var entered = new CompletableFuture<Void>();
    var release = new CompletableFuture<Void>();
    var maintained = new CompletableFuture<Void>();
    var errors = new LinkedBlockingQueue<Throwable>();
    var client =
        new Client(databases.open(directory.resolve("river.db")))
            .withPilot(
                new Pilot() {
                  @Override
                  public void maintain(Client client, Instant now) {
                    if (entered.isDone()) maintained.complete(null);
                  }
                });
    var workers =
        client.toBuilder()
            .queue("default", 1)
            .pollOnly(true)
            .stopTimeout(Duration.ofSeconds(5))
            .errorHandler(errors::add)
            .worker(TYPE, context -> {})
            .build()
            .start();
    try {
      var handle =
          workers
              .periodicJobs()
              .add(
                  "removed",
                  after -> {
                    entered.complete(null);
                    release.join();
                    return Optional.of(after.plusDays(1));
                  },
                  TYPE,
                  "args",
                  InsertOptions.defaults(),
                  true);
      entered.get(5, TimeUnit.SECONDS);
      switch (removal) {
        case "handle" -> assertTrue(workers.periodicJobs().remove(handle));
        case "id" -> assertTrue(workers.periodicJobs().removeById("removed"));
        case "handles" -> workers.periodicJobs().removeMany(java.util.List.of(handle));
        case "ids" -> workers.periodicJobs().removeManyById(java.util.List.of("removed"));
        case "clear" -> workers.periodicJobs().clear();
        default -> fail("Unknown removal mode");
      }
      release.complete(null);
      maintained.get(5, TimeUnit.SECONDS);
      assertTrue(client.list(JobQuery.all()).jobs().isEmpty());
    } finally {
      release.complete(null);
      workers.stop();
    }
    assertTrue(errors.isEmpty(), () -> "Unexpected worker errors: " + errors);
  }
}
