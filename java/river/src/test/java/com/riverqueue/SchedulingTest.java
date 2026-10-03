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

  @ParameterizedTest
  @ValueSource(strings = {"initial schedule", "following schedule", "insertion"})
  void failedPeriodicJobDoesNotBlockMaintenanceOrOtherJobs(String failurePoint) throws Exception {
    var errors = new LinkedBlockingQueue<Throwable>();
    var worked = new LinkedBlockingQueue<String>();
    var client =
        new Client(databases.open(directory.resolve("river.db")))
            .withPlugin(
                new Plugin() {
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
        client
            .workers()
            .queue("default", 1)
            .pollOnly(true)
            .serviceInterval(Duration.ofMillis(20))
            .stopTimeout(Duration.ofSeconds(5))
            .errorHandler(errors::add)
            .add(TYPE, context -> worked.add(context.args()))
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
  void failedScheduleDoesNotInsertTheOccurrenceTwice() throws Exception {
    var calls = new AtomicInteger();
    var maintained = new CompletableFuture<Void>();
    var errors = new LinkedBlockingQueue<Throwable>();
    var client =
        new Client(databases.open(directory.resolve("river.db")))
            .withPlugin(
                new Plugin() {
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
        client
            .workers()
            .queue("default", 1)
            .pollOnly(true)
            .serviceInterval(Duration.ofMillis(20))
            .stopTimeout(Duration.ofSeconds(5))
            .errorHandler(errors::add)
            .add(TYPE, context -> {})
            .periodic(
                "retry",
                schedule,
                TYPE,
                "args",
                InsertOptions.builder().queue("periodic").build(),
                false)
            .start()) {
      maintained.get(5, TimeUnit.SECONDS);
      assertEquals(1, client.list(JobQuery.all()).jobs().size());
      assertEquals("schedule failed", errors.remove().getMessage());
      assertTrue(errors.isEmpty());
    }
  }

  @Test
  void periodicSchedulesKeepTheirPlannedCadence() throws Exception {
    var inserted = new AtomicReference<Job<JsonNode>>();
    var client =
        new Client(databases.open(directory.resolve("river.db")))
            .withPlugin(
                new Plugin() {
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
        client
            .workers()
            .queue("default", 1)
            .pollOnly(true)
            .serviceInterval(Duration.ofMillis(20))
            .stopTimeout(Duration.ofSeconds(5))
            .errorHandler(errors::add)
            .add(TYPE, context -> worked.complete(context.job()))
            .periodic("cadence", schedule, TYPE, "args", InsertOptions.defaults(), false)
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
          client
              .workers()
              .queue("default", 1)
              .pollOnly(true)
              .serviceInterval(Duration.ofMillis(20))
              .stopTimeout(Duration.ofSeconds(5))
              .errorHandler(errors::add)
              .add(TYPE, context -> {})
              .periodic("local-zone", schedule, TYPE, "args", InsertOptions.defaults(), runOnStart)
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
}
