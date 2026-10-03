package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.ValueSource;

class WorkersTest {
  private static final JobType<String> TYPE = JobType.of("runtime_test", String.class);
  @TempDir Path directory;
  @RegisterExtension final TestDatabase databases = new TestDatabase();

  private Client client() {
    var database = databases.open(directory.resolve("river.db"));
    return new Client(database);
  }

  private Workers.Builder workers(Client client) {
    return client
        .workers()
        .queue("default", 1)
        .leadership(false)
        .pollOnly(true)
        .stopTimeout(Duration.ofSeconds(1));
  }

  @Test
  void builderGeneratesAnIdForEachRuntime() {
    var builder = workers(client()).add(TYPE, context -> {});
    try (var first = builder.start();
        var second = builder.start()) {
      assertNotEquals(first.id(), second.id());
    }
    try (var explicit = builder.id("chosen-id").start()) {
      assertEquals("chosen-id", explicit.id());
    }
  }

  @ParameterizedTest
  @CsvSource({
    "PT0.000000001S,true",
    "PT1H,true",
    "PT23H59M59.999999999S,true",
    "PT24H,false",
    "PT24H0.000000001S,false",
    "PT48H,false"
  })
  void builderRequiresServiceIntervalsBelowQueueRetention(String value, boolean valid) {
    var builder = new Client(Database.connect("jdbc:postgresql://127.0.0.1:1/unused")).workers();
    var interval = Duration.parse(value);
    if (valid) assertSame(builder, builder.serviceInterval(interval));
    else
      assertEquals(
          "serviceInterval must be shorter than the queue retention period of one day",
          assertThrows(IllegalArgumentException.class, () -> builder.serviceInterval(interval))
              .getMessage());
  }

  @ParameterizedTest
  @CsvSource({"REMOTE,false", "REMOTE,true", "TIMEOUT,false", "TIMEOUT,true"})
  void cancellationRespectsTheHandlersOutcome(WorkContext.Cancellation cause, boolean interrupted)
      throws Exception {
    var client = client();
    var entered = new CompletableFuture<WorkContext<String>>();
    var outcome = new CompletableFuture<Workers.Event>();
    var reported = new LinkedBlockingQueue<Throwable>();
    try (var workers =
            workers(client)
                .stopTimeout(Duration.ofSeconds(5))
                .errorHandler(reported::add)
                .add(
                    TYPE,
                    context -> {
                      entered.complete(context);
                      context.awaitCancellation();
                      if (interrupted) context.checkCancelled();
                    })
                .start();
        var subscription = workers.subscribe(outcome::complete)) {
      var inserted =
          client.insert(TYPE, "cancel", InsertOptions.builder().maxAttempts(1).build()).job();
      var context = entered.get(5, TimeUnit.SECONDS);
      if (cause == WorkContext.Cancellation.REMOTE) client.cancel(inserted.id());
      else context.requestCancellation(cause);
      var event = outcome.get(5, TimeUnit.SECONDS);
      var expected =
          !interrupted
              ? Job.State.COMPLETED
              : cause == WorkContext.Cancellation.REMOTE
                  ? Job.State.CANCELLED
                  : Job.State.DISCARDED;
      assertEquals(cause, context.cancellation());
      assertEquals(expected, event.job().state());
      var job = client.get(inserted.id());
      assertEquals(expected, job.state());
      assertEquals(interrupted ? 1 : 0, job.errors().size());
      assertEquals(1, job.attempt());
      assertNotNull(job.finalizedAt());
    }
    assertTrue(reported.isEmpty(), () -> "Unexpected worker errors: " + reported);
  }

  @ParameterizedTest
  @EnumSource(
      value = Job.State.class,
      names = {"AVAILABLE", "PENDING", "RETRYABLE", "RUNNING", "SCHEDULED"})
  void completionEventsReflectPersistedState(Job.State state) throws Exception {
    var client = client();
    var database = client.database();
    var changed = new CompletableFuture<Void>();
    var events = new LinkedBlockingQueue<Workers.Event>();
    var reported = new LinkedBlockingQueue<Throwable>();
    try (var workers =
            workers(client)
                .stopTimeout(Duration.ofSeconds(5))
                .errorHandler(reported::add)
                .add(
                    TYPE,
                    context -> {
                      context.transaction(
                          connection -> {
                            // A rescuer or another attempt changed this row before the old handler
                            // returned.
                            try (var statement =
                                Sql.prepare(
                                    connection,
                                    "UPDATE "
                                        + database.prefix()
                                        + "river_job SET state='"
                                        + state.value()
                                        + "', scheduled_at=?, attempt=? WHERE id=?",
                                    database.timestamp(Instant.now().plusSeconds(3600)),
                                    state == Job.State.RUNNING ? 2 : 1,
                                    context.job().id())) {
                              assertEquals(1, statement.executeUpdate());
                            }
                            return null;
                          });
                      changed.complete(null);
                    })
                .start();
        var subscription = workers.subscribe(events::add)) {
      var job = client.insert(TYPE, "changed").job();
      changed.get(5, TimeUnit.SECONDS);
      workers.stop();
      assertEquals(state, client.get(job.id()).state());
      var expected =
          switch (state) {
            case AVAILABLE, RETRYABLE -> Workers.EventKind.JOB_FAILED;
            case SCHEDULED -> Workers.EventKind.JOB_SNOOZED;
            default -> null;
          };
      if (expected == null) assertTrue(events.isEmpty());
      else {
        assertEquals(1, events.size());
        assertEquals(expected, events.element().kind());
        assertEquals(state, events.element().job().state());
      }
    }
    assertTrue(reported.isEmpty(), () -> "Unexpected worker errors: " + reported);
  }

  @Test
  void completionRetriesSurviveAnInterruptedHandler() throws Exception {
    var attempts = new AtomicInteger();
    var client =
        client()
            .withPlugin(
                new Plugin() {
                  @Override
                  public void afterAttempt(
                      Connection connection,
                      Client.Driver driver,
                      Job<tools.jackson.databind.JsonNode> job)
                      throws SQLException {
                    if (attempts.getAndIncrement() == 0)
                      throw new SQLException("transient completion failure");
                  }
                });
    var completed = new LinkedBlockingQueue<Workers.Event>();
    var reported = new LinkedBlockingQueue<Throwable>();
    try (var workers =
            workers(client)
                .errorHandler(reported::add)
                .add(TYPE, context -> Thread.currentThread().interrupt())
                .start();
        var subscription = workers.subscribe(completed::add, Workers.EventKind.JOB_COMPLETED)) {
      for (int i = 0; i < 2; i++) {
        var job = client.insert(TYPE, "job " + i).job();
        var event = completed.poll(5, TimeUnit.SECONDS);
        assertNotNull(
            event, "An interrupted handler must not strand completion or its worker slot");
        assertEquals(job.id(), event.job().id());
        assertEquals(Job.State.COMPLETED, client.get(job.id()).state());
      }
      assertEquals(3, attempts.get());
      assertEquals(1, reported.size());
    }
  }

  @ParameterizedTest
  @ValueSource(strings = {"args", "kind", "row"})
  void completionRetriesSurviveInvalidJobs(String invalid) throws Exception {
    var attempts = new AtomicInteger();
    var client =
        client()
            .withPlugin(
                new Plugin() {
                  @Override
                  public void afterAttempt(
                      Connection connection,
                      Client.Driver driver,
                      Job<tools.jackson.databind.JsonNode> job)
                      throws SQLException {
                    if (job.args().isObject() && attempts.getAndIncrement() == 0)
                      throw new SQLException("transient completion failure");
                  }

                  @Override
                  public java.util.List<Client.Decoded> claim(
                      Connection connection,
                      Client.Driver driver,
                      Plugin.Claim claim,
                      Client.Transaction<java.util.List<Client.Decoded>> next)
                      throws Exception {
                    var rows = next.run(connection);
                    return rows.stream()
                        .map(
                            row ->
                                invalid.equals("row") && row.job().args().isObject()
                                    ? new Client.Decoded(
                                        row.job(), new IllegalArgumentException("invalid row"))
                                    : row)
                        .toList();
                  }
                });
    var type =
        JobType.of(
            invalid.equals("kind") ? "unknown_kind" : TYPE.kind(),
            tools.jackson.databind.JsonNode.class);
    var handled = new LinkedBlockingQueue<Long>();
    var reported = new LinkedBlockingQueue<Throwable>();
    var finalized = new LinkedBlockingQueue<Workers.Event>();
    try (var workers =
            workers(client)
                .queue("default", 2)
                .stopTimeout(Duration.ofSeconds(5))
                .errorHandler(reported::add)
                .add(TYPE, context -> handled.add(context.job().id()))
                .start();
        var subscription = workers.subscribe(finalized::add)) {
      var jobs =
          client.insertMany(
              java.util.List.of(
                  type.submission(Json.object(), InsertOptions.builder().maxAttempts(1).build()),
                  TYPE.submission("valid")));
      var bad = jobs.getFirst().job();
      var good = jobs.getLast().job();
      assertNotNull(finalized.poll(5, TimeUnit.SECONDS), "The first claimed job must settle");
      assertNotNull(finalized.poll(5, TimeUnit.SECONDS), "Every committed claim must settle");
      workers.stop();
      assertEquals(java.util.List.of(good.id()), java.util.List.copyOf(handled));
      assertEquals(Job.State.DISCARDED, client.get(bad.id()).state());
      assertEquals(Job.State.COMPLETED, client.get(good.id()).state());
      assertEquals(1, client.get(bad.id()).errors().size());
      assertEquals(2, attempts.get());
      // A rolled-back completion batch may report the same failure for both claims.
      assertFalse(reported.isEmpty());
      assertTrue(
          reported.stream()
              .allMatch(error -> error.toString().contains("transient completion failure")));
    }
  }

  @Test
  void completedHandlerIsNotCancelledWhileAcknowledging() throws Exception {
    var acknowledging = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var context = new CompletableFuture<WorkContext<String>>();
    var client =
        client()
            .withPlugin(
                new Plugin() {
                  @Override
                  public void afterAttempt(
                      Connection connection,
                      Client.Driver driver,
                      Job<tools.jackson.databind.JsonNode> job)
                      throws InterruptedException {
                    acknowledging.countDown();
                    release.await();
                  }
                });
    var completed = new CompletableFuture<Workers.Event>();
    var workers = workers(client).add(TYPE, context::complete).start();
    try (var subscription = workers.subscribe(completed::complete)) {
      var job = client.insert(TYPE, "acknowledge").job();
      assertTrue(acknowledging.await(5, TimeUnit.SECONDS));
      // Stop requests cancellation while the handler's successful outcome is awaiting commit.
      assertThrows(RiverException.class, workers::stopAndCancel);
      assertNull(context.get(5, TimeUnit.SECONDS).cancellation());
      release.countDown();
      assertEquals(Workers.EventKind.JOB_COMPLETED, completed.get(5, TimeUnit.SECONDS).kind());
      assertEquals(Job.State.COMPLETED, client.get(job.id()).state());
    } finally {
      release.countDown();
      workers.stopAndCancel();
    }
  }

  @Test
  void jobTimeoutDoesNotDependOnTheDispatcher() throws Exception {
    var blocked = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var client =
        client()
            .withPlugin(
                new Plugin() {
                  @Override
                  public void producer(
                      Connection connection, Client.Driver driver, Producer producer)
                      throws InterruptedException {
                    if (!producer.active().isEmpty()) {
                      blocked.countDown();
                      release.await();
                    }
                  }
                });
    var cancelled = new CompletableFuture<WorkContext.Cancellation>();
    var failed = new CompletableFuture<Workers.Event>();
    var workers =
        workers(client)
            .jobTimeout(Duration.ofNanos(1))
            .serviceInterval(Duration.ofNanos(1))
            .stuckThreshold(Duration.ofMinutes(1))
            .add(
                TYPE,
                context -> {
                  context.awaitCancellation();
                  cancelled.complete(context.cancellation());
                  release.await();
                  context.checkCancelled();
                })
            .start();
    try (var subscription = workers.subscribe(failed::complete)) {
      client.insert(TYPE, "timeout", InsertOptions.builder().maxAttempts(1).build());
      assertTrue(blocked.await(5, TimeUnit.SECONDS));
      assertEquals(WorkContext.Cancellation.TIMEOUT, cancelled.get(5, TimeUnit.SECONDS));
      release.countDown();
      assertEquals(Job.State.DISCARDED, failed.get(5, TimeUnit.SECONDS).job().state());
    } finally {
      release.countDown();
      workers.stopAndCancel();
    }
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "job",
        "poll",
        "service",
        "stop",
        "stuck",
        "rescue",
        "maintenance",
        "reindex",
        "retention"
      })
  void oversizedDurationsFailDuringConfiguration(String option) {
    var builder = new Client(Database.connect("jdbc:postgresql://127.0.0.1:1/unused")).workers();
    var oversized = Duration.ofSeconds(Long.MAX_VALUE);
    assertThrows(
        IllegalArgumentException.class,
        () -> {
          switch (option) {
            case "job" -> builder.jobTimeout(oversized);
            case "poll" -> builder.pollInterval(oversized);
            case "service" -> builder.serviceInterval(oversized);
            case "stop" -> builder.stopTimeout(oversized);
            case "stuck" -> builder.stuckThreshold(oversized);
            case "rescue" -> builder.rescueAfter(oversized);
            case "maintenance" -> builder.maintenanceInterval(oversized);
            case "reindex" -> builder.reindex(java.util.List.of(), oversized);
            case "retention" -> builder.retention(oversized, Duration.ZERO, Duration.ZERO);
            default -> throw new AssertionError(option);
          }
        });
  }

  @Test
  void oversizedSnoozeFailsTheAttemptInsteadOfStrandingIt() throws Exception {
    var client = client();
    var failed = new CompletableFuture<Workers.Event>();
    try (var workers =
            workers(client)
                .add(TYPE, context -> context.snooze(Duration.ofSeconds(Long.MAX_VALUE)))
                .start();
        var subscription = workers.subscribe(failed::complete)) {
      var job = client.insert(TYPE, "snooze", InsertOptions.builder().maxAttempts(1).build()).job();
      var event = failed.get(5, TimeUnit.SECONDS);
      assertEquals(job.id(), event.job().id());
      assertEquals(Job.State.DISCARDED, event.job().state());
      assertTrue(event.job().errors().getFirst().error().contains("Snooze duration"));
    }
  }

  @Test
  void rescueCannotPreemptAConfiguredTimeout() {
    var builder =
        new Client(Database.connect("jdbc:postgresql://127.0.0.1:1/unused"))
            .workers()
            .queue("default", 1)
            .add(TYPE, context -> {})
            .jobTimeout(Duration.ofHours(2))
            .rescueAfter(Duration.ofHours(1));
    assertEquals(
        "rescueAfter must not be less than jobTimeout",
        assertThrows(IllegalArgumentException.class, builder::start).getMessage());
  }

  @ParameterizedTest
  @ValueSource(ints = {-1, 0, 120})
  void rescueDefaultAccountsForTheJobTimeout(int timeoutMinutes) throws Exception {
    var rescue = new CompletableFuture<Plugin.Rescue>();
    var client =
        client()
            .withPlugin(
                new Plugin() {
                  @Override
                  public boolean rescue(Client client, Rescue request) {
                    rescue.complete(request);
                    return true;
                  }
                });
    var builder = workers(client).leadership(true).add(TYPE, context -> {});
    if (timeoutMinutes != 0) builder.jobTimeout(Duration.ofMinutes(timeoutMinutes));
    try (var workers = builder.start()) {
      var request = rescue.get(5, TimeUnit.SECONDS);
      assertEquals(
          Duration.ofMinutes(60 + Math.max(0, timeoutMinutes)),
          Duration.between(request.horizon(), request.now()));
    }
  }

  @Test
  void goQueueNotificationsRespectTopicsAndSurviveMalformedPayloads() throws Exception {
    var client = client();
    var ready = new CountDownLatch(1);
    var errors = new LinkedBlockingQueue<Throwable>();
    var events = new LinkedBlockingQueue<Workers.Event>();
    var pause = notification("pause");
    var resume = notification("resume");
    String insertTopic = notification("insert").required("topic").asString();
    try (var workers =
            client
                .workers()
                .queue("priority", 1)
                .leadership(false)
                .errorHandler(errors::add)
                .observe(
                    event -> {
                      if (event.equals("listen_ready")) ready.countDown();
                    })
                .add(TYPE, context -> {})
                .start();
        var subscription =
            workers.subscribe(
                events::add, Workers.EventKind.QUEUE_PAUSED, Workers.EventKind.QUEUE_RESUMED)) {
      assertTrue(ready.await(5, TimeUnit.SECONDS));
      client.transaction(
          connection -> {
            client.driver().notify(connection, insertTopic, Json.encode(pause.required("payload")));
            // The later malformed notification acts as a barrier: the preceding one was dispatched.
            client.driver().notify(connection, pause.required("topic").asString(), "{");
            return null;
          });
      assertNotNull(errors.poll(5, TimeUnit.SECONDS));
      assertTrue(events.isEmpty(), "A control payload on the insert topic must not pause a queue");

      for (var fixture : java.util.List.of(pause, resume)) {
        client.transaction(
            connection -> {
              client
                  .driver()
                  .notify(
                      connection,
                      fixture.required("topic").asString(),
                      Json.encode(fixture.required("payload")));
              return null;
            });
        var event = events.poll(5, TimeUnit.SECONDS);
        assertNotNull(event, "Malformed JSON must not stop later notification delivery");
        assertEquals(
            fixture == pause ? Workers.EventKind.QUEUE_PAUSED : Workers.EventKind.QUEUE_RESUMED,
            event.kind());
        assertEquals("priority", event.queue().name());
      }
      assertTrue(events.isEmpty());
      assertTrue(errors.isEmpty());
    }
  }

  @Test
  void goResignationWakesLeadershipBeforeNextPoll() throws Exception {
    var client = client();
    var ready = new CountDownLatch(1);
    var checked = new CountDownLatch(1);
    var elected = new CountDownLatch(1);
    var errors = new LinkedBlockingQueue<Throwable>();
    var fixture = notification("resigned");
    var leaderId = fixture.required("payload").required("leader_id").asString();
    Instant previousTerm =
        client.transaction(
            connection -> {
              var now = Instant.now();
              try (var statement =
                      Sql.prepare(
                          connection,
                          Sql.query(client.database(), "leader_elect"),
                          leaderId,
                          client.database().timestamp(now),
                          client.database().timestamp(now.plusSeconds(3600)));
                  var rows = statement.executeQuery()) {
                assertTrue(rows.next());
                return Database.instant(rows.getString("elected_at"));
              }
            });
    var observer =
        client.withExtension(
            new Extension() {
              @Override
              public void periodicStarted() {
                elected.countDown();
              }
            });
    try (var workers =
        observer
            .workers()
            .id("observer")
            .queue("priority", 1)
            .serviceInterval(Duration.ofHours(1))
            .stopTimeout(Duration.ofSeconds(5))
            .errorHandler(errors::add)
            .observe(
                event -> {
                  if (event.equals("listen_ready")) ready.countDown();
                  if (event.equals("leadership_check")) checked.countDown();
                })
            .add(TYPE, context -> {})
            .start()) {
      assertTrue(ready.await(5, TimeUnit.SECONDS));
      assertTrue(checked.await(5, TimeUnit.SECONDS));
      assertFalse(workers.isLeader());
      client.transaction(
          connection -> {
            try (var statement =
                Sql.prepare(
                    connection,
                    Sql.query(client.database(), "leader_resign"),
                    leaderId,
                    client.database().timestamp(previousTerm))) {
              assertEquals(1, statement.executeUpdate());
            }
            client
                .driver()
                .notify(
                    connection,
                    fixture.required("topic").asString(),
                    Json.encode(fixture.required("payload")));
            return null;
          });
      assertTrue(
          elected.await(5, TimeUnit.SECONDS),
          "Peer resignation must wake election without waiting for its poll interval");
      assertTrue(workers.isLeader());
      assertTrue(errors.isEmpty(), () -> errors.toString());
    }
  }

  private static tools.jackson.databind.JsonNode notification(String name) throws Exception {
    for (var fixture : Conformance.fixture("protocol_values.json").required("notifications"))
      if (fixture.required("name").asString().equals(name)) return fixture;
    throw new AssertionError("Missing Go notification fixture: " + name);
  }

  @ParameterizedTest
  @ValueSource(strings = {"observer", "subscriber", "errorHandler"})
  void callbackFailuresDoNotLoseWorkerSlots(String callback) throws Exception {
    var client = client();
    var completed = new LinkedBlockingQueue<Workers.Event>();
    var reported = new LinkedBlockingQueue<Throwable>();
    var handled = new AtomicInteger();
    var observedEnds = new AtomicInteger();
    try (var workers =
            workers(client)
                .errorHandler(
                    error -> {
                      reported.add(error);
                      if (callback.equals("errorHandler"))
                        throw new IllegalStateException("error handler");
                    })
                .observe(
                    event -> {
                      if (callback.equals("observer") && event.equals("work_end"))
                        throw new AssertionError("observer");
                    })
                .observe(
                    event -> {
                      if (event.equals("work_end")) observedEnds.incrementAndGet();
                    })
                .add(TYPE, context -> handled.incrementAndGet())
                .start();
        var broken =
            workers.subscribe(
                event -> {
                  if (callback.equals("subscriber")) throw new AssertionError("subscriber");
                  if (callback.equals("errorHandler"))
                    throw new IllegalArgumentException("subscriber");
                });
        var subscription = workers.subscribe(completed::add, Workers.EventKind.JOB_COMPLETED)) {
      for (int i = 0; i < 2; i++) {
        var inserted = client.insert(TYPE, "job " + i);
        var event = completed.poll(5, TimeUnit.SECONDS);
        assertNotNull(event, "Callback failure must not strand the attempt or its worker slot");
        assertEquals(inserted.job().id(), event.job().id());
        assertEquals(Job.State.COMPLETED, client.get(event.job().id()).state());
      }
    }
    assertEquals(2, handled.get());
    assertEquals(2, observedEnds.get());
    assertEquals(2, reported.size());
  }

  @ParameterizedTest
  @ValueSource(
      strings = {
        "throw",
        "null",
        "negative",
        "overflow",
        "nanosecond_overflow",
        "timestamp_overflow"
      })
  void invalidRetryPoliciesFallBack(String policy) throws Exception {
    var client = client();
    var failed = new LinkedBlockingQueue<Workers.Event>();
    var reported = new LinkedBlockingQueue<Throwable>();
    try (var workers =
            workers(client)
                .errorHandler(reported::add)
                .maintenanceInterval(Duration.ofMillis(1))
                .serviceInterval(Duration.ofMillis(1))
                .retryPolicy(
                    job ->
                        switch (policy) {
                          case "throw" -> throw new IllegalStateException("retry policy");
                          case "null" -> null;
                          case "negative" -> Duration.ofSeconds(-1);
                          case "nanosecond_overflow" ->
                              Duration.ofNanos(Long.MAX_VALUE).plusNanos(1);
                          case "timestamp_overflow" -> Duration.ofDays(365_000_000);
                          default -> Duration.ofSeconds(Long.MAX_VALUE);
                        })
                .add(
                    TYPE,
                    context -> {
                      throw new IllegalStateException("worker failed");
                    })
                .start();
        var subscription = workers.subscribe(failed::add, Workers.EventKind.JOB_FAILED)) {
      client.insert(TYPE, "retry");
      var event = failed.poll(5, TimeUnit.SECONDS);
      assertNotNull(event, "A broken retry policy must not strand the attempt");
      assertEquals(Job.State.RETRYABLE, event.job().state());
      assertTrue(event.job().scheduledAt().isAfter(event.job().attemptedAt()));
      assertEquals("worker failed", event.job().errors().getFirst().error());
      assertEquals(1, reported.size());
    }
  }

  @ParameterizedTest
  @CsvSource({"false,1,5000", "true,1,5000", "false,3600,1", "true,3600,1"})
  void shortDelaysWakeFetchingBeforeThePollInterval(
      boolean snooze, long serviceSeconds, long maintenanceMillis) throws Exception {
    var client = client();
    var attempts = new AtomicInteger();
    var deferred = new CompletableFuture<Workers.Event>();
    var completed = new CompletableFuture<Workers.Event>();
    var reported = new LinkedBlockingQueue<Throwable>();
    var delay = Duration.ofSeconds(1);
    try (var workers =
            workers(client)
                .pollInterval(Duration.ofMinutes(1))
                .maintenanceInterval(Duration.ofMillis(maintenanceMillis))
                .serviceInterval(Duration.ofSeconds(serviceSeconds))
                .retryPolicy(job -> delay)
                .errorHandler(reported::add)
                .add(
                    TYPE,
                    context -> {
                      if (attempts.incrementAndGet() == 1) {
                        if (snooze) context.snooze(delay);
                        throw new IllegalStateException("retry");
                      }
                    })
                .start();
        var subscription =
            workers.subscribe(
                event -> {
                  if (event.kind() == Workers.EventKind.JOB_COMPLETED) completed.complete(event);
                  else deferred.complete(event);
                })) {
      var inserted = client.insert(TYPE, "short delay").job();
      var pending = deferred.get(5, TimeUnit.SECONDS);
      assertEquals(
          snooze ? Workers.EventKind.JOB_SNOOZED : Workers.EventKind.JOB_FAILED, pending.kind());
      assertEquals(Job.State.AVAILABLE, pending.job().state());
      var done = completed.get(10, TimeUnit.SECONDS).job();
      assertEquals(inserted.id(), done.id());
      assertFalse(done.attemptedAt().isBefore(pending.job().scheduledAt()));
      assertEquals(snooze ? 1 : 2, done.attempt());
      assertEquals(2, attempts.get());
    }
    assertTrue(reported.isEmpty(), () -> "Unexpected worker errors: " + reported);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void shutdownRespectsTheHandlersOutcome(boolean interrupted) throws Exception {
    var client = client();
    var entered = new CompletableFuture<WorkContext<String>>();
    var outcome = new CompletableFuture<Workers.Event>();
    var reported = new LinkedBlockingQueue<Throwable>();
    var workers =
        workers(client)
            .stopTimeout(Duration.ofSeconds(5))
            .errorHandler(reported::add)
            .add(
                TYPE,
                context -> {
                  entered.complete(context);
                  context.awaitCancellation();
                  if (interrupted) context.checkCancelled();
                })
            .start();
    try (var subscription = workers.subscribe(outcome::complete)) {
      var inserted =
          client.insert(TYPE, "stop", InsertOptions.builder().maxAttempts(1).build()).job();
      var context = entered.get(5, TimeUnit.SECONDS);
      workers.stopAndCancel();
      assertEquals(WorkContext.Cancellation.SHUTDOWN, context.cancellation());
      var event = outcome.get(5, TimeUnit.SECONDS);
      assertEquals(
          interrupted ? Workers.EventKind.JOB_INTERRUPTED : Workers.EventKind.JOB_COMPLETED,
          event.kind());
      var job = client.get(inserted.id());
      assertEquals(interrupted ? Job.State.AVAILABLE : Job.State.COMPLETED, job.state());
      assertEquals(interrupted ? 0 : 1, job.attempt());
      assertTrue(job.errors().isEmpty());
      if (interrupted) assertNull(job.finalizedAt());
      else assertNotNull(job.finalizedAt());
    } finally {
      workers.stopAndCancel();
    }
    assertTrue(reported.isEmpty(), () -> "Unexpected worker errors: " + reported);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void stoppingDuringAClaimPreservesCancellation(boolean cancel) throws Exception {
    var claimed = new CountDownLatch(1);
    var commit = new CountDownLatch(1);
    var client =
        client()
            .withPlugin(
                new Plugin() {
                  @Override
                  public java.util.List<Client.Decoded> claim(
                      Connection connection,
                      Client.Driver driver,
                      Claim claim,
                      Client.Transaction<java.util.List<Client.Decoded>> next)
                      throws Exception {
                    var rows = next.run(connection);
                    if (!rows.isEmpty()) {
                      claimed.countDown();
                      commit.await();
                    }
                    return rows;
                  }
                });
    var entered = new CompletableFuture<WorkContext<String>>();
    var outcome = new CompletableFuture<Workers.Event>();
    var reported = new LinkedBlockingQueue<Throwable>();
    var workers =
        workers(client)
            .errorHandler(reported::add)
            .add(
                TYPE,
                context -> {
                  entered.complete(context);
                  context.checkCancelled();
                })
            .start();
    try (var subscription = workers.subscribe(outcome::complete)) {
      var inserted = client.insert(TYPE, "late claim").job();
      assertTrue(claimed.await(5, TimeUnit.SECONDS));
      // Let the active-map scan finish while the claim is still uncommitted and unregistered.
      var error =
          assertThrows(
              RiverException.class,
              () -> {
                if (cancel) workers.stopAndCancel();
                else workers.stop();
              });
      assertTrue(error.getMessage().contains("Dispatcher did not stop"));
      commit.countDown();
      var context = entered.get(5, TimeUnit.SECONDS);
      assertEquals(cancel ? WorkContext.Cancellation.SHUTDOWN : null, context.cancellation());
      var event = outcome.get(5, TimeUnit.SECONDS);
      assertEquals(
          cancel ? Workers.EventKind.JOB_INTERRUPTED : Workers.EventKind.JOB_COMPLETED,
          event.kind());
      workers.stop();
      var job = client.get(inserted.id());
      assertEquals(cancel ? Job.State.AVAILABLE : Job.State.COMPLETED, job.state());
      assertEquals(cancel ? 0 : 1, job.attempt());
      assertTrue(job.errors().isEmpty());
    } finally {
      commit.countDown();
      workers.stopAndCancel();
    }
    assertTrue(reported.isEmpty(), () -> "Unexpected worker errors: " + reported);
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void terminalFailuresDoNotConsultRetryPolicy(boolean cancel) throws Exception {
    var client = client();
    var failed = new LinkedBlockingQueue<Workers.Event>();
    var policyCalls = new AtomicInteger();
    try (var workers =
            workers(client)
                .cancelOnError(cancel)
                .retryPolicy(
                    job -> {
                      policyCalls.incrementAndGet();
                      return Duration.ofDays(1);
                    })
                .add(
                    TYPE,
                    context -> {
                      throw new IllegalStateException("worker failed");
                    })
                .start();
        var subscription = workers.subscribe(failed::add)) {
      client.insert(TYPE, "terminal", InsertOptions.builder().maxAttempts(1).build());
      var event = failed.poll(5, TimeUnit.SECONDS);
      assertNotNull(event);
      assertEquals(cancel ? Job.State.CANCELLED : Job.State.DISCARDED, event.job().state());
      assertEquals(0, policyCalls.get());
    }
  }
}
