package com.riverqueue.conformance;

import com.riverqueue.Client;
import com.riverqueue.Extension;
import com.riverqueue.InsertOptions;
import com.riverqueue.Job;
import com.riverqueue.JobType;
import com.riverqueue.Json;
import com.riverqueue.RiverException;
import com.riverqueue.Schedule;
import com.riverqueue.Unique;
import com.riverqueue.WorkContext;
import com.riverqueue.Workers;
import java.sql.Connection;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import tools.jackson.databind.JsonNode;

/** Real worker registrations and instrumentation used only by conformance. */
final class RuntimeAdapter {
  private static final JobType<Adapter.Echo> ECHO =
      JobType.of("conformance_echo", Adapter.Echo.class);
  private final Map<String, CountDownLatch> barriers = new ConcurrentHashMap<>();
  private Probe probe = new Probe();
  private Workers workers;
  private volatile CountDownLatch claimBarrier;
  private final java.util.concurrent.atomic.AtomicBoolean firstClaim =
      new java.util.concurrent.atomic.AtomicBoolean();

  JsonNode handle(Client river, String method, JsonNode params) throws Exception {
    return switch (method) {
      case "barrier_create" -> {
        if (barriers.putIfAbsent(params.path("name").asString(), new CountDownLatch(1)) != null)
          throw new IllegalArgumentException("Barrier already exists");
        yield Json.object();
      }
      case "barrier_release" -> {
        barrier(params.path("name").asString()).countDown();
        yield Json.object();
      }
      case "queue_add" -> {
        requireWorkers()
            .addQueue(params.path("name").asString(), params.path("max_workers").asInt(1));
        yield Json.object();
      }
      case "queue_remove" -> {
        requireWorkers().removeQueue(params.path("name").asString());
        yield Json.object();
      }
      case "runtime_stats" -> probe.snapshot();
      case "start" -> {
        start(river, params);
        yield Json.object();
      }
      case "stop" -> {
        stop(params.path("cancel").asBoolean(false));
        yield Json.object();
      }
      case "wait" -> waitFor(river, params);
      case "work" -> {
        if (workers != null) throw new IllegalArgumentException("Client already running");
        var job = river.get(params.path("id").asLong());
        var start =
            Json.object()
                .put("client_id", params.path("client_id").asString("java-conformance-worker"))
                .put("queue", job.queue());
        start(river, start);
        try {
          yield waitFor(river, params);
        } finally {
          stop(false);
        }
      }
      default -> throw new IllegalArgumentException("Unknown runtime method: " + method);
    };
  }

  Extension instrumentation() {
    return new Extension() {
      @Override
      public void afterClaim(Job<?> job) throws Exception {
        if (claimBarrier != null && firstClaim.compareAndSet(false, true)) claimBarrier.await();
      }

      @Override
      public void beforeInsert(
          Connection connection, JobType<?> type, Object args, InsertOptions options) {
        if (probe.instrumented) probe.trace.add("hook:insert_begin");
      }

      @Override
      public void beforeWork(WorkContext<?> context) {
        if (probe.instrumented) probe.trace.add("hook:work_begin");
      }

      @Override
      public void afterWork(WorkContext<?> context, Throwable failure) {
        if (probe.instrumented) probe.trace.add("hook:work_end");
      }

      @Override
      public <T> T insert(Connection connection, Callable<T> next) throws Exception {
        if (probe.instrumented) probe.trace.add("middleware:insert_before");
        T result = next.call();
        if (probe.instrumented) probe.trace.add("middleware:insert_after");
        return result;
      }

      @Override
      public void periodicStarted() {
        probe.periodicStarts.incrementAndGet();
        if (probe.instrumented) probe.trace.add("hook:periodic_start");
      }

      @Override
      public void work(WorkContext<?> context, WorkContext.Step next) throws Exception {
        if (probe.instrumented) probe.trace.add("middleware:work_before");
        try {
          next.run();
        } finally {
          if (probe.instrumented) probe.trace.add("middleware:work_after");
        }
      }
    };
  }

  void stop(boolean cancel) {
    if (workers == null) return;
    if (claimBarrier != null) claimBarrier.countDown();
    Workers previous = workers;
    workers = null;
    if (cancel) previous.stopAndCancel();
    else previous.stop();
  }

  private CountDownLatch barrier(String name) {
    var value = barriers.get(name);
    if (value == null)
      throw new RiverException(RiverException.Code.NOT_FOUND, "Barrier not found: " + name);
    return value;
  }

  private Workers requireWorkers() {
    if (workers == null) throw new IllegalArgumentException("No client running");
    return workers;
  }

  private void start(Client river, JsonNode params) {
    if (workers != null) throw new IllegalArgumentException("Client already running");
    probe = new Probe();
    probe.instrumented = params.path("instrumented").asBoolean(false);
    claimBarrier =
        params.has("claim_barrier") ? barriers.get(params.path("claim_barrier").asString()) : null;
    if (params.has("claim_barrier") && claimBarrier == null)
      throw new Adapter.Failure(-32602, "Unknown claim barrier");
    firstClaim.set(false);
    boolean periodic = params.path("periodic_run_on_start").asBoolean(false);
    boolean unique = params.path("periodic_unique").asBoolean(false);
    if (unique && !periodic)
      throw new IllegalArgumentException("periodic_unique requires periodic_run_on_start");
    var builder =
        river
            .workers()
            .id(params.path("client_id").asString())
            .queue(params.path("queue").asString("default"), params.path("max_workers").asInt(1))
            .handle(ECHO, this::work)
            .pollOnly(params.path("poll_only").asBoolean(false))
            .pollInterval(Duration.ofMillis(params.path("fetch_poll_interval_ms").asLong(10)))
            .leadership(!params.path("leader_election_disabled").asBoolean(false))
            .maintenanceInterval(
                Duration.ofMillis(params.path("scheduler_interval_ms").asLong(5000)))
            .serviceInterval(Duration.ofMillis(params.path("elect_interval_ms").asLong(250)))
            .rescueAfter(Duration.ofMillis(params.path("rescue_after_ms").asLong(3600000)))
            .stuckThreshold(Duration.ofMillis(params.path("job_stuck_threshold_ms").asLong(5000)))
            .observe(
                name -> {
                  if (name.equals("stuck_job")) probe.stuckJobs.incrementAndGet();
                })
            .cancelOnError(params.path("error_handler_cancel").asBoolean(false));
    builder.retention(
        Duration.ofMillis(params.path("cancelled_job_retention_ms").asLong(86400000)),
        Duration.ofMillis(params.path("completed_job_retention_ms").asLong(86400000)),
        Duration.ofMillis(params.path("discarded_job_retention_ms").asLong(604800000)));
    if (params.has("reindexer_index_names")) {
      var names = new java.util.ArrayList<String>();
      for (var name : params.path("reindexer_index_names")) names.add(name.asString());
      builder.reindex(
          names,
          params.has("reindexer_interval_ms")
              ? Duration.ofMillis(params.path("reindexer_interval_ms").asLong())
              : null);
    }
    if (params.has("retry_delay_ms"))
      builder.retryPolicy(_ -> Duration.ofMillis(params.path("retry_delay_ms").asLong()));
    if (params.path("job_timeout_disabled").asBoolean(false))
      builder.jobTimeout(Duration.ofMillis(-1));
    else if (params.has("job_timeout_ms"))
      builder.jobTimeout(Duration.ofMillis(params.path("job_timeout_ms").asLong()));
    if (periodic) {
      var opts =
          InsertOptions.builder().unique(unique ? Unique.args().perQueue() : Unique.none()).build();
      builder.periodic(
          "conformance-periodic",
          Schedule.every(Duration.ofHours(1)),
          ECHO,
          new Adapter.Echo("", 0, "periodic run on start"),
          opts,
          true);
      if (unique)
        builder.periodic(
            "conformance-periodic-marker",
            Schedule.every(Duration.ofHours(1)),
            ECHO,
            new Adapter.Echo("", 0, "periodic marker"),
            InsertOptions.defaults(),
            true);
    }
    workers = builder.start();
    workers.subscribe(
        event -> {
          probe.events.add(event.kind().value());
          if (params.path("error_handler_cancel").asBoolean(false)
              && event.kind() == Workers.EventKind.JOB_CANCELLED)
            probe.errorHandlerCalls.incrementAndGet();
        });
  }

  private JsonNode waitFor(Client river, JsonNode params) throws Exception {
    long deadline = System.nanoTime() + Duration.ofSeconds(20).toNanos();
    while (System.nanoTime() < deadline) {
      var job = river.get(params.path("id").asLong());
      boolean match = params.has("states") ? false : job.state().isFinalized();
      for (var state : params.path("states"))
        if (job.state().value().equals(state.asString())) match = true;
      if (match) return Adapter.normalized(job);
      TimeUnit.MILLISECONDS.sleep(5);
    }
    throw new RiverException(RiverException.Code.REJECTED, "Timed out waiting for job state");
  }

  private void work(WorkContext<Adapter.Echo> context) throws Exception {
    var args = context.args();
    switch (args.behavior()) {
      case "barrier_wait", "barrier_output" -> {
        barrier(args.message()).await();
        if (args.behavior().equals("barrier_output")) context.output(Map.of("race", "worker"));
      }
      case "cancel" -> context.cancel("conformance cancel");
      case "cancel_error" -> {
        context.awaitCancellation();
        throw new IllegalStateException("conformance failure after cancellation");
      }
      case "cancel_panic" -> {
        context.awaitCancellation();
        throw new AssertionError("conformance panic after cancellation");
      }
      case "cooperative_cancel", "snooze_then_cancel" -> {
        if (args.behavior().equals("snooze_then_cancel")
            && !context.job().metadata().has("snoozes"))
          context.snooze(Duration.ofMillis(Math.max(1, args.durationMs())));
        if (context.isCancelled()) probe.cancelledAtStart.incrementAndGet();
        context.awaitCancellation();
        context.checkCancelled();
      }
      case "discard" -> context.discard("conformance discard");
      case "error" -> throw new IllegalStateException("conformance retryable error");
      case "ignored_cancel" -> new CountDownLatch(1).await();
      case "output" -> context.output(Map.of("message", args.message()));
      case "panic" -> throw new AssertionError("conformance worker panic");
      case "sleep" -> TimeUnit.MILLISECONDS.sleep(args.durationMs());
      case "snooze_once" -> {
        if (!context.job().metadata().has("snoozes"))
          context.snooze(Duration.ofMillis(Math.max(1, args.durationMs())));
      }
      case "resumable", "resumable_duplicate" -> {
        context.step("first", () -> probe.resumableFirstRuns.incrementAndGet());
        context.step(
            args.behavior().equals("resumable_duplicate") ? "first" : "second",
            () -> {
              probe.resumableSecondRuns.incrementAndGet();
              if (context.job().attempt() == 1)
                throw new IllegalStateException("fail second resumable step once");
            });
      }
      case "resumable_cursor" -> {
        context.step("first", () -> context.metadata("first_attempt", context.job().attempt()));
        context.stepWithCursor(
            "second",
            cursor -> {
              if (context.job().attempt() == 1) {
                context.cursor(7);
                throw new IllegalStateException("retry with cursor");
              }
              if (cursor.asLong(0) != 7) throw new IllegalStateException("expected cursor 7");
              context.metadata("cursor_observed", 7);
            });
        context.step(
            "third",
            () -> {
              if (context.job().attempt() == 2)
                throw new IllegalStateException("retry after consuming cursor");
            });
      }
      case "transactional_complete" -> {
        context.metadata("transactional_completion", true);
        context.transaction(c -> context.complete(c));
      }
      default -> {}
    }
  }

  private static final class Probe {
    final AtomicInteger cancelledAtStart = new AtomicInteger();
    final AtomicInteger errorHandlerCalls = new AtomicInteger();
    final List<String> events = new CopyOnWriteArrayList<>();
    boolean instrumented;
    final AtomicInteger periodicStarts = new AtomicInteger();
    final AtomicInteger resumableFirstRuns = new AtomicInteger();
    final AtomicInteger resumableSecondRuns = new AtomicInteger();
    final AtomicInteger stuckJobs = new AtomicInteger();
    final List<String> trace = new CopyOnWriteArrayList<>();

    JsonNode snapshot() {
      return Json.tree(
          Map.of(
              "cancelled_at_start",
              cancelledAtStart.get(),
              "error_handler_calls",
              errorHandlerCalls.get(),
              "events",
              events,
              "periodic_starts",
              periodicStarts.get(),
              "resumable_first_runs",
              resumableFirstRuns.get(),
              "resumable_second_runs",
              resumableSecondRuns.get(),
              "stuck_jobs",
              stuckJobs.get(),
              "trace",
              trace));
    }
  }
}
