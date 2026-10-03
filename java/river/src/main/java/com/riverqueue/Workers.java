package com.riverqueue;

import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import org.postgresql.PGConnection;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

/** A bounded worker runtime using one virtual thread per active job. */
public final class Workers implements AutoCloseable {
  private final Map<Long, WorkContext<?>> active = new ConcurrentHashMap<>();
  private final Map<Long, Thread> activeThreads = new ConcurrentHashMap<>();
  private final Map<Long, Long> pendingCancellation = new ConcurrentHashMap<>();
  private final AtomicBoolean maintenanceBusy = new AtomicBoolean();
  private volatile Instant nextReindex =
      Instant.now()
          .atOffset(java.time.ZoneOffset.UTC)
          .toLocalDate()
          .plusDays(1)
          .atStartOfDay()
          .toInstant(java.time.ZoneOffset.UTC);
  private final Builder config;
  private final java.util.concurrent.BlockingQueue<Completion> completions =
      new java.util.concurrent.LinkedBlockingQueue<>();
  private final java.util.concurrent.CountDownLatch dispatcherStopped =
      new java.util.concurrent.CountDownLatch(1);
  private volatile boolean cancelOnStop;
  private final AtomicBoolean stopFinalized = new AtomicBoolean();
  private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
  private final String id;
  private volatile Instant leaderElectedAt;
  private volatile long leaderEligibleAt;
  private volatile Connection listener;
  private final Map<String, Integer> queues = new ConcurrentHashMap<>();
  private final Map<String, Instant> periodicNext = new ConcurrentHashMap<>();
  private final java.util.Set<String> pausedQueues = ConcurrentHashMap.newKeySet();
  private final Object queueLock = new Object();
  private final java.util.Set<String> drainingQueues = ConcurrentHashMap.newKeySet();
  private final java.util.concurrent.CountDownLatch stopping =
      new java.util.concurrent.CountDownLatch(1);
  private final Client river;
  private final Runnable unsubscribeInsert;
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final List<Consumer<Event>> subscribers = new CopyOnWriteArrayList<>();
  private final Semaphore wake = new Semaphore(0);

  private Workers(Builder config) {
    this.config = config;
    river = config.river;
    id = config.id;
    queues.putAll(config.queues);
    if (queues.isEmpty() || config.handlers.isEmpty())
      throw new IllegalArgumentException("Workers require queues and handlers");
    if (config.reindexInterval != null) nextReindex = Instant.now().plus(config.reindexInterval);
    for (String queue : queues.keySet()) heartbeat(queue);
    unsubscribeInsert = river.onInsertCommit(wake::release);
    if (!config.pollOnly) executor.submit(this::listen);
    executor.submit(this::completeBatches);
    executor.submit(this::supervise);
    executor.submit(this::dispatch);
    if (config.leadership) executor.submit(this::elect);
  }

  public void addQueue(String name, int maxWorkers) {
    validateQueue(name, maxWorkers);
    synchronized (queueLock) {
      if (drainingQueues.contains(name))
        throw new IllegalStateException("Queue is draining: " + name);
      queues.put(name, maxWorkers);
      heartbeat(name);
    }
    wake.release();
  }

  @Override
  public void close() {
    stop();
  }

  public String id() {
    return id;
  }

  public boolean isLeader() {
    return leaderElectedAt != null;
  }

  public void removeQueue(String name) {
    synchronized (queueLock) {
      if (!queues.containsKey(name) || !drainingQueues.add(name))
        throw new RiverException(RiverException.Code.NOT_FOUND, "Queue not configured: " + name);
      try {
        // Keep the producer lease alive until every committed claim has settled.
        long deadline = System.nanoTime() + config.stopTimeout.toNanos();
        while (active.values().stream().anyMatch(c -> c.job().queue().equals(name))) {
          long left = deadline - System.nanoTime();
          if (left <= 0)
            throw new RiverException(RiverException.Code.REJECTED, "Queue did not drain: " + name);
          TimeUnit.NANOSECONDS.timedWait(queueLock, left);
        }
        queues.remove(name);
        heartbeat(name);
      } catch (InterruptedException error) {
        Thread.currentThread().interrupt();
        throw new RiverException(
            RiverException.Code.REJECTED, "Interrupted draining queue: " + name, error);
      } finally {
        drainingQueues.remove(name);
      }
    }
  }

  /**
   * Stops fetching jobs and waits for active attempts to finish. A timeout leaves attempts running;
   * call again to continue waiting, or use {@link #stopAndCancel()} to request cancellation.
   */
  public void stop() {
    stop(false);
  }

  private void stop(boolean cancel) {
    if (running.getAndSet(false)) {
      unsubscribeInsert.run();
      stopping.countDown();
    }
    if (cancel) cancelOnStop = true;
    long deadline = System.nanoTime() + config.stopTimeout.toNanos();
    if (cancel)
      active.values().forEach(c -> c.requestCancellation(WorkContext.Cancellation.SHUTDOWN));
    wake.release();
    try {
      if (!dispatcherStopped.await(Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS))
        throw new RiverException(
            RiverException.Code.REJECTED, "Dispatcher did not stop within the stop deadline");
      executor.shutdown();
      if (!executor.awaitTermination(
          Math.max(0, deadline - System.nanoTime()), TimeUnit.NANOSECONDS))
        throw new RiverException(
            RiverException.Code.REJECTED, "Workers have not stopped within the stop deadline");
    } catch (InterruptedException e) {
      Thread.currentThread().interrupt();
      throw new RiverException(RiverException.Code.REJECTED, "Interrupted waiting for workers", e);
    }
    if (stopFinalized.compareAndSet(false, true)) {
      queues.keySet().forEach(this::heartbeat);
      resign();
    }
  }

  /** Stops fetching, requests cancellation of active attempts, and waits for them to finish. */
  public void stopAndCancel() {
    stop(true);
  }

  /** Subscribes to the requested event kinds, or all kinds when none are supplied. */
  public Subscription subscribe(Consumer<Event> subscriber, EventKind... kinds) {
    Objects.requireNonNull(subscriber, "subscriber");
    Set<EventKind> selected = Set.copyOf(List.of(kinds));
    Consumer<Event> filtered =
        event -> {
          if (selected.isEmpty() || selected.contains(event.kind())) subscriber.accept(event);
        };
    subscribers.add(filtered);
    return () -> subscribers.remove(filtered);
  }

  private void dispatch() {
    long nextPoll = 0;
    long nextMaintenance = 0;
    long nextCancelPoll = 0;
    while (running.get()) {
      try {
        long now = System.nanoTime();
        boolean notified = wake.drainPermits() > 0;
        if (notified || now >= nextPoll) {
          synchronized (queueLock) {
            for (var queue : queues.entrySet()) {
              if (drainingQueues.contains(queue.getKey())) continue;
              int available =
                  river.plugin().workerLimit(queue.getKey(), queue.getValue())
                      - (int)
                          active.values().stream()
                              .filter(c -> c.job().queue().equals(queue.getKey()))
                              .count();
              if (available > 0) claim(queue.getKey(), available);
            }
          }
          nextPoll = now + config.pollInterval.toNanos();
        }
        if (now >= nextMaintenance) {
          queues.keySet().forEach(this::heartbeat);
          nextMaintenance = now + config.serviceInterval.toNanos();
        }
        if (now >= nextCancelPoll) {
          river.transaction(
              c -> {
                try (var statement =
                        Sql.prepare(c, Sql.query(river.database(), "cancel_requested"));
                    var rows = statement.executeQuery()) {
                  while (rows.next()) {
                    var context = active.get(rows.getLong(1));
                    if (context != null)
                      context.requestCancellation(WorkContext.Cancellation.REMOTE);
                  }
                }
                return null;
              });
          for (var context : active.values()) {
            if (!config.jobTimeout.isNegative()
                && Duration.between(context.job().attemptedAt(), Instant.now())
                        .compareTo(config.jobTimeout)
                    > 0) context.requestCancellation(WorkContext.Cancellation.TIMEOUT);
          }
          pendingCancellation
              .entrySet()
              .removeIf(entry -> now - entry.getValue() > Duration.ofMinutes(1).toNanos());
          nextCancelPoll = now + TimeUnit.MILLISECONDS.toNanos(50);
        }
        if (wake.tryAcquire(10, TimeUnit.MILLISECONDS)) wake.release();
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        break;
      } catch (Exception e) {
        report(e);
        try {
          TimeUnit.MILLISECONDS.sleep(100);
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          break;
        }
      }
    }
    dispatcherStopped.countDown();
  }

  private void claim(String queue, int count) {
    List<Client.Decoded> jobs =
        river.transaction(
            c ->
                river
                    .plugin()
                    .claim(
                        c,
                        river.driver(),
                        new Plugin.Claim(
                            id,
                            queue,
                            count,
                            Instant.now(),
                            java.util.Set.copyOf(config.handlers.keySet()),
                            config.fetchOnlyKnownKinds),
                        ignored -> {
                          var result = new ArrayList<Client.Decoded>();
                          try (var statement =
                                  Sql.prepare(
                                      c,
                                      Sql.query(
                                          river.database(),
                                          config.fetchOnlyKnownKinds ? "claim_known" : "claim"),
                                      config.fetchOnlyKnownKinds
                                          ? new Object[] {
                                            river.database().timestamp(Instant.now()),
                                            id,
                                            queue,
                                            river.database().timestamp(Instant.now()),
                                            Json.encode(config.handlers.keySet()),
                                            count
                                          }
                                          : new Object[] {
                                            river.database().timestamp(Instant.now()),
                                            id,
                                            queue,
                                            river.database().timestamp(Instant.now()),
                                            count
                                          });
                              var rows = statement.executeQuery()) {
                            while (rows.next()) result.add(river.readPartial(rows));
                          }
                          return result;
                        }));
    for (var decoded : jobs) {
      var job = decoded.job();
      var registration = config.handlers.get(job.kind());
      if (decoded.failure() != null) complete(job, null, decoded.failure());
      else launch(job, registration);
    }
  }

  private <A> void launch(Job<JsonNode> row, Registration<A> registration) {
    if (registration == null) {
      complete(
          row,
          null,
          new IllegalArgumentException(
              "job kind is not registered in the client's Workers bundle: " + row.kind()));
      return;
    }
    WorkContext<A> context;
    try {
      context = new WorkContext<>(river, river.typed(row, registration.type));
      context.attach(this, null);
    } catch (Exception e) {
      complete(row, null, e);
      return;
    }
    active.put(row.id(), context);
    if (row.metadata().has("cancel_attempted_at"))
      context.requestCancellation(WorkContext.Cancellation.REMOTE);
    if (pendingCancellation.remove(row.id()) != null)
      context.requestCancellation(WorkContext.Cancellation.REMOTE);
    if (cancelOnStop) context.requestCancellation(WorkContext.Cancellation.SHUTDOWN);
    executor.submit(
        () -> {
          activeThreads.put(row.id(), Thread.currentThread());
          Exception failure = null;
          try {
            for (var extension : river.extensions()) extension.afterClaim(row);
            for (var observer : config.observers) observer.accept("work_begin");
            workMiddleware(
                context,
                0,
                () -> {
                  Throwable workFailure = null;
                  try {
                    for (var extension : river.extensions()) extension.beforeWork(context);
                    registration.handler.work(context);
                  } catch (Exception | Error e) {
                    workFailure = e;
                    throw e;
                  } finally {
                    for (var extension : river.extensions())
                      extension.afterWork(context, workFailure);
                  }
                });
          } catch (Exception e) {
            failure = e;
          } catch (Throwable e) {
            failure = new RuntimeException("Worker threw " + e, e);
          } finally {
            for (var observer : config.observers) observer.accept("work_end");
          }
          failure = context.finish(failure);
          // A claimed job keeps its slot until completion is durably acknowledged.
          while (true) {
            try {
              for (var peer : List.copyOf(context.peers.values()))
                completePeer(
                    context,
                    peer.context(),
                    failure == null
                        ? new IllegalStateException("Batch worker did not provide a peer outcome")
                        : failure);
              complete(row, context, failure);
              break;
            } catch (Exception e) {
              report(e);
              try {
                TimeUnit.MILLISECONDS.sleep(100);
              } catch (InterruptedException interrupted) {
                Thread.currentThread().interrupt();
                return;
              }
            }
          }
          synchronized (queueLock) {
            active.remove(row.id(), context);
            queueLock.notifyAll();
          }
          activeThreads.remove(row.id());
          wake.release();
        });
  }

  private void workMiddleware(WorkContext<?> context, int index, WorkContext.Step next)
      throws Exception {
    if (index == river.extensions().size()) next.run();
    else
      river.extensions().get(index).work(context, () -> workMiddleware(context, index + 1, next));
  }

  <A> List<WorkContext<A>> claimPeers(
      WorkContext<?> parent, JobType<A> type, Client.Transaction<List<Client.Decoded>> operation) {
    parent.checkRuntimeCancellation();
    var rows =
        river.transaction(
            connection -> {
              var claimed = operation.run(connection);
              river.plugin().afterPeerClaim(connection, river.driver(), claimed);
              return claimed;
            });
    // Register every committed claim before decoding any arguments. A decoding failure must still
    // leave all claimed jobs owned by this attempt so its failure settles the entire batch.
    for (var decoded : rows) {
      var context = new WorkContext<>(river, decoded.job(), false);
      context.attach(this, parent);
      parent.peers.put(decoded.job().id(), new WorkContext.Peer(decoded.job(), context));
      active.put(decoded.job().id(), context);
    }
    var contexts = new ArrayList<WorkContext<A>>();
    for (var decoded : rows) {
      if (decoded.failure() != null) throw decoded.failure();
      var context = new WorkContext<>(river, river.typed(decoded.job(), type));
      context.attach(this, parent);
      parent.peers.put(decoded.job().id(), new WorkContext.Peer(decoded.job(), context));
      active.put(decoded.job().id(), context);
      contexts.add(context);
    }
    return List.copyOf(contexts);
  }

  void completePeer(WorkContext<?> parent, WorkContext<?> context, Exception failure) {
    var peer = parent.peers.get(context.job().id());
    if (peer == null || peer.context() != context)
      throw new IllegalArgumentException("Peer is not owned by this batch attempt");
    if (parent.cancellation() != null) context.requestCancellation(parent.cancellation());
    complete(peer.row(), context, context.finish(failure));
    parent.peers.remove(context.job().id(), peer);
    synchronized (queueLock) {
      active.remove(context.job().id(), context);
      queueLock.notifyAll();
    }
  }

  private void complete(Job<JsonNode> original, WorkContext<?> context, Exception failure) {
    Instant now = Instant.now();
    var updates = context == null ? Json.object() : context.metadataUpdates();
    Job.State state = Job.State.COMPLETED;
    Instant scheduled = original.scheduledAt();
    int attempt = original.attempt();
    EventKind event = EventKind.JOB_COMPLETED;
    String error = null;
    if (context != null && context.cancellation() == WorkContext.Cancellation.REMOTE) {
      state = Job.State.CANCELLED;
      error = "JobCancelError: job cancelled remotely";
      event = EventKind.JOB_CANCELLED;
    } else if (failure instanceof WorkContext.Control control) {
      if (control.delay != null) {
        scheduled = now.plus(control.delay);
        state =
            control.delay.compareTo(config.maintenanceInterval) <= 0
                ? Job.State.AVAILABLE
                : Job.State.SCHEDULED;
        attempt--;
        updates.put("snoozes", original.metadata().path("snoozes").asLong(0) + 1);
        event = EventKind.JOB_SNOOZED;
      } else {
        state = control.state;
        error = control.getMessage();
        event = state == Job.State.CANCELLED ? EventKind.JOB_CANCELLED : EventKind.JOB_FAILED;
      }
    } else if (failure instanceof InterruptedException
        && context != null
        && context.cancellation() == WorkContext.Cancellation.SHUTDOWN) {
      state = Job.State.AVAILABLE;
      scheduled = now;
      attempt--;
      event = EventKind.JOB_INTERRUPTED;
    } else if (failure != null
        || (context != null && context.cancellation() == WorkContext.Cancellation.TIMEOUT)) {
      error =
          failure == null
              ? "Job timeout"
              : failure.getMessage() == null ? failure.toString() : failure.getMessage();
      boolean cancelled = config.cancelOnError;
      state =
          cancelled
              ? Job.State.CANCELLED
              : original.attempt() >= original.maxAttempts()
                  ? Job.State.DISCARDED
                  : Job.State.RETRYABLE;
      Duration delay = config.retryPolicy.delay(original);
      if (state == Job.State.RETRYABLE) {
        scheduled = now.plus(delay);
        if (delay.compareTo(config.maintenanceInterval) <= 0) state = Job.State.AVAILABLE;
      }
      event = cancelled ? EventKind.JOB_CANCELLED : EventKind.JOB_FAILED;
    }
    String trace = "";
    if (failure != null && failure.getCause() instanceof Error) {
      var text = new java.io.StringWriter();
      failure.printStackTrace(new java.io.PrintWriter(text));
      trace = text.toString();
    }
    String errors =
        error == null
            ? null
            : Json.encode(
                new Job.AttemptError(original.attemptedAt(), original.attempt(), error, trace));
    var done = new java.util.concurrent.CompletableFuture<Job<JsonNode>>();
    completions.add(
        new Completion(
            original,
            new Object[] {
              state.value(),
              river.database().timestamp(now),
              state.value(),
              river.database().timestamp(state.isFinalized() ? now : null),
              river.database().timestamp(scheduled),
              attempt,
              Json.encode(updates),
              errors,
              errors,
              original.id(),
              original.attempt(),
              river.database().timestamp(original.attemptedAt())
            },
            done));
    var updated = done.join();
    // An ephemeral job may already have been completed and deleted in the application's
    // transaction.
    if (updated == null) return;
    if (updated.state() == Job.State.COMPLETED) event = EventKind.JOB_COMPLETED;
    if (updated.state() == Job.State.DISCARDED) event = EventKind.JOB_FAILED;
    if (updated.state() == Job.State.CANCELLED) event = EventKind.JOB_CANCELLED;
    publish(new Event(event, updated));
  }

  private void completeBatches() {
    while (running.get()
        || dispatcherStopped.getCount() != 0
        || !active.isEmpty()
        || !completions.isEmpty()) {
      try {
        var first = completions.poll(20, TimeUnit.MILLISECONDS);
        if (first == null) continue;
        var batch = new ArrayList<Completion>();
        batch.add(first);
        completions.drainTo(batch, 999);
        try {
          var results =
              river.transaction(
                  c -> {
                    var rows = new ArrayList<Job<JsonNode>>();
                    for (var completion : batch)
                      try (var statement =
                              Sql.prepare(
                                  c, Sql.query(river.database(), "complete"), completion.params);
                          var result = statement.executeQuery()) {
                        if (result.next()) rows.add(river.readPartial(result).job());
                        else {
                          try {
                            rows.add(river.get(c, completion.original.id()));
                          } catch (RiverException error) {
                            if (error.code() != RiverException.Code.NOT_FOUND) throw error;
                            rows.add(null);
                          }
                        }
                      }
                    for (var job : rows)
                      if (job != null) river.plugin().afterStateChange(c, river.driver(), job);
                    for (var completion : batch)
                      river.plugin().afterAttempt(c, river.driver(), completion.original);
                    return rows;
                  });
          for (int i = 0; i < batch.size(); i++) batch.get(i).done.complete(results.get(i));
        } catch (Exception e) {
          for (var completion : batch) completion.done.completeExceptionally(e);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private void heartbeat(String queue) {
    river.transaction(
        c -> {
          try (var statement =
              Sql.prepare(
                  c,
                  Sql.query(river.database(), "queue_heartbeat"),
                  queue,
                  river.database().timestamp(Instant.now()),
                  river.database().timestamp(Instant.now()))) {
            statement.executeUpdate();
          }
          river
              .plugin()
              .producer(
                  c,
                  river.driver(),
                  new Plugin.Producer(
                      id,
                      queue,
                      queues.getOrDefault(queue, config.queues.getOrDefault(queue, 1)),
                      active.values().stream().<Job<?>>map(WorkContext::job).toList(),
                      !running.get() || !queues.containsKey(queue),
                      Instant.now()));
          return null;
        });
  }

  private void listen() {
    if (river.database().dialect() == Database.Dialect.SQLITE) {
      listenSqlite();
      return;
    }
    while (running.get()) {
      try (var connection = river.database().connection()) {
        if (!river.database().supportsNotifications(connection)) return;
        listener = connection;
        String schema = river.database().schema();
        if (schema.isEmpty())
          try (var statement = connection.createStatement();
              var rows = statement.executeQuery("SELECT current_schema()")) {
            rows.next();
            schema = rows.getString(1);
          }
        for (String topic : List.of("river_insert", "river_control", "river_leadership"))
          try (var statement = connection.createStatement()) {
            statement.execute("LISTEN \"" + schema.replace("\"", "\"\"") + "." + topic + "\"");
          }
        wake.release();
        var postgres = connection.unwrap(PGConnection.class);
        try {
          while (running.get()) {
            var notifications = postgres.getNotifications(25);
            if (notifications != null)
              for (var notification : notifications) deliver(notification.getParameter());
          }
        } finally {
          try (var statement = connection.createStatement()) {
            statement.execute("UNLISTEN *");
          }
        }
      } catch (Exception e) {
        if (running.get()) {
          report(e);
          try {
            TimeUnit.MILLISECONDS.sleep(100);
          } catch (InterruptedException interrupted) {
            Thread.currentThread().interrupt();
            return;
          }
        }
      } finally {
        listener = null;
      }
    }
  }

  private void deliver(String payload) {
    // An unrelated publisher must not discard the remaining notifications in this batch.
    try {
      notification(Json.parse(payload));
    } catch (RuntimeException error) {
      report(error);
    }
  }

  private void listenSqlite() {
    long after =
        river.transaction(
            c -> {
              try (var statement =
                      Sql.prepare(c, Sql.query(river.database(), "notification_cursor"));
                  var rows = statement.executeQuery()) {
                rows.next();
                return rows.getLong(1);
              }
            });
    while (running.get()) {
      try {
        final long cursor = after;
        var messages =
            river.transaction(
                c -> {
                  var result = new ArrayList<Notification>();
                  try (var statement =
                          Sql.prepare(c, Sql.query(river.database(), "notifications"), cursor);
                      var rows = statement.executeQuery()) {
                    while (rows.next())
                      result.add(new Notification(rows.getLong("id"), rows.getString("payload")));
                  }
                  return result;
                });
        // Release the read snapshot before a control message starts a write transaction.
        for (var message : messages) {
          after = message.id();
          deliver(message.payload());
        }
        TimeUnit.MILLISECONDS.sleep(25);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      } catch (Exception e) {
        report(e);
      }
    }
  }

  private record Notification(long id, String payload) {}

  private void notification(JsonNode value) {
    String action = value.path("action").asString("");
    if (action.equals("cancel")) {
      long jobId = value.path("job_id").asLong();
      pendingCancellation.put(jobId, System.nanoTime());
      var context = active.get(jobId);
      if (context != null) {
        context.requestCancellation(WorkContext.Cancellation.REMOTE);
        pendingCancellation.remove(jobId);
      }
    } else if (action.equals("pause") || action.equals("resume")) {
      String queue = value.path("queue").asString();
      for (String name : queues.keySet())
        if (queue.equals("*") || queue.equals(name)) {
          boolean changed =
              action.equals("pause") ? pausedQueues.add(name) : pausedQueues.remove(name);
          if (changed)
            publish(
                new Event(
                    action.equals("pause") ? EventKind.QUEUE_PAUSED : EventKind.QUEUE_RESUMED,
                    null,
                    river.queues().get(name)));
        }
    } else if (action.equals("request_resign") && isLeader()) resign();
    wake.release();
  }

  private void elect() {
    while (running.get()) {
      try {
        maintain();
      } catch (Exception error) {
        report(error);
      }
      try {
        stopping.await(config.serviceInterval.toNanos(), TimeUnit.NANOSECONDS);
      } catch (InterruptedException error) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private synchronized void maintain() {
    if (System.nanoTime() < leaderEligibleAt) return;
    Instant now = Instant.now();
    boolean previouslyLeader = leaderElectedAt != null;
    boolean elected =
        river.transaction(
            c -> {
              try (var statement =
                  Sql.prepare(
                      c,
                      Sql.query(river.database(), "leader_expire"),
                      river.database().timestamp(now))) {
                statement.executeUpdate();
              }
              if (leaderElectedAt != null) {
                try (var statement =
                    Sql.prepare(
                        c,
                        Sql.query(river.database(), "leader_renew"),
                        river.database().timestamp(now.plusSeconds(10)),
                        id,
                        river.database().timestamp(leaderElectedAt),
                        river.database().timestamp(now))) {
                  if (statement.executeUpdate() > 0) return true;
                }
                leaderElectedAt = null;
              }
              try (var statement =
                      Sql.prepare(
                          c,
                          Sql.query(river.database(), "leader_elect"),
                          id,
                          river.database().timestamp(now),
                          river.database().timestamp(now.plusSeconds(10)));
                  var rows = statement.executeQuery()) {
                if (!rows.next()) return false;
                leaderElectedAt = Database.instant(rows.getString("elected_at"));
                return true;
              }
            });
    if (!elected) return;
    if (!previouslyLeader) {
      periodicNext.clear();
      river.plugin().leadershipStarted();
      river.extensions().forEach(Extension::periodicStarted);
    }
    Instant term = leaderElectedAt;
    if (maintenanceBusy.compareAndSet(false, true))
      executor.submit(
          () -> {
            try {
              services(now, term);
            } catch (Exception error) {
              report(error);
            } finally {
              maintenanceBusy.set(false);
            }
          });
  }

  private void services(Instant now, Instant term) throws SQLException {
    if (!running.get() || !term.equals(leaderElectedAt)) return;
    for (var periodic : config.periodic) {
      Instant next = periodicNext.get(periodic.id);
      if (next == null)
        next =
            periodic.runOnStart
                ? now
                : periodic
                    .schedule
                    .next(now.atOffset(java.time.ZoneOffset.UTC))
                    .map(java.time.OffsetDateTime::toInstant)
                    .orElse(Instant.MAX);
      if (!next.isAfter(now)) {
        periodic.insert(river);
        next =
            periodic
                .schedule
                .next(now.atOffset(java.time.ZoneOffset.UTC))
                .map(java.time.OffsetDateTime::toInstant)
                .orElse(Instant.MAX);
      }
      periodicNext.put(periodic.id, next);
    }
    schedule(now);
    river.plugin().maintain(river, now);
    rescue(now);
    if (!running.get() || !term.equals(leaderElectedAt)) return;
    clean(now);
    if (!now.isBefore(nextReindex) && running.get() && term.equals(leaderElectedAt)) {
      nextReindex =
          config.reindexInterval == null
              ? now.atOffset(java.time.ZoneOffset.UTC)
                  .toLocalDate()
                  .plusDays(1)
                  .atStartOfDay()
                  .toInstant(java.time.ZoneOffset.UTC)
              : now.plus(config.reindexInterval);
      reindex();
    }
  }

  private void clean(Instant now) {
    if (river
        .plugin()
        .clean(
            river,
            now,
            config.cancelledRetention,
            config.completedRetention,
            config.discardedRetention)) return;
    river.transaction(
        c -> {
          Object cancelled =
              config.cancelledRetention.isNegative()
                  ? null
                  : river.database().timestamp(now.minus(config.cancelledRetention));
          Object completed =
              config.completedRetention.isNegative()
                  ? null
                  : river.database().timestamp(now.minus(config.completedRetention));
          Object discarded =
              config.discardedRetention.isNegative()
                  ? null
                  : river.database().timestamp(now.minus(config.discardedRetention));
          try (var statement =
              Sql.prepare(
                  c, Sql.query(river.database(), "clean_jobs"), cancelled, completed, discarded)) {
            statement.executeUpdate();
          }
          try (var statement =
              Sql.prepare(
                  c,
                  Sql.query(river.database(), "clean_queues"),
                  river.database().timestamp(now.minus(Duration.ofDays(1))))) {
            statement.executeUpdate();
          }
          return null;
        });
  }

  private void reindex() throws SQLException {
    if (river.database().dialect() != Database.Dialect.POSTGRES) return;
    try (var c = river.database().connection()) {
      for (String index : config.reindexNames) {
        String qualified;
        try (var statement =
                Sql.prepare(c, Sql.query(river.database(), "reindex_candidate"), index, index);
            var rows = statement.executeQuery()) {
          if (!rows.next()) continue;
          qualified = rows.getString(1);
        }
        try (var statement = c.createStatement()) {
          statement.setQueryTimeout(300);
          statement.execute(Sql.query(river.database(), "reindex").replace("{index}", qualified));
        }
      }
    }
  }

  private void schedule(Instant now) {
    river.transaction(
        c -> {
          if (river.database().dialect() == Database.Dialect.SQLITE)
            try (var statement = Sql.prepare(c, Sql.query(river.database(), "writer_lock"))) {
              statement.executeUpdate();
            }
          var due = new ArrayList<Scheduled>();
          try (var statement =
                  Sql.prepare(
                      c,
                      Sql.query(river.database(), "schedule_select"),
                      river.database().timestamp(now));
              var rows = statement.executeQuery()) {
            while (rows.next())
              due.add(new Scheduled(rows.getLong("id"), rows.getBytes("unique_key")));
          }
          for (var job : due) {
            boolean collision = false;
            if (job.key() != null)
              try (var statement =
                      Sql.prepare(
                          c,
                          Sql.query(river.database(), "schedule_collision"),
                          job.id(),
                          job.key());
                  var rows = statement.executeQuery()) {
                collision = rows.next();
              }
            var savepoint = c.setSavepoint();
            try {
              if (!collision)
                try (var statement =
                        Sql.prepare(
                            c, Sql.query(river.database(), "schedule_available"), job.id());
                    var rows = statement.executeQuery()) {
                  if (rows.next()) river.notifyInsert(c, rows.getString(1));
                }
            } catch (SQLException error) {
              if (!"23505".equals(error.getSQLState())) throw error;
              c.rollback(savepoint);
              collision = true;
            } finally {
              c.releaseSavepoint(savepoint);
            }
            if (collision)
              try (var statement =
                  Sql.prepare(
                      c,
                      Sql.query(river.database(), "schedule_discard"),
                      river.database().timestamp(now),
                      job.id())) {
                statement.executeUpdate();
              }
          }
          return null;
        });
  }

  private record Scheduled(long id, byte[] key) {}

  private void rescue(Instant now) {
    if (river
        .plugin()
        .rescue(
            river,
            new Plugin.Rescue(
                now,
                now.minus(config.rescueAfter),
                java.util.Set.copyOf(active.keySet()),
                java.util.Set.copyOf(config.handlers.keySet()),
                config.jobTimeout.isNegative(),
                config.retryPolicy))) return;
    long after = 0;
    while (running.get()) {
      final long cursor = after;
      var batch =
          river.transaction(
              c -> {
                var stuck = new ArrayList<Job<JsonNode>>();
                try (var statement =
                        Sql.prepare(
                            c,
                            Sql.query(river.database(), "rescue_select"),
                            river.database().timestamp(now.minus(config.rescueAfter)),
                            cursor);
                    var rows = statement.executeQuery()) {
                  while (rows.next()) stuck.add(river.read(rows));
                }
                for (var job : stuck) {
                  if (active.containsKey(job.id())
                      || (config.jobTimeout.isNegative()
                          && config.handlers.containsKey(job.kind()))) continue;
                  boolean cancel = false;
                  if (job.metadata().path("cancel_attempted_at").isString())
                    try {
                      cancel =
                          !Instant.parse(job.metadata().path("cancel_attempted_at").asString())
                              .equals(Instant.parse("0001-01-01T00:00:00Z"));
                    } catch (RuntimeException ignored) {
                    }
                  Job.State state =
                      cancel
                          ? Job.State.CANCELLED
                          : !config.handlers.containsKey(job.kind())
                                  || job.attempt() >= job.maxAttempts()
                              ? Job.State.DISCARDED
                              : Job.State.RETRYABLE;
                  Instant scheduled =
                      state.isFinalized()
                          ? job.scheduledAt()
                          : now.plus(config.retryPolicy.delay(job));
                  String error =
                      Json.encode(
                          new Job.AttemptError(
                              now, job.attempt(), "Stuck job rescued by JobRescuer", ""));
                  String metadata =
                      Json.encode(
                          Map.of(
                              "river:rescue_count",
                              job.metadata().path("river:rescue_count").asLong(0) + 1));
                  try (var statement =
                      Sql.prepare(
                          c,
                          Sql.query(river.database(), "rescue_update"),
                          state.value(),
                          river.database().timestamp(state.isFinalized() ? now : null),
                          river.database().timestamp(scheduled),
                          job.attempt(),
                          metadata,
                          error,
                          error,
                          job.id(),
                          job.attempt(),
                          river.database().timestamp(job.attemptedAt()))) {
                    statement.execute();
                  }
                }
                return stuck;
              });
      if (batch.size() < 1000) break;
      after = batch.getLast().id();
    }
  }

  private void supervise() {
    while (running.get() || dispatcherStopped.getCount() != 0 || !active.isEmpty()) {
      for (var context : active.values())
        if (context.forceIfStuck(config.stuckThreshold)) {
          for (var observer : config.observers) observer.accept("stuck_job");
          var thread = activeThreads.get(context.job().id());
          if (thread != null) thread.interrupt();
        }
      try {
        TimeUnit.MILLISECONDS.sleep(10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private void publish(Event event) {
    for (var subscriber : subscribers)
      try {
        subscriber.accept(event);
      } catch (Exception e) {
        report(e);
      }
  }

  private void report(Throwable error) {
    config.errorHandler.accept(error);
  }

  private synchronized void resign() {
    Instant elected = leaderElectedAt;
    leaderElectedAt = null;
    leaderEligibleAt = System.nanoTime() + config.serviceInterval.toNanos();
    if (elected == null) return;
    river.transaction(
        c -> {
          try (var statement =
              Sql.prepare(
                  c,
                  Sql.query(river.database(), "leader_resign"),
                  id,
                  river.database().timestamp(elected))) {
            if (statement.executeUpdate() > 0
                && river.database().dialect() == Database.Dialect.POSTGRES)
              river.notify(
                  c,
                  "river_leadership",
                  Json.encode(Map.of("action", "resigned", "leader_id", id)));
          }
          return null;
        });
  }

  private static void validateQueue(String name, int workers) {
    if (workers < 1) throw new IllegalArgumentException("maxWorkers must be positive");
    Client.validate(InsertOptions.builder().queue(name).build().resolve(InsertOptions.defaults()));
  }

  /** Runtime configuration; no worker-specific implementation classes are needed. */
  public static final class Builder {
    private Duration cancelledRetention = Duration.ofDays(1);
    private Duration completedRetention = Duration.ofDays(1);
    private Duration discardedRetention = Duration.ofDays(7);
    private Duration reindexInterval;
    private List<String> reindexNames =
        List.of(
            "river_job_args_index",
            "river_job_kind",
            "river_job_metadata_index",
            "river_job_pkey",
            "river_job_prioritized_fetching_index",
            "river_job_state_and_finalized_at_index",
            "river_job_unique_idx");
    private boolean cancelOnError;
    private boolean fetchOnlyKnownKinds;
    private Consumer<Throwable> errorHandler =
        error ->
            System.getLogger("com.riverqueue")
                .log(System.Logger.Level.WARNING, "River runtime failure", error);
    private final Map<String, Registration<?>> handlers = new java.util.LinkedHashMap<>();
    private String id = "java-" + UUID.randomUUID();
    private Duration jobTimeout = Duration.ofMinutes(1);
    private boolean leadership = true;
    private Duration maintenanceInterval = Duration.ofSeconds(5);
    private final List<Consumer<String>> observers = new ArrayList<>();
    private final List<Periodic<?>> periodic = new ArrayList<>();
    private Duration pollInterval = Duration.ofSeconds(1);
    private boolean pollOnly;
    private final Map<String, Integer> queues = new java.util.LinkedHashMap<>();
    private RetryPolicy retryPolicy = RetryPolicy.defaults();
    private Duration rescueAfter = Duration.ofHours(1);
    private final Client river;
    private Duration stopTimeout = Duration.ofSeconds(30);
    private Duration serviceInterval = Duration.ofSeconds(1);
    private Duration stuckThreshold = Duration.ofSeconds(5);

    Builder(Client river) {
      this.river = river;
    }

    private Builder(Builder source) {
      this(source.river);
      cancelledRetention = source.cancelledRetention;
      completedRetention = source.completedRetention;
      discardedRetention = source.discardedRetention;
      reindexInterval = source.reindexInterval;
      reindexNames = source.reindexNames;
      cancelOnError = source.cancelOnError;
      fetchOnlyKnownKinds = source.fetchOnlyKnownKinds;
      errorHandler = source.errorHandler;
      handlers.putAll(source.handlers);
      id = source.id;
      jobTimeout = source.jobTimeout;
      leadership = source.leadership;
      maintenanceInterval = source.maintenanceInterval;
      observers.addAll(source.observers);
      periodic.addAll(source.periodic);
      pollInterval = source.pollInterval;
      pollOnly = source.pollOnly;
      queues.putAll(source.queues);
      retryPolicy = source.retryPolicy;
      rescueAfter = source.rescueAfter;
      stopTimeout = source.stopTimeout;
      serviceInterval = source.serviceInterval;
      stuckThreshold = source.stuckThreshold;
    }

    public Builder retention(Duration cancelled, Duration completed, Duration discarded) {
      cancelledRetention = Objects.requireNonNull(cancelled, "cancelled");
      completedRetention = Objects.requireNonNull(completed, "completed");
      discardedRetention = Objects.requireNonNull(discarded, "discarded");
      return this;
    }

    public Builder reindex(List<String> names, Duration interval) {
      reindexNames = List.copyOf(names);
      reindexInterval = interval == null ? null : positive(interval, "reindex interval");
      return this;
    }

    public Builder cancelOnError(boolean value) {
      cancelOnError = value;
      return this;
    }

    public Builder fetchOnlyKnownKinds(boolean value) {
      fetchOnlyKnownKinds = value;
      return this;
    }

    public Builder errorHandler(Consumer<Throwable> value) {
      errorHandler = Objects.requireNonNull(value, "errorHandler");
      return this;
    }

    public <A> Builder handle(JobType<A> type, Handler<A> handler) {
      Objects.requireNonNull(handler, "handler");
      if (handlers.putIfAbsent(type.kind(), new Registration<>(type, handler)) != null)
        throw new IllegalArgumentException("Duplicate worker kind");
      return this;
    }

    public Builder id(String value) {
      if (value.isEmpty() || value.length() > 127)
        throw new IllegalArgumentException("Invalid client ID");
      id = value;
      return this;
    }

    public Builder jobTimeout(Duration value) {
      Objects.requireNonNull(value, "jobTimeout");
      if (value.isZero())
        throw new IllegalArgumentException(
            "Job timeout must be positive or negative to disable it");
      jobTimeout = value;
      return this;
    }

    public Builder leadership(boolean value) {
      leadership = value;
      return this;
    }

    public Builder maintenanceInterval(Duration value) {
      maintenanceInterval = positive(value, "maintenanceInterval");
      return this;
    }

    public Builder observe(Consumer<String> value) {
      observers.add(Objects.requireNonNull(value, "observer"));
      return this;
    }

    public <A> Builder periodic(
        String id,
        Schedule schedule,
        JobType<A> type,
        A args,
        InsertOptions options,
        boolean runOnStart) {
      if (id == null || id.isBlank() || id.length() > 127)
        throw new IllegalArgumentException("Invalid periodic job ID");
      if (periodic.stream().anyMatch(job -> job.id().equals(id)))
        throw new IllegalArgumentException("Duplicate periodic job ID: " + id);
      periodic.add(
          new Periodic<>(
              id,
              Objects.requireNonNull(schedule),
              Objects.requireNonNull(type),
              Objects.requireNonNull(args),
              Objects.requireNonNull(options),
              runOnStart));
      return this;
    }

    public Builder pollInterval(Duration value) {
      pollInterval = positive(value, "pollInterval");
      return this;
    }

    public Builder pollOnly(boolean value) {
      pollOnly = value;
      return this;
    }

    public Builder queue(String name, int maxWorkers) {
      validateQueue(name, maxWorkers);
      queues.put(name, maxWorkers);
      return this;
    }

    public Builder retryPolicy(RetryPolicy value) {
      retryPolicy = Objects.requireNonNull(value, "retryPolicy");
      return this;
    }

    public Builder rescueAfter(Duration value) {
      rescueAfter = positive(value, "rescueAfter");
      return this;
    }

    public Builder serviceInterval(Duration value) {
      serviceInterval = positive(value, "serviceInterval");
      return this;
    }

    public Builder stopTimeout(Duration value) {
      stopTimeout = positive(value, "stopTimeout");
      return this;
    }

    public Builder stuckThreshold(Duration value) {
      stuckThreshold = positive(value, "stuckThreshold");
      return this;
    }

    private static Duration positive(Duration value, String name) {
      Objects.requireNonNull(value, name);
      if (value.isNegative() || value.isZero())
        throw new IllegalArgumentException(name + " must be positive");
      return value;
    }

    public Workers start() {
      if (!leadership && !periodic.isEmpty())
        throw new IllegalArgumentException("Periodic jobs require leader election");
      river
          .plugin()
          .starting(leadership, cancelledRetention, completedRetention, discardedRetention);
      return new Workers(new Builder(this));
    }
  }

  /** A job or queue event. The payload corresponding to the event kind is non-null. */
  public record Event(EventKind kind, Job<JsonNode> job, Queues.Queue queue) {
    public Event(EventKind kind, Job<JsonNode> job) {
      this(kind, job, null);
    }
  }

  /** Event names shared with Go and the conformance protocol. */
  public enum EventKind {
    JOB_CANCELLED,
    JOB_COMPLETED,
    JOB_FAILED,
    JOB_INTERRUPTED,
    JOB_SNOOZED,
    QUEUE_PAUSED,
    QUEUE_RESUMED;

    @com.fasterxml.jackson.annotation.JsonValue
    public String value() {
      return name().toLowerCase(java.util.Locale.ROOT);
    }
  }

  /** An idempotent subscription handle whose close method cannot throw a checked exception. */
  @FunctionalInterface
  public interface Subscription extends AutoCloseable {
    @Override
    void close();
  }

  private record Completion(
      Job<JsonNode> original,
      Object[] params,
      java.util.concurrent.CompletableFuture<Job<JsonNode>> done) {}

  @FunctionalInterface
  public interface Handler<A> {
    void work(WorkContext<A> context) throws Exception;
  }

  private record Registration<A>(JobType<A> type, Handler<A> handler) {}

  private record Periodic<A>(
      String id,
      Schedule schedule,
      JobType<A> type,
      A args,
      InsertOptions options,
      boolean runOnStart) {
    void insert(Client river) {
      var base = options.resolve(type.defaults());
      var metadata = (ObjectNode) base.metadata().deepCopy();
      metadata.put("periodic", true).put("river:periodic_job_id", id);
      river.insert(
          type,
          args,
          new InsertOptions(
              base.maxAttempts(),
              metadata,
              base.pending(),
              base.priority(),
              base.queue(),
              base.scheduledAt(),
              base.tags(),
              base.unique()));
    }
  }
}
