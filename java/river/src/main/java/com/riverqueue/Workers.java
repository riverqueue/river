package com.riverqueue;

import java.nio.charset.StandardCharsets;
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
import java.util.concurrent.PriorityBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.Consumer;
import org.postgresql.PGConnection;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

/** A bounded worker runtime using one virtual thread per active job. */
public final class Workers implements AutoCloseable {
  private static final Duration QUEUE_RETENTION = Duration.ofDays(1);

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
  private final RetryPolicy defaultRetryPolicy = RetryPolicy.defaults();
  private final java.util.concurrent.BlockingQueue<Completion> completions =
      new java.util.concurrent.LinkedBlockingQueue<>();
  private final java.util.concurrent.CountDownLatch dispatcherStopped =
      new java.util.concurrent.CountDownLatch(1);
  // Claims may commit after stop's active-map scan; launch must see the cancellation request.
  private volatile boolean cancelOnStop;
  private boolean stopFinalized;
  private final ReentrantLock stopLock = new ReentrantLock();
  private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
  private final PriorityBlockingQueue<FetchWakeup> fetchWakeups = new PriorityBlockingQueue<>();
  private final String id;
  private final Protocol.Dispatcher notifications;
  // Blocking under monitor locks pins virtual threads on Java 21; use locks that allow yielding.
  private final ReentrantLock leadershipLock = new ReentrantLock();
  private volatile LeadershipTerm leaderTerm;
  private Instant initializedTerm;
  private Instant resigningTerm;
  private long leaderEligibleAt = System.nanoTime();
  private volatile Connection listener;
  private final Map<String, Integer> queues = new ConcurrentHashMap<>();
  private final Map<String, Instant> periodicNext = new ConcurrentHashMap<>();
  private final java.util.Set<String> pausedQueues = ConcurrentHashMap.newKeySet();
  private final ReentrantLock queueLock = new ReentrantLock();
  private final Condition queueChanged = queueLock.newCondition();
  private final java.util.Set<String> drainingQueues = ConcurrentHashMap.newKeySet();
  private final Semaphore leadershipWake = new Semaphore(0);
  private final Client river;
  private final Runnable unsubscribeInsert;
  private final AtomicBoolean running = new AtomicBoolean(true);
  private final List<Consumer<Event>> subscribers = new CopyOnWriteArrayList<>();
  private final Semaphore wake = new Semaphore(0);

  private Workers(Builder config) {
    this.config = config;
    river = config.river;
    id = config.id == null ? "java-" + UUID.randomUUID() : config.id;
    notifications =
        new Protocol.Dispatcher(
            id, active, pendingCancellation, this::queueNotification, this::leadershipNotification);
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
    queueLock.lock();
    try {
      if (drainingQueues.contains(name))
        throw new IllegalStateException("Queue is draining: " + name);
      queues.put(name, maxWorkers);
      heartbeat(name);
    } finally {
      queueLock.unlock();
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

  /** Reports whether this runtime still trusts its committed leadership lease. */
  public boolean isLeader() {
    return isLeader(System.nanoTime());
  }

  boolean isLeader(long now) {
    var term = leaderTerm;
    return running.get() && term != null && term.active(now);
  }

  public void removeQueue(String name) {
    queueLock.lock();
    try {
      if (!queues.containsKey(name) || !drainingQueues.add(name))
        throw new RiverException(RiverException.Code.NOT_FOUND, "Queue not configured: " + name);
      try {
        // Keep the producer lease alive until every committed claim has settled.
        long deadline = System.nanoTime() + config.stopTimeout.toNanos();
        while (active.values().stream().anyMatch(c -> c.job().queue().equals(name))) {
          long left = deadline - System.nanoTime();
          if (left <= 0)
            throw new RiverException(RiverException.Code.REJECTED, "Queue did not drain: " + name);
          queueChanged.awaitNanos(left);
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
    } finally {
      queueLock.unlock();
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
      leadershipWake.release();
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
    stopLock.lock();
    try {
      if (!stopFinalized) {
        RuntimeException failure = null;
        try {
          queues.keySet().forEach(this::heartbeat);
        } catch (RuntimeException error) {
          failure = error;
        }
        try {
          resign();
        } catch (RuntimeException error) {
          if (failure == null) failure = error;
          else failure.addSuppressed(error);
        }
        if (failure != null) throw failure;
        stopFinalized = true;
      }
    } finally {
      stopLock.unlock();
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
        if (wakeDueFetches()) notified = true;
        if (notified || now >= nextPoll) {
          queueLock.lock();
          try {
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
          } finally {
            queueLock.unlock();
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
        // A failed claim must not consume an insert notification or a timed retry.
        nextPoll = 0;
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

  private record FetchWakeup(Instant scheduledAt, String notifyQueue)
      implements Comparable<FetchWakeup> {
    @Override
    public int compareTo(FetchWakeup other) {
      return scheduledAt.compareTo(other.scheduledAt);
    }
  }

  private boolean wakeDueFetches() {
    Instant now = Instant.now();
    var ready = new ArrayList<FetchWakeup>();
    FetchWakeup due;
    while ((due = fetchWakeups.peek()) != null && !due.scheduledAt().isAfter(now))
      ready.add(fetchWakeups.poll());
    var notifyQueues = new java.util.HashSet<String>();
    for (var wakeup : ready)
      if (wakeup.notifyQueue() != null) notifyQueues.add(wakeup.notifyQueue());
    if (!notifyQueues.isEmpty()) {
      try {
        // Rescued jobs may belong to queues consumed only by peers. Notify once due so a peer
        // doesn't fetch too early and then wait for its next periodic poll.
        river.transaction(
            connection -> {
              for (String queue : notifyQueues) river.notifyInsert(connection, queue);
              return null;
            });
      } catch (RuntimeException error) {
        fetchWakeups.addAll(ready);
        throw error;
      }
    }
    return !ready.isEmpty();
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
    for (var decoded : jobs) launch(decoded, config.handlers.get(decoded.job().kind()));
  }

  private <A> void launch(Client.Decoded decoded, Registration<A> registration) {
    var row = decoded.job();
    WorkContext<A> context;
    try {
      if (decoded.failure() != null) throw decoded.failure();
      if (registration == null)
        throw new IllegalArgumentException(
            "job kind is not registered in the client's Workers bundle: " + row.kind());
      context = new WorkContext<>(river, river.typed(row, registration.type));
    } catch (Exception e) {
      // Invalid jobs own committed claims too: retain their slots and retry acknowledgement.
      launch(
          row,
          new WorkContext<>(river, row, false),
          () -> {
            throw e;
          });
      return;
    }
    launch(
        row,
        context,
        () -> {
          for (var extension : river.extensions()) extension.afterClaim(row);
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
        });
  }

  private void launch(Job<JsonNode> row, WorkContext<?> context, WorkContext.Step work) {
    context.attach(this, null);
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
            observe("work_begin");
            work.run();
          } catch (Exception e) {
            failure = e;
          } catch (Throwable e) {
            failure = new RuntimeException("Worker threw " + e, e);
          } finally {
            observe("work_end");
          }
          // Interruption belongs to the handler, not to the durable acknowledgement that follows.
          // Stop supervising its thread while retaining the slot until completion is committed.
          failure = context.finish(failure);
          activeThreads.remove(row.id());
          Thread.interrupted();
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
                // A cancellation already in flight must not abandon this claimed job.
              }
            }
          }
          queueLock.lock();
          try {
            active.remove(row.id(), context);
            queueChanged.signalAll();
          } finally {
            queueLock.unlock();
          }
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
      parent.peers.put(decoded.job().id(), new WorkContext.Peer(decoded.job(), context));
      active.put(decoded.job().id(), context);
      // Publish before checking the parent so a concurrent cancellation cannot miss both.
      context.attach(this, parent);
    }
    var contexts = new ArrayList<WorkContext<A>>();
    for (var decoded : rows) {
      if (decoded.failure() != null) throw decoded.failure();
      var provisional = parent.peers.get(decoded.job().id()).context();
      var context = provisional.withDecodedJob(river.typed(decoded.job(), type));
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
    queueLock.lock();
    try {
      active.remove(context.job().id(), context);
      queueChanged.signalAll();
    } finally {
      queueLock.unlock();
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
    if (failure != null
        && context != null
        && context.cancellation() == WorkContext.Cancellation.REMOTE) {
      state = Job.State.CANCELLED;
      error = "JobCancelError: job cancelled remotely";
      event = EventKind.JOB_CANCELLED;
    } else if (failure instanceof WorkContext.Control control) {
      if (control.delay != null) {
        scheduled = now.plus(control.delay);
        state = bypassScheduler(control.delay) ? Job.State.AVAILABLE : Job.State.SCHEDULED;
        attempt--;
        updates.put("snoozes", nextSnoozeCount(original.metadata()));
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
    } else if (failure != null) {
      error = failure.getMessage() == null ? failure.toString() : failure.getMessage();
      boolean cancelled = config.cancelOnError;
      state =
          cancelled
              ? Job.State.CANCELLED
              : original.attempt() >= original.maxAttempts()
                  ? Job.State.DISCARDED
                  : Job.State.RETRYABLE;
      if (state == Job.State.RETRYABLE) {
        Duration delay = retryDelay(original, now);
        scheduled = now.plus(delay);
        if (bypassScheduler(delay)) state = Job.State.AVAILABLE;
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
    // Short retries and snoozes bypass the scheduler. The immediate completion wake-up may
    // precede scheduled_at, so also wake fetching when the persisted timestamp becomes due.
    if (updated.state() == Job.State.AVAILABLE)
      fetchWakeups.add(new FetchWakeup(updated.scheduledAt(), null));
    // Another process may have changed the attempt. Match Go's events to the persisted state.
    event =
        switch (updated.state()) {
          case AVAILABLE ->
              switch (event) {
                case JOB_INTERRUPTED, JOB_SNOOZED -> event;
                default -> EventKind.JOB_FAILED;
              };
          case CANCELLED -> EventKind.JOB_CANCELLED;
          case COMPLETED -> EventKind.JOB_COMPLETED;
          case DISCARDED, RETRYABLE -> EventKind.JOB_FAILED;
          case SCHEDULED -> EventKind.JOB_SNOOZED;
          case PENDING, RUNNING -> null;
        };
    if (event != null) publish(new Event(event, updated));
  }

  private boolean bypassScheduler(Duration delay) {
    // Maintenance runs from election ticks. Longer service intervals must not strand short
    // delays in states that only the scheduler can make available.
    return delay.compareTo(config.maintenanceInterval) <= 0
        || delay.compareTo(config.serviceInterval) <= 0;
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
        for (String topic :
            List.of(Protocol.TOPIC_INSERT, Protocol.TOPIC_CONTROL, Protocol.TOPIC_LEADERSHIP))
          try (var statement = connection.createStatement()) {
            statement.execute("LISTEN \"" + schema.replace("\"", "\"\"") + "." + topic + "\"");
          }
        wake.release();
        observe("listen_ready");
        var postgres = connection.unwrap(PGConnection.class);
        try {
          while (running.get()) {
            var notifications = postgres.getNotifications(25);
            if (notifications != null)
              for (var notification : notifications)
                if (notification.getName().startsWith(schema + "."))
                  deliver(
                      notification.getName().substring(schema.length() + 1),
                      notification.getParameter());
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

  private void deliver(String topic, String payload) {
    // An unrelated publisher must not discard the remaining notifications in this batch.
    try {
      notifications.dispatch(topic, payload);
    } catch (RuntimeException error) {
      report(error);
    }
  }

  private void listenSqlite() {
    long after = 0;
    boolean initialized = false;
    while (running.get()) {
      try {
        if (!initialized) {
          after =
              river.transaction(
                  c -> {
                    try (var statement =
                            Sql.prepare(c, Sql.query(river.database(), "notification_cursor"));
                        var rows = statement.executeQuery()) {
                      rows.next();
                      return rows.getLong(1);
                    }
                  });
          initialized = true;
          // The initial cursor skips earlier notifications, including inserts since the first
          // fetch. Poll again once listening starts, as the PostgreSQL listener does.
          wake.release();
          observe("listen_ready");
        }
        final long cursor = after;
        var messages =
            river.transaction(
                c -> {
                  var result = new ArrayList<Notification>();
                  try (var statement =
                          Sql.prepare(c, Sql.query(river.database(), "notifications"), cursor);
                      var rows = statement.executeQuery()) {
                    while (rows.next())
                      result.add(
                          new Notification(
                              rows.getLong("id"),
                              rows.getString("topic"),
                              rows.getString("payload")));
                  }
                  return result;
                });
        // Release the read snapshot before a control message starts a write transaction.
        for (var message : messages) {
          after = message.id();
          deliver(message.topic(), message.payload());
        }
      } catch (Exception e) {
        report(e);
      }
      try {
        TimeUnit.MILLISECONDS.sleep(25);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private record Notification(long id, String topic, String payload) {}

  private void leadershipNotification(Protocol.LeadershipNotice notice) {
    if (notice == Protocol.LeadershipNotice.REQUEST_RESIGN) {
      try {
        if (isLeader()) resign();
      } finally {
        leadershipWake.release();
      }
    } else leadershipWake.release();
  }

  private void queueNotification(Protocol.QueueNotice notice) {
    String action = notice.action();
    if (action.equals("pause") || action.equals("resume")) {
      String queue = notice.queue();
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
    }
    wake.release();
  }

  private void elect() {
    while (running.get()) {
      leadershipWake.drainPermits();
      long delay = config.serviceInterval.toNanos();
      try {
        maintain();
        observe("leadership_check");
      } catch (Exception error) {
        report(error);
        delay = Math.min(delay, TimeUnit.SECONDS.toNanos(1));
      }
      if (!running.get()) return;
      try {
        leadershipWake.tryAcquire(delay, TimeUnit.NANOSECONDS);
      } catch (InterruptedException error) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private void maintain() {
    leadershipLock.lock();
    try {
      if (resigningTerm != null) resign();
      if (System.nanoTime() - leaderEligibleAt < 0) return;
      long attemptStarted = System.nanoTime();
      Instant now = Instant.now();
      Instant expiresAt = now.plus(config.serviceInterval).plusSeconds(10);
      var previousTerm = leaderTerm;
      if (previousTerm != null && !previousTerm.active(attemptStarted)) {
        resign();
        return;
      }
      Instant elected =
          river.transaction(
              c -> {
                try (var statement =
                    Sql.prepare(
                        c,
                        Sql.query(river.database(), "leader_expire"),
                        river.database().timestamp(now))) {
                  statement.executeUpdate();
                }
                if (previousTerm != null) {
                  try (var statement =
                      Sql.prepare(
                          c,
                          Sql.query(river.database(), "leader_renew"),
                          river.database().timestamp(expiresAt),
                          id,
                          river.database().timestamp(previousTerm.electedAt()),
                          river.database().timestamp(now))) {
                    if (statement.executeUpdate() > 0) return previousTerm.electedAt();
                  }
                }
                try (var statement =
                        Sql.prepare(
                            c,
                            Sql.query(river.database(), "leader_elect"),
                            id,
                            river.database().timestamp(now),
                            river.database().timestamp(expiresAt));
                    var rows = statement.executeQuery()) {
                  return rows.next() ? Database.instant(rows.getString("elected_at")) : null;
                }
              });
      // A lease is ours only after commit, including when regaining an expired term.
      // Like Go, stop trusting it one second before its database expiry, measuring elapsed
      // time from before the transaction so pool waits and slow commits consume the lease.
      leaderTerm =
          elected == null
              ? null
              : new LeadershipTerm(
                  elected, attemptStarted + config.serviceInterval.plusSeconds(9).toNanos());
      if (elected == null) return;
      long committedAt = System.nanoTime();
      if (!leaderTerm.active(committedAt)
          || (previousTerm != null && !previousTerm.active(committedAt))) {
        resign();
        return;
      }
      if (!running.get()) return;
      if (!elected.equals(initializedTerm)) {
        periodicNext.clear();
        river.plugin().leadershipStarted();
        river.extensions().forEach(Extension::periodicStarted);
        initializedTerm = elected;
      }
      Instant term = elected;
      if (running.get() && maintenanceBusy.compareAndSet(false, true))
        try {
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
        } catch (java.util.concurrent.RejectedExecutionException error) {
          maintenanceBusy.set(false);
          // Stop can close the executor after the running check while election is finishing.
          if (running.get()) throw error;
        }
    } finally {
      leadershipLock.unlock();
    }
  }

  private void services(Instant now, Instant term) throws SQLException {
    for (var periodic : config.periodic) {
      if (!ownsTerm(term)) return;
      try {
        Instant next = periodicNext.get(periodic.id);
        if (next == null)
          next =
              periodic.runOnStart
                  ? now
                  : periodic
                      .schedule
                      .next(now.atZone(java.time.ZoneId.systemDefault()))
                      .map(java.time.ZonedDateTime::toInstant)
                      .orElse(Instant.MAX);
        if (!next.isAfter(now)) {
          Instant following =
              periodic
                  .schedule
                  .next(next.atZone(java.time.ZoneId.systemDefault()))
                  .map(java.time.ZonedDateTime::toInstant)
                  .orElse(Instant.MAX);
          if (!ownsTerm(term)) return;
          periodic.insert(river, next);
          next = following;
        }
        if (!ownsTerm(term)) return;
        periodicNext.put(periodic.id, next);
      } catch (Exception error) {
        // One failing schedule or insertion must not block other jobs or leader maintenance.
        report(error);
      }
    }
    if (!ownsTerm(term)) return;
    schedule(now);
    if (!ownsTerm(term)) return;
    river.plugin().maintain(river, now);
    if (!ownsTerm(term)) return;
    rescue(now, term);
    if (!ownsTerm(term)) return;
    cleanNotifications(now);
    if (!ownsTerm(term)) return;
    clean(now);
    if (!now.isBefore(nextReindex) && ownsTerm(term)) {
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

  private boolean ownsTerm(Instant term) {
    var current = leaderTerm;
    return running.get()
        && current != null
        && term.equals(current.electedAt())
        && current.active(System.nanoTime());
  }

  private record LeadershipTerm(Instant electedAt, long trustedUntil) {
    boolean active(long now) {
      return trustedUntil - now > 0;
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
                  river.database().timestamp(now.minus(QUEUE_RETENTION)))) {
            statement.executeUpdate();
          }
          return null;
        });
  }

  private void cleanNotifications(Instant now) {
    if (river.database().dialect() != Database.Dialect.SQLITE) return;
    river.transaction(
        c -> {
          try (var statement =
              Sql.prepare(
                  c,
                  Sql.query(river.database(), "clean_notifications"),
                  river.database().timestamp(now.minus(Duration.ofMinutes(5))))) {
            return statement.executeUpdate();
          }
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

  private void rescue(Instant now, Instant term) {
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
    while (ownsTerm(term)) {
      final long cursor = after;
      var ready = new ArrayList<FetchWakeup>();
      var batch =
          river.transaction(
              c -> {
                // Reserve the SQLite writer before reading: another commit would make this
                // snapshot impossible to upgrade when persisting the rescue.
                if (river.database().dialect() == Database.Dialect.SQLITE)
                  try (var statement = Sql.prepare(c, Sql.query(river.database(), "writer_lock"))) {
                    statement.executeUpdate();
                  }
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
                  Instant scheduled = job.scheduledAt();
                  if (!state.isFinalized()) {
                    Duration delay = retryDelay(job, now);
                    scheduled = now.plus(delay);
                    if (bypassScheduler(delay)) state = Job.State.AVAILABLE;
                  }
                  String error =
                      Json.encode(
                          new Job.AttemptError(
                              now, job.attempt(), "Stuck job rescued by JobRescuer", ""));
                  String metadata =
                      Json.encode(
                          Map.of(
                              Protocol.METADATA_RESCUE_COUNT,
                              job.metadata().path(Protocol.METADATA_RESCUE_COUNT).asLong(0) + 1));
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
                              river.database().timestamp(job.attemptedAt()));
                      var rows = statement.executeQuery()) {
                    if (rows.next() && state == Job.State.AVAILABLE)
                      ready.add(
                          new FetchWakeup(
                              Database.instant(rows.getString("scheduled_at")), job.queue()));
                  }
                }
                return stuck;
              });
      // Only committed rescues may wake fetching; use the database's timestamp precision.
      fetchWakeups.addAll(ready);
      if (!ready.isEmpty()) wake.release();
      if (batch.size() < 1000) break;
      after = batch.getLast().id();
    }
  }

  private void supervise() {
    while (running.get() || dispatcherStopped.getCount() != 0 || !active.isEmpty()) {
      for (var context : active.values()) {
        // Attempts retain their deadlines while fetching is blocked or gracefully stopping.
        if (!config.jobTimeout.isNegative()
            && Duration.between(context.job().attemptedAt(), Instant.now())
                    .compareTo(config.jobTimeout)
                >= 0) context.requestCancellation(WorkContext.Cancellation.TIMEOUT);
        if (context.forceIfStuck(config.stuckThreshold)) {
          observe("stuck_job");
          var thread = activeThreads.get(context.job().id());
          if (thread != null) thread.interrupt();
        }
      }
      try {
        TimeUnit.MILLISECONDS.sleep(10);
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        return;
      }
    }
  }

  private void observe(String event) {
    for (var observer : config.observers)
      try {
        observer.accept(event);
      } catch (Throwable error) {
        report(error);
      }
  }

  private void publish(Event event) {
    for (var subscriber : subscribers)
      try {
        subscriber.accept(event);
      } catch (Throwable e) {
        report(e);
      }
  }

  private void report(Throwable error) {
    try {
      config.errorHandler.accept(error);
    } catch (Throwable handlerFailure) {
      // Reporting failures must not strand claimed jobs or terminate a background service.
      var logger = System.getLogger("com.riverqueue");
      logger.log(System.Logger.Level.WARNING, "River runtime failure", error);
      logger.log(System.Logger.Level.WARNING, "River error handler failed", handlerFailure);
    }
  }

  private Duration retryDelay(Job<?> job, Instant now) {
    try {
      var delay = Objects.requireNonNull(config.retryPolicy.delay(job), "Retry delay");
      if (delay.isNegative())
        throw new IllegalArgumentException("Retry delay must not be negative");
      delay.toNanos();
      now.plus(delay);
      return delay;
    } catch (Throwable error) {
      report(new IllegalArgumentException("Retry policy failed; using the default delay", error));
      return defaultRetryPolicy.delay(job);
    }
  }

  private void resign() {
    leadershipLock.lock();
    try {
      if (leaderTerm != null) {
        resigningTerm = leaderTerm.electedAt();
        leaderTerm = null;
        initializedTerm = null;
        leaderEligibleAt = System.nanoTime() + config.serviceInterval.toNanos();
      }
      // Stop local services immediately, but retain the identity until deletion commits so a
      // failed resignation can be retried without waiting for the lease to expire.
      Instant elected = resigningTerm;
      if (elected == null) return;
      river.transaction(
          c -> {
            try (var statement =
                Sql.prepare(
                    c,
                    Sql.query(river.database(), "leader_resign"),
                    id,
                    river.database().timestamp(elected))) {
              if (statement.executeUpdate() > 0) river.notify(c, Protocol.resigned(id));
            }
            return null;
          });
      resigningTerm = null;
    } finally {
      leadershipLock.unlock();
    }
  }

  static long nextSnoozeCount(JsonNode metadata) {
    var value = metadata.path("snoozes");
    // Go's gjson treats true as one; Jackson's numeric coercion does not.
    return (value.isBoolean() ? (value.asBoolean() ? 1 : 0) : value.asLong(0)) + 1;
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
    private String id;
    private Duration jobTimeout = Duration.ofMinutes(1);
    private boolean jobTimeoutConfigured;
    private boolean leadership = true;
    private Duration maintenanceInterval = Duration.ofSeconds(5);
    private final List<Consumer<String>> observers = new ArrayList<>();
    private final List<Periodic<?>> periodic = new ArrayList<>();
    private Duration pollInterval = Duration.ofSeconds(1);
    private boolean pollOnly;
    private final Map<String, Integer> queues = new java.util.LinkedHashMap<>();
    private RetryPolicy retryPolicy = RetryPolicy.defaults();
    private Duration rescueAfter;
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
      jobTimeoutConfigured = source.jobTimeoutConfigured;
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
      cancelledRetention = duration(cancelled, "cancelled retention");
      completedRetention = duration(completed, "completed retention");
      discardedRetention = duration(discarded, "discarded retention");
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

    /** Reports runtime failures. Failures in this callback are logged without stopping workers. */
    public Builder errorHandler(Consumer<Throwable> value) {
      errorHandler = Objects.requireNonNull(value, "errorHandler");
      return this;
    }

    /** Adds a worker handler for a job type. Each job kind may be added only once. */
    public <A> Builder add(JobType<A> type, Handler<A> handler) {
      Objects.requireNonNull(handler, "handler");
      if (handlers.putIfAbsent(type.kind(), new Registration<>(type, handler)) != null)
        throw new IllegalArgumentException("Duplicate worker kind");
      return this;
    }

    /** Overrides the runtime ID (at most 100 UTF-8 bytes). Each start otherwise generates an ID. */
    public Builder id(String value) {
      if (value.isEmpty() || value.getBytes(StandardCharsets.UTF_8).length > 100)
        throw new IllegalArgumentException("Invalid client ID");
      id = value;
      return this;
    }

    /** Sets the attempt timeout; a negative duration disables it, including during stop. */
    public Builder jobTimeout(Duration value) {
      duration(value, "jobTimeout");
      if (value.isZero())
        throw new IllegalArgumentException(
            "Job timeout must be positive or negative to disable it");
      jobTimeout = value;
      jobTimeoutConfigured = true;
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

    /** Observes runtime activity. Callback failures are reported without affecting execution. */
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
      if (id == null
          || id.getBytes(StandardCharsets.UTF_8).length > 127
          || !Protocol.USER_SPECIFIED_ID_OR_KIND.matcher(id).matches())
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

    /**
     * Sets when abandoned jobs may be rescued. Must not be shorter than the attempt timeout.
     * Defaults to one hour, plus an explicitly configured positive job timeout.
     */
    public Builder rescueAfter(Duration value) {
      rescueAfter = positive(value, "rescueAfter");
      return this;
    }

    /**
     * Sets the election and heartbeat interval; leadership leases include ten seconds of margin.
     * Must be positive and shorter than one day so active queues do not expire between heartbeats.
     */
    public Builder serviceInterval(Duration value) {
      positive(value, "serviceInterval");
      if (value.compareTo(QUEUE_RETENTION) >= 0)
        throw new IllegalArgumentException(
            "serviceInterval must be shorter than the queue retention period of one day");
      serviceInterval = value;
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
      duration(value, name);
      if (value.isNegative() || value.isZero())
        throw new IllegalArgumentException(name + " must be positive");
      return value;
    }

    private static Duration duration(Duration value, String name) {
      Objects.requireNonNull(value, name);
      try {
        value.toNanos();
      } catch (ArithmeticException error) {
        throw new IllegalArgumentException(name + " must fit in signed 64-bit nanoseconds", error);
      }
      return value;
    }

    public Workers start() {
      if (!leadership && !periodic.isEmpty())
        throw new IllegalArgumentException("Periodic jobs require leader election");
      var config = new Builder(this);
      if (config.rescueAfter == null)
        config.rescueAfter =
            positive(
                !jobTimeoutConfigured || jobTimeout.isNegative()
                    ? Duration.ofHours(1)
                    : jobTimeout.plus(Duration.ofHours(1)),
                "rescueAfter");
      if (config.rescueAfter.compareTo(jobTimeout) < 0)
        throw new IllegalArgumentException("rescueAfter must not be less than jobTimeout");
      river
          .plugin()
          .starting(leadership, cancelledRetention, completedRetention, discardedRetention);
      return new Workers(config);
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
    void insert(Client river, Instant scheduledAt) {
      var base = options.resolve(type.defaults());
      var metadata = (ObjectNode) base.metadata().deepCopy();
      metadata.put("periodic", true).put(Protocol.METADATA_PERIODIC_JOB_ID, id);
      var options =
          new InsertOptions(
              base.maxAttempts(),
              metadata,
              base.pending(),
              base.priority(),
              base.queue(),
              base.scheduledAt(),
              base.tags(),
              base.unique());
      // A periodic occurrence supplies the default date without turning an available job into
      // a scheduled one or overriding an explicitly configured date, matching Go's enqueuer.
      river.transaction(c -> river.insert(c, type, args, options, scheduledAt));
    }
  }
}
