package com.riverqueue;

import java.sql.Connection;
import java.time.Duration;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

/** Per-attempt arguments, cancellation, output, and resumable checkpoints. */
public final class WorkContext<A> {
  private final CancellationState cancellation;
  private ObjectNode checkpointFallback = Json.object();
  private String checkpointStep;
  private boolean checkpointed;
  private String completedStep;
  private String currentStep;
  private final ObjectNode cursors;
  private boolean cursorsChanged;
  private Exception stepFailure;
  private final Job<A> job;
  private final ObjectNode metadata = Json.object();
  private final Client client;
  private Workers runtime;
  final java.util.Map<Long, Peer> peers;
  private ObjectNode previousCursors;
  private String previousStep;
  private final Set<String> steps = new HashSet<>();
  private boolean resumeMatched;
  volatile Duration timeout;

  WorkContext(Client client, Job<A> job) {
    this(client, job, true);
  }

  WorkContext(Client client, Job<A> job, boolean validate) {
    this(client, job, validate, new CancellationState());
  }

  private WorkContext(Client client, Job<A> job, boolean validate, CancellationState cancellation) {
    this.cancellation = cancellation;
    this.client = client;
    this.job = job;
    peers = cancellation.peers;
    JsonNode saved = job.metadata().path(Protocol.METADATA_RESUMABLE_STEP);
    if (validate && !saved.isMissingNode() && !saved.isString())
      throw new IllegalArgumentException("Invalid resumable step metadata");
    resumeMatched = saved.asString("").isEmpty();
    var cursors = job.metadata().path(Protocol.METADATA_RESUMABLE_CURSOR);
    if (validate && cursors.isArray())
      throw new IllegalArgumentException("Invalid resumable cursor metadata");
    this.cursors = cursors.isObject() ? (ObjectNode) cursors.deepCopy() : Json.object();
  }

  public A args() {
    return job.args();
  }

  /**
   * Internal companion-attempt seam used by River Pro's batch worker. Peers use the runtime timeout
   * during decoding, then their worker override. A peer that ignores cancellation beyond the stuck
   * threshold interrupts the thread claiming and working the batch.
   */
  public <T> java.util.List<WorkContext<T>> claimPeers(
      JobType<T> type, Client.Transaction<java.util.List<Client.Decoded>> claim) {
    if (runtime == null) throw new IllegalStateException("No worker runtime");
    return runtime.claimPeers(this, type, claim);
  }

  /** Internal companion-attempt seam used by River Pro's batch worker. */
  public void completePeer(WorkContext<?> peer, Exception failure) {
    if (runtime == null) throw new IllegalStateException("No worker runtime");
    runtime.completePeer(this, peer, failure);
  }

  void attach(Workers runtime, WorkContext<?> parent) {
    this.runtime = runtime;
    if (parent != null && parent.cancellation() != null) requestCancellation(parent.cancellation());
  }

  <T> WorkContext<T> withDecodedJob(Job<T> decoded) {
    // Notifications and supervision may still hold the provisional context during decoding.
    var context = new WorkContext<>(client, decoded, true, cancellation);
    context.runtime = runtime;
    context.timeout = timeout;
    return context;
  }

  record Peer(Job<JsonNode> row, WorkContext<?> context) {}

  /**
   * Waits for the cancellation signal. Call {@link #checkCancelled()} afterward if leaving work
   * unfinished; returning normally from a handler during shutdown reports success.
   */
  public void awaitCancellation() throws InterruptedException {
    cancellation.signal.await();
  }

  /**
   * Waits for cancellation for at most the given duration. As with {@link #awaitCancellation()},
   * the signal alone does not throw; call {@link #checkCancelled()} if leaving work unfinished.
   */
  public boolean awaitCancellation(Duration duration) throws InterruptedException {
    return cancellation.signal.await(duration.toNanos(), java.util.concurrent.TimeUnit.NANOSECONDS);
  }

  public void cancel(String reason) {
    throw new Control(Job.State.CANCELLED, null, reason);
  }

  public Cancellation cancellation() {
    return cancellation.cause.get();
  }

  /**
   * Throws if cancellation was requested. Propagating this interruption from a handler during
   * shutdown requeues the job without consuming an attempt.
   */
  public void checkCancelled() throws InterruptedException {
    if (isCancelled()) throw new InterruptedException("Job cancellation requested");
  }

  void checkRuntimeCancellation() {
    if (isCancelled()) throw new IllegalStateException("Batch attempt cancelled");
  }

  /**
   * Persists the current resumable step in the caller's transaction. Call inside a step, after its
   * application writes. Let transaction failures escape the step so retries use committed progress.
   * The connection must have auto-commit disabled.
   */
  public Job<A> checkpoint(Connection transaction) {
    return persistCheckpoint(transaction, cursors.deepCopy());
  }

  /** Persists the current resumable step and JSON cursor alongside application writes. */
  public Job<A> checkpoint(Connection transaction, Object cursor) {
    var updates = cursors.deepCopy();
    if (currentStep == null)
      throw new IllegalStateException("Checkpoint requires an active resumable step");
    updates.set(currentStep, Json.tree(cursor));
    return persistCheckpoint(transaction, updates);
  }

  synchronized ObjectNode checkpointFallback() {
    return checkpointFallback.deepCopy();
  }

  String checkpointStep() {
    return checkpointStep;
  }

  /** Completes this attempt atomically with application writes in the supplied transaction. */
  public Job<A> complete(Connection transaction) {
    return client.complete(transaction, job, metadataUpdates());
  }

  public void discard(String reason) {
    throw new Control(Job.State.DISCARDED, null, reason);
  }

  public boolean isCancelled() {
    return cancellation.cause.get() != null;
  }

  public Job<A> job() {
    return job;
  }

  public synchronized void metadata(String key, Object value) {
    metadata.set(key, Json.tree(value));
  }

  synchronized ObjectNode metadataUpdates() {
    return metadata.deepCopy();
  }

  synchronized Exception finish(Exception failure) {
    synchronized (cancellation) {
      cancellation.finished = true;
      if (cancellation.forced)
        failure =
            new IllegalStateException("Worker ignored cancellation beyond the stuck threshold");
    }
    if (stepFailure != null) failure = stepFailure;
    if (failure == null && !resumeMatched)
      failure = new IllegalArgumentException("Saved resumable step was not found");
    if (failure != null && (completedStep != null || cursorsChanged))
      metadata.setAll(resumableMetadata(completedStep, cursors));
    if (failure == null || completedStep != null || cursorsChanged)
      checkpointFallback = Json.object();
    return failure;
  }

  public void output(Object value) {
    metadata(Protocol.METADATA_OUTPUT, value);
  }

  private Job<A> persistCheckpoint(Connection transaction, ObjectNode updates) {
    if (currentStep == null)
      throw new IllegalStateException("Checkpoint requires an active resumable step");
    if (job.state() != Job.State.RUNNING) throw new IllegalStateException("Job must be running");
    // Only this step's progress belongs to the caller's transaction. Completion restores the
    // prior buffered progress if the checkpoint rolled back, without replacing a committed one.
    checkpointStep = currentStep;
    checkpointFallback = resumableMetadata(previousStep, previousCursors);
    completedStep = null;
    cursorsChanged = false;
    var updated =
        client.transaction(
            transaction,
            connection -> {
              try (var statement =
                      Sql.prepare(
                          connection,
                          Sql.query(client.database(), "checkpoint"),
                          currentStep,
                          Json.encode(updates.isEmpty() ? null : updates),
                          job.id(),
                          job.attempt(),
                          client.database().timestamp(job.attemptedAt()));
                  var rows = statement.executeQuery()) {
                if (!rows.next())
                  throw new RiverException(
                      RiverException.Code.REJECTED, "Job attempt is no longer running");
                return client.read(rows).mapArgs(ignored -> job.args());
              }
            });
    checkpointed = true;
    cursors.removeAll();
    cursors.setAll(updates);
    return updated;
  }

  private ObjectNode resumableMetadata(String step, ObjectNode cursors) {
    var updates = Json.object();
    if (step != null) updates.put(Protocol.METADATA_RESUMABLE_STEP, step);
    if (!cursors.isEmpty()
        || checkpointed
        || job.metadata().has(Protocol.METADATA_RESUMABLE_CURSOR))
      updates.set(
          Protocol.METADATA_RESUMABLE_CURSOR,
          cursors.isEmpty() ? Json.tree(null) : cursors.deepCopy());
    return updates;
  }

  public Client client() {
    return client;
  }

  void requestCancellation(Cancellation value) {
    synchronized (cancellation) {
      if (!cancellation.finished && cancellation.cause.compareAndSet(null, value)) {
        cancellation.cancelledAt = System.nanoTime();
        cancellation.signal.countDown();
        peers.values().forEach(peer -> peer.context.requestCancellation(value));
      }
    }
  }

  boolean forceIfStuck(Duration threshold) {
    synchronized (cancellation) {
      if (cancellation.finished
          || cancellation.cause.get() == null
          || cancellation.cancelledAt == 0
          || cancellation.forced
          || System.nanoTime() - cancellation.cancelledAt < threshold.toNanos()) return false;
      cancellation.forced = true;
      return true;
    }
  }

  public void snooze(Duration duration) {
    if (duration.isNegative())
      throw new IllegalArgumentException("Snooze duration cannot be negative");
    try {
      duration.toNanos();
    } catch (ArithmeticException error) {
      throw new IllegalArgumentException(
          "Snooze duration must fit in signed 64-bit nanoseconds", error);
    }
    throw new Control(null, duration, "snoozed");
  }

  public void step(String name, Step action) throws Exception {
    executeStep(name, false, ignored -> action.run());
  }

  public void stepWithCursor(String name, CursorStep action) throws Exception {
    executeStep(name, true, action);
  }

  public void cursor(Object value) {
    if (currentStep == null)
      throw new IllegalStateException("Cursor requires an active resumable step");
    cursors.set(currentStep, Json.tree(value));
    cursorsChanged = true;
  }

  private void executeStep(String name, boolean withCursor, CursorStep action) throws Exception {
    if (stepFailure != null) throw stepFailure;
    if (currentStep != null) throw new IllegalStateException("Resumable steps cannot be nested");
    if (name.isEmpty() || !steps.add(name)) {
      stepFailure = new IllegalArgumentException("duplicate resumable step or empty name: " + name);
      throw stepFailure;
    }
    if (!resumeMatched) {
      if (job.metadata().path(Protocol.METADATA_RESUMABLE_STEP).asString().equals(name)) {
        resumeMatched = true;
        completedStep = name;
      }
      if (!resumeMatched || !withCursor || !cursors.has(name)) return;
    }
    previousStep = completedStep;
    previousCursors = cursors.deepCopy();
    currentStep = name;
    try {
      action.run(cursors.path(name));
      completedStep = name;
      cursors.remove(name);
    } catch (Exception failure) {
      stepFailure = failure;
      throw failure;
    } finally {
      currentStep = null;
    }
  }

  public <T> T transaction(Client.Transaction<T> action) {
    return client.transaction(action);
  }

  public enum Cancellation {
    REMOTE,
    SHUTDOWN,
    TIMEOUT
  }

  private static final class CancellationState {
    long cancelledAt;
    final AtomicReference<Cancellation> cause = new AtomicReference<>();
    boolean finished;
    boolean forced;
    final java.util.Map<Long, Peer> peers = new java.util.concurrent.ConcurrentHashMap<>();
    final CountDownLatch signal = new CountDownLatch(1);
  }

  static final class Control extends RuntimeException {
    private static final long serialVersionUID = 1L;
    final Duration delay;
    final Job.State state;

    Control(Job.State state, Duration delay, String reason) {
      super(reason);
      this.state = state;
      this.delay = delay;
    }
  }

  @FunctionalInterface
  public interface Step {
    void run() throws Exception;
  }

  @FunctionalInterface
  public interface CursorStep {
    void run(JsonNode cursor) throws Exception;
  }
}
