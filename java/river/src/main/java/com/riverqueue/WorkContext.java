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
  private final CountDownLatch cancellation = new CountDownLatch(1);
  private final AtomicReference<Cancellation> cause = new AtomicReference<>();
  private volatile long cancelledAt;
  private volatile boolean forced;
  private String completedStep;
  private String currentStep;
  private final ObjectNode cursors;
  private Exception stepFailure;
  private final Job<A> job;
  private final ObjectNode metadata = Json.object();
  private final Client client;
  private Workers runtime;
  final java.util.Map<Long, Peer> peers = new java.util.concurrent.ConcurrentHashMap<>();
  private final Set<String> steps = new HashSet<>();
  private boolean resumeMatched;

  WorkContext(Client client, Job<A> job) {
    this(client, job, true);
  }

  WorkContext(Client client, Job<A> job, boolean validate) {
    this.client = client;
    this.job = job;
    JsonNode saved = job.metadata().path("river:resumable_step");
    if (validate && !saved.isMissingNode() && !saved.isString())
      throw new IllegalArgumentException("Invalid resumable step metadata");
    resumeMatched = saved.asString("").isEmpty();
    var cursors = job.metadata().path("river:resumable_cursor");
    if (validate && cursors.isArray())
      throw new IllegalArgumentException("Invalid resumable cursor metadata");
    this.cursors = cursors.isObject() ? (ObjectNode) cursors.deepCopy() : Json.object();
  }

  public A args() {
    return job.args();
  }

  /** Internal companion-attempt seam used by River Pro's batch worker. */
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

  record Peer(Job<JsonNode> row, WorkContext<?> context) {}

  public void awaitCancellation() throws InterruptedException {
    cancellation.await();
  }

  /** Waits for cancellation for at most the given duration. */
  public boolean awaitCancellation(Duration duration) throws InterruptedException {
    return cancellation.await(duration.toNanos(), java.util.concurrent.TimeUnit.NANOSECONDS);
  }

  public void cancel(String reason) {
    throw new Control(Job.State.CANCELLED, null, reason);
  }

  public Cancellation cancellation() {
    return cause.get();
  }

  public void checkCancelled() throws InterruptedException {
    if (cause.get() != null) throw new InterruptedException("Job cancellation requested");
  }

  void checkRuntimeCancellation() {
    if (isCancelled()) throw new IllegalStateException("Batch attempt cancelled");
  }

  /** Completes this attempt atomically with application writes in the supplied transaction. */
  public Job<A> complete(Connection transaction) {
    return client.complete(transaction, job, metadataUpdates());
  }

  public void discard(String reason) {
    throw new Control(Job.State.DISCARDED, null, reason);
  }

  public boolean isCancelled() {
    return cause.get() != null;
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

  Exception finish(Exception failure) {
    if (forced)
      failure = new IllegalStateException("Worker ignored cancellation beyond the stuck threshold");
    if (stepFailure != null) failure = stepFailure;
    if (failure == null && !resumeMatched)
      failure = new IllegalArgumentException("Saved resumable step was not found");
    if (failure != null && completedStep != null) {
      metadata("river:resumable_step", completedStep);
      if (!cursors.isEmpty() || job.metadata().has("river:resumable_cursor"))
        metadata("river:resumable_cursor", cursors.isEmpty() ? null : cursors);
    }
    return failure;
  }

  public void output(Object value) {
    metadata("output", value);
  }

  public Client client() {
    return client;
  }

  void requestCancellation(Cancellation value) {
    if (cause.compareAndSet(null, value)) {
      cancelledAt = System.nanoTime();
      cancellation.countDown();
      peers.values().forEach(peer -> peer.context.requestCancellation(value));
    }
  }

  boolean forceIfStuck(Duration threshold) {
    if (cause.get() == null
        || cancelledAt == 0
        || forced
        || System.nanoTime() - cancelledAt < threshold.toNanos()) return false;
    forced = true;
    return true;
  }

  public void snooze(Duration duration) {
    if (duration.isNegative())
      throw new IllegalArgumentException("Snooze duration cannot be negative");
    throw new Control(null, duration, "snoozed");
  }

  public void step(String name, Step action) throws Exception {
    executeStep(name, false, _ -> action.run());
  }

  public void stepWithCursor(String name, CursorStep action) throws Exception {
    executeStep(name, true, action);
  }

  public void cursor(Object value) {
    if (currentStep == null)
      throw new IllegalStateException("Cursor requires an active resumable step");
    cursors.set(currentStep, Json.tree(value));
  }

  private void executeStep(String name, boolean withCursor, CursorStep action) throws Exception {
    if (stepFailure != null) throw stepFailure;
    if (name.isEmpty() || !steps.add(name)) {
      stepFailure = new IllegalArgumentException("duplicate resumable step or empty name: " + name);
      throw stepFailure;
    }
    if (!resumeMatched) {
      if (job.metadata().path("river:resumable_step").asString().equals(name)) {
        resumeMatched = true;
        completedStep = name;
      }
      if (!resumeMatched || !withCursor || !cursors.has(name)) return;
    }
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
