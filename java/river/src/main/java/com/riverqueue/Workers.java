package com.riverqueue;

import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;

/** Registry of typed job handlers. Clients snapshot registrations when configured. */
public final class Workers {
  private final Map<String, Registration<?>> registrations = new LinkedHashMap<>();

  /** Creates an empty handler registry. */
  public Workers() {}

  /** Registers a handler for a job kind, rejecting duplicate kinds. */
  public <A> Workers add(JobType<A> type, Handler<A> handler) {
    Objects.requireNonNull(type, "type");
    Objects.requireNonNull(handler, "handler");
    if (registrations.putIfAbsent(type.kind(), new Registration<>(type, handler)) != null)
      throw new IllegalArgumentException("Duplicate worker kind");
    return this;
  }

  Map<String, Registration<?>> snapshot() {
    return Map.copyOf(registrations);
  }

  /** A worker handler. Lambdas inherit the runtime's attempt timeout. */
  @FunctionalInterface
  public interface Handler<A> {
    /**
     * Overrides the timeout for this job. Null or zero inherits the runtime default; a negative
     * duration disables timeouts. Positive values must fit in signed 64-bit nanoseconds. Called
     * once before work and when evaluating an abandoned job for rescue, so implementations must be
     * thread-safe and free of side effects.
     */
    default Duration timeout(Job<A> job) {
      return null;
    }

    /** Performs one job attempt; a normal return marks it successful. */
    void work(WorkContext<A> context) throws Exception;
  }

  record Registration<A>(JobType<A> type, Handler<A> handler) {}
}
