package com.riverqueue;

import java.time.Instant;
import java.util.List;
import tools.jackson.databind.JsonNode;

/** A persisted job and its decoded arguments. IDs retain all 64 bits. */
public record Job<A>(
    long id,
    A args,
    int attempt,
    Instant attemptedAt,
    List<String> attemptedBy,
    Instant createdAt,
    List<AttemptError> errors,
    Instant finalizedAt,
    String kind,
    int maxAttempts,
    JsonNode metadata,
    int priority,
    String queue,
    Instant scheduledAt,
    State state,
    List<String> tags,
    String uniqueKey,
    List<State> uniqueStates) {
  public Job {
    attemptedBy = List.copyOf(attemptedBy);
    errors = List.copyOf(errors);
    metadata = metadata.deepCopy();
    tags = List.copyOf(tags);
    uniqueStates = uniqueStates == null ? null : List.copyOf(uniqueStates);
  }

  /** Returns a snapshot with converted arguments and the same persisted job fields. */
  public <B> Job<B> mapArgs(java.util.function.Function<? super A, ? extends B> mapper) {
    return new Job<>(
        id,
        mapper.apply(args),
        attempt,
        attemptedAt,
        attemptedBy,
        createdAt,
        errors,
        finalizedAt,
        kind,
        maxAttempts,
        metadata,
        priority,
        queue,
        scheduledAt,
        state,
        tags,
        uniqueKey,
        uniqueStates);
  }

  /** One failed attempt, in River's persisted error format. */
  public record AttemptError(Instant at, int attempt, String error, String trace) {}

  /**
   * The persisted insertion result. Arguments remain JSON because uniqueness may return an existing
   * job of a different kind or argument schema.
   */
  public record InsertResult(Job<JsonNode> job, boolean uniqueSkippedAsDuplicate) {}

  /** Names and bit positions are part of the cross-language database protocol. */
  public enum State {
    AVAILABLE,
    CANCELLED,
    COMPLETED,
    DISCARDED,
    PENDING,
    RETRYABLE,
    RUNNING,
    SCHEDULED;

    public int bit() {
      return 1 << ordinal();
    }

    public boolean isFinalized() {
      return this == CANCELLED || this == COMPLETED || this == DISCARDED;
    }

    @com.fasterxml.jackson.annotation.JsonValue
    public String value() {
      return name().toLowerCase(java.util.Locale.ROOT);
    }

    public static State of(String value) {
      return valueOf(value.toUpperCase(java.util.Locale.ROOT));
    }
  }
}
