package com.riverqueue;

import java.time.Instant;
import java.util.List;
import tools.jackson.databind.JsonNode;

/** Immutable per-insert overrides; unspecified fields inherit the job type's defaults. */
public record InsertOptions(
    Integer maxAttempts,
    JsonNode metadata,
    Boolean pending,
    Integer priority,
    String queue,
    Instant scheduledAt,
    List<String> tags,
    Unique unique) {
  public InsertOptions {
    metadata = metadata == null ? null : metadata.deepCopy();
    tags = tags == null ? null : List.copyOf(tags);
  }

  @Override
  public JsonNode metadata() {
    return metadata == null ? null : metadata.deepCopy();
  }

  public static Builder builder() {
    return new Builder();
  }

  public static InsertOptions defaults() {
    return builder().build();
  }

  InsertOptions resolve(InsertOptions fallback) {
    return new InsertOptions(
        first(maxAttempts, fallback.maxAttempts, 25),
        first(metadata, fallback.metadata, Json.object()),
        first(pending, fallback.pending, false),
        first(priority, fallback.priority, 1),
        first(queue, fallback.queue, "default"),
        first(scheduledAt, fallback.scheduledAt, null),
        first(tags, fallback.tags, List.of()),
        first(unique, fallback.unique, Unique.none()));
  }

  private static <T> T first(T value, T fallback, T defaultValue) {
    return value != null ? value : fallback != null ? fallback : defaultValue;
  }

  /** Fluent construction avoids long positional option lists. */
  public static final class Builder {
    private Integer maxAttempts;
    private JsonNode metadata;
    private Boolean pending;
    private Integer priority;
    private String queue;
    private Instant scheduledAt;
    private List<String> tags;
    private Unique unique;

    public InsertOptions build() {
      return new InsertOptions(
          maxAttempts, metadata, pending, priority, queue, scheduledAt, tags, unique);
    }

    public Builder maxAttempts(int value) {
      maxAttempts = value;
      return this;
    }

    public Builder metadata(Object value) {
      metadata = Json.tree(value);
      return this;
    }

    public Builder pending(boolean value) {
      pending = value;
      return this;
    }

    public Builder priority(int value) {
      priority = value;
      return this;
    }

    public Builder queue(String value) {
      queue = value;
      return this;
    }

    public Builder scheduledAt(Instant value) {
      scheduledAt = value;
      return this;
    }

    public Builder tags(String... value) {
      tags = List.of(value);
      return this;
    }

    public Builder unique(Unique value) {
      unique = value;
      return this;
    }
  }
}
