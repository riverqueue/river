package com.riverqueue;

import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

/**
 * Immutable updates to job metadata and recorded output. Metadata is merged at the top level;
 * supplied values replace whole values, including nested objects and JSON null. Explicit output
 * takes precedence over the metadata's "output" key.
 *
 * @param metadata metadata keys to merge; null means no metadata updates
 * @param output output to set; a null reference leaves it unchanged, a JSON null node stores null
 */
public record JobUpdateOptions(JsonNode metadata, JsonNode output) {
  public JobUpdateOptions {
    metadata = metadata == null ? Json.object() : metadata.deepCopy();
    if (!metadata.isObject()) throw new IllegalArgumentException("metadata must be a JSON object");
    output = output == null ? null : output.deepCopy();
  }

  /** Returns a builder for named updates. */
  public static Builder builder() {
    return new Builder();
  }

  /** Returns options that leave the job unchanged. */
  public static JobUpdateOptions defaults() {
    return builder().build();
  }

  /** Returns a copy of the metadata updates. */
  @Override
  public JsonNode metadata() {
    return metadata.deepCopy();
  }

  ObjectNode metadataUpdates() {
    var updates = (ObjectNode) metadata.deepCopy();
    if (output != null) updates.set(Protocol.METADATA_OUTPUT, output.deepCopy());
    return updates;
  }

  /** Returns a copy of the output, or null when output was not supplied. */
  @Override
  public JsonNode output() {
    return output == null ? null : output.deepCopy();
  }

  /** Fluent construction distinguishes omitted output from {@code output(null)}. */
  public static final class Builder {
    private JsonNode metadata;
    private JsonNode output;

    /** Creates immutable options from the supplied updates. */
    public JobUpdateOptions build() {
      return new JobUpdateOptions(metadata, output);
    }

    /** Merges the supplied JSON object into existing metadata at the top level. */
    public Builder metadata(Object value) {
      metadata = Json.tree(value).deepCopy();
      return this;
    }

    /** Replaces recorded output; passing null stores JSON null. */
    public Builder output(Object value) {
      output = Json.tree(value).deepCopy();
      return this;
    }
  }
}
