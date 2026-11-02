package com.riverqueue;

import java.nio.charset.StandardCharsets;
import tools.jackson.databind.JsonNode;

/** Size validation shared by immediate and buffered job output. */
final class RecordedOutput {
  private static final int MAX_BYTES = 32 * 1024 * 1024;

  private RecordedOutput() {}

  static void checkSize(JsonNode value) {
    int size = Json.encode(value).getBytes(StandardCharsets.UTF_8).length;
    if (size > MAX_BYTES)
      throw new IllegalArgumentException("output is too large: " + size + " bytes (max 32 MiB)");
  }
}
