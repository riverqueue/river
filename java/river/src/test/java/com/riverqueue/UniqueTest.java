package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

class UniqueTest {
  @TestFactory
  Stream<DynamicTest> goGoldens() throws Exception {
    String raw;
    try (var input = getClass().getResourceAsStream("/fixtures/unique_keys.json")) {
      raw = new String(input.readAllBytes(), StandardCharsets.UTF_8);
    }
    var cases = new ArrayList<String>();
    // Preserve number tokens and nested argument order from the generated Go fixture.
    try (var parser = Json.MAPPER.createParser(Json.members(raw).get("cases"))) {
      parser.nextToken();
      String array = Json.members(raw).get("cases");
      while (parser.nextToken() != tools.jackson.core.JsonToken.END_ARRAY) {
        int start = (int) parser.currentTokenLocation().getCharOffset();
        parser.skipChildren();
        cases.add(array.substring(start, (int) parser.currentLocation().getCharOffset()));
      }
    }
    return cases.stream()
        .map(
            source -> {
              var value = Json.parse(source);
              return DynamicTest.dynamicTest(
                  value.path("name").asString(),
                  () -> {
                    var options = value.path("options");
                    var states = options.has("by_state") ? new HashSet<Job.State>() : null;
                    if (states != null)
                      for (var state : options.path("by_state"))
                        states.add(Job.State.of(state.asString()));
                    long nanos = options.path("by_period_nanos").asLong();
                    var unique =
                        new Unique(
                            options.path("by_args").asBoolean(),
                            nanos == 0 ? null : Duration.ofNanos(nanos),
                            options.path("by_queue").asBoolean(),
                            states,
                            options.path("exclude_kind").asBoolean());
                    var paths = new ArrayList<List<String>>();
                    for (var path : value.path("selected_unique_components")) {
                      var components = new ArrayList<String>();
                      for (var component : path) components.add(component.asString());
                      paths.add(components);
                    }
                    var scheduledAt =
                        value.path("scheduled_at").isString()
                            ? Instant.parse(value.path("scheduled_at").asString())
                            : null;
                    org.junit.jupiter.api.function.ThrowingSupplier<String> key =
                        () ->
                            unique.key(
                                value.path("kind").asString(),
                                Json.compact(Json.members(source).get("args")),
                                paths,
                                Instant.parse(value.path("now").asString()),
                                value.path("queue").asString(),
                                scheduledAt);
                    if (value.has("expected_error"))
                      assertThrows(IllegalArgumentException.class, key::get);
                    else {
                      assertEquals(value.path("expected_sha256").asString(), key.get());
                      assertEquals(value.path("expected_state_mask").asInt(), unique.stateMask());
                    }
                  });
            });
  }

  @Test
  void preservesRawTokensAndFirstDuplicate() {
    assertEquals("1e2", Json.members("{\"a\":1e2,\"a\":2}").get("a"));
    assertEquals("\"abc\"", Json.members("{\"a\":\"abc\"}").get("a"));
    assertEquals(
        "[1, {\"z\":1, \"y\":2}]", Json.members("{\"a\":[1, {\"z\":1, \"y\":2}]}").get("a"));
  }
}
