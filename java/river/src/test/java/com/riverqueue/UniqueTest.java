package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;

class UniqueTest {
  @TestFactory
  @Tag("conformance")
  Stream<DynamicTest> goFixtures() throws Exception {
    String raw = Conformance.read("unique_keys.json");
    var cases = new ArrayList<String>();
    // Preserve number tokens and nested argument order from the generated Go fixture.
    var groups = Json.members(raw);
    for (String group : List.of("cases", "typed_only_cases")) {
      String array = groups.get(group);
      assertNotNull(array, "Missing unique-key fixture group: " + group);
      assertFalse(Json.parse(array).isEmpty(), "Empty unique-key fixture group: " + group);
      try (var parser = Json.MAPPER.createParser(array)) {
        assertEquals(tools.jackson.core.JsonToken.START_ARRAY, parser.nextToken());
        while (parser.nextToken() != tools.jackson.core.JsonToken.END_ARRAY) {
          int start = (int) parser.currentTokenLocation().getCharOffset();
          parser.skipChildren();
          cases.add(array.substring(start, (int) parser.currentLocation().getCharOffset()));
        }
      }
    }
    return cases.stream()
        .map(
            source -> {
              var value = Json.parse(source);
              return DynamicTest.dynamicTest(
                  value.required("name").asString(),
                  () -> {
                    var options = value.required("options");
                    var states = options.has("by_state") ? new HashSet<Job.State>() : null;
                    if (states != null)
                      for (var state : options.path("by_state"))
                        states.add(Job.State.of(state.asString()));
                    long nanos = options.required("by_period_nanos").asLong();
                    var unique =
                        new Unique(
                            options.required("by_args").asBoolean(),
                            nanos == 0 ? null : Duration.ofNanos(nanos),
                            options.required("by_queue").asBoolean(),
                            states,
                            options.required("exclude_kind").asBoolean());
                    var paths = new ArrayList<List<String>>();
                    for (var path : value.path("selected_unique_components")) {
                      var components = new ArrayList<String>();
                      for (var component : path) components.add(component.asString());
                      paths.add(components);
                    }
                    var scheduledAt =
                        value.required("scheduled_at").isString()
                            ? Instant.parse(value.required("scheduled_at").asString())
                            : null;
                    org.junit.jupiter.api.function.ThrowingSupplier<String> key =
                        () ->
                            unique.key(
                                value.required("kind").asString(),
                                Json.compact(Json.members(source).get("args")),
                                paths,
                                Instant.parse(value.required("now").asString()),
                                value.required("queue").asString(),
                                scheduledAt);
                    if (value.has("expected_error")) {
                      assertEquals("rejected", value.required("expected_error").asString());
                      assertThrows(IllegalArgumentException.class, key::get);
                    } else {
                      assertEquals(value.required("expected_sha256").asString(), key.get());
                      Collections.reverse(paths);
                      assertEquals(value.required("expected_sha256").asString(), key.get());
                      assertEquals(
                          value.required("expected_state_mask").asInt(), unique.stateMask());
                    }
                  });
            });
  }

  @Test
  void ordersLiteralAndNestedPathsIndependentlyOfInputOrder() {
    var unique = Unique.args();
    String args = "{\"a.b\":1,\"a\":{\"b\":2}}";
    var paths = new ArrayList<>(List.of(List.of("a.b"), List.of("a", "b")));
    // Go orders the nested path a.b before the escaped literal path a\\.b.
    String expected =
        unique.key(
            "paths", "{\"a\":{\"b\":2},\"a.b\":1}", List.of(), Instant.EPOCH, "default", null);
    assertEquals(expected, unique.key("paths", args, paths, Instant.EPOCH, "default", null));
    Collections.reverse(paths);
    assertEquals(expected, unique.key("paths", args, paths, Instant.EPOCH, "default", null));
  }

  @Test
  void preservesGoPrimaryPathOrdering() throws Exception {
    var unique = Unique.args();
    String args = "{\"a\":{\"z\":2},\"a-\":1}";
    var paths = new ArrayList<>(List.of(List.of("a", "z"), List.of("a-")));
    // Go sorts the joined names a- before a.z, unlike a component-by-component comparison.
    String expected =
        HexFormat.of()
            .formatHex(
                MessageDigest.getInstance("SHA-256")
                    .digest(
                        "&kind=paths&args={\"a-\":1,\"a\":{\"z\":2}}"
                            .getBytes(StandardCharsets.UTF_8)));
    assertEquals(expected, unique.key("paths", args, paths, Instant.EPOCH, "default", null));
    Collections.reverse(paths);
    assertEquals(expected, unique.key("paths", args, paths, Instant.EPOCH, "default", null));
  }

  @Test
  void preservesRawTokensAndFirstDuplicate() {
    assertEquals("1e2", Json.members("{\"a\":1e2,\"a\":2}").get("a"));
    assertEquals("\"abc\"", Json.members("{\"a\":\"abc\"}").get("a"));
    assertEquals(
        "[1, {\"z\":1, \"y\":2}]", Json.members("{\"a\":[1, {\"z\":1, \"y\":2}]}").get("a"));
  }
}
