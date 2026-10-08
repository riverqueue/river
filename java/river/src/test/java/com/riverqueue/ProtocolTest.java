package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;
import org.junit.jupiter.api.DynamicTest;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import tools.jackson.databind.JsonNode;

@Tag("conformance")
class ProtocolTest {
  @Test
  void attemptError() throws Exception {
    var expected = Conformance.fixture("protocol_values.json").required("attempt_error");
    var error = Json.decode(expected, Job.AttemptError.class);
    // Independent expectations catch renamed fields that a decoder might silently ignore.
    assertEquals(
        new Job.AttemptError(
            Instant.parse("2026-01-02T03:04:05.6789Z"),
            3,
            "worker failed: escaped \"detail\"",
            "frame one\nframe two"),
        error);
    assertEquals(expected, Json.tree(error));
  }

  @Test
  void metadataKeysAndTopics() throws Exception {
    var fixture = Conformance.fixture("protocol_values.json");
    assertEquals(
        fixture.required("metadata_keys"),
        Json.tree(
            Map.of(
                "output", Protocol.METADATA_OUTPUT,
                "periodic_job_id", Protocol.METADATA_PERIODIC_JOB_ID,
                "rescue_count", Protocol.METADATA_RESCUE_COUNT,
                "resumable_cursor", Protocol.METADATA_RESUMABLE_CURSOR,
                "resumable_step", Protocol.METADATA_RESUMABLE_STEP,
                "unique_nonce", Protocol.METADATA_UNIQUE_NONCE)));
    assertEquals(
        fixture.required("topics"),
        Json.tree(
            Map.of(
                "control", Protocol.TOPIC_CONTROL,
                "insert", Protocol.TOPIC_INSERT,
                "leadership", Protocol.TOPIC_LEADERSHIP)));
  }

  @TestFactory
  Stream<DynamicTest> notificationDispatch() throws Exception {
    var fixtures = Conformance.fixture("protocol_values.json").required("notifications");
    var names =
        java.util.Set.of(
            "cancel",
            "insert",
            "metadata_changed",
            "pause",
            "request_resign",
            "resigned",
            "resume");
    assertEquals(names.size(), fixtures.size());
    var seen = new java.util.HashSet<String>();
    var tests = new ArrayList<DynamicTest>();
    for (var fixture : fixtures) {
      String name = fixture.required("name").asString();
      assertTrue(seen.add(name), "Duplicate notification fixture: " + name);
      assertTrue(names.contains(name), "Uncovered Go notification: " + name);
      for (String clientId : List.of("client-1", "observer")) {
        tests.add(
            DynamicTest.dynamicTest(
                name + " received by " + clientId,
                () -> {
                  var target = context(42);
                  var unrelated = context(43);
                  var queues = new ArrayList<Protocol.QueueNotice>();
                  var leadership = new ArrayList<Protocol.LeadershipNotice>();
                  var pending = new HashMap<Long, Long>();
                  var dispatcher =
                      new Protocol.Dispatcher(
                          clientId,
                          Map.of(42L, target, 43L, unrelated),
                          pending,
                          queues::add,
                          leadership::add);
                  dispatcher.dispatch(
                      fixture.required("topic").asString(),
                      Json.encode(fixture.required("payload")));

                  assertEquals(
                      name.equals("cancel") ? WorkContext.Cancellation.REMOTE : null,
                      target.cancellation());
                  assertNull(unrelated.cancellation(), "Notification must not cancel another job");
                  assertTrue(pending.isEmpty(), "A registered attempt consumes its cancellation");
                  switch (name) {
                    case "insert", "metadata_changed", "pause", "resume" -> {
                      assertEquals(List.of(new Protocol.QueueNotice("priority", name)), queues);
                      assertTrue(leadership.isEmpty());
                    }
                    case "request_resign" -> {
                      assertTrue(queues.isEmpty());
                      assertEquals(List.of(Protocol.LeadershipNotice.REQUEST_RESIGN), leadership);
                    }
                    case "resigned" -> {
                      assertTrue(queues.isEmpty());
                      assertEquals(
                          clientId.equals("client-1")
                              ? List.of()
                              : List.of(Protocol.LeadershipNotice.CHANGED),
                          leadership);
                    }
                    case "cancel" -> {
                      assertTrue(queues.isEmpty());
                      assertTrue(leadership.isEmpty());
                    }
                    default -> fail("Uncovered Go notification: " + name);
                  }
                }));
      }
    }
    return tests.stream();
  }

  @Test
  void notificationTopicsAndPendingCancellation() throws Exception {
    var fixture = Conformance.fixture("protocol_values.json").required("notifications");
    var cancel =
        java.util.stream.StreamSupport.stream(fixture.spliterator(), false)
            .filter(value -> value.required("name").asString().equals("cancel"))
            .findFirst()
            .orElseThrow();
    var unrelated = context(43);
    var pending = new HashMap<Long, Long>();
    var queues = new ArrayList<Protocol.QueueNotice>();
    var leadership = new ArrayList<Protocol.LeadershipNotice>();
    var dispatcher =
        new Protocol.Dispatcher(
            "observer", Map.of(43L, unrelated), pending, queues::add, leadership::add);
    String payload = Json.encode(cancel.required("payload"));
    dispatcher.dispatch("unrelated", payload);
    dispatcher.dispatch(Protocol.TOPIC_LEADERSHIP, payload);
    assertTrue(pending.isEmpty());
    assertTrue(queues.isEmpty());
    assertTrue(leadership.isEmpty());

    dispatcher.dispatch(Protocol.TOPIC_INSERT, payload);
    assertEquals(List.of(new Protocol.QueueNotice("priority", "insert")), queues);
    assertTrue(
        pending.isEmpty(), "A control payload on the insert topic must not cancel an attempt");
    queues.clear();

    dispatcher.dispatch(cancel.required("topic").asString(), payload);
    assertEquals(java.util.Set.of(42L), pending.keySet());
    assertNull(unrelated.cancellation());
    assertTrue(queues.isEmpty());
    assertTrue(leadership.isEmpty());
  }

  private static WorkContext<String> context(long id) {
    return context(id, Json.object());
  }

  private static WorkContext<String> context(long id, JsonNode metadata) {
    var now = Instant.parse("2026-01-02T03:04:05Z");
    var job =
        new Job<>(
            id,
            "fixture",
            1,
            now,
            List.of("fixture"),
            now,
            List.<Job.AttemptError>of(),
            null,
            "fixture",
            25,
            metadata,
            1,
            "priority",
            now,
            Job.State.RUNNING,
            List.<String>of(),
            null,
            null);
    // A fixture test must not open a connection, even when constructing a work context.
    return new WorkContext<>(
        new Client(Database.connect("jdbc:postgresql://127.0.0.1:1/unused")), job);
  }

  @TestFactory
  Stream<DynamicTest> notificationEncoding() throws Exception {
    var notifications =
        Map.of(
            "cancel", Protocol.cancel(42, "priority"),
            "insert", Protocol.insert("priority"),
            "metadata_changed",
                Protocol.queueMetadataChanged("priority", Map.of("owner", "candidate")),
            "pause", Protocol.queuePause("priority", true),
            "request_resign", Protocol.requestResign(),
            "resigned", Protocol.resigned("client-1"),
            "resume", Protocol.queuePause("priority", false));
    var fixtures = Conformance.fixture("protocol_values.json").required("notifications");
    assertEquals(notifications.size(), fixtures.size());
    var tests = new ArrayList<DynamicTest>();
    for (var fixture : fixtures) {
      String name = fixture.required("name").asString();
      tests.add(
          DynamicTest.dynamicTest(
              name,
              () -> {
                var notification = notifications.get(name);
                assertNotNull(notification, "Uncovered Go notification: " + name);
                assertEquals(fixture.required("topic").asString(), notification.topic());
                assertEquals(fixture.required("payload"), Json.parse(notification.payload()));
              }));
    }
    return tests.stream();
  }

  @TestFactory
  Stream<DynamicTest> retryBounds() throws Exception {
    var fixtures = Conformance.fixture("protocol_values.json").required("retry_cases");
    assertFalse(fixtures.isEmpty());
    var tests = new ArrayList<DynamicTest>();
    for (var fixture : fixtures) {
      int count = fixture.required("error_count").asInt();
      tests.add(
          DynamicTest.dynamicTest(
              "error count " + count,
              () -> {
                var now = Instant.parse(fixture.required("now").asString());
                var error = new Job.AttemptError(now, 1, "previous failure", "");
                // Attempts include snoozes, whereas the retry policy counts only failures.
                var job =
                    new Job<>(
                        fixture.required("job_id").asLong(),
                        Json.object(),
                        999,
                        now,
                        java.util.List.of("fixture"),
                        now,
                        Collections.nCopies(count - 1, error),
                        null,
                        "fixture_retry",
                        1000,
                        Json.object().put("snoozes", 50),
                        1,
                        "default",
                        now,
                        Job.State.RETRYABLE,
                        java.util.List.of(),
                        null,
                        null);
                var policy =
                    RetryPolicy.defaults(
                        new Random(
                            new java.math.BigInteger(fixture.required("seed").asString())
                                .longValue()));
                long min = fixture.required("min_delay_ns").asLong();
                long max = fixture.required("max_delay_ns").asLong();
                // Seeded samples alone need not exercise either jitter boundary or the duration
                // cap.
                for (double jitter : new double[] {0, 0.5, Math.nextDown(1.0)}) {
                  var boundaryPolicy =
                      RetryPolicy.defaults(
                          new Random(0) {
                            @Override
                            public double nextDouble() {
                              return jitter;
                            }
                          });
                  long delay = boundaryPolicy.delay(job).toNanos();
                  assertTrue(
                      delay >= min && delay <= max,
                      "Jitter "
                          + jitter
                          + ": delay "
                          + delay
                          + " outside Go bounds "
                          + min
                          + ".."
                          + max);
                }
                for (int sample = 0; sample < 100; sample++) {
                  long delay = policy.delay(job).toNanos();
                  assertTrue(
                      delay >= min && delay <= max,
                      "Delay " + delay + " outside Go bounds " + min + ".." + max);
                }
              }));
    }
    return tests.stream();
  }

  @Test
  void resumableMetadata() throws Exception {
    var keys = Conformance.fixture("protocol_values.json").required("metadata_keys");
    String cursorKey = keys.required("resumable_cursor").asString();
    String stepKey = keys.required("resumable_step").asString();
    var context =
        context(
            42,
            Json.tree(
                Map.of(cursorKey, Map.of("process", Map.of("offset", 2)), stepKey, "process")));
    context.step("before", () -> fail("A completed step must not run again"));
    var failure = new Exception("retry");
    assertSame(
        failure,
        assertThrows(
            Exception.class,
            () ->
                context.stepWithCursor(
                    "process",
                    cursor -> {
                      assertEquals(Json.tree(Map.of("offset", 2)), cursor);
                      context.cursor(Map.of("offset", 3));
                      throw failure;
                    })));
    assertSame(failure, context.finish(failure));
    assertEquals(
        Json.tree(Map.of(cursorKey, Map.of("process", Map.of("offset", 3)), stepKey, "process")),
        context.metadataUpdates());
  }

  @TestFactory
  Stream<DynamicTest> snoozeCounters() throws Exception {
    var fixtures = Conformance.fixture("snooze_counters.json").required("snooze_counters");
    assertFalse(fixtures.isEmpty());
    var tests = new ArrayList<DynamicTest>();
    for (var fixture : fixtures)
      tests.add(
          DynamicTest.dynamicTest(
              fixture.required("name").asString(),
              () ->
                  assertEquals(
                      fixture.required("expected_snoozes").asLong(),
                      Workers.nextSnoozeCount(fixture.required("metadata")))));
    return tests.stream();
  }

  @Test
  void states() throws Exception {
    var states = Conformance.fixture("protocol_values.json").required("job_states");
    assertEquals(Job.State.values().length, states.size());
    var seen = java.util.EnumSet.noneOf(Job.State.class);
    for (var fixture : states) {
      String value = fixture.required("state").asString();
      var state = Job.State.of(value);
      assertTrue(seen.add(state));
      assertEquals(value, state.value());
      assertEquals(fixture.required("unique_bit").asInt(), state.bit());
    }
  }
}
