package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class RecordedOutputTest {
  private static final int MAX_BYTES = 32 * 1024 * 1024;
  private static final JobType<String> TYPE = JobType.of("output_test", String.class);
  @RegisterExtension final TestDatabase databases = new TestDatabase();
  @TempDir Path directory;

  @ParameterizedTest
  @ValueSource(strings = {"x", "é", "\"", "<"})
  void jobUpdatesEnforceEncodedOutputLimit(String character) {
    var client = new Client(databases.open(directory.resolve("river.db")));
    var original = client.insert(TYPE, "work").job();
    var belowLimit = outputOfSize(character, MAX_BYTES - 1);
    assertDoesNotThrow(() -> JobUpdateOptions.builder().output(belowLimit).build());
    var atLimit = outputOfSize(character, MAX_BYTES);
    var updated =
        client
            .jobs()
            .update(
                original.id(),
                JobUpdateOptions.builder().metadata(Map.of("kept", true)).output(atLimit).build());
    assertEquals(atLimit, updated.metadata().path("output").asString());

    var oversized = outputOfSize(character, MAX_BYTES + 1);
    var error =
        assertThrows(
            IllegalArgumentException.class,
            () ->
                client
                    .jobs()
                    .update(
                        original.id(),
                        JobUpdateOptions.builder()
                            .metadata(Map.of("kept", false))
                            .output(oversized)
                            .build()));
    assertEquals("output is too large: 33554433 bytes (max 32 MiB)", error.getMessage());
    assertThrows(
        IllegalArgumentException.class, () -> new JobUpdateOptions(null, Json.tree(oversized)));
    assertEquals(updated, client.jobs().get(original.id()));
  }

  private static String outputOfSize(String character, int bytes) {
    int encodedCharacterSize = Json.encode(character).getBytes(StandardCharsets.UTF_8).length - 2;
    // Account for the JSON string's quotes as well as escaped and multibyte characters.
    return character.repeat((bytes - 2) / encodedCharacterSize)
        + "x".repeat((bytes - 2) % encodedCharacterSize);
  }

  @ParameterizedTest
  @ValueSource(strings = {"x", "é", "\"", "<"})
  void workerOutputEnforcesEncodedOutputLimitAndPreservesPreviousOutput(String character)
      throws Exception {
    var completed = new CompletableFuture<Client.Event>();
    var atLimit = outputOfSize(character, MAX_BYTES);
    var client =
        Client.builder(databases.open(directory.resolve("river.db")))
            .queue("default", 1)
            .worker(
                TYPE,
                context -> {
                  context.metadata("kept", true);
                  context.output(outputOfSize(character, MAX_BYTES - 1));
                  context.output(atLimit);
                  var error =
                      assertThrows(
                          IllegalArgumentException.class,
                          () -> context.output(outputOfSize(character, MAX_BYTES + 1)));
                  assertEquals(
                      "output is too large: 33554433 bytes (max 32 MiB)", error.getMessage());
                })
            .build();
    client.subscribe(
        completed::complete, Client.EventKind.JOB_COMPLETED, Client.EventKind.JOB_FAILED);
    var inserted = client.insert(TYPE, "work").job();
    try (var run = client.start()) {
      assertSame(client, run.client());
      var event = completed.get(30, TimeUnit.SECONDS);
      assertEquals(Client.EventKind.JOB_COMPLETED, event.kind());
      var saved = client.jobs().get(inserted.id());
      assertEquals(atLimit, saved.metadata().path("output").asString());
      assertTrue(saved.metadata().path("kept").asBoolean());
    }
  }

  @Test
  void workerOutputSnapshotsMutableValuesAndAcceptsJsonNull() {
    var client = new Client(databases.open(directory.resolve("river.db")));
    var context = new WorkContext<>(client, client.insert(TYPE, "work").job());
    var output = Json.object().put("value", "saved");
    context.output(output);
    output.put("value", "changed");
    assertEquals("saved", context.metadataUpdates().path("output").path("value").asString());
    context.output(null);
    assertTrue(context.metadataUpdates().path("output").isNull());
  }
}
