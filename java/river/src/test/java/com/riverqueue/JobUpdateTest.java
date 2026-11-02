package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import tools.jackson.databind.JsonNode;
import tools.jackson.databind.node.ObjectNode;

class JobUpdateTest {
  private static final JobType<String> TYPE = JobType.of("update_test", String.class);
  @RegisterExtension final TestDatabase databases = new TestDatabase();
  @TempDir Path directory;

  @Test
  void applicationOwnsCommitAndRollback() throws Exception {
    var client = client();
    var original = client.insert(TYPE, "original").job();
    var updates = JobUpdateOptions.builder().output("saved").build();
    try (var connection = client.database().connection()) {
      connection.setAutoCommit(false);
      var updated = client.jobs().update(connection, original.id(), updates);
      assertEquals("saved", updated.metadata().path("output").asString());
      assertEquals(updated, client.jobs().get(connection, original.id()));
      assertEquals(original, client.jobs().get(original.id()));
      connection.rollback();
      assertEquals(original, client.jobs().get(original.id()));

      client.jobs().update(connection, original.id(), updates);
      connection.commit();
      assertEquals(updated, client.jobs().get(original.id()));
      assertFalse(connection.isClosed());
      assertFalse(connection.getAutoCommit());
    }
  }

  private Client client() {
    return new Client(databases.open(directory.resolve("river.db")));
  }

  @Test
  void connectionRequiresAnExplicitTransaction() throws Exception {
    var client = client();
    var original = client.insert(TYPE, "original").job();
    try (var connection = client.database().connection()) {
      assertThrows(
          IllegalArgumentException.class,
          () ->
              client
                  .jobs()
                  .update(
                      connection, original.id(), JobUpdateOptions.builder().output("new").build()));
      assertThrows(
          IllegalArgumentException.class,
          () -> client.jobs().update(connection, original.id(), JobUpdateOptions.defaults()));
    }
    assertEquals(original, client.jobs().get(original.id()));
  }

  @Test
  void emptyOptionsAndOmittedOutputLeaveValuesUnchanged() {
    var client = client();
    var original =
        client
            .insert(
                TYPE,
                "original",
                InsertOptions.builder().metadata(Map.of("output", "saved")).build())
            .job();
    assertEquals(original, client.jobs().update(original.id(), JobUpdateOptions.defaults()));
    assertEquals(
        original,
        client.jobs().update(original.id(), JobUpdateOptions.builder().metadata(Map.of()).build()));
    assertEquals(
        "saved",
        client
            .jobs()
            .update(original.id(), JobUpdateOptions.builder().metadata(Map.of("new", true)).build())
            .metadata()
            .path("output")
            .asString());
  }

  @Test
  void explicitOutputOverridesMetadataWithoutChangingOtherJobFields() {
    var client = client();
    var original =
        client
            .insert(
                TYPE,
                "original",
                InsertOptions.builder().metadata(Map.of("keep", true, "output", "old")).build())
            .job();
    var updates =
        JobUpdateOptions.builder()
            .metadata(Map.of("output", "metadata value", "new", true))
            .output(Map.of("explicit", true))
            .build();
    var updated = client.jobs().update(original.id(), updates);
    var expected = (ObjectNode) original.metadata().deepCopy();
    expected.put("new", true);
    expected.set("output", Json.tree(Map.of("explicit", true)));

    assertEquals(expected, updated.metadata());
    assertEquals(updated, client.jobs().get(original.id()));
    // Job metadata is a detached snapshot; compare all remaining job fields too.
    ((ObjectNode) updated.metadata()).removeAll();
    ((ObjectNode) original.metadata()).removeAll();
    assertEquals(original, updated);
    assertEquals("metadata value", updates.metadata().path("output").asString());
  }

  @Test
  void mergesWholeMetadataValuesAndPreservesExactJson() {
    var client = client();
    var stored =
        Json.parse(
            "{\"keep\":{\"x\":1},\"nested\":{\"old\":true},\"river:resumable_step\":\"normalize\",\"large\":123456789012345678901234567890,\"fraction\":1.234567890123456789}");
    var original =
        client.insert(TYPE, "original", InsertOptions.builder().metadata(stored).build()).job();
    client
        .jobs()
        .update(original.id(), JobUpdateOptions.builder().metadata(Map.of("later", true)).build());
    var metadata = Json.object();
    metadata.set("nested", Json.object());
    metadata.putNull("nullable");
    metadata.set("a\"b.c", Json.parse("[true,false,null]"));
    metadata.put("", false);
    metadata.putNull("a\\b");
    for (int i = 0; i < 1200; i++) metadata.put("key_" + i, i);
    var expected = (ObjectNode) original.metadata().deepCopy();
    expected.put("later", true);
    metadata.properties().forEach(entry -> expected.set(entry.getKey(), entry.getValue()));

    var updated =
        client.jobs().update(original.id(), JobUpdateOptions.builder().metadata(metadata).build());

    assertEquals(expected, updated.metadata());
    assertEquals(updated, client.jobs().get(original.id()));
  }

  @Test
  void missingJobsReportNotFoundWithoutPoisoningTheTransaction() throws Exception {
    var client = client();
    for (var updates :
        new JobUpdateOptions[] {
          JobUpdateOptions.defaults(), JobUpdateOptions.builder().output("new").build()
        }) {
      assertEquals(
          RiverException.Code.NOT_FOUND,
          assertThrows(RiverException.class, () -> client.jobs().update(-1, updates)).code());
      client.transaction(
          connection -> {
            assertEquals(
                RiverException.Code.NOT_FOUND,
                assertThrows(
                        RiverException.class, () -> client.jobs().update(connection, -1, updates))
                    .code());
            return client.insert(connection, TYPE, "usable");
          });
    }
  }

  @ParameterizedTest
  @MethodSource("outputValues")
  void outputReplacesThePreviousValueAndPreservesOtherMetadata(JsonNode output) {
    var client = client();
    var original =
        client
            .insert(
                TYPE,
                "original",
                InsertOptions.builder()
                    .metadata(Map.of("output", Map.of("old", true), "keep", true))
                    .build())
            .job();
    var updated =
        client.jobs().update(original.id(), JobUpdateOptions.builder().output(output).build());
    var expected = (ObjectNode) original.metadata().deepCopy();
    expected.set("output", output);

    assertEquals(expected, updated.metadata());
    assertEquals(updated, client.jobs().get(original.id()));
  }

  static Stream<JsonNode> outputValues() {
    return Json.parse(
            "[null,false,true,42,9007199254740993,1.25,\"null\",\"a 'quoted' string\",{},[],[true,null]]")
        .valueStream();
  }

  @Test
  void updateIsVisibleBeforeTheWorkerFinishesAndSurvivesCompletion() throws Exception {
    var database = databases.open(directory.resolve("river.db"));
    var observed = new CompletableFuture<Job<JsonNode>>();
    var completed = new CompletableFuture<Client.Event>();
    var client =
        Client.builder(database)
            .queue("default", 1)
            .worker(
                TYPE,
                context -> {
                  context
                      .client()
                      .jobs()
                      .update(
                          context.job().id(), JobUpdateOptions.builder().output("saved").build());
                  observed.complete(context.client().jobs().get(context.job().id()));
                })
            .build();
    client.subscribe(completed::complete, Client.EventKind.JOB_COMPLETED);
    var inserted = client.insert(TYPE, "original").job();
    try (var run = client.start()) {
      assertSame(client, run.client());
      var running = observed.get(10, TimeUnit.SECONDS);
      assertEquals(Job.State.RUNNING, running.state());
      assertEquals("saved", running.metadata().path("output").asString());
      assertEquals(inserted.id(), completed.get(10, TimeUnit.SECONDS).job().id());
      assertEquals("saved", client.jobs().get(inserted.id()).metadata().path("output").asString());
    }
  }
}
