package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import tools.jackson.databind.JsonNode;

@Tag("conformance")
class ProtocolStorageTest {
  private static final JobType<String> TYPE = JobType.of("conformance", String.class);
  @TempDir Path directory;
  @RegisterExtension final TestDatabase databases = new TestDatabase();

  private Client client() {
    return new Client(databases.open(directory.resolve("river.db")));
  }

  @Test
  void periodicMetadata() throws Exception {
    var keys = Conformance.fixture("protocol_values.json").required("metadata_keys");
    var client = client();
    var claimed = new CompletableFuture<Job<String>>();
    var errors = new LinkedBlockingQueue<Throwable>();
    try (var workers =
        client
            .workers()
            .queue("default", 1)
            .pollOnly(true)
            .errorHandler(errors::add)
            .add(TYPE, context -> claimed.complete(context.job()))
            .periodic(
                "conformance_periodic",
                Schedule.every(Duration.ofDays(1)),
                TYPE,
                "periodic",
                InsertOptions.builder().metadata(Map.of("source", "conformance")).build(),
                true)
            .start()) {
      var job = claimed.get(5, TimeUnit.SECONDS);
      var metadata = client.get(job.id()).metadata();
      assertTrue(metadata.required("periodic").asBoolean());
      assertEquals(
          "conformance_periodic",
          metadata.required(keys.required("periodic_job_id").asString()).asString());
      assertEquals("conformance", metadata.required("source").asString());
      if (!errors.isEmpty()) throw new AssertionError("Unexpected worker failure", errors.peek());
    }
  }

  @Test
  void rescueCountAndOutput() throws Exception {
    var fixture = Conformance.fixture("protocol_values.json");
    var keys = fixture.required("metadata_keys");
    String rescueKey = keys.required("rescue_count").asString();
    var maintained = new CompletableFuture<Void>();
    var client =
        client()
            .withPlugin(
                new Plugin() {
                  @Override
                  public boolean clean(Client client, Instant now) {
                    // Maintenance calls clean only after rescue has committed.
                    maintained.complete(null);
                    return true;
                  }
                });
    var inserted =
        client
            .insert(TYPE, "rescue", InsertOptions.builder().metadata(Map.of(rescueKey, 2)).build())
            .job();
    var database = client.database();
    boolean postgres = database.dialect() == Database.Dialect.POSTGRES;
    if (!postgres) {
      var nonce = inserted.metadata().required(keys.required("unique_nonce").asString());
      assertTrue(nonce.isString());
      assertFalse(nonce.asString().isEmpty());
    }
    client.transaction(
        connection -> {
          // Seed an abandoned Go attempt without waiting for a real worker to become stuck.
          try (var statement =
              Sql.prepare(
                  connection,
                  "UPDATE "
                      + database.prefix()
                      + "river_job SET state = 'running', attempt = 3, attempted_at = ?, errors = "
                      + (postgres ? "ARRAY[?::jsonb]" : "json_array(json(?))")
                      + " WHERE id = ?",
                  database.timestamp(Instant.EPOCH),
                  Json.encode(fixture.required("attempt_error")),
                  inserted.id())) {
            assertEquals(1, statement.executeUpdate());
          }
          return null;
        });
    assertEquals(
        fixture.required("attempt_error"),
        Json.tree(client.get(inserted.id()).errors().getFirst()));

    var completed = new CompletableFuture<Workers.Event>();
    var writeConflict = new CompletableFuture<java.sql.SQLException>();
    var errors = new LinkedBlockingQueue<Throwable>();
    try (var workers =
            client
                .workers()
                .queue("default", 1)
                .pollOnly(true)
                .errorHandler(errors::add)
                .retryPolicy(
                    job -> {
                      if (!postgres) {
                        // Retry selection is between rescue's read and update. A second writer
                        // must already be excluded, or its commit would invalidate that snapshot.
                        try (var connection = database.connection();
                            var statement = connection.createStatement()) {
                          statement.execute("PRAGMA busy_timeout=0");
                          statement.executeUpdate(
                              "UPDATE river_job SET priority = 2 WHERE id = " + job.id());
                          writeConflict.complete(null);
                        } catch (java.sql.SQLException error) {
                          writeConflict.complete(error);
                        }
                      }
                      return Duration.ofDays(1);
                    })
                .add(TYPE, context -> context.output(Map.of("ok", true)))
                .start();
        var subscription =
            workers.subscribe(completed::complete, Workers.EventKind.JOB_COMPLETED)) {
      if (!postgres) {
        var conflict = writeConflict.get(5, TimeUnit.SECONDS);
        assertNotNull(conflict, "Rescue must reserve the writer before selecting jobs");
        assertEquals(5, conflict.getErrorCode(), "Expected SQLITE_BUSY for the competing writer");
      }
      maintained.get(5, TimeUnit.SECONDS);
      var rescued = client.get(inserted.id());
      assertEquals(Job.State.RETRYABLE, rescued.state());
      assertEquals(3, rescued.metadata().required(rescueKey).asInt());
      assertEquals(2, rescued.errors().size());
      assertEquals(fixture.required("attempt_error"), Json.tree(rescued.errors().getFirst()));

      client.retry(inserted.id());
      assertEquals(inserted.id(), completed.get(5, TimeUnit.SECONDS).job().id());
      var job = client.get(inserted.id());
      assertEquals(Job.State.COMPLETED, job.state());
      assertEquals(
          Json.tree(Map.of("ok", true)),
          job.metadata().required(keys.required("output").asString()));
      assertEquals(3, job.metadata().required(rescueKey).asInt());
      assertEquals(rescued.errors(), job.errors());
      if (!errors.isEmpty()) throw new AssertionError("Unexpected worker failure", errors.peek());
    }
  }

  @Test
  void storedStatesAndUniqueBits() throws Exception {
    var client = client();
    var database = client.database();
    boolean postgres = database.dialect() == Database.Dialect.POSTGRES;
    var inserted = client.insert(TYPE, "state").job();
    for (JsonNode fixture : Conformance.fixture("protocol_values.json").required("job_states")) {
      String state = fixture.required("state").asString();
      int bit = fixture.required("unique_bit").asInt();
      Object mask =
          postgres ? String.format("%8s", Integer.toBinaryString(bit)).replace(' ', '0') : bit;
      var finalizedAt =
          List.of("cancelled", "completed", "discarded").contains(state)
              ? inserted.createdAt()
              : null;
      client.transaction(
          connection -> {
            try (var statement =
                Sql.prepare(
                    connection,
                    "UPDATE "
                        + database.prefix()
                        + "river_job SET state = "
                        + (postgres ? "?::" + database.prefix() + "river_job_state" : "?")
                        + ", finalized_at = ?, unique_states = "
                        + (postgres ? "?::bit(8)" : "?")
                        + " WHERE id = ?",
                    state,
                    database.timestamp(finalizedAt),
                    mask,
                    inserted.id())) {
              assertEquals(1, statement.executeUpdate());
            }
            return null;
          });
      var job = client.get(inserted.id());
      assertEquals(state, job.state().value());
      assertEquals(List.of(job.state()), job.uniqueStates());
      assertEquals(bit, job.uniqueStates().getFirst().bit());
    }
  }
}
