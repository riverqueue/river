package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.sql.Connection;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tools.jackson.databind.JsonNode;

class PeerDecodingTest {
  private static final JobType<String> TYPE = JobType.of("peer_decoding", String.class);
  @RegisterExtension final TestDatabase databases = new TestDatabase();
  @TempDir Path directory;

  @ParameterizedTest
  @CsvSource({
    "arguments,false,false",
    "arguments,true,false",
    "arguments,false,true",
    "metadata,false,false",
    "pilot,false,false",
    "row,false,false"
  })
  @SuppressWarnings("try") // Resources scope subscriptions and the background runtime.
  void decodingFailuresRetainTheirOwnOutcomes(
      String failureKind, boolean handlerFails, boolean retryCompletion) throws Exception {
    var database = databases.open(directory.resolve("river.db"));
    var raw = new Client(database);
    var options = InsertOptions.builder().queue("peers").maxAttempts(1).build();
    var first = raw.insert(TYPE, "first", options).job();
    var bad =
        raw.insert(
                JobType.of(TYPE.kind(), JsonNode.class),
                failureKind.equals("arguments") ? Json.object() : Json.tree("bad"),
                InsertOptions.builder()
                    .queue("peers")
                    .maxAttempts(2)
                    .metadata(
                        failureKind.equals("metadata")
                            ? Map.of("river:resumable_cursor", List.of(1))
                            : Map.of())
                    .build())
            .job();
    var last = raw.insert(TYPE, "last", options).job();
    var completions = new AtomicInteger();
    var client =
        raw.withPilot(
            new Pilot() {
              @Override
              public void afterAttempt(
                  Connection connection, Client.Driver driver, Job<JsonNode> job) {
                if (job.id() == bad.id() && retryCompletion && completions.getAndIncrement() == 0)
                  throw new IllegalStateException("completion failed once");
              }

              @Override
              public JsonNode decode(Job<JsonNode> job) {
                if (job.id() == bad.id() && failureKind.equals("pilot"))
                  throw new IllegalArgumentException("invalid peer payload");
                return job.args();
              }
            });
    var claimed = new CompletableFuture<WorkContext.PeerClaims<String>>();
    var errors = new LinkedBlockingQueue<Throwable>();
    var events = new LinkedBlockingQueue<Client.Event>();
    try (var workers =
            client.toBuilder()
                .queue("default", 1)
                .leadership(false)
                .pollOnly(true)
                .retryPolicy(job -> Duration.ofDays(1))
                .errorHandler(errors::add)
                .worker(
                    TYPE,
                    context -> {
                      var peers =
                          context.claimPeers(
                              TYPE,
                              connection -> {
                                var jobs = new ArrayList<Client.Decoded>();
                                var now = database.timestamp(Instant.now());
                                try (var statement =
                                        Sql.prepare(
                                            connection,
                                            Sql.query(database, "claim"),
                                            now,
                                            "batch",
                                            "peers",
                                            now,
                                            3);
                                    var rows = statement.executeQuery()) {
                                  while (rows.next()) {
                                    var decoded = client.readPartial(rows);
                                    if (decoded.job().id() == bad.id() && failureKind.equals("row"))
                                      decoded =
                                          new Client.Decoded(
                                              decoded.job(),
                                              new IllegalArgumentException("invalid peer row"));
                                    jobs.add(decoded);
                                  }
                                }
                                return jobs;
                              });
                      claimed.complete(peers);
                      if (handlerFails) throw new IllegalStateException("handler failed");
                      for (var peer : peers.peers()) context.completePeer(peer, null);
                    })
                .build()
                .start();
        var subscription = workers.client().subscribe(events::add)) {
      var leader =
          client.insert(TYPE, "leader", InsertOptions.builder().maxAttempts(1).build()).job();
      var peers = claimed.get(5, TimeUnit.SECONDS);
      assertEquals(3, peers.count());
      assertEquals(
          List.of(first.id(), last.id()), peers.peers().stream().map(p -> p.job().id()).toList());
      for (int i = 0; i < 4; i++) assertNotNull(events.poll(5, TimeUnit.SECONDS));
      for (long id : List.of(leader.id(), first.id(), last.id())) {
        var job = client.jobs().get(id);
        assertEquals(handlerFails ? Job.State.DISCARDED : Job.State.COMPLETED, job.state());
        assertEquals(handlerFails ? 1 : 0, job.errors().size());
        if (handlerFails) assertEquals("handler failed", job.errors().getFirst().error());
      }
      var failed = client.jobs().get(bad.id());
      assertEquals(Job.State.RETRYABLE, failed.state());
      assertEquals(1, failed.attempt());
      assertEquals(1, failed.errors().size());
      String message = failed.errors().getFirst().error();
      assertTrue(
          message.contains(
              switch (failureKind) {
                case "arguments" -> "String";
                case "metadata" -> "Invalid resumable cursor metadata";
                case "pilot" -> "invalid peer payload";
                case "row" -> "invalid peer row";
                default -> throw new AssertionError(failureKind);
              }),
          message);
      assertEquals(retryCompletion ? 1 : 0, errors.size(), errors.toString());
      if (retryCompletion) assertEquals(2, completions.get());
    }
  }
}
