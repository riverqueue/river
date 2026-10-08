package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import tools.jackson.databind.JsonNode;

class PeerCancellationTest {
  private static final JobType<String> TYPE = JobType.of("peer_cancellation", String.class);
  @RegisterExtension final TestDatabase databases = new TestDatabase();
  @TempDir Path directory;

  @ParameterizedTest
  @CsvSource({
    "REMOTE,before_decode",
    "REMOTE,after_decode",
    "SHUTDOWN,before_decode",
    "SHUTDOWN,after_decode",
    "SHUTDOWN,forced",
    "TIMEOUT,before_decode",
    "TIMEOUT,after_decode"
  })
  void cancellationDuringArgumentDecodingReachesTheTypedPeer(
      WorkContext.Cancellation cause, String when) throws Exception {
    var decoding = new CompletableFuture<Void>();
    var decode = new CompletableFuture<Void>();
    var database = databases.open(directory.resolve("river.db"));
    var client =
        new Client(database)
            .withPlugin(
                new Plugin() {
                  @Override
                  public JsonNode decode(Job<JsonNode> job) {
                    if (job.queue().equals("peers")) {
                      decoding.complete(null);
                      decode.join();
                    }
                    return job.args();
                  }
                });
    var inserted =
        client.insert(TYPE, "peer", InsertOptions.builder().queue("peers").build()).job();
    var parent = new CompletableFuture<WorkContext<String>>();
    var typed = new CompletableFuture<WorkContext<String>>();
    var finish = new CompletableFuture<Void>();
    var completed = new CompletableFuture<Void>();
    var errors = new LinkedBlockingQueue<Throwable>();
    var workers =
        client
            .workers()
            .queue("default", 1)
            .leadership(false)
            .pollOnly(true)
            .stopTimeout(Duration.ofSeconds(5))
            .errorHandler(errors::add)
            .add(
                TYPE,
                context -> {
                  parent.complete(context);
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
                                        1);
                                var rows = statement.executeQuery()) {
                              while (rows.next()) jobs.add(client.readPartial(rows));
                            }
                            return jobs;
                          });
                  var peer = peers.getFirst();
                  typed.complete(peer);
                  finish.join();
                  context.completePeer(peer, new InterruptedException("peer interrupted"));
                  completed.complete(null);
                })
            .start();
    try {
      client.insert(TYPE, "parent");
      decoding.get(5, TimeUnit.SECONDS);
      var provisional = parent.get(5, TimeUnit.SECONDS).peers.get(inserted.id()).context();
      if (!when.equals("after_decode")) provisional.requestCancellation(cause);
      if (when.equals("forced")) assertTrue(provisional.forceIfStuck(Duration.ZERO));
      decode.complete(null);
      var context = typed.get(5, TimeUnit.SECONDS);
      if (when.equals("after_decode")) provisional.requestCancellation(cause);
      assertEquals(cause, context.cancellation());
      assertTrue(context.awaitCancellation(Duration.ZERO));
      finish.complete(null);
      completed.get(5, TimeUnit.SECONDS);
      assertFalse(
          provisional.forceIfStuck(Duration.ZERO), "Finished attempts must stop being supervised");
      workers.stop();
      var job = client.get(inserted.id());
      if (when.equals("forced")) {
        assertEquals(1, job.attempt());
        assertEquals(1, job.errors().size());
        assertTrue(job.errors().getFirst().error().contains("ignored cancellation"));
      } else if (cause == WorkContext.Cancellation.REMOTE)
        assertEquals(Job.State.CANCELLED, job.state());
      else if (cause == WorkContext.Cancellation.SHUTDOWN) {
        assertEquals(Job.State.AVAILABLE, job.state());
        assertEquals(0, job.attempt());
        assertTrue(job.errors().isEmpty());
      } else assertEquals(1, job.errors().size());
    } finally {
      decode.complete(null);
      finish.complete(null);
      workers.stopAndCancel();
    }
    assertTrue(errors.isEmpty(), () -> "Unexpected worker errors: " + errors);
  }
}
