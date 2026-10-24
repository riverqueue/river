package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class TimeoutTest {
  private static final JobType<String> TYPE = JobType.of("timeout_test", String.class);
  @RegisterExtension final TestDatabase databases = new TestDatabase();
  @TempDir Path directory;

  @ParameterizedTest
  @ValueSource(strings = {"inherit", "override", "disabled", "longer", "zero", "overflow", "throw"})
  void handlerTimeoutControlsTheAttempt(String mode) throws Exception {
    var client = new Client(databases.open(directory.resolve("river.db")));
    var calls = new AtomicInteger();
    var worked = new AtomicInteger();
    var result = new CompletableFuture<Workers.Event>();
    var errors = new LinkedBlockingQueue<Throwable>();
    boolean invalid = java.util.Set.of("overflow", "throw").contains(mode);
    boolean completes = mode.equals("disabled") || mode.equals("longer");
    try (var workers =
            client
                .workers()
                .queue("default", 1)
                .pollOnly(true)
                .leadership(false)
                .jobTimeout(mode.equals("override") ? Duration.ofHours(1) : Duration.ofNanos(1))
                .stopTimeout(Duration.ofSeconds(5))
                .errorHandler(errors::add)
                .add(
                    TYPE,
                    new Workers.Handler<>() {
                      @Override
                      public Duration timeout(Job<String> job) {
                        assertEquals(mode, job.args());
                        calls.incrementAndGet();
                        return switch (job.args()) {
                          case "disabled" -> Duration.ofNanos(-1);
                          case "longer" -> Duration.ofHours(1);
                          case "override" -> Duration.ofNanos(1);
                          case "zero" -> Duration.ZERO;
                          case "overflow" -> Duration.ofSeconds(Long.MAX_VALUE);
                          case "throw" -> throw new IllegalStateException("bad timeout");
                          default -> null;
                        };
                      }

                      @Override
                      public void work(WorkContext<String> context) throws Exception {
                        worked.incrementAndGet();
                        if (completes)
                          assertFalse(context.awaitCancellation(Duration.ofMillis(100)));
                        else {
                          assertTrue(context.awaitCancellation(Duration.ofSeconds(5)));
                          assertEquals(WorkContext.Cancellation.TIMEOUT, context.cancellation());
                          context.checkCancelled();
                        }
                      }
                    })
                .start();
        var subscription = workers.subscribe(result::complete)) {
      client.insert(TYPE, mode, InsertOptions.builder().maxAttempts(1).build());
      var event = result.get(5, TimeUnit.SECONDS);
      assertEquals(
          completes ? Workers.EventKind.JOB_COMPLETED : Workers.EventKind.JOB_FAILED, event.kind());
      assertEquals(completes ? Job.State.COMPLETED : Job.State.DISCARDED, event.job().state());
      assertEquals(1, calls.get());
      assertEquals(invalid ? 0 : 1, worked.get());
    }
    assertTrue(errors.isEmpty(), () -> "Unexpected worker errors: " + errors);
  }
}
