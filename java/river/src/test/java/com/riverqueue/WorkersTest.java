package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

class WorkersTest {
  @RegisterExtension final TestDatabase databases = new TestDatabase();
  @TempDir Path directory;

  @Test
  void duplicateKindsAndNullHandlersAreRejected() {
    var type = JobType.of("registry_test", String.class);
    var workers = new Workers();
    assertThrows(NullPointerException.class, () -> workers.add(null, context -> {}));
    assertThrows(NullPointerException.class, () -> workers.add(type, null));
    assertSame(workers, workers.add(type, context -> {}));
    assertThrows(
        IllegalArgumentException.class,
        () -> workers.add(JobType.of(type.kind(), Integer.class), context -> {}));
  }

  @Test
  @SuppressWarnings("try") // Resources intentionally scope background activity.
  void registryAndBuilderAreSnapshotted() throws Exception {
    var first = JobType.of("first", String.class);
    var second = JobType.of("second", String.class);
    var third = JobType.of("third", String.class);
    var workers = new Workers().add(first, context -> {});
    var builder =
        Client.builder(databases.open(directory.resolve("river.db")))
            .leadership(false)
            .pollOnly(true)
            .pollInterval(Duration.ofMillis(10))
            .queue("default", 1)
            .fetchOnlyKnownKinds(true)
            .workers(workers);
    // Changing the registry after supplying it does not change the builder.
    workers.add(second, context -> fail("Registry mutation leaked into client"));
    var client = builder.build();
    builder.worker(third, context -> fail("Builder mutation leaked into client"));
    var events = new LinkedBlockingQueue<Client.Event>();
    var secondJob = client.insert(second, "unregistered").job();
    var thirdJob = client.insert(third, "unregistered").job();
    var firstJob = client.insert(first, "registered").job();
    try (var subscription = client.subscribe(events::add, Client.EventKind.JOB_COMPLETED);
        var run = client.start()) {
      var event = events.poll(5, TimeUnit.SECONDS);
      assertNotNull(event);
      assertEquals(firstJob.id(), event.job().id());
    }
    assertEquals(Job.State.AVAILABLE, client.get(secondJob.id()).state());
    assertEquals(Job.State.AVAILABLE, client.get(thirdJob.id()).state());
  }
}
