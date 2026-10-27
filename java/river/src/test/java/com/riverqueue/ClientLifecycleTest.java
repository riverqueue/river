package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.SQLException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

@SuppressWarnings("try") // Resources intentionally scope subscriptions and background activity.
class ClientLifecycleTest {
  private static final JobType<String> TYPE = JobType.of("lifecycle_test", String.class);
  @RegisterExtension final TestDatabase databases = new TestDatabase();
  @TempDir Path directory;

  private Client.Builder builder() {
    return Client.builder(databases.open(directory.resolve("river.db")))
        .leadership(false)
        .pollInterval(Duration.ofMillis(10))
        .pollOnly(true)
        .queue("default", 1)
        .stopTimeout(Duration.ofSeconds(5));
  }

  @Test
  void clientCanInsertWithoutStarting() {
    var database = databases.open(directory.resolve("river.db"));
    var client = Client.builder(database).build();
    var job = client.insert(TYPE, "insert only").job();
    assertEquals(Job.State.AVAILABLE, client.get(job.id()).state());
    assertThrows(IllegalArgumentException.class, client::start);
    assertEquals(Job.State.AVAILABLE, new Client(database).get(job.id()).state());
  }

  @Test
  void concurrentStartsAllowOnlyOneRun() throws Exception {
    var client = builder().worker(TYPE, context -> {}).build();
    var ready = new CountDownLatch(4);
    var start = new CountDownLatch(1);
    var runs = new LinkedBlockingQueue<RunHandle>();
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var attempts = new ArrayList<Future<Boolean>>();
      for (int i = 0; i < 4; i++) {
        attempts.add(
            executor.submit(
                () -> {
                  ready.countDown();
                  start.await();
                  try {
                    runs.add(client.start());
                    return true;
                  } catch (IllegalStateException expected) {
                    return false;
                  }
                }));
      }
      try {
        assertTrue(ready.await(5, TimeUnit.SECONDS));
        start.countDown();
        int started = 0;
        for (var attempt : attempts) if (attempt.get(5, TimeUnit.SECONDS)) started++;
        assertEquals(1, started);
        assertEquals(1, runs.size());
      } finally {
        start.countDown();
        // Wait for every contender before closing the winner: otherwise another could start.
        for (var attempt : attempts) attempt.get(5, TimeUnit.SECONDS);
        for (var run : runs) run.close();
      }
    }
  }

  @Test
  void failedDatabaseStartupCanBeRetried() throws Exception {
    var database = databases.open(directory.resolve("river.db"));
    var fail = new AtomicBoolean(true);
    var source =
        (DataSource)
            Proxy.newProxyInstance(
                DataSource.class.getClassLoader(),
                new Class<?>[] {DataSource.class},
                (proxy, method, args) -> {
                  if (!method.getName().equals("getConnection"))
                    throw new UnsupportedOperationException();
                  if (fail.compareAndSet(true, false))
                    throw new SQLException("temporary startup failure");
                  return database.connection();
                });
    var client =
        Client.builder(new Database(source, database.dialect()).withSchema(database.schema()))
            .leadership(false)
            .pollOnly(true)
            .queue("default", 1)
            .workers(new Workers().add(TYPE, context -> {}))
            .build();
    assertEquals(
        RiverException.Code.DATABASE, assertThrows(RiverException.class, client::start).code());
    var events = new LinkedBlockingQueue<Client.Event>();
    try (var subscription = client.subscribe(events::add, Client.EventKind.JOB_COMPLETED);
        var run = client.start()) {
      var job = client.insert(TYPE, "retry").job();
      var event = events.poll(5, TimeUnit.SECONDS);
      assertNotNull(event);
      assertEquals(job.id(), event.job().id());
    }
  }

  @Test
  void failedPilotStartupCanBeRetried() {
    var fail = new AtomicBoolean(true);
    var client =
        builder()
            .worker(TYPE, context -> {})
            .build()
            .withPilot(
                new Pilot() {
                  @Override
                  public void starting(boolean leadership) {
                    if (fail.compareAndSet(true, false))
                      throw new IllegalStateException("temporary pilot failure");
                  }
                });
    assertEquals(
        "temporary pilot failure",
        assertThrows(IllegalStateException.class, client::start).getMessage());
    try (var run = client.start()) {
      assertSame(client, run.client());
    }
  }

  @Test
  void sameClientInsertWakesRuntimeWithoutPolling() throws Exception {
    var fetched = new CompletableFuture<Void>();
    var worked = new CompletableFuture<Client>();
    var client =
        builder()
            .pollInterval(Duration.ofHours(1))
            .worker(TYPE, context -> worked.complete(context.client()))
            .build()
            .withPilot(
                new Pilot() {
                  @Override
                  public java.util.List<Client.Decoded> claim(
                      java.sql.Connection connection,
                      Client.Driver driver,
                      Claim claim,
                      Client.Transaction<java.util.List<Client.Decoded>> next)
                      throws Exception {
                    var rows = next.run(connection);
                    if (rows.isEmpty()) fetched.complete(null);
                    return rows;
                  }
                });
    try (var run = client.start()) {
      fetched.get(5, TimeUnit.SECONDS);
      client.insert(TYPE, "wake");
      assertSame(client, worked.get(5, TimeUnit.SECONDS));
    }
  }

  @Test
  void subscriptionsSurviveRestartAndOldHandlesCannotStopNewRuns() throws Exception {
    var client = builder().workers(new Workers().add(TYPE, context -> {})).build();
    var events = new LinkedBlockingQueue<Client.Event>();
    var removed = new LinkedBlockingQueue<Client.Event>();
    try (var subscription = client.subscribe(events::add, Client.EventKind.JOB_COMPLETED)) {
      // Already queued work may finish immediately after start; subscribe beforehand.
      var firstJob = client.insert(TYPE, "first").job();
      var first = client.start();
      try (first) {
        assertEquals(client.id(), first.id());
        assertThrows(IllegalStateException.class, client::start);
        var event = events.poll(5, TimeUnit.SECONDS);
        assertNotNull(event);
        assertEquals(firstJob.id(), event.job().id());
      }
      var closed = client.subscribe(removed::add);
      closed.close();
      closed.close();
      try (var second = client.start()) {
        assertNotSame(first, second);
        assertEquals(first.id(), second.id());
        first.stopAndCancel();
        first.close();
        assertThrows(IllegalStateException.class, client::start);
        var secondJob = client.insert(TYPE, "second").job();
        var event = events.poll(5, TimeUnit.SECONDS);
        assertNotNull(event);
        assertEquals(secondJob.id(), event.job().id());
        assertTrue(removed.isEmpty());
      }
    }
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void timedOutStopPreventsOverlappingRuns(boolean cancel) throws Exception {
    var entered = new CountDownLatch(1);
    var release = new CountDownLatch(1);
    var events = new LinkedBlockingQueue<Client.Event>();
    var client =
        builder()
            .stopTimeout(Duration.ofMillis(100))
            .worker(
                TYPE,
                context -> {
                  entered.countDown();
                  release.await();
                })
            .build();
    try (var subscription = client.subscribe(events::add)) {
      var run = client.start();
      try {
        client.insert(TYPE, "blocked");
        assertTrue(entered.await(5, TimeUnit.SECONDS));
        assertThrows(
            RiverException.class,
            () -> {
              if (cancel) run.stopAndCancel();
              else run.stop();
            });
        assertThrows(IllegalStateException.class, client::start);
      } finally {
        release.countDown();
        assertNotNull(events.poll(5, TimeUnit.SECONDS));
        run.stop();
      }
      try (var restarted = client.start()) {
        assertEquals(run.id(), restarted.id());
      }
    }
  }
}
