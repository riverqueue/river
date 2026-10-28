package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class CancellationCheckTest {
  @RegisterExtension final TestDatabase databases = new TestDatabase();
  @TempDir Path directory;

  @Test
  void cancellationQueryOnlyReadsRequestedJobs() throws Exception {
    var database = databases.open(directory.resolve("river.db"));
    var client = Client.builder(database).build();
    var type = JobType.of("cancellation_query", String.class);
    var first = client.insert(type, "first").job();
    var second = client.insert(type, "second").job();
    client.transaction(
        connection -> {
          try (var statement =
              Sql.prepare(
                  connection,
                  "UPDATE "
                      + database.prefix()
                      + "river_job SET state='running', metadata='{\"cancel_attempted_at\":null}' WHERE id IN (?, ?)",
                  first.id(),
                  second.id())) {
            statement.executeUpdate();
          }
          try (var statement =
                  Sql.prepare(
                      connection,
                      Sql.query(database, "cancel_requested"),
                      Json.encode(List.of(first.id())));
              var rows = statement.executeQuery()) {
            assertTrue(rows.next());
            assertEquals(first.id(), rows.getLong(1));
            assertFalse(rows.next());
          }
          return null;
        });
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  @SuppressWarnings("try") // Resources intentionally scope background activity.
  void checksOnlyOnHintsAndRetriesFailures(boolean failFirst) throws Exception {
    var database = databases.open(directory.resolve("river.db"));
    var fail = new AtomicBoolean();
    var queries = new AtomicInteger();
    var checked = new LinkedBlockingQueue<Integer>();
    var source =
        (DataSource)
            Proxy.newProxyInstance(
                DataSource.class.getClassLoader(),
                new Class<?>[] {DataSource.class},
                (proxy, method, args) -> {
                  if (!method.getName().equals("getConnection"))
                    throw new UnsupportedOperationException();
                  var connection = database.connection();
                  return Proxy.newProxyInstance(
                      Connection.class.getClassLoader(),
                      new Class<?>[] {Connection.class},
                      (ignored, operation, arguments) -> {
                        if (operation.getName().equals("prepareStatement")
                            && arguments[0].equals(Sql.query(database, "cancel_requested"))) {
                          checked.add(queries.incrementAndGet());
                          if (fail.compareAndSet(true, false))
                            throw new SQLException("temporary cancellation read failure");
                        }
                        try {
                          return operation.invoke(connection, arguments);
                        } catch (InvocationTargetException error) {
                          throw error.getCause();
                        }
                      });
                });
    var ready = new CompletableFuture<Void>();
    var entered = new CompletableFuture<Void>();
    var finish = new CompletableFuture<Void>();
    var errors = new LinkedBlockingQueue<Throwable>();
    var type = JobType.of("cancellation_check", String.class);
    var client =
        Client.builder(new Database(source, database.dialect()).withSchema(database.schema()))
            .queue("default", 1)
            .leadership(false)
            .pollInterval(Duration.ofMillis(10))
            .stopTimeout(Duration.ofSeconds(5))
            .errorHandler(errors::add)
            .observe(
                event -> {
                  if (event.equals("listen_ready")) ready.complete(null);
                })
            .worker(
                type,
                context -> {
                  entered.complete(null);
                  finish.join();
                  context.checkCancelled();
                })
            .build();
    try (var run = client.start()) {
      try {
        ready.get(5, TimeUnit.SECONDS);
        var job = client.insert(type, "working").job();
        entered.get(5, TimeUnit.SECONDS);
        // Let the startup hint settle, then leave a running attempt idle for several old ticks.
        TimeUnit.MILLISECONDS.sleep(200);
        int before = queries.get();
        checked.clear();
        TimeUnit.MILLISECONDS.sleep(200);
        assertEquals(before, queries.get());
        fail.set(failFirst);
        client.transaction(
            connection -> {
              client.notify(connection, Protocol.cancel(job.id(), "default"));
              return null;
            });
        assertNotNull(checked.poll(5, TimeUnit.SECONDS));
        if (failFirst) assertNotNull(checked.poll(5, TimeUnit.SECONDS));
        TimeUnit.MILLISECONDS.sleep(200);
        assertEquals(before + (failFirst ? 2 : 1), queries.get());
        assertEquals(failFirst ? 1 : 0, errors.size());
      } finally {
        finish.complete(null);
      }
    }
  }

  @Test
  @SuppressWarnings("try")
  void identifiedNoticeBypassesBlockedReadAndIgnoresOlderAttempts() throws Exception {
    var database = databases.open(directory.resolve("river.db"));
    var block = new AtomicBoolean();
    var readStarted = new CompletableFuture<Void>();
    var releaseRead = new CompletableFuture<Void>();
    var source =
        (DataSource)
            Proxy.newProxyInstance(
                DataSource.class.getClassLoader(),
                new Class<?>[] {DataSource.class},
                (proxy, method, args) -> {
                  if (!method.getName().equals("getConnection"))
                    throw new UnsupportedOperationException();
                  var connection = database.connection();
                  return Proxy.newProxyInstance(
                      Connection.class.getClassLoader(),
                      new Class<?>[] {Connection.class},
                      (ignored, operation, arguments) -> {
                        if (operation.getName().equals("prepareStatement")
                            && arguments[0].equals(Sql.query(database, "cancel_requested"))
                            && block.compareAndSet(true, false)) {
                          readStarted.complete(null);
                          releaseRead.get(5, TimeUnit.SECONDS);
                        }
                        try {
                          return operation.invoke(connection, arguments);
                        } catch (InvocationTargetException error) {
                          throw error.getCause();
                        }
                      });
                });
    var ready = new CompletableFuture<Void>();
    var entered = new CompletableFuture<WorkContext<String>>();
    var cancelled = new CompletableFuture<Void>();
    var type = JobType.of("cancellation_delivery", String.class);
    var client =
        Client.builder(new Database(source, database.dialect()).withSchema(database.schema()))
            .queue("default", 1)
            .leadership(false)
            .pollInterval(Duration.ofMillis(10))
            .stopTimeout(Duration.ofSeconds(5))
            .observe(
                event -> {
                  if (event.equals("listen_ready")) ready.complete(null);
                })
            .worker(
                type,
                context -> {
                  entered.complete(context);
                  context.awaitCancellation();
                  cancelled.complete(null);
                  context.checkCancelled();
                })
            .build();
    try (var run = client.start()) {
      try {
        ready.get(5, TimeUnit.SECONDS);
        var inserted = client.insert(type, "working").job();
        var context = entered.get(5, TimeUnit.SECONDS);
        var job = context.job();
        block.set(true);
        client.transaction(
            connection -> {
              client.notify(connection, Protocol.cancel(inserted.id(), "default"));
              return null;
            });
        readStarted.get(5, TimeUnit.SECONDS);
        client.transaction(
            connection -> {
              client.notify(
                  connection,
                  Protocol.cancel(job.id(), job.queue(), job.attempt() + 1, job.attemptedAt()));
              client.notify(
                  connection,
                  Protocol.cancel(
                      job.id(), job.queue(), job.attempt(), job.attemptedAt().minusSeconds(1)));
              return null;
            });
        assertThrows(
            java.util.concurrent.TimeoutException.class,
            () -> cancelled.get(100, TimeUnit.MILLISECONDS));
        client.cancel(job.id());
        cancelled.get(2, TimeUnit.SECONDS);
      } finally {
        releaseRead.complete(null);
        if (entered.isDone()) entered.join().requestCancellation(WorkContext.Cancellation.SHUTDOWN);
      }
    }
  }
}
