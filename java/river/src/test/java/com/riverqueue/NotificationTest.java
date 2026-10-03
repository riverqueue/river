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
import javax.sql.DataSource;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class NotificationTest {
  @TempDir Path directory;

  @Test
  void sqliteListenerRecoversFromAnInitialDatabaseFailure() throws Exception {
    var database = TestDatabase.sqlite(directory.resolve("river.db"));
    var fail = new AtomicBoolean(true);
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
                            && arguments[0].equals(Sql.query(database, "notification_cursor"))
                            && fail.compareAndSet(true, false))
                          throw new SQLException("temporary cursor failure");
                        try {
                          return operation.invoke(connection, arguments);
                        } catch (InvocationTargetException error) {
                          throw error.getCause();
                        }
                      });
                });
    var initiallyFetched = new CompletableFuture<Void>();
    var client =
        new Client(new Database(source, Database.Dialect.SQLITE))
            .withPlugin(
                new Plugin() {
                  @Override
                  public List<Client.Decoded> claim(
                      Connection connection,
                      Client.Driver driver,
                      Claim claim,
                      Client.Transaction<List<Client.Decoded>> next)
                      throws Exception {
                    var rows = next.run(connection);
                    if (rows.isEmpty()) initiallyFetched.complete(null);
                    return rows;
                  }
                });
    var type = JobType.of("notification_test", String.class);
    var worked = new CompletableFuture<Job<String>>();
    var retry = new CompletableFuture<Void>();
    var ready = new CompletableFuture<Void>();
    var paused = new CompletableFuture<Workers.Event>();
    var errors = new LinkedBlockingQueue<Throwable>();
    try (var workers =
            client
                .workers()
                .queue("default", 1)
                .leadership(false)
                .pollInterval(Duration.ofHours(1))
                .serviceInterval(Duration.ofHours(1))
                .stopTimeout(Duration.ofSeconds(5))
                .errorHandler(
                    error -> {
                      errors.add(error);
                      retry.join();
                    })
                .observe(
                    event -> {
                      if (event.equals("listen_ready")) ready.complete(null);
                    })
                .add(type, context -> worked.complete(context.job()))
                .start();
        var subscription = workers.subscribe(paused::complete, Workers.EventKind.QUEUE_PAUSED)) {
      try {
        var error = errors.poll(5, TimeUnit.SECONDS);
        assertNotNull(error);
        assertTrue(error.getMessage().contains("temporary cursor failure"));
        initiallyFetched.get(5, TimeUnit.SECONDS);
        var peer = new Client(database);
        var inserted = peer.insert(type, "during startup").job();
        retry.complete(null);
        ready.get(5, TimeUnit.SECONDS);
        assertFalse(fail.get());
        assertEquals(inserted.id(), worked.get(5, TimeUnit.SECONDS).id());
        peer.queues().pause("default");
        assertEquals("default", paused.get(5, TimeUnit.SECONDS).queue().name());
      } finally {
        retry.complete(null);
      }
    }
    assertTrue(errors.isEmpty(), () -> "Unexpected worker errors: " + errors);
  }
}
