package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class NotificationCleanupTest {
  @TempDir Path directory;

  @Test
  void deletesOldestRowsInBoundedBatchesAndRetainsCutoff() {
    var database = TestDatabase.sqlite(directory.resolve("river.db"));
    var client = new Client(database);
    Instant cutoff = Instant.parse("2026-01-02T03:04:05Z");
    client.transaction(
        connection -> {
          try (var statement =
              Sql.prepare(
                  connection,
                  "INSERT INTO river_notification (id, created_at, topic, payload) VALUES (?, ?, 'test', '{}')")) {
            for (int id = 1; id <= 1003; id++) {
              Instant createdAt =
                  switch (id) {
                    case 1 -> cutoff.minusMillis(1);
                    case 1002 -> cutoff;
                    case 1003 -> cutoff.plusMillis(1);
                    default -> cutoff.minusSeconds(1);
                  };
              statement.setInt(1, id);
              statement.setObject(2, database.timestamp(createdAt));
              statement.addBatch();
            }
            statement.executeBatch();
          }
          return null;
        });
    assertEquals(1000, clean(client, cutoff));
    assertEquals(List.of(1L, 1002L, 1003L), ids(client));
    assertEquals(1, clean(client, cutoff));
    assertEquals(0, clean(client, cutoff));
    assertEquals(List.of(1002L, 1003L), ids(client));
  }

  @ParameterizedTest
  @ValueSource(booleans = {false, true})
  void leaderCleansNotificationsEvenWhenPluginOwnsJobCleaning(boolean pluginOwnsCleaning)
      throws Exception {
    var database = TestDatabase.sqlite(directory.resolve("river.db"));
    var base = new Client(database);
    base.transaction(
        connection -> {
          try (var statement =
              Sql.prepare(
                  connection,
                  "INSERT INTO river_notification (id, created_at, topic, payload) VALUES (?, ?, 'test', '{}')")) {
            statement.setInt(1, 1);
            statement.setObject(2, database.timestamp(Instant.EPOCH));
            statement.executeUpdate();
            statement.setInt(1, 2);
            statement.setObject(2, database.timestamp(Instant.now().plus(Duration.ofDays(1))));
            statement.executeUpdate();
          }
          return null;
        });
    var cleaned = new CompletableFuture<List<Long>>();
    var errors = new LinkedBlockingQueue<Throwable>();
    var client =
        base.withPlugin(
            new Plugin() {
              @Override
              public boolean clean(Client river, Instant now) {
                // The next maintenance stage observes the notification cleanup's committed result.
                cleaned.complete(ids(river));
                return pluginOwnsCleaning;
              }
            });
    try (var workers =
        client
            .workers()
            .queue("default", 1)
            .add(JobType.of("unused", String.class), context -> {})
            .pollOnly(true)
            .serviceInterval(Duration.ofHours(1))
            .errorHandler(errors::add)
            .start()) {
      assertEquals(List.of(2L), cleaned.get(5, TimeUnit.SECONDS));
    }
    assertTrue(errors.isEmpty(), () -> "Unexpected worker errors: " + errors);
  }

  private static int clean(Client client, Instant cutoff) {
    return client.transaction(
        connection -> {
          try (var statement =
              Sql.prepare(
                  connection,
                  Sql.query(client.database(), "clean_notifications"),
                  client.database().timestamp(cutoff))) {
            return statement.executeUpdate();
          }
        });
  }

  private static List<Long> ids(Client client) {
    return client.transaction(
        connection -> {
          var result = new ArrayList<Long>();
          try (var statement = connection.createStatement();
              var rows =
                  statement.executeQuery(
                      "SELECT id FROM river_notification WHERE topic='test' ORDER BY id")) {
            while (rows.next()) result.add(rows.getLong(1));
          }
          return result;
        });
  }
}
