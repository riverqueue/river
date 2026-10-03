package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfEnvironmentVariable;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;

class MigratorTest {
  @TempDir Path directory;

  private Database database() {
    return Database.connect("jdbc:sqlite:" + directory.resolve("migration.db"));
  }

  @ParameterizedTest
  @CsvSource({"UP,false", "UP,true", "DOWN,false", "DOWN,true"})
  void concurrentMigratorsRecheckHistoryBetweenCommits(
      Migrator.Direction direction, boolean limited) {
    var database = database();
    boolean down = direction == Migrator.Direction.DOWN;
    if (down) new Migrator(database).migrate();
    int steps = limited ? 2 : Migrator.LATEST;
    var options =
        Migrator.Options.defaults().targetVersion(down ? 0 : Migrator.LATEST).maxSteps(steps);
    var peer = new AtomicReference<Migrator.Result>();
    var interleaved =
        interleaveFirstCommit(
            database,
            () -> peer.set(new Migrator(database).migrate(direction, options.maxSteps(steps - 1))));

    var result = new Migrator(interleaved).migrate(direction, options);

    assertNotNull(peer.get());
    assertEquals(steps - 1, peer.get().applied().size());
    assertEquals(List.of(down ? Migrator.LATEST : 1), result.applied());
    assertEquals(
        IntStream.rangeClosed(1, down ? Migrator.LATEST - steps : steps).boxed().toList(),
        result.existing());
    assertEquals(!down && !limited, result.valid());
  }

  @Test
  void concurrentMigratorsShareHistory() throws Exception {
    var database = database();
    var ready = new CountDownLatch(2);
    var start = new CountDownLatch(1);
    try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
      var results =
          java.util.stream.IntStream.range(0, 2)
              .mapToObj(
                  ignored ->
                      executor.submit(
                          () -> {
                            ready.countDown();
                            assertTrue(start.await(5, TimeUnit.SECONDS));
                            return new Migrator(database).migrate();
                          }))
              .toList();
      assertTrue(ready.await(5, TimeUnit.SECONDS));
      start.countDown();
      int applied = 0;
      for (var result : results) applied += result.get(10, TimeUnit.SECONDS).applied().size();
      assertEquals(Migrator.LATEST, applied);
    }
    assertTrue(new Migrator(database).list().stream().allMatch(Migrator.Status::applied));
  }

  @Test
  void failedMigrationRollsBackDdlAndHistory() throws Exception {
    var database = database();
    new Migrator(database).migrate();
    var migrator =
        new Migrator(
            database,
            "test",
            List.of(
                new Migrator.Migration(
                    1,
                    "broken",
                    "CREATE TABLE migration_probe (id integer); INSERT INTO missing_table VALUES (1);",
                    "DROP TABLE migration_probe;")));
    assertThrows(RiverException.class, migrator::migrate);
    try (var connection = database.connection();
        var statement = connection.createStatement();
        var rows =
            statement.executeQuery(
                "SELECT count(*) FROM sqlite_master WHERE name='migration_probe'")) {
      assertTrue(rows.next());
      assertEquals(0, rows.getInt(1));
    }
    assertFalse(migrator.list().getFirst().applied());
  }

  private static Database interleaveFirstCommit(Database database, Runnable peer) {
    var first = new AtomicBoolean(true);
    var source =
        new org.sqlite.SQLiteDataSource() {
          @Override
          public Connection getConnection() throws SQLException {
            var connection = database.connection();
            return (Connection)
                Proxy.newProxyInstance(
                    Connection.class.getClassLoader(),
                    new Class<?>[] {Connection.class},
                    (proxy, method, args) -> {
                      if (method.getName().equals("commit") && first.compareAndSet(true, false)) {
                        // Xerial releases the writer before beginning the next transaction. Run
                        // the peer in that gap so the interleaving never depends on scheduling.
                        connection.setAutoCommit(true);
                        try {
                          peer.run();
                        } finally {
                          connection.setAutoCommit(false);
                        }
                        return null;
                      }
                      try {
                        return method.invoke(connection, args);
                      } catch (InvocationTargetException error) {
                        throw error.getCause();
                      }
                    });
          }
        };
    return new Database(source, Database.Dialect.SQLITE);
  }

  @ParameterizedTest
  @ValueSource(strings = {"cli_", "RiverJobs_"})
  @Tag("postgres")
  @EnabledIfEnvironmentVariable(named = "RIVER_TEST_DATABASE_URL", matches = ".+")
  void postgresSchemaDryRunAndRoundTrip(String prefix) throws Exception {
    String schema = prefix + UUID.randomUUID().toString().replace("-", "");
    var database = Database.connect(System.getenv("RIVER_TEST_DATABASE_URL")).withSchema(schema);
    var migrator = new Migrator(database);
    try {
      assertEquals(
          Migrator.LATEST,
          migrator
              .migrate(
                  Migrator.Direction.UP,
                  Migrator.Options.defaults()
                      .targetVersion(Migrator.LATEST)
                      .maxSteps(Integer.MAX_VALUE)
                      .dryRun(true))
              .applied()
              .size());
      try (var connection = database.connection();
          var statement =
              connection.prepareStatement(
                  "SELECT EXISTS(SELECT 1 FROM information_schema.schemata WHERE schema_name=?)")) {
        statement.setString(1, schema);
        try (var rows = statement.executeQuery()) {
          assertTrue(rows.next());
          assertFalse(rows.getBoolean(1));
        }
      }
      assertTrue(migrator.migrate().valid());
      assertTrue(migrator.list().stream().allMatch(Migrator.Status::applied));
      assertEquals(
          List.of(8, 7, 6, 5, 4, 3, 2, 1),
          migrator
              .migrate(
                  Migrator.Direction.DOWN,
                  Migrator.Options.defaults()
                      .targetVersion(0)
                      .maxSteps(Integer.MAX_VALUE)
                      .dryRun(false))
              .applied());
      assertTrue(migrator.list().stream().noneMatch(Migrator.Status::applied));
    } finally {
      try (var connection = database.connection();
          var statement = connection.createStatement()) {
        statement.execute("DROP SCHEMA IF EXISTS \"" + schema + "\" CASCADE");
      }
    }
  }

  @Test
  void sqliteDryRunAndLegacyHistoryRoundTrip() throws Exception {
    var database = database();
    var migrator = new Migrator(database);
    assertEquals(8, migrator.list().size());
    assertEquals(
        8,
        migrator
            .migrate(
                Migrator.Direction.UP,
                Migrator.Options.defaults()
                    .targetVersion(8)
                    .maxSteps(Integer.MAX_VALUE)
                    .dryRun(true))
            .applied()
            .size());
    try (var connection = database.connection();
        var statement = connection.createStatement();
        var rows =
            statement.executeQuery(
                "SELECT count(*) FROM sqlite_master WHERE name='river_migration'")) {
      assertTrue(rows.next());
      assertEquals(0, rows.getInt(1));
    }
    assertEquals(
        List.of(1, 2, 3, 4),
        migrator
            .migrate(
                Migrator.Direction.UP,
                Migrator.Options.defaults()
                    .targetVersion(4)
                    .maxSteps(Integer.MAX_VALUE)
                    .dryRun(false))
            .applied());
    assertEquals(4, migrator.list().stream().filter(Migrator.Status::applied).count());
    assertTrue(migrator.migrate().valid());
    assertEquals(
        List.of(8, 7, 6),
        migrator
            .migrate(
                Migrator.Direction.DOWN,
                Migrator.Options.defaults().targetVersion(0).maxSteps(3).dryRun(false))
            .applied());
    assertEquals(
        List.of(5, 4, 3, 2, 1),
        migrator
            .migrate(
                Migrator.Direction.DOWN,
                Migrator.Options.defaults()
                    .targetVersion(0)
                    .maxSteps(Integer.MAX_VALUE)
                    .dryRun(false))
            .applied());
    assertTrue(migrator.list().stream().noneMatch(Migrator.Status::applied));
    assertTrue(migrator.migrate().valid());
  }
}
