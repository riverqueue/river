package com.riverqueue;

import static org.junit.jupiter.api.Assertions.*;

import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.extension.RegisterExtension;
import org.junit.jupiter.api.io.TempDir;

@EnabledIfSystemProperty(named = "river.test.database", matches = "mysql")
class MySqlTest {
  @TempDir Path directory;
  @RegisterExtension final TestDatabase databases = new TestDatabase();

  @Test
  void matchesKindsAndQueuesExactly() throws Exception {
    var database = databases.open(directory.resolve("unused"));
    var client = new Client(database);
    var type = JobType.of("message", String.class);
    var wanted = client.insert(type, "first").job();
    var padded = client.insert(type, "second").job();
    try (var connection = database.connection();
        var statement = connection.createStatement()) {
      statement.execute(
          "UPDATE "
              + database.prefix()
              + "river_job SET kind='message ', queue='default ' WHERE id="
              + padded.id());
    }
    assertEquals(
        List.of(wanted.id()),
        client.list(JobQuery.builder().kinds("message").build()).jobs().stream()
            .map(Job::id)
            .toList());
    assertEquals(
        List.of(wanted.id()),
        client.list(JobQuery.builder().queues("default").build()).jobs().stream()
            .map(Job::id)
            .toList());
  }

  @Test
  void migrationRoundTrip() {
    var database = databases.open(directory.resolve("unused"));
    var migrator = new Migrator(database);
    assertTrue(migrator.migrate().valid());
    assertEquals(
        List.of(8, 7, 6, 5, 4, 3, 2, 1),
        migrator
            .migrate(Migrator.Direction.DOWN, Migrator.Options.defaults().targetVersion(0))
            .applied());
    assertTrue(migrator.list().stream().noneMatch(Migrator.Status::applied));
    assertTrue(migrator.migrate().valid());
  }

  @Test
  void largeIdsRemainDistinct() throws Exception {
    var database = databases.open(directory.resolve("unused"));
    try (var connection = database.connection();
        var statement = connection.createStatement()) {
      statement.execute(
          "ALTER TABLE " + database.prefix() + "river_job AUTO_INCREMENT=9007199254740993");
    }
    var client = new Client(database);
    var type = JobType.of("message", String.class);
    var first = client.insert(type, "first").job();
    var second = client.insert(type, "second").job();
    assertEquals(9007199254740993L, first.id());
    assertEquals(first.id() + 1, second.id());
    assertEquals(
        List.of(second.id()),
        client.list(JobQuery.builder().ids(second.id()).build()).jobs().stream()
            .map(Job::id)
            .toList());
  }
}
