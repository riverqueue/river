package com.riverqueue;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.extension.AfterEachCallback;
import org.junit.jupiter.api.extension.ExtensionContext;

/** Isolates each test in a SQLite file or a Postgres schema owned by that test. */
final class TestDatabase implements AfterEachCallback {
  private final List<Database> databases = new ArrayList<>();

  @Override
  public void afterEach(ExtensionContext context) throws Exception {
    for (var database : databases)
      try (var connection = database.connection();
          var statement = connection.createStatement()) {
        statement.execute("DROP SCHEMA IF EXISTS \"" + database.schema() + "\" CASCADE");
      }
  }

  Database open(Path file) {
    String url = System.getenv("RIVER_TEST_DATABASE_URL");
    String backend = System.getProperty("river.test.database", url == null ? "sqlite" : "postgres");
    if (backend.equals("sqlite")) return sqlite(file);
    if (!backend.equals("postgres"))
      throw new IllegalArgumentException("Unknown test backend: " + backend);
    if (url == null || url.isBlank())
      throw new IllegalStateException("RIVER_TEST_DATABASE_URL is required for Postgres tests");
    var database =
        Database.connect(url).withSchema("java_" + UUID.randomUUID().toString().replace("-", ""));
    if (database.dialect() != Database.Dialect.POSTGRES)
      throw new IllegalArgumentException("RIVER_TEST_DATABASE_URL must select Postgres");
    databases.add(database);
    new Migrator(database).migrate();
    return database;
  }

  static Database sqlite(Path file) {
    var database = Database.connect("jdbc:sqlite:" + file);
    new Migrator(database).migrate();
    return database;
  }
}
