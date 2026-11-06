package com.riverqueue.cli;

import static org.junit.jupiter.api.Assertions.*;

import com.riverqueue.Database;
import com.riverqueue.Migrator;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class MigrationCliTest {
  @TempDir Path directory;

  private Result run(Map<String, String> environment, String... args) {
    var out = new StringWriter();
    var err = new StringWriter();
    int exit =
        MigrationCli.run(
            args,
            environment,
            new PrintWriter(out),
            new PrintWriter(err),
            "river",
            "test-version",
            List.of("main"),
            (database, line) -> new Migrator(database));
    return new Result(exit, out.toString(), err.toString());
  }

  @Test
  void environmentAndExplicitDatabaseSelection() {
    String first = "jdbc:sqlite:" + directory.resolve("first.db");
    String second = "jdbc:sqlite:" + directory.resolve("second.db");
    var environment = Map.of("DATABASE_URL", first);
    assertEquals(0, run(environment, "migrate-up", "--target-version=4").exit());
    assertEquals(0, run(environment, "migrate-up", "--database-url", second).exit());
    assertEquals(
        4,
        new Migrator(Database.connect(first))
            .list().stream().filter(Migrator.Status::applied).count());
    assertEquals(
        8,
        new Migrator(Database.connect(second))
            .list().stream().filter(Migrator.Status::applied).count());
    assertTrue(run(environment, "migrate-list").out().contains("pending"));
  }

  @Test
  void helpAndVersionDoNotRequireDatabase() {
    assertEquals(0, run(Map.of()).exit());
    assertTrue(run(Map.of(), "migrate-up", "--help").out().contains("--dry-run"));
    assertEquals("river test-version\n", run(Map.of(), "--version").out());
  }

  @Test
  void invalidUsageDoesNotCreateDatabase() {
    Path file = directory.resolve("unused.db");
    var environment = Map.of("DATABASE_URL", "jdbc:sqlite:" + file);
    for (String[] args :
        List.of(
            new String[] {"migrate-up", "--target-version", "999"},
            new String[] {"migrate-up", "--max-steps", "-1"},
            new String[] {"migrate-list", "--dry-run"},
            new String[] {"migrate-up", "--dry-run=false"},
            new String[] {"migrate-up", "--line", "pro"},
            new String[] {"migrate-up", "--schema", "bad-schema"},
            new String[] {"migrate-up", "--driver", "postgres"},
            new String[] {"migrate-up", "--database-url"},
            new String[] {"migrate-get", "--all", "--up", "--down"},
            new String[] {"migrate-get", "--version", "1,999", "--up"})) {
      var result = run(environment, args);
      assertEquals(2, result.exit(), result.err());
      assertTrue(result.out().isEmpty());
    }
    assertFalse(java.nio.file.Files.exists(file));
    assertEquals(2, run(Map.of(), "migrate-up").exit());
  }

  @Test
  void offlineSqlExportSelectsDialectDirectionAndSchema() {
    var sqlite =
        run(
            Map.of("DATABASE_URL", "postgres://localhost:1/unreachable"),
            "migrate-get",
            "--driver",
            "sqlite",
            "--version",
            "6",
            "--up");
    assertEquals(0, sqlite.exit(), sqlite.err());
    assertTrue(sqlite.out().contains("river_job"));
    var postgres =
        run(
            Map.of(),
            "migrate-get",
            "--all",
            "--exclude-version",
            "1",
            "--down",
            "--schema",
            "jobs");
    assertEquals(0, postgres.exit(), postgres.err());
    assertTrue(postgres.out().startsWith("-- River migration 008 [down]"));
    assertTrue(postgres.out().contains("\"jobs\"."));
    assertFalse(postgres.out().contains("migration 001 [down]"));
  }

  @Test
  void operationFailureHasNonzeroExit() {
    var result = run(Map.of("DATABASE_URL", "jdbc:sqlite:" + directory), "migrate-up");
    assertEquals(1, result.exit());
    assertTrue(result.err().contains("Error:"));
    assertTrue(result.out().isEmpty());
  }

  @Test
  void sqliteUrlForms() {
    for (String prefix : List.of("sqlite:", "sqlite://", "jdbc:sqlite:")) {
      var hint = run(Map.of(), "migrate-get", "--database-url", prefix, "--version", "1", "--up");
      assertEquals(0, hint.exit(), hint.err());
      assertTrue(hint.out().contains("id integer PRIMARY KEY"));
      Path file = directory.resolve("forms-" + prefix.length() + ".db");
      String relative = Path.of("").toAbsolutePath().relativize(file).toString();
      String url = prefix + relative;
      var exported = run(Map.of(), "migrate-get", "--database-url", url, "--version", "1", "--up");
      assertEquals(0, exported.exit(), exported.err());
      assertTrue(exported.out().contains("id integer PRIMARY KEY"));
      assertFalse(java.nio.file.Files.exists(file));
      var up = run(Map.of(), "migrate-up", "--database-url", url);
      assertEquals(0, up.exit(), up.err());
      assertTrue(java.nio.file.Files.exists(file));
      var listed = run(Map.of(), "migrate-list", "--database-url", "sqlite://" + file);
      assertEquals(0, listed.exit(), listed.err());
      assertTrue(listed.out().contains("008      applied"), listed.out());
      var memory = run(Map.of(), "migrate-list", "--database-url", prefix + ":memory:");
      assertEquals(0, memory.exit(), memory.err());
    }
  }

  @Test
  void targetsLimitsAndDryRunsPreserveExpectedHistory() {
    var environment = Map.of("DATABASE_URL", "jdbc:sqlite:" + directory.resolve("river.db"));
    assertEquals(0, run(environment, "migrate-up", "--dry-run", "--show-sql").exit());
    assertFalse(run(environment, "migrate-list").out().contains("applied"));
    assertEquals(0, run(environment, "migrate-up").exit());
    assertTrue(run(environment, "migrate-down").out().contains("008 [down]"));
    assertEquals(0, run(environment, "migrate-down", "--max-steps", "2").exit());
    assertEquals(
        5,
        new Migrator(Database.connect(environment.get("DATABASE_URL")))
            .list().stream().filter(Migrator.Status::applied).count());
    assertEquals(0, run(environment, "migrate-down", "--target-version", "0").exit());
    assertFalse(run(environment, "migrate-list").out().contains("applied"));
  }

  private record Result(int exit, String out, String err) {}
}
