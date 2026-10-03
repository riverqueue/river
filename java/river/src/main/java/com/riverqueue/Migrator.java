package com.riverqueue;

import java.sql.Connection;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Objects;

/** Applies the exact, versioned River migration line shared with Go. */
public final class Migrator {
  public static final int LATEST = 8;
  private static final List<String> NAMES =
      List.of(
          "001_create_river_migration",
          "002_initial_schema",
          "003_river_job_tags_non_null",
          "004_pending_and_more",
          "005_migration_unique_client",
          "006_bulk_unique",
          "007_notification_outbox_sqlite_jsonb_and_sql_cleanup",
          "008_job_id_autoincrement");
  private final Database database;
  private final String line;
  private final List<Migration> migrations;

  public Migrator(Database database) {
    this(database, "main", mainMigrations(database.dialect()));
  }

  /** Internal companion migration seam used by the matched River Pro release. */
  public Migrator(Database database, String line, List<Migration> migrations) {
    this.database = Objects.requireNonNull(database);
    if (line == null || !line.matches("[a-z][a-z0-9_]*"))
      throw new IllegalArgumentException("Invalid migration line");
    this.line = line;
    this.migrations = List.copyOf(migrations);
    if (migrations.isEmpty()) throw new IllegalArgumentException("Migration line is empty");
    for (int i = 0; i < migrations.size(); i++) {
      if (migrations.get(i).version() != i + 1)
        throw new IllegalArgumentException("Migration versions must be consecutive starting at 1");
    }
  }

  private void apply(Connection connection, Migration migration, boolean down) throws SQLException {
    int version = migration.version();
    boolean main = line.equals("main");
    if (down && (!main || version > 1)) {
      String query = main && version < 5 ? "migration_delete_legacy" : "migration_line_delete";
      Object[] params = main && version < 5 ? new Object[] {version} : new Object[] {line, version};
      try (var statement = Sql.prepare(connection, Sql.query(database, query), params)) {
        statement.executeUpdate();
      }
    }
    execute(connection, sql(version, down ? Direction.DOWN : Direction.UP));
    if (!down) {
      String query = main && version < 5 ? "migration_insert_legacy" : "migration_line_insert";
      Object[] params = main && version < 5 ? new Object[] {version} : new Object[] {line, version};
      try (var statement = Sql.prepare(connection, Sql.query(database, query), params)) {
        statement.executeUpdate();
      }
    }
  }

  private boolean booleanQuery(Connection connection, String name) throws SQLException {
    try (var statement = Sql.prepare(connection, Sql.query(database, name));
        var rows = statement.executeQuery()) {
      rows.next();
      return rows.getBoolean(1);
    }
  }

  private void execute(Connection connection, String sql) throws SQLException {
    try (var statement = connection.createStatement()) {
      // Xerial's executeUpdate executes the entire script, including trigger bodies.
      if (database.dialect() == Database.Dialect.SQLITE) statement.executeUpdate(sql);
      else statement.execute(sql);
    }
  }

  private List<Integer> existing(Connection connection) throws SQLException {
    if (!booleanQuery(connection, "migration_exists")) return List.of();
    boolean hasLine = booleanQuery(connection, "migration_has_line");
    if (!hasLine && !line.equals("main")) return List.of();
    var versions = new ArrayList<Integer>();
    try (var statement =
            Sql.prepare(
                connection,
                Sql.query(
                    database, hasLine ? "migration_line_versions" : "migration_versions_legacy"),
                hasLine ? new Object[] {line} : new Object[0]);
        var rows = statement.executeQuery()) {
      while (rows.next()) versions.add(rows.getInt(1));
    }
    return List.copyOf(versions);
  }

  /** Returns the latest version bundled for this migration line. */
  public int latest() {
    return migrations.size();
  }

  /** Lists available and applied migrations without creating a schema or migration table. */
  public List<Status> list() {
    try (var connection = database.connection()) {
      var existing = existing(connection);
      var result = new ArrayList<Status>();
      for (var migration : migrations)
        result.add(
            new Status(
                migration.version(), migration.name(), existing.contains(migration.version())));
      for (int version : existing)
        if (version > latest()) result.add(new Status(version, "(unknown version)", true));
      return List.copyOf(result);
    } catch (SQLException error) {
      throw Client.databaseError("List " + line + " migrations", error);
    }
  }

  private static List<Migration> mainMigrations(Database.Dialect dialect) {
    String directory =
        "migration/" + (dialect == Database.Dialect.POSTGRES ? "postgres/" : "sqlite/");
    var migrations = new ArrayList<Migration>();
    for (int i = 0; i < NAMES.size(); i++) {
      String name = NAMES.get(i);
      migrations.add(
          new Migration(
              i + 1,
              name.substring(4),
              Sql.resource(directory + name + ".up.sql"),
              Sql.resource(directory + name + ".down.sql")));
    }
    return List.copyOf(migrations);
  }

  /** Applies all pending migrations for this line, committing each migration separately. */
  public Result migrate() {
    return migrate(Direction.UP);
  }

  /** Migrates up to the latest version, or down by one version. */
  public Result migrate(Direction direction) {
    return migrate(direction, Options.defaults());
  }

  /**
   * Applies or previews migrations. Target version zero removes all migrations when moving down.
   */
  public Result migrate(Direction direction, Options options) {
    Objects.requireNonNull(direction, "direction");
    Objects.requireNonNull(options, "options");
    boolean down = direction == Direction.DOWN;
    boolean dryRun = options.dryRun();
    int target = options.targetVersion() == null ? (down ? 0 : latest()) : options.targetVersion();
    int maxSteps =
        options.maxSteps() == null
            ? (down && options.targetVersion() == null ? 1 : Integer.MAX_VALUE)
            : options.maxSteps();
    if (target > latest())
      throw new IllegalArgumentException("Unknown migration version: " + target);
    try (var connection = database.connection()) {
      if (dryRun) {
        var existing = existing(connection);
        return new Result(plan(existing, down, target, maxSteps), existing, valid(existing));
      }
      Runnable restoreTransactionMode = () -> {};
      if (database.dialect() == Database.Dialect.SQLITE) {
        // Reserve the writer before reading migration history to serialize concurrent migrators.
        var config = connection.unwrap(org.sqlite.SQLiteConnection.class).getConnectionConfig();
        var previousMode = config.getTransactionMode();
        restoreTransactionMode = () -> config.setTransactionMode(previousMode);
        config.setTransactionMode(org.sqlite.SQLiteConfig.TransactionMode.IMMEDIATE);
      }
      boolean locked = false;
      try {
        connection.setAutoCommit(false);
        if (database.dialect() == Database.Dialect.POSTGRES) {
          execute(connection, Sql.query(database, "migration_lock"));
          locked = true;
        }
        var existing = existing(connection);
        var planned = plan(existing, down, target, maxSteps);
        if (!planned.isEmpty()) {
          if (!line.equals("main")) {
            var main = new Migrator(database).existing(connection);
            if (!main.contains(LATEST))
              throw new IllegalArgumentException(
                  "Migrate the main line to version " + LATEST + " first");
          } else if (down
              && booleanQuery(connection, "migration_has_line")
              && booleanQuery(connection, "migration_other_lines")) {
            throw new IllegalArgumentException("Migrate other lines down before the main line");
          }
          if (!database.schema().isEmpty() && !down)
            execute(connection, Sql.query(database, "create_schema"));
        }
        for (int version : planned) {
          apply(connection, migrations.get(version - 1), down);
          connection.commit();
        }
        var actual = existing(connection);
        connection.commit();
        return new Result(planned, actual, valid(actual));
      } catch (Exception error) {
        connection.rollback();
        throw error;
      } finally {
        restoreTransactionMode.run();
        if (locked) execute(connection, Sql.query(database, "migration_unlock"));
      }
    } catch (SQLException error) {
      throw Client.databaseError("Migrate " + line, error);
    }
  }

  private List<Integer> plan(List<Integer> existing, boolean down, int target, int maxSteps) {
    if (existing.stream().anyMatch(version -> version > latest()))
      throw new IllegalArgumentException(
          "Database contains newer " + line + " migrations; upgrade the CLI");
    var versions = new ArrayList<Integer>();
    for (int version = 1; version <= latest(); version++) {
      if (down
          ? existing.contains(version) && version > target
          : !existing.contains(version) && version <= target) versions.add(version);
    }
    if (down) Collections.reverse(versions);
    return List.copyOf(versions.subList(0, Math.min(maxSteps, versions.size())));
  }

  /**
   * Returns canonical migration SQL with the selected schema substituted; no connection is opened.
   */
  public String sql(int version, Direction direction) {
    Objects.requireNonNull(direction, "direction");
    if (version < 1 || version > latest())
      throw new IllegalArgumentException("Unknown migration version: " + version);
    var migration = migrations.get(version - 1);
    return (direction == Direction.DOWN ? migration.downSql() : migration.upSql())
        .replace("/* TEMPLATE: schema */", database.prefix());
  }

  private boolean valid(List<Integer> existing) {
    return existing.equals(migrations.stream().map(Migration::version).toList());
  }

  /** Direction in which to apply the migration line. */
  public enum Direction {
    DOWN,
    UP
  }

  /** Optional target and step limit; omitted values use the direction's defaults. */
  public record Options(Integer targetVersion, Integer maxSteps, boolean dryRun) {
    public Options {
      if (targetVersion != null && targetVersion < 0)
        throw new IllegalArgumentException("Target version must be nonnegative");
      if (maxSteps != null && maxSteps < 1)
        throw new IllegalArgumentException("Maximum steps must be positive");
    }

    public static Options defaults() {
      return new Options(null, null, false);
    }

    public Options dryRun(boolean value) {
      return new Options(targetVersion, maxSteps, value);
    }

    public Options maxSteps(int value) {
      return new Options(targetVersion, value, dryRun);
    }

    public Options targetVersion(int value) {
      return new Options(value, maxSteps, dryRun);
    }
  }

  /** One canonical migration and its forward and reverse SQL scripts. */
  public record Migration(int version, String name, String upSql, String downSql) {
    public Migration {
      Objects.requireNonNull(name);
      Objects.requireNonNull(upSql);
      Objects.requireNonNull(downSql);
    }
  }

  /**
   * Versions applied (or planned in a dry run), persisted versions, and whether the line is
   * current.
   */
  public record Result(List<Integer> applied, List<Integer> existing, boolean valid) {
    public Result {
      applied = List.copyOf(applied);
      existing = List.copyOf(existing);
    }
  }

  /** A migration's identity and whether it is recorded in the database. */
  public record Status(int version, String name, boolean applied) {}
}
