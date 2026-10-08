package com.riverqueue.cli;

import com.riverqueue.Database;
import com.riverqueue.Migrator;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.BiFunction;

/** Migration command runner shared with the matched River Pro CLI release. */
public final class MigrationCli {
  private static final Set<String> CONNECTION = Set.of("database-url", "driver", "line", "schema");
  private static final Set<String> FLAGS = Set.of("all", "down", "dry-run", "show-sql", "up");
  private static final Set<String> VALUES =
      Set.of(
          "database-url",
          "driver",
          "exclude-version",
          "line",
          "max-steps",
          "schema",
          "target-version",
          "version");

  private MigrationCli() {}

  private static Database database(
      Options options, Map<String, String> environment, boolean offline) {
    String url = options.values.getOrDefault("database-url", environment.get("DATABASE_URL"));
    String driver = options.values.get("driver");
    if (driver != null && !Set.of("postgres", "sqlite").contains(driver))
      throw new Usage("--driver must be postgres or sqlite");
    if (offline && driver != null && !options.values.containsKey("database-url")) url = null;
    if (url == null || url.isBlank()) {
      if (!offline) throw new Usage("Set DATABASE_URL or pass --database-url");
      url = "sqlite".equals(driver) ? "jdbc:sqlite::memory:" : "jdbc:postgresql://localhost/unused";
    }
    if (!url.startsWith("postgres://")
        && !url.startsWith("postgresql://")
        && !url.startsWith("jdbc:postgresql:")
        && !url.startsWith("sqlite:")
        && !url.startsWith("jdbc:sqlite:"))
      throw new Usage("Expected a Postgres URL or a SQLite JDBC URL");
    Database database;
    try {
      database =
          Database.connect(url, "river-java-cli")
              .withSchema(options.values.getOrDefault("schema", ""));
    } catch (IllegalArgumentException error) {
      throw new Usage("Invalid database URL or schema");
    }
    if (driver != null
        && !driver.equals(database.dialect() == Database.Dialect.SQLITE ? "sqlite" : "postgres"))
      throw new Usage("--driver does not match the database URL");
    return database;
  }

  private static void export(Options options, Migrator migrator, PrintWriter out) {
    if (options.flags.contains("up") == options.flags.contains("down"))
      throw new Usage("migrate-get requires exactly one of --up or --down");
    if (options.flags.contains("all") == options.values.containsKey("version"))
      throw new Usage("migrate-get requires exactly one of --version or --all");
    if (options.values.containsKey("exclude-version") && !options.flags.contains("all"))
      throw new Usage("--exclude-version requires --all");
    boolean down = options.flags.contains("down");
    var versions = new ArrayList<Integer>();
    if (options.flags.contains("all")) {
      var excluded = numbers(options.values.getOrDefault("exclude-version", ""));
      for (int version = 1; version <= migrator.latest(); version++)
        if (!excluded.contains(version)) versions.add(version);
      if (down) java.util.Collections.reverse(versions);
    } else versions.addAll(numbers(options.values.get("version")));
    if (versions.stream().anyMatch(version -> version > migrator.latest()))
      throw new Usage("Unknown migration version");
    // Validate every requested version before writing SQL that may be piped into another tool.
    var scripts =
        versions.stream()
            .map(
                version ->
                    migrator.sql(version, down ? Migrator.Direction.DOWN : Migrator.Direction.UP))
            .toList();
    for (int i = 0; i < versions.size(); i++) {
      out.printf("-- River migration %03d [%s]%n", versions.get(i), down ? "down" : "up");
      out.println(scripts.get(i).strip());
      out.println();
    }
  }

  private static void help(PrintWriter out, String name, List<String> lines) {
    out.printf("Usage: %s <command> [options]%n%n", name);
    out.println("Commands:");
    out.println("  migrate-up     Apply pending migrations (all by default)");
    out.println("  migrate-down   Reverse migrations (one by default; removes schema/data)");
    out.println("  migrate-list   List migration versions and their applied status");
    out.println("  migrate-get    Print canonical SQL without connecting to a database");
    out.println("  version        Print CLI version");
    out.println();
    out.println("Connection options:");
    out.println("  --database-url URL    Postgres or SQLite JDBC URL; defaults to DATABASE_URL");
    out.println(
        "  --driver DRIVER       postgres or sqlite; selects dialect for offline SQL export");
    out.println("  --schema NAME         Postgres schema (default: connection's current schema)");
    out.printf(
        "  --line NAME           Migration line: %s (default: main)%n", String.join(", ", lines));
    out.println();
    out.println("migrate-up / migrate-down:");
    out.println("  --target-version N    Stop at N; down to 0 removes the entire line");
    out.println("  --max-steps N         Limit migration count (0 means the command default)");
    out.println("  --dry-run             Preview without applying migrations or creating schemas");
    out.println("  --show-sql            Include SQL for the applied or planned migrations");
    out.println();
    out.println("migrate-get:");
    out.println("  --up | --down         Required direction");
    out.println("  --version N[,N...]    Select versions, in the supplied order");
    out.println("  --all                 Select all versions, in migration order");
    out.println("  --exclude-version N   Exclude comma-separated versions when using --all");
    out.println();
    out.println("Use --help for help and --version for the CLI version.");
    out.println("Exit codes: 0 success, 1 operation failed, 2 invalid usage.");
  }

  /** Launches the OSS migration CLI. */
  public static void main(String[] args) {
    String version = MigrationCli.class.getPackage().getImplementationVersion();
    System.exit(
        run(
            args,
            System.getenv(),
            new PrintWriter(System.out, true),
            new PrintWriter(System.err, true),
            "river",
            version == null ? "development" : version,
            List.of("main"),
            (database, line) -> new Migrator(database)));
  }

  private static int number(String value, String name) {
    try {
      int result = Integer.parseInt(value);
      if (result < 0) throw new NumberFormatException();
      return result;
    } catch (NumberFormatException error) {
      throw new Usage(name + " must be a nonnegative integer");
    }
  }

  private static List<Integer> numbers(String source) {
    if (source.isEmpty()) return List.of();
    var result = new ArrayList<Integer>();
    for (String entry : source.split(",", -1)) {
      int version = number(entry, "Migration version");
      if (version == 0 || result.contains(version))
        throw new Usage("Migration versions must be positive and distinct");
      result.add(version);
    }
    return result;
  }

  private static Options parse(String[] args) {
    String command = args[0];
    Set<String> allowed = new HashSet<>(CONNECTION);
    switch (command) {
      case "migrate-down", "migrate-up" ->
          allowed.addAll(Set.of("dry-run", "max-steps", "show-sql", "target-version"));
      case "migrate-get" ->
          allowed.addAll(Set.of("all", "down", "exclude-version", "up", "version"));
      case "migrate-list" -> {}
      default -> throw new Usage("Unknown command: " + command);
    }
    var flags = new HashSet<String>();
    var values = new HashMap<String, String>();
    for (int i = 1; i < args.length; i++) {
      if (!args[i].startsWith("--")) throw new Usage("Expected an option, got: " + args[i]);
      String[] option = args[i].substring(2).split("=", 2);
      String name = option[0];
      if (!allowed.contains(name))
        throw new Usage("Unsupported option for " + command + ": --" + name);
      if (flags.contains(name) || values.containsKey(name))
        throw new Usage("Repeated option: --" + name);
      if (FLAGS.contains(name)) {
        if (option.length != 1) throw new Usage("--" + name + " does not take a value");
        flags.add(name);
      } else if (VALUES.contains(name)) {
        String value;
        if (option.length == 2) value = option[1];
        else {
          if (++i == args.length || args[i].startsWith("--"))
            throw new Usage("Missing value for --" + name);
          value = args[i];
        }
        if (value.isBlank()) throw new Usage("Missing value for --" + name);
        values.put(name, value);
      }
    }
    return new Options(command, Set.copyOf(flags), Map.copyOf(values));
  }

  /**
   * Runs a CLI invocation with explicit I/O and a migration-line factory; does not terminate the
   * JVM.
   */
  public static int run(
      String[] args,
      Map<String, String> environment,
      PrintWriter out,
      PrintWriter err,
      String name,
      String version,
      List<String> lines,
      BiFunction<Database, String, Migrator> factory) {
    String databaseUrl = environment.get("DATABASE_URL");
    try {
      if (args.length == 0
          || Arrays.asList(args).contains("--help")
          || Arrays.asList(args).contains("-h")) {
        help(out, name, lines);
        return 0;
      }
      if (args.length == 1 && (args[0].equals("version") || args[0].equals("--version"))) {
        out.println(name + " " + version);
        return 0;
      }
      var options = parse(args);
      String line = options.values.getOrDefault("line", "main");
      if (!lines.contains(line)) throw new Usage("Unknown migration line: " + line);
      databaseUrl = options.values.getOrDefault("database-url", databaseUrl);
      var database = database(options, environment, options.command.equals("migrate-get"));
      var migrator = factory.apply(database, line);
      switch (options.command) {
        case "migrate-get" -> export(options, migrator, out);
        case "migrate-list" -> {
          out.println("VERSION  STATE    NAME");
          for (var migration : migrator.list())
            out.printf(
                "%03d      %-7s  %s%n",
                migration.version(), migration.applied() ? "applied" : "pending", migration.name());
        }
        default -> {
          boolean down = options.command.equals("migrate-down");
          boolean targetSet = options.values.containsKey("target-version");
          int target =
              targetSet
                  ? number(options.values.get("target-version"), "--target-version")
                  : down ? 0 : migrator.latest();
          if (target > migrator.latest()) throw new Usage("Unknown migration version: " + target);
          int steps = number(options.values.getOrDefault("max-steps", "0"), "--max-steps");
          if (steps == 0) steps = down && !targetSet ? 1 : Integer.MAX_VALUE;
          boolean dryRun = options.flags.contains("dry-run");
          var result =
              migrator.migrate(
                  down ? Migrator.Direction.DOWN : Migrator.Direction.UP,
                  Migrator.Options.defaults().targetVersion(target).maxSteps(steps).dryRun(dryRun));
          for (int applied : result.applied()) {
            out.printf(
                "%s %s %03d [%s]%n",
                dryRun ? "Would apply" : "Applied", line, applied, down ? "down" : "up");
            if (options.flags.contains("show-sql"))
              out.println(
                  migrator
                      .sql(applied, down ? Migrator.Direction.DOWN : Migrator.Direction.UP)
                      .strip());
          }
          if (result.applied().isEmpty()) out.println("No migrations to apply");
        }
      }
      return 0;
    } catch (Usage error) {
      err.println("Error: " + error.getMessage());
      err.println("Run " + name + " --help for usage");
      return 2;
    } catch (Exception error) {
      String message =
          error.getMessage() == null ? error.getClass().getSimpleName() : error.getMessage();
      if (databaseUrl != null && !databaseUrl.isEmpty())
        message = message.replace(databaseUrl, "<database>");
      err.println("Error: " + message);
      return 1;
    } finally {
      out.flush();
      err.flush();
    }
  }

  private record Options(String command, Set<String> flags, Map<String, String> values) {}

  private static final class Usage extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    Usage(String message) {
      super(message);
    }
  }
}
