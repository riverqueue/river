import { quoteIdentifier } from "riverqueue/unstable-driver";
import {
  createMigrator,
  loadMigrations,
  MIGRATION_LINE_MAIN,
  type Migration,
  type MigrationBackend,
  type MigrationDirection,
  type MigrateResult,
  type Migrator,
} from "@riverqueue/migrate";

import { writeLine, type Command, type CommandContext } from "./command.js";
import {
  openPostgresPool,
  openSqliteDatabase,
  parseDatabaseUrl,
  STATEMENT_TIMEOUT_OPTION,
  statementTimeoutValue,
} from "./database.js";
import {
  booleanValue,
  integerValue,
  parseInteger,
  stringValue,
  stringValues,
  UsageError,
  type OptionSpec,
  type OptionValues,
} from "./options.js";

const TEMPLATE_SCHEMA = "/* TEMPLATE: schema */";

const DATABASE_URL_OPTION: OptionSpec = {
  description:
    "Database to use: postgres://..., postgresql://..., or sqlite://PATH " +
    "(defaults to PG* environment variables when PGDATABASE is set)",
  type: "string",
  valueName: "URL",
};

const LINE_OPTION: OptionSpec = {
  description: `Migration line to use (default: ${MIGRATION_LINE_MAIN})`,
  type: "string",
  valueName: "NAME",
};

const SCHEMA_OPTION: OptionSpec = {
  description:
    "Postgres schema containing River's tables (default: the search_path)",
  type: "string",
  valueName: "NAME",
};

const MIGRATE_OPTIONS = {
  "database-url": DATABASE_URL_OPTION,
  "dry-run": {
    description: "Print the migrations that would run without running them",
    type: "boolean",
  },
  line: LINE_OPTION,
  "max-steps": {
    description: "Run at most N migrations",
    type: "string",
    valueName: "N",
  },
  schema: SCHEMA_OPTION,
  "show-sql": {
    description: "Print the SQL of each migration",
    type: "boolean",
  },
  "statement-timeout": STATEMENT_TIMEOUT_OPTION,
  "target-version": {
    description:
      "Version to end at; migrate-down reverts every version above it, and 0 reverts all of them",
    type: "string",
    valueName: "VERSION",
  },
} as const satisfies Record<string, OptionSpec>;

export const migrateDownCommand: Command = {
  description: `
Revert River migrations, newest first.

Reverts one migration by default. Use --max-steps or --target-version to
revert more; --target-version 0 reverts every migration and drops River's
tables and their data. Combine --dry-run and --show-sql to print the SQL
without running it.`,
  name: "migrate-down",
  options: MIGRATE_OPTIONS,
  summary: "Revert River migrations",
  run: async (values, context) => runMigrate("down", values, context),
};

export const migrateUpCommand: Command = {
  description: `
Apply River migrations that aren't applied yet, oldest first.

Applies every missing migration by default. Use --max-steps or
--target-version to apply fewer. Combine --dry-run and --show-sql to print
the SQL without running it.`,
  name: "migrate-up",
  options: MIGRATE_OPTIONS,
  summary: "Apply River migrations",
  run: async (values, context) => runMigrate("up", values, context),
};

export const migrateGetCommand: Command = {
  description: `
Print the SQL of River migrations for use with another migration tool.

Choose versions with --version (comma-separated or repeated) or --all, and
a direction with --up or --down. With --all, down migrations print newest
first. --exclude-version 1 skips the tables River uses to track its own
migrations. No database connection is made: --database-url only selects
Postgres (the default) or SQLite SQL.

  {program} migrate-get --version 3 --up > river_3.up.sql
  {program} migrate-get --all --exclude-version 1 --up > river.up.sql
  {program} migrate-get --all --down --database-url sqlite:// > river.down.sql`,
  name: "migrate-get",
  options: {
    all: { description: "Print every migration", type: "boolean" },
    "database-url": {
      description:
        "Selects the SQL dialect: postgres:// (default) or sqlite://",
      type: "string",
      valueName: "URL",
    },
    down: { description: "Print down migrations", type: "boolean" },
    "exclude-version": {
      description: "Leave out these versions (comma-separated or repeated)",
      multiple: true,
      type: "string",
      valueName: "VERSIONS",
    },
    line: LINE_OPTION,
    schema: SCHEMA_OPTION,
    up: { description: "Print up migrations", type: "boolean" },
    version: {
      description: "Versions to print (comma-separated or repeated)",
      multiple: true,
      type: "string",
      valueName: "VERSIONS",
    },
  },
  summary: "Print the SQL of River migrations",
  run: async (values, context) => runMigrateGet(values, context),
};

export const migrateListCommand: Command = {
  description: `
List River migrations, marking the newest one applied to the database with *.`,
  name: "migrate-list",
  options: {
    "database-url": DATABASE_URL_OPTION,
    line: LINE_OPTION,
    schema: SCHEMA_OPTION,
    "statement-timeout": STATEMENT_TIMEOUT_OPTION,
  },
  summary: "List River migrations and show which is applied",
  run: async (values, context) =>
    withMigrator("migrate-list", values, context, async (migrator) => {
      const existing = await migrator.existingVersions();
      const known = new Set(migrator.migrations.map(({ version }) => version));
      const current = Math.max(
        0,
        ...existing.filter((version) => known.has(version))
      );
      for (const { name, version } of migrator.migrations) {
        const prefix = version === current ? "* " : current > 0 ? "  " : "";
        writeLine(context.stdout, `${prefix}${formatVersion(version)} ${name}`);
      }
      const unknown = existing.filter((version) => !known.has(version));
      if (unknown.length > 0) {
        writeLine(
          context.stderr,
          `the database also has migration versions this riverqueue ` +
            `release doesn't know: ${unknown.join(", ")}`
        );
      }
      return 0;
    }),
};

export const validateCommand: Command = {
  description: `
Check that every River migration is applied. Exits with status 1 and lists the
missing versions if any are not.

Pair it with migrate-up --dry-run --show-sql to see the SQL that would fix it.`,
  name: "validate",
  options: {
    "database-url": DATABASE_URL_OPTION,
    line: LINE_OPTION,
    schema: SCHEMA_OPTION,
    "statement-timeout": STATEMENT_TIMEOUT_OPTION,
    "target-version": {
      description: "Only require versions up to and including VERSION",
      type: "string",
      valueName: "VERSION",
    },
  },
  summary: "Check that River migrations are applied",
  run: async (values, context) =>
    withMigrator("validate", values, context, async (migrator) => {
      const targetVersion = integerValue(
        "validate",
        values,
        "target-version",
        1
      );
      const result = await migrator.validate({ targetVersion });
      for (const message of result.messages) {
        writeLine(context.stderr, message);
      }
      return result.ok ? 0 : 1;
    }),
};

async function runMigrate(
  direction: MigrationDirection,
  values: OptionValues,
  context: CommandContext
): Promise<number> {
  const command = `migrate-${direction}`;
  const dryRun = booleanValue(values, "dry-run");
  const maxSteps = integerValue(command, values, "max-steps", 0);
  const showSql = booleanValue(values, "show-sql");
  const targetVersion = integerValue(command, values, "target-version", 0);
  return withMigrator(command, values, context, async (migrator) => {
    const options = { dryRun, maxSteps, targetVersion };
    const result =
      direction === "up"
        ? await migrator.migrateUp(options)
        : await migrator.migrateDown(options);
    printResult(context, migrator.line, result, { dryRun, maxSteps, showSql });
    return 0;
  });
}

function printResult(
  context: CommandContext,
  line: string,
  result: MigrateResult,
  options: {
    readonly dryRun: boolean;
    readonly maxSteps: number | undefined;
    readonly showSql: boolean;
  }
): void {
  const { direction, versions } = result;
  if (versions.length === 0) {
    writeLine(context.stdout, "no migrations to apply");
    return;
  }
  const nameWidth = Math.max(...versions.map(({ name }) => name.length));
  for (const { duration, name, sql, version } of versions) {
    const prefix = `${formatVersion(version)} [${direction}] ${name.padEnd(nameWidth)}`;
    writeLine(
      context.stdout,
      options.dryRun
        ? `migration ${prefix} [dry run]`
        : `applied migration ${prefix} [${formatDuration(duration.total("milliseconds"))}]`
    );
    if (options.showSql) {
      writeLine(context.stdout, "-".repeat(80));
      writeLine(context.stdout, migrationComment(line, version, direction));
      writeLine(context.stdout, sql.trim());
      writeLine(context.stdout);
    }
  }
  if (options.maxSteps !== undefined && versions.length < options.maxSteps) {
    writeLine(context.stdout, "no more migrations to apply");
  }
}

async function runMigrateGet(
  values: OptionValues,
  context: CommandContext
): Promise<number> {
  const command = "migrate-get";
  const all = booleanValue(values, "all");
  const down = booleanValue(values, "down");
  const up = booleanValue(values, "up");
  const requested = parseVersionList(command, "--version", values, "version");
  const excluded = new Set(
    parseVersionList(command, "--exclude-version", values, "exclude-version")
  );
  if (all === requested.length > 0) {
    throw new UsageError("pass exactly one of --all or --version", command);
  }
  if (down === up) {
    throw new UsageError("pass exactly one of --up or --down", command);
  }
  const line = resolveLine(command, context, stringValue(values, "line"));
  const backend = sqlDialect(command, stringValue(values, "database-url"));
  const schema = stringValue(values, "schema");
  if (backend === "sqlite" && schema !== undefined) {
    throw new UsageError("--schema only applies to Postgres", command);
  }

  const migrations =
    lineMigrations(context, line, backend) ?? loadMigrations(backend);
  const selected: Migration[] = all
    ? down
      ? [...migrations].reverse()
      : [...migrations]
    : requested.map((version) => {
        const migration = migrations.find((m) => m.version === version);
        if (migration === undefined) {
          throw new UsageError(
            `migration ${version} does not exist (available versions: ` +
              `${migrations.map((m) => m.version).join(", ")})`,
            command
          );
        }
        return migration;
      });

  const direction = down ? "down" : "up";
  const prefix = schema === undefined ? "" : `${quoteIdentifier(schema)}.`;
  let printed = false;
  for (const migration of selected) {
    if (excluded.has(migration.version)) continue;
    if (printed) writeLine(context.stdout);
    printed = true;
    const sql = down ? migration.downSql : migration.upSql;
    writeLine(
      context.stdout,
      migrationComment(line, migration.version, direction)
    );
    writeLine(context.stdout, sql.replaceAll(TEMPLATE_SCHEMA, prefix).trim());
  }
  return 0;
}

async function withMigrator(
  command: string,
  values: OptionValues,
  context: CommandContext,
  run: (migrator: Migrator) => Promise<number>
): Promise<number> {
  const line = resolveLine(command, context, stringValue(values, "line"));
  const schema = stringValue(values, "schema");
  const location = parseDatabaseUrl(
    command,
    stringValue(values, "database-url"),
    context.env
  );

  const statementTimeoutMs = statementTimeoutValue(command, values);

  if (location.backend === "sqlite") {
    for (const [flag, value] of [
      ["--schema", schema],
      ["--statement-timeout", statementTimeoutMs],
    ] as const) {
      if (value !== undefined) {
        throw new UsageError(`${flag} only applies to Postgres`, command);
      }
    }
    const database = openSqliteDatabase(location);
    try {
      return await run(
        createMigrator(
          { database },
          { line, migrations: lineMigrations(context, line, "sqlite") }
        )
      );
    } finally {
      database.close();
    }
  }

  const pool = openPostgresPool(location, { max: 2, statementTimeoutMs });
  try {
    return await run(
      createMigrator(
        { pool, schema },
        { line, migrations: lineMigrations(context, line, "postgres") }
      )
    );
  } finally {
    await pool.end();
  }
}

function formatDuration(milliseconds: number): string {
  return milliseconds < 1_000
    ? `${milliseconds.toFixed(2)}ms`
    : `${(milliseconds / 1_000).toFixed(2)}s`;
}

function formatVersion(version: number): string {
  return version.toString().padStart(3, "0");
}

function migrationComment(
  line: string,
  version: number,
  direction: MigrationDirection
): string {
  return `-- River ${line} migration ${formatVersion(version)} [${direction}]`;
}

function parseVersionList(
  command: string,
  flag: string,
  values: OptionValues,
  name: string
): number[] {
  return stringValues(values, name).flatMap((value) =>
    value.split(",").map((part) => parseInteger(command, flag, part, 1))
  );
}

/**
 * The migrations of an additional line, or undefined for River's bundled
 * main line.
 */
function lineMigrations(
  context: CommandContext,
  line: string,
  backend: MigrationBackend
): readonly Migration[] | undefined {
  return line === MIGRATION_LINE_MAIN
    ? undefined
    : context.migrationLines[line]?.(backend);
}

/** Check `--line` against the main line and any additional lines. */
function resolveLine(
  command: string,
  context: CommandContext,
  line: string | undefined
): string {
  if (
    line === undefined ||
    line === MIGRATION_LINE_MAIN ||
    Object.hasOwn(context.migrationLines, line)
  ) {
    return line ?? MIGRATION_LINE_MAIN;
  }
  const available = [
    MIGRATION_LINE_MAIN,
    ...Object.keys(context.migrationLines).sort(),
  ];
  throw new UsageError(
    `migration line does not exist: ${line} (available lines: ${available.join(", ")})`,
    command
  );
}

function sqlDialect(
  command: string,
  url: string | undefined
): MigrationBackend {
  if (url === undefined) return "postgres";
  const scheme = /^([a-z][a-z0-9+.-]*):\/\//i.exec(url)?.[1]?.toLowerCase();
  if (scheme === "postgres" || scheme === "postgresql") return "postgres";
  if (scheme === "sqlite") return "sqlite";
  throw new UsageError(
    "--database-url must start with postgres://, postgresql://, or sqlite://",
    command
  );
}
