import { DatabaseSync } from "node:sqlite";

import pg from "pg";

import {
  parseDuration,
  stringValue,
  UsageError,
  type OptionSpec,
  type OptionValues,
} from "./options.js";

/** Where a command's database lives, as given by `--database-url`. */
export type DatabaseLocation =
  | {
      readonly backend: "postgres";
      readonly connectionString: string | undefined;
    }
  | { readonly backend: "sqlite"; readonly path: string | URL };

// Match River's Go CLI: bound statements so a stuck lock can't hang a
// deploy. Settings in the database URL take precedence over these defaults,
// and --statement-timeout takes precedence over both.
const POSTGRES_DEFAULTS = {
  application_name: "riverqueue CLI",
  idle_in_transaction_session_timeout: 11_000,
  statement_timeout: 10_000,
} as const;

/** `--statement-timeout`, accepted by commands that use PostgreSQL. */
export const STATEMENT_TIMEOUT_OPTION: OptionSpec = {
  description:
    "PostgreSQL statement_timeout, such as 30s or 5m (default: a " +
    "statement_timeout parameter in --database-url, otherwise 10s)",
  type: "string",
  valueName: "DURATION",
};

const SQLITE_BUSY_TIMEOUT_MS = 5_000;

/**
 * Interpret `--database-url`.
 *
 * `postgres://` and `postgresql://` URLs select PostgreSQL. `sqlite://PATH`
 * selects SQLite, where `PATH` is a file path (`sqlite:///abs/river.db` or
 * `sqlite://relative/river.db`), `:memory:`, or a `file:` URL. Without a URL,
 * PostgreSQL is configured from `PG*` environment variables when
 * `PGDATABASE` is set, as node-postgres and River's Go CLI do.
 */
export function parseDatabaseUrl(
  command: string,
  url: string | undefined,
  env: NodeJS.ProcessEnv
): DatabaseLocation {
  if (url === undefined) {
    if (env.PGDATABASE !== undefined && env.PGDATABASE !== "") {
      return { backend: "postgres", connectionString: undefined };
    }
    throw new UsageError(
      "--database-url is required unless PGDATABASE and other PG* " +
        "environment variables configure PostgreSQL",
      command
    );
  }

  const scheme = /^([a-z][a-z0-9+.-]*):\/\//i.exec(url)?.[1]?.toLowerCase();
  switch (scheme) {
    case "postgres":
    case "postgresql":
      return { backend: "postgres", connectionString: url };
    case "sqlite": {
      const path = url.slice("sqlite://".length);
      if (path.length === 0) {
        throw new UsageError(
          "a SQLite --database-url needs a path, such as sqlite:///var/lib/river.db",
          command
        );
      }
      return {
        backend: "sqlite",
        path: path.startsWith("file:") ? new URL(path) : path,
      };
    }
    default:
      throw new UsageError(
        "--database-url must start with postgres://, postgresql://, or sqlite://",
        command
      );
  }
}

/** Read `--statement-timeout` in milliseconds. */
export function statementTimeoutValue(
  command: string,
  values: OptionValues
): number | undefined {
  const value = stringValue(values, "statement-timeout");
  return value === undefined
    ? undefined
    : parseDuration(command, "--statement-timeout", value);
}

/** Open a PostgreSQL pool for a CLI command. */
export function openPostgresPool(
  location: Extract<DatabaseLocation, { backend: "postgres" }>,
  options: {
    readonly max?: number | undefined;
    readonly statementTimeoutMs?: number | undefined;
  } = {}
): pg.Pool {
  const { statementTimeoutMs } = options;
  let connectionString = location.connectionString;
  // node-postgres lets URL parameters override pool options, so an explicit
  // timeout has to replace any in the URL.
  if (connectionString !== undefined && statementTimeoutMs !== undefined) {
    const url = new URL(connectionString);
    url.searchParams.set("statement_timeout", String(statementTimeoutMs));
    connectionString = url.toString();
  }
  return new pg.Pool({
    ...POSTGRES_DEFAULTS,
    ...(connectionString === undefined ? {} : { connectionString }),
    ...(options.max === undefined ? {} : { max: options.max }),
    ...(statementTimeoutMs === undefined
      ? {}
      : { statement_timeout: statementTimeoutMs }),
  });
}

export function openSqliteDatabase(
  location: Extract<DatabaseLocation, { backend: "sqlite" }>
): DatabaseSync {
  const database = new DatabaseSync(location.path);
  database.exec(`PRAGMA busy_timeout = ${SQLITE_BUSY_TIMEOUT_MS}`);
  return database;
}

/**
 * Describe a PostgreSQL target for confirmation prompts without exposing a
 * password.
 */
export function describePostgresTarget(connectionString: string): string {
  try {
    const url = new URL(connectionString);
    if (url.password !== "") url.password = "****";
    return url.toString();
  } catch {
    return "the database in --database-url";
  }
}
