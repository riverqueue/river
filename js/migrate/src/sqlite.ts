import type { DatabaseSync } from "node:sqlite";

import { MigrationError } from "riverqueue";

import {
  isInTargetState,
  MIGRATION_LINE_MAIN,
  requireLineStorage,
  versionRecord,
  type MigrationDirection,
} from "./plan.js";
import type {
  MigrationSession,
  MigrationStep,
  MigrationStorage,
} from "./storage.js";

// Reverting main version 5 rebuilds `river_migration` without its `line`
// column, which would lose other lines' history. PostgreSQL's SQL refuses
// this itself; SQLite cannot raise from plain SQL, so the migrator checks.
const LINE_COLUMN_VERSION = 5;
const TEMPLATE_SCHEMA = "/* TEMPLATE: schema */";

/**
 * Runs a synchronous migration attempt on a SQLite connection. A driver's
 * runner holds the driver's lock and, while another connection holds the
 * write lock, retries the whole attempt after an asynchronous backoff; the
 * attempt leaves no transaction open when it throws.
 */
export type SqliteMigrationRunner = <T>(
  attempt: (database: DatabaseSync) => T
) => Promise<T>;

export class SqliteMigrationStorage implements MigrationStorage {
  readonly backend = "sqlite" as const;
  readonly #run: SqliteMigrationRunner;

  /**
   * Without `run`, as for `{ database }`, statements run directly on the
   * handle, which waits for a busy database for its own `timeout`.
   */
  constructor(database: DatabaseSync, run?: SqliteMigrationRunner) {
    this.#run =
      run ??
      (<T>(attempt: (database: DatabaseSync) => T) =>
        Promise.resolve(attempt(database)));
  }

  async readVersions(line: string): Promise<readonly number[]> {
    return this.#readVersionsOnce(line);
  }

  renderSql(sql: string): string {
    // SQLite has no schemas, so the placeholder shared with PostgreSQL's SQL
    // renders as nothing.
    return sql.replaceAll(TEMPLATE_SCHEMA, "");
  }

  async withSession<T>(
    run: (session: MigrationSession) => Promise<T>
  ): Promise<T> {
    return run({
      apply: async (step) => this.#apply(step),
      readVersions: async (line) => this.#readVersionsOnce(line),
    });
  }

  async #apply(step: MigrationStep): Promise<boolean> {
    const { direction, line, migration } = step;
    try {
      return await this.#run((database) => this.#applyOnce(database, step));
    } catch (error: unknown) {
      if (error instanceof MigrationError) throw error;
      throw new MigrationError(
        `failed to apply ${direction} migration ${migration.version} ` +
          `(${migration.name}) on line ${JSON.stringify(line)}`,
        { backend: "sqlite", operation: "apply", cause: error }
      );
    }
  }

  /** Apply one step in its own transaction, rolling back when it throws. */
  #applyOnce(database: DatabaseSync, step: MigrationStep): boolean {
    const { direction, line, migration, sql } = step;
    // IMMEDIATE takes SQLite's write lock up front, which serializes
    // concurrent migrators on the same file.
    database.exec("BEGIN IMMEDIATE");
    try {
      const applied = readVersions(database, line);
      if (isInTargetState(direction, applied, migration.version)) {
        database.exec("COMMIT");
        return false;
      }
      if (
        direction === "down" &&
        line === MIGRATION_LINE_MAIN &&
        migration.version === LINE_COLUMN_VERSION &&
        hasOtherLines(database)
      ) {
        throw new MigrationError(
          "main migration 5 cannot be reverted while other migration lines " +
            "are applied because it would lose their history",
          { backend: "sqlite", operation: "apply" }
        );
      }
      database.exec(sql);
      recordVersion(database, direction, line, migration.version);
      database.exec("COMMIT");
      return true;
    } catch (error: unknown) {
      try {
        database.exec("ROLLBACK");
      } catch {
        // Keep the migration failure as the reported cause.
      }
      throw error;
    }
  }

  #readVersionsOnce(line: string): Promise<readonly number[]> {
    return this.#run((database) => readVersions(database, line));
  }
}

function exists(database: DatabaseSync, sql: string): boolean {
  const statement = database.prepare(sql);
  statement.setReadBigInts(true);
  return statement.get()?.value === 1n;
}

function hasOtherLines(database: DatabaseSync): boolean {
  const statement = database.prepare(
    "SELECT EXISTS (SELECT 1 FROM river_migration WHERE line <> ?) AS value"
  );
  statement.setReadBigInts(true);
  return statement.get(MIGRATION_LINE_MAIN)?.value === 1n;
}

function readVersions(database: DatabaseSync, line: string): readonly number[] {
  const tableExists = exists(
    database,
    "SELECT EXISTS (SELECT 1 FROM sqlite_schema " +
      "WHERE type = 'table' AND name = 'river_migration') AS value"
  );
  const hasLineColumn =
    tableExists &&
    exists(
      database,
      "SELECT EXISTS (SELECT 1 FROM pragma_table_info('river_migration') " +
        "WHERE name = 'line') AS value"
    );
  requireLineStorage("sqlite", line, { hasLineColumn, tableExists });
  if (!tableExists) return [];

  const statement = database.prepare(
    hasLineColumn
      ? "SELECT version FROM river_migration WHERE line = ? ORDER BY version"
      : "SELECT version FROM river_migration ORDER BY version"
  );
  statement.setReadBigInts(true);
  const rows = hasLineColumn ? statement.all(line) : statement.all();
  return rows.map(({ version }) => {
    if (typeof version !== "bigint") {
      throw new MigrationError(
        `invalid River migration version ${String(version)}`,
        { backend: "sqlite", operation: "read_versions" }
      );
    }
    return Number(version);
  });
}

function recordVersion(
  database: DatabaseSync,
  direction: MigrationDirection,
  line: string,
  version: number
): void {
  const record = versionRecord(direction, line, version);
  switch (record.kind) {
    case "delete":
      database
        .prepare("DELETE FROM river_migration WHERE line = ? AND version = ?")
        .run(record.line, record.version);
      return;
    case "delete_without_line":
      database
        .prepare("DELETE FROM river_migration WHERE version = ?")
        .run(record.version);
      return;
    case "insert":
      database
        .prepare("INSERT INTO river_migration (line, version) VALUES (?, ?)")
        .run(record.line, record.version);
      return;
    case "insert_without_line":
      database
        .prepare("INSERT INTO river_migration (version) VALUES (?)")
        .run(record.version);
      return;
    case "none":
      return;
  }
}
