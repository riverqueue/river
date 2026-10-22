import { MigrationError } from "riverqueue";
import { isInTargetState, requireLineStorage, versionRecord } from "./plan.js";
import type {
  MigrationSession,
  MigrationStep,
  MigrationStorage,
} from "./storage.js";

/** The mysql2 promise connection methods used by the migrator. */
export interface MySqlMigrationConnection {
  query(sql: string, values?: unknown[]): Promise<[unknown, unknown]>;
  release(): void;
}
/** A caller-owned mysql2 promise pool. */
export interface MySqlMigrationPool {
  getConnection(): Promise<MySqlMigrationConnection>;
}

/** MySQL DDL commits implicitly; the named lock spans all migration steps. */
export class MySqlMigrationStorage implements MigrationStorage {
  readonly backend = "mysql";
  readonly #pool: MySqlMigrationPool;
  readonly #schema: string | undefined;
  readonly #prefix: string;
  constructor(pool: MySqlMigrationPool, schema: string | undefined) {
    if (schema !== undefined && !/^[A-Za-z_][A-Za-z0-9_]{0,45}$/.test(schema))
      throw new MigrationError("Invalid MySQL schema name", {
        backend: "mysql",
        operation: "configure",
      });
    this.#pool = pool;
    this.#schema = schema;
    this.#prefix = schema === undefined ? "" : `\`${schema}\`.`;
  }
  renderSql(sql: string): string {
    return sql.replaceAll("/* TEMPLATE: schema */", this.#prefix);
  }
  async readVersions(line: string): Promise<readonly number[]> {
    const connection = await this.#pool.getConnection();
    try {
      return await this.#versions(connection, line);
    } finally {
      connection.release();
    }
  }
  async #versions(
    connection: MySqlMigrationConnection,
    line: string
  ): Promise<readonly number[]> {
    const rows = await query(
      connection,
      "SELECT column_name FROM information_schema.columns WHERE table_schema=COALESCE(?,DATABASE()) AND table_name='river_migration'",
      [this.#schema ?? null]
    );
    const tableExists = rows.length > 0;
    const hasLineColumn = rows.some((row) => row.column_name === "line");
    requireLineStorage("mysql", line, { tableExists, hasLineColumn });
    if (!tableExists) return [];
    return (
      await query(
        connection,
        `SELECT version FROM ${this.#prefix}river_migration${hasLineColumn ? " WHERE line=?" : ""} ORDER BY version`,
        hasLineColumn ? [line] : []
      )
    ).map((row) => Number(row.version));
  }
  async withSession<T>(
    run: (session: MigrationSession) => Promise<T>
  ): Promise<T> {
    const connection = await this.#pool.getConnection();
    let locked = false;
    try {
      const result = await query(
        connection,
        "SELECT GET_LOCK(CONCAT('river_migrate:',COALESCE(?,DATABASE())),0) AS acquired",
        [this.#schema ?? null]
      );
      if (Number(result[0]?.acquired) !== 1)
        throw new Error("Another migrator holds the database lock");
      locked = true;
      return await run({
        readVersions: (line) => this.#versions(connection, line),
        apply: (step) => this.#apply(connection, step),
      });
    } catch (cause) {
      if (cause instanceof MigrationError) throw cause;
      throw new MigrationError("MySQL migration failed", {
        backend: "mysql",
        operation: "migrate",
        cause,
      });
    } finally {
      try {
        if (locked)
          await connection.query(
            "SELECT RELEASE_LOCK(CONCAT('river_migrate:',COALESCE(?,DATABASE())))",
            [this.#schema ?? null]
          );
      } finally {
        connection.release();
      }
    }
  }
  async #apply(
    connection: MySqlMigrationConnection,
    step: MigrationStep
  ): Promise<boolean> {
    const { direction, line, migration } = step;
    if (
      isInTargetState(
        direction,
        await this.#versions(connection, line),
        migration.version
      )
    )
      return false;
    if (line === "main" && direction === "down" && migration.version <= 5) {
      const rows = await query(
        connection,
        "SELECT column_name FROM information_schema.columns WHERE table_schema=COALESCE(?,DATABASE()) AND table_name='river_migration' AND column_name='line'",
        [this.#schema ?? null]
      );
      if (
        rows.length &&
        (
          await query(
            connection,
            `SELECT 1 FROM ${this.#prefix}river_migration WHERE line<>'main' LIMIT 1`
          )
        ).length
      )
        throw new Error("Migrate other lines down before the main line");
    }
    if (direction === "up" && this.#schema !== undefined)
      await connection.query(
        `CREATE DATABASE IF NOT EXISTS \`${this.#schema}\``
      );
    for (const sql of statements(step.sql)) await connection.query(sql);
    // Record only successful DDL. MySQL cannot roll back partially applied DDL.
    const record = versionRecord(direction, line, migration.version);
    switch (record.kind) {
      case "none":
        break;
      case "insert":
        await connection.query(
          `INSERT INTO ${this.#prefix}river_migration(line,version) VALUES (?,?)`,
          [record.line, record.version]
        );
        break;
      case "insert_without_line":
        await connection.query(
          `INSERT INTO ${this.#prefix}river_migration(version) VALUES (?)`,
          [record.version]
        );
        break;
      case "delete":
        await connection.query(
          `DELETE FROM ${this.#prefix}river_migration WHERE line=? AND version=?`,
          [record.line, record.version]
        );
        break;
      case "delete_without_line":
        await connection.query(
          `DELETE FROM ${this.#prefix}river_migration WHERE version=?`,
          [record.version]
        );
        break;
    }
    return true;
  }
}

async function query(
  connection: MySqlMigrationConnection,
  sql: string,
  values: unknown[] = []
): Promise<Record<string, unknown>[]> {
  const [rows] = await connection.query(sql, values);
  return rows as Record<string, unknown>[];
}

/** Split scripts without requiring MULTI_STATEMENTS on application pools. */
function statements(sql: string): string[] {
  const tokens =
    sql.match(
      /'(?:''|\\.|[^'\\])*'|"(?:""|\\.|[^"\\])*"|`(?:``|[^`])*`|--[^\n]*|#[^\n]*|\/\*[\s\S]*?\*\/|;|[\s\S]/g
    ) ?? [];
  const result: string[] = [];
  let buffer = "";
  for (const token of tokens) {
    if (/^(--|#|\/\*)/.test(token)) buffer += " ";
    else if (token === ";") {
      if (buffer.trim()) result.push(buffer.trim());
      buffer = "";
    } else buffer += token;
  }
  if (buffer.trim()) result.push(buffer.trim());
  return result;
}
