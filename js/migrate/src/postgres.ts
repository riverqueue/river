import { quoteIdentifier } from "riverqueue/unstable-driver";
import { Buffer } from "node:buffer";

import { MigrationError } from "riverqueue";

import {
  isInTargetState,
  requireLineStorage,
  versionRecord,
  type MigrationDirection,
} from "./plan.js";
import type {
  MigrationSession,
  MigrationStep,
  MigrationStorage,
} from "./storage.js";

/** Result shape that River reads from Postgres queries. */
export interface PgMigrationQueryResult<TRow> {
  rows: TRow[];
}

/**
 * A Postgres connection that can run River's migrations, such as a
 * node-postgres `Client` or a `PoolClient` checked out of a pool.
 *
 * The connection must not be inside a transaction: every migration version
 * runs in its own transaction.
 */
export interface PgMigrationClient {
  /** Run one SQL command, optionally with positional parameters. */
  query<TRow = Record<string, unknown>>(
    text: string,
    values?: readonly unknown[]
  ): Promise<PgMigrationQueryResult<TRow>>;
}

/** A connection checked out of a {@link PgMigrationPool}. */
export interface PgMigrationPoolClient extends PgMigrationClient {
  /** Return the connection, destroying it when `error` is given. */
  release(error?: Error): void;
}

/** A Postgres connection pool such as a node-postgres `Pool`. */
export interface PgMigrationPool {
  /** Check out a dedicated connection for migrating. */
  connect(): Promise<PgMigrationPoolClient>;
  /** Run one SQL command on any pooled connection. */
  query<TRow = Record<string, unknown>>(
    text: string,
    values?: readonly unknown[]
  ): Promise<PgMigrationQueryResult<TRow>>;
}

const POSTGRES_IDENTIFIER_MAX_BYTES = 63;
const TEMPLATE_SCHEMA = "/* TEMPLATE: schema */";

// Transaction-scoped so that it is released on commit or rollback and works
// through transaction-pooling proxies. The key uses the schema Postgres
// resolves rather than how the caller spelled it, so an omitted schema and
// its explicit name serialize against each other.
const LOCK_SQL =
  "SELECT pg_advisory_xact_lock(hashtext(current_database()::text), " +
  "hashtext('river_migration:' || coalesce($1::text, current_schema()::text, '')))";

const PRODUCT_SQL = "SELECT version()::text AS product";

const STORAGE_SQL =
  "SELECT to_regclass($1) IS NOT NULL AS table_exists, " +
  "EXISTS (SELECT 1 FROM pg_catalog.pg_attribute " +
  "WHERE attrelid = to_regclass($1) AND attname = 'line' " +
  "AND NOT attisdropped) AS has_line_column";

export class PgMigrationStorage implements MigrationStorage {
  readonly backend = "postgres" as const;
  readonly #connection:
    { readonly client: PgMigrationClient } | { readonly pool: PgMigrationPool };
  readonly #relation: string;
  readonly #schema: string | undefined;
  readonly #schemaPrefix: string;

  constructor(
    connection:
      | { readonly client: PgMigrationClient }
      | { readonly pool: PgMigrationPool },
    schema: string | undefined
  ) {
    if (schema !== undefined) validateSchema(schema);
    this.#connection = connection;
    this.#schema = schema;
    this.#schemaPrefix =
      schema === undefined ? "" : `${quoteIdentifier(schema)}.`;
    this.#relation = `${this.#schemaPrefix}${quoteIdentifier("river_migration")}`;
  }

  async readVersions(line: string): Promise<readonly number[]> {
    const connection = this.#connection;
    return this.#readVersions(
      "pool" in connection ? connection.pool : connection.client,
      line
    );
  }

  renderSql(sql: string): string {
    return sql.replaceAll(TEMPLATE_SCHEMA, this.#schemaPrefix);
  }

  async withSession<T>(
    run: (session: MigrationSession) => Promise<T>
  ): Promise<T> {
    const connection = this.#connection;
    if (!("pool" in connection)) {
      return run(this.#session(connection.client, () => undefined));
    }

    let client: PgMigrationPoolClient;
    try {
      client = await connection.pool.connect();
    } catch (error: unknown) {
      throw new MigrationError("failed to connect to Postgres", {
        backend: "postgres",
        operation: "connect",
        cause: error,
      });
    }
    let broken: Error | undefined;
    try {
      return await run(
        this.#session(client, (error) => {
          broken ??= error;
        })
      );
    } finally {
      client.release(broken);
    }
  }

  async #readVersions(
    queryable: PgMigrationClient,
    line: string
  ): Promise<readonly number[]> {
    const storage = await queryable.query<{
      has_line_column: boolean;
      table_exists: boolean;
    }>(STORAGE_SQL, [this.#relation]);
    const row = storage.rows[0];
    const tableExists = row?.table_exists === true;
    const hasLineColumn = row?.has_line_column === true;
    requireLineStorage("postgres", line, { hasLineColumn, tableExists });
    if (!tableExists) return [];

    const result = hasLineColumn
      ? await queryable.query<{ version: number | string }>(
          `SELECT version FROM ${this.#relation} WHERE line = $1 ORDER BY version`,
          [line]
        )
      : await queryable.query<{ version: number | string }>(
          `SELECT version FROM ${this.#relation} ORDER BY version`
        );
    return result.rows.map(({ version }) => toVersion(version));
  }

  #session(
    client: PgMigrationClient,
    markBroken: (error: Error) => void
  ): MigrationSession {
    let locks: Promise<boolean> | undefined;
    // YugabyteDB has advisory locks only behind a preview flag, and River
    // for Go's migrator takes none, so migrations there run unlocked like
    // Go's.
    const takesLock = (): Promise<boolean> =>
      (locks ??= client
        .query<{ product: string }>(PRODUCT_SQL)
        .then(({ rows }) => !isYugabyte(rows[0]?.product ?? "")));
    return {
      apply: async (step) => this.#apply(client, step, markBroken, takesLock),
      readVersions: async (line) => this.#readVersions(client, line),
    };
  }

  async #apply(
    client: PgMigrationClient,
    step: MigrationStep,
    markBroken: (error: Error) => void,
    takesLock: () => Promise<boolean>
  ): Promise<boolean> {
    const { direction, line, migration, sql } = step;
    let inTransaction = false;
    try {
      await client.query("BEGIN");
      inTransaction = true;
      if (await takesLock()) {
        await client.query(LOCK_SQL, [this.#schema ?? null]);
      }
      const applied = await this.#readVersions(client, line);
      if (isInTargetState(direction, applied, migration.version)) {
        // Another migrator finished this step while this one waited.
        await client.query("COMMIT");
        return false;
      }
      await client.query(sql);
      await this.#recordVersion(client, direction, line, migration.version);
      await client.query("COMMIT");
      return true;
    } catch (error: unknown) {
      if (inTransaction) {
        try {
          await client.query("ROLLBACK");
        } catch (rollbackError: unknown) {
          markBroken(
            rollbackError instanceof Error
              ? rollbackError
              : new Error(String(rollbackError))
          );
        }
      }
      throw new MigrationError(
        `failed to apply ${direction} migration ${migration.version} ` +
          `(${migration.name}) on line ${JSON.stringify(line)}`,
        { backend: "postgres", operation: "apply", cause: error }
      );
    }
  }

  async #recordVersion(
    client: PgMigrationClient,
    direction: MigrationDirection,
    line: string,
    version: number
  ): Promise<void> {
    const record = versionRecord(direction, line, version);
    const table = this.#relation;
    switch (record.kind) {
      case "delete":
        await client.query(
          `DELETE FROM ${table} WHERE line = $1 AND version = $2`,
          [record.line, record.version]
        );
        return;
      case "delete_without_line":
        await client.query(`DELETE FROM ${table} WHERE version = $1`, [
          record.version,
        ]);
        return;
      case "insert":
        await client.query(
          `INSERT INTO ${table} (line, version) VALUES ($1, $2)`,
          [record.line, record.version]
        );
        return;
      case "insert_without_line":
        await client.query(`INSERT INTO ${table} (version) VALUES ($1)`, [
          record.version,
        ]);
        return;
      case "none":
        return;
    }
  }
}

/** Throw unless `value` can name a Postgres schema. */
function validateSchema(value: string): void {
  const fail = (message: string): never => {
    throw new MigrationError(message, {
      backend: "postgres",
      operation: "configure",
    });
  };
  if (typeof value !== "string" || value.length === 0) {
    fail("Postgres schema must be a non-empty string");
  }
  if (value.includes("\0")) {
    fail("Postgres schema must not contain a NUL byte");
  }
  if (Buffer.byteLength(value, "utf8") > POSTGRES_IDENTIFIER_MAX_BYTES) {
    fail(
      `Postgres schema must not exceed ${POSTGRES_IDENTIFIER_MAX_BYTES} bytes`
    );
  }
}

function toVersion(value: number | string): number {
  const version = Number(value);
  if (!Number.isSafeInteger(version)) {
    throw new MigrationError(`invalid River migration version ${value}`, {
      backend: "postgres",
      operation: "read_versions",
    });
  }
  return version;
}

/** Whether `product`, the server's `version()`, names YugabyteDB. */
function isYugabyte(product: string): boolean {
  const lower = product.toLowerCase();
  return lower.includes("-yb") || lower.includes("yugabyte");
}
