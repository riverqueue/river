import type { DatabaseSync } from "node:sqlite";

import { MigrationError, type ClientDriver } from "riverqueue";
import { driverMigrationTarget } from "riverqueue/unstable-driver";

import { loadMigrations } from "./bundle.js";
import type { Migration, MigrationBackend } from "./bundle.js";
import {
  MIGRATION_LINE_MAIN,
  planMigrations,
  requireKnownVersion,
  validatePlanOptions,
  type MigrationDirection,
} from "./plan.js";
import {
  PgMigrationStorage,
  type PgMigrationClient,
  type PgMigrationPool,
} from "./postgres.js";
import {
  SqliteMigrationStorage,
  type SqliteMigrationRunner,
} from "./sqlite.js";
import type { MigrationStorage } from "./storage.js";

export { MIGRATION_LINE_MAIN };
export type { MigrationDirection };

/** Options for {@link Migrator.migrateDown} and {@link Migrator.migrateUp}. */
export interface MigrateOptions {
  /**
   * Report the versions and SQL that would run without changing the
   * database. Defaults to `false`.
   */
  readonly dryRun?: boolean | undefined;
  /**
   * Run at most this many versions. `0` runs none, which is useful with
   * `dryRun` to check the plan.
   *
   * Up migrations are unlimited by default. Down migrations revert one
   * version by default, or every version above `targetVersion` when it is
   * set.
   */
  readonly maxSteps?: number | undefined;
  /**
   * Version the schema should end at.
   *
   * Migrating up applies missing versions up to and including this one; if
   * it is already applied, nothing runs. Migrating down reverts versions
   * above it, so it must be applied, or `0` to revert every version.
   */
  readonly targetVersion?: number | undefined;
}

export interface MigrateResult {
  readonly direction: MigrationDirection;
  /**
   * Versions applied (up) or reverted (down) in the order they ran. For a
   * dry run, the versions that would run.
   */
  readonly versions: readonly MigrateVersion[];
}

/** One version applied or reverted by a migrate call. */
export interface MigrateVersion {
  /** Time taken to run the version's SQL, or zero for a dry run. */
  readonly duration: Temporal.Duration;
  /** Short name of the migration, such as `bulk_unique`. */
  readonly name: string;
  /** SQL that ran, or would run, with the schema filled in. */
  readonly sql: string;
  readonly version: number;
}

/**
 * A migration runner for one database, schema, and migration line.
 *
 * Create one with {@link createMigrator}. Each version runs in its own
 * transaction together with its `river_migration` bookkeeping. Concurrent
 * migrators for the same schema serialize: Postgres uses a
 * transaction-scoped advisory lock and SQLite uses an immediate write
 * transaction, and a version finished by another migrator is skipped rather
 * than run twice.
 */
export interface Migrator {
  /** Database backend being migrated. */
  readonly backend: MigrationBackend;
  /** Migration line being migrated, such as {@link MIGRATION_LINE_MAIN}. */
  readonly line: string;
  /** Every migration version known for the line, ordered by version. */
  readonly migrations: readonly Migration[];

  /**
   * Read the versions of the line that are applied in the database, in
   * ascending order. The result can include versions newer than
   * {@link Migrator.migrations} if a newer River release migrated the
   * database.
   */
  existingVersions(): Promise<readonly number[]>;
  /**
   * Revert applied versions, newest first. Reverts one version unless
   * `maxSteps` or `targetVersion` says otherwise; `targetVersion: 0` removes
   * every version, dropping River's tables and their data.
   */
  migrateDown(options?: MigrateOptions): Promise<MigrateResult>;
  /** Apply missing versions, oldest first. Applies every version by default. */
  migrateUp(options?: MigrateOptions): Promise<MigrateResult>;
  /**
   * Check that every known version, or every version up to `targetVersion`,
   * is applied. Applied versions newer than this package are ignored.
   */
  validate(options?: ValidateOptions): Promise<ValidateResult>;
}

/** Options for {@link createMigrator}. */
export interface MigratorOptions {
  /**
   * Migration line to operate on. Defaults to {@link MIGRATION_LINE_MAIN}.
   *
   * Lines other than main require `migrations` and a database whose main
   * line is at version 5 or later.
   */
  readonly line?: string | undefined;
  /**
   * Migrations for `line`, which packages that ship their own migration
   * line provide. Versions must start at 1 and increase by 1. Defaults to
   * River's bundled main line for the backend.
   */
  readonly migrations?: readonly Migration[] | undefined;
}

/**
 * Something to build a migrator from: a River driver such as `PgDriver` or
 * `SqliteDriver`, or a connection given directly.
 */
export type MigratorSource = ClientDriver | MigrationTarget;

/** A database connection to migrate, given without a River driver. */
export type MigrationTarget =
  PgClientMigrationTarget | PgPoolMigrationTarget | SqliteMigrationTarget;

/** Migrate Postgres through one dedicated connection. */
export interface PgClientMigrationTarget {
  /**
   * A connected node-postgres `Client` or `PoolClient` that is not inside a
   * transaction. The caller keeps ownership and closes it.
   */
  readonly client: PgMigrationClient;
  /**
   * Schema containing River's tables. Defaults to the connection's
   * `search_path`. Must match the schema given to `PgDriver`.
   */
  readonly schema?: string | undefined;
}

/** Migrate Postgres through a connection pool. */
export interface PgPoolMigrationTarget {
  /**
   * A node-postgres `Pool`. The migrator checks out one connection per call
   * and never ends the pool.
   */
  readonly pool: PgMigrationPool;
  /**
   * Schema containing River's tables. Defaults to the connection's
   * `search_path`. Must match the schema given to `PgDriver`.
   */
  readonly schema?: string | undefined;
}

/** Migrate a `node:sqlite` database. */
export interface SqliteMigrationTarget {
  /** An open database. The caller keeps ownership and closes it. */
  readonly database: DatabaseSync;
}

/** Options for {@link Migrator.validate}. */
export interface ValidateOptions {
  /** Only require versions up to and including this one. */
  readonly targetVersion?: number | undefined;
}

/** The result of {@link Migrator.validate}. */
export interface ValidateResult {
  /** Why validation failed. Empty when `ok` is `true`. */
  readonly messages: readonly string[];
  /** Whether every required version is applied. */
  readonly ok: boolean;
}

const INVALID_SOURCE_MESSAGE =
  "createMigrator requires a River driver that supports migrations, { pool }, { client }, or { database }";
const LINE_MAX_LENGTH = 127;

/**
 * Create a migrator for a River driver or a database connection.
 *
 * Passing the driver used by the client migrates the same database and
 * schema, so the schema is configured in one place:
 *
 * ```ts
 * const driver = new PgDriver(pool, { schema: "river" });
 * await createMigrator(driver).migrateUp();
 * ```
 *
 * Deploy scripts can pass a connection instead, such as `{ pool, schema }`,
 * `{ client, schema }`, or `{ database }` for SQLite.
 *
 * Migrations never run implicitly; run them as a deployment step, before
 * starting workers.
 *
 * @throws {@link MigrationError} if the source or options are invalid.
 */
export function createMigrator(
  source: MigratorSource,
  options: MigratorOptions = {}
): Migrator {
  const target = resolveTarget(source);
  const storage: MigrationStorage =
    "database" in target
      ? new SqliteMigrationStorage(target.database, target.run)
      : new PgMigrationStorage(
          "pool" in target ? { pool: target.pool } : { client: target.client },
          target.schema
        );
  const line = options.line ?? MIGRATION_LINE_MAIN;
  validateLine(storage.backend, line);
  const migrations =
    options.migrations === undefined
      ? line === MIGRATION_LINE_MAIN
        ? loadMigrations(storage.backend)
        : configurationError(
            storage.backend,
            `migration line ${JSON.stringify(line)} is not bundled with ` +
              "@riverqueue/migrate; pass its migrations"
          )
      : copyMigrations(storage.backend, options.migrations);
  return new StorageMigrator(storage, line, migrations);
}

class StorageMigrator implements Migrator {
  readonly backend: MigrationBackend;
  readonly line: string;
  readonly migrations: readonly Migration[];
  readonly #storage: MigrationStorage;

  constructor(
    storage: MigrationStorage,
    line: string,
    migrations: readonly Migration[]
  ) {
    this.backend = storage.backend;
    this.line = line;
    this.migrations = migrations;
    this.#storage = storage;
  }

  async existingVersions(): Promise<readonly number[]> {
    return this.#readVersions();
  }

  async migrateDown(options: MigrateOptions = {}): Promise<MigrateResult> {
    return this.#migrate("down", options);
  }

  async migrateUp(options: MigrateOptions = {}): Promise<MigrateResult> {
    return this.#migrate("up", options);
  }

  async validate(options: ValidateOptions = {}): Promise<ValidateResult> {
    const { targetVersion } = options;
    if (targetVersion !== undefined) {
      requireKnownVersion(this.backend, this.migrations, targetVersion);
    }
    const applied = new Set(await this.#readVersions());
    const missing = this.migrations
      .map(({ version }) => version)
      .filter(
        (version) =>
          (targetVersion === undefined || version <= targetVersion) &&
          !applied.has(version)
      );
    return missing.length === 0
      ? { messages: [], ok: true }
      : {
          messages: [`unapplied migrations: ${missing.join(", ")}`],
          ok: false,
        };
  }

  async #migrate(
    direction: MigrationDirection,
    options: MigrateOptions
  ): Promise<MigrateResult> {
    const plan = {
      maxSteps: options.maxSteps,
      targetVersion: options.targetVersion,
    };
    validatePlanOptions(this.backend, this.migrations, direction, plan);
    const dryRun = options.dryRun ?? false;
    if (typeof dryRun !== "boolean") {
      configurationError(this.backend, "dryRun must be a boolean");
    }

    if (dryRun) {
      const selected = planMigrations(
        this.backend,
        this.migrations,
        direction,
        plan,
        await this.#readVersions()
      );
      return {
        direction,
        versions: selected.map((migration) => ({
          duration: new Temporal.Duration(),
          name: migration.name,
          sql: this.#sql(direction, migration),
          version: migration.version,
        })),
      };
    }

    return this.#storage.withSession(async (session) => {
      const selected = planMigrations(
        this.backend,
        this.migrations,
        direction,
        plan,
        await this.#wrapRead(session.readVersions(this.line))
      );
      const versions: MigrateVersion[] = [];
      for (const migration of selected) {
        const sql = this.#sql(direction, migration);
        const startedAt = performance.now();
        const applied = await session.apply({
          direction,
          line: this.line,
          migration,
          sql,
        });
        if (!applied) continue;
        versions.push({
          duration: measuredDuration(performance.now() - startedAt),
          name: migration.name,
          sql,
          version: migration.version,
        });
      }
      return { direction, versions };
    });
  }

  async #readVersions(): Promise<readonly number[]> {
    return this.#wrapRead(this.#storage.readVersions(this.line));
  }

  #sql(direction: MigrationDirection, migration: Migration): string {
    return this.#storage.renderSql(
      direction === "up" ? migration.upSql : migration.downSql
    );
  }

  async #wrapRead(
    read: Promise<readonly number[]>
  ): Promise<readonly number[]> {
    try {
      return await read;
    } catch (error: unknown) {
      if (error instanceof MigrationError) throw error;
      throw new MigrationError("failed to read applied River migrations", {
        backend: this.backend,
        operation: "read_versions",
        cause: error,
      });
    }
  }
}

function configurationError(backend: string, message: string): never {
  throw new MigrationError(message, { backend, operation: "configure" });
}

function copyMigrations(
  backend: MigrationBackend,
  migrations: readonly Migration[]
): readonly Migration[] {
  const value: unknown = migrations;
  if (!Array.isArray(value) || migrations.length === 0) {
    configurationError(backend, "migrations must be a non-empty array");
  }
  return Object.freeze(
    migrations.map((migration, index) => {
      const expected = index + 1;
      if (migration.version !== expected) {
        configurationError(
          backend,
          `migration versions must start at 1 and increase by 1; ` +
            `expected ${expected}, received ${String(migration.version)}`
        );
      }
      for (const field of ["downSql", "name", "upSql"] as const) {
        const value: unknown = migration[field];
        if (typeof value !== "string" || value.trim().length === 0) {
          configurationError(
            backend,
            `migration ${expected} must have a non-empty ${field}`
          );
        }
      }
      return Object.freeze({
        downSql: migration.downSql,
        name: migration.name,
        upSql: migration.upSql,
        version: migration.version,
      });
    })
  );
}

function resolveTarget(
  source: MigratorSource
):
  | PgClientMigrationTarget
  | PgPoolMigrationTarget
  | (SqliteMigrationTarget & { readonly run?: SqliteMigrationRunner }) {
  // A registered driver says which connection it migrates, and for SQLite
  // how to run on it under the driver's lock; its own properties are never
  // read.
  const registered = driverMigrationTarget(source);
  if (registered !== undefined) {
    if (!("database" in registered)) return registered as MigrationTarget;
    return {
      database: registered.database as DatabaseSync,
      ...(registered.run === undefined
        ? {}
        : { run: registered.run as SqliteMigrationRunner }),
    };
  }
  if (typeof source === "object" && (source as unknown) !== null) {
    if ("database" in source) return { database: source.database };
    if ("pool" in source) {
      return { pool: source.pool, schema: source.schema };
    }
    if ("client" in source) {
      return { client: source.client, schema: source.schema };
    }
  }
  return configurationError("unknown", INVALID_SOURCE_MESSAGE);
}

function validateLine(backend: MigrationBackend, line: string): void {
  if (
    typeof line !== "string" ||
    line.length === 0 ||
    line.length > LINE_MAX_LENGTH
  ) {
    configurationError(
      backend,
      `migration line must be a string of 1 to ${LINE_MAX_LENGTH} characters`
    );
  }
}

/** A measured time in fractional milliseconds as a balanced duration. */
function measuredDuration(milliseconds: number): Temporal.Duration {
  return Temporal.Duration.from({
    nanoseconds: Math.max(0, Math.round(milliseconds * 1_000_000)),
  }).round({ largestUnit: "hours" });
}
