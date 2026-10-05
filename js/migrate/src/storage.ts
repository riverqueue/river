import type { Migration, MigrationBackend } from "./bundle.js";
import type { MigrationDirection } from "./plan.js";

/** One migration version to apply in one direction. */
export interface MigrationStep {
  readonly direction: MigrationDirection;
  readonly line: string;
  readonly migration: Migration;
  /** SQL with any schema placeholder already rendered. */
  readonly sql: string;
}

/** Database operations used while applying migrations. */
export interface MigrationSession {
  /**
   * Apply one step and its `river_migration` bookkeeping atomically while
   * holding the backend's migration lock. Returns `false` without changing
   * anything when another migrator already applied the step.
   */
  apply(step: MigrationStep): Promise<boolean>;
  readVersions(line: string): Promise<readonly number[]>;
}

/** Backend-specific access to River's migration table. */
export interface MigrationStorage {
  readonly backend: MigrationBackend;
  /** Read the applied versions of `line` without holding a connection. */
  readVersions(line: string): Promise<readonly number[]>;
  /** Fill in placeholders such as the PostgreSQL schema. */
  renderSql(sql: string): string;
  /** Run `run` with a session bound to one connection. */
  withSession<T>(run: (session: MigrationSession) => Promise<T>): Promise<T>;
}
