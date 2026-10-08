/**
 * River's database migrations for Postgres and SQLite, and a runner that
 * applies them explicitly during deployment.
 *
 * @packageDocumentation
 */
export { loadMigrations } from "./bundle.js";
export type { Migration, MigrationBackend } from "./bundle.js";

export { MigrationError } from "riverqueue";
export { createMigrator, MIGRATION_LINE_MAIN } from "./migrator.js";
export type {
  MigrateOptions,
  MigrateResult,
  MigrateVersion,
  MigrationDirection,
  MigrationTarget,
  Migrator,
  MigratorOptions,
  MigratorSource,
  PgClientMigrationTarget,
  PgPoolMigrationTarget,
  SqliteMigrationTarget,
  ValidateOptions,
  ValidateResult,
} from "./migrator.js";
export type {
  PgMigrationClient,
  PgMigrationPool,
  PgMigrationPoolClient,
  PgMigrationQueryResult,
} from "./postgres.js";
