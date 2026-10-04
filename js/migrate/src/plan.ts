import { MigrationError } from "riverqueue";

import type { Migration, MigrationBackend } from "./bundle.js";

export type MigrationDirection = "down" | "up";

/** Validated options for one migration run. */
export interface MigrationPlanOptions {
  readonly maxSteps: number | undefined;
  readonly targetVersion: number | undefined;
}

/**
 * Validate `maxSteps` and `targetVersion` before touching the database so
 * that mistakes surface even when there is nothing to migrate.
 */
export function validatePlanOptions(
  backend: MigrationBackend,
  migrations: readonly Migration[],
  direction: MigrationDirection,
  options: MigrationPlanOptions
): void {
  const { maxSteps, targetVersion } = options;
  if (
    maxSteps !== undefined &&
    (!Number.isSafeInteger(maxSteps) || maxSteps < 0)
  ) {
    throw planError(
      backend,
      `maxSteps must be a non-negative integer; received ${String(maxSteps)}`
    );
  }
  if (targetVersion === undefined) return;
  if (!Number.isSafeInteger(targetVersion) || targetVersion < 0) {
    throw planError(
      backend,
      `targetVersion must be a non-negative integer; received ${String(targetVersion)}`
    );
  }
  if (targetVersion === 0) {
    if (direction === "up") {
      throw planError(
        backend,
        "targetVersion 0 is only valid when migrating down, where it removes every version"
      );
    }
    return;
  }
  requireKnownVersion(backend, migrations, targetVersion);
}

/** Throw unless `version` is one of the line's migrations. */
export function requireKnownVersion(
  backend: MigrationBackend,
  migrations: readonly Migration[],
  version: number
): void {
  if (!migrations.some((migration) => migration.version === version)) {
    const available = migrations.map((migration) => migration.version);
    throw planError(
      backend,
      `version ${version} is not a migration version on this line ` +
        `(available versions: ${available.join(", ")})`
    );
  }
}

/**
 * Choose the migrations to run in order, following River's Go migrator.
 *
 * Applied versions that this package does not know about, such as versions
 * added by a newer River release, are ignored: up migrations apply only
 * missing known versions and down migrations revert only known versions.
 */
export function planMigrations(
  backend: MigrationBackend,
  migrations: readonly Migration[],
  direction: MigrationDirection,
  options: MigrationPlanOptions,
  appliedVersions: readonly number[]
): readonly Migration[] {
  const applied = new Set(appliedVersions);
  const { maxSteps, targetVersion } = options;
  let selected: readonly Migration[];
  let defaultMaxSteps: number | undefined;

  if (direction === "up") {
    // Like Go, an up migration whose target is already applied does
    // nothing, even with other versions missing.
    selected =
      targetVersion !== undefined && applied.has(targetVersion)
        ? []
        : migrations.filter(
            ({ version }) =>
              !applied.has(version) &&
              (targetVersion === undefined || version <= targetVersion)
          );
  } else {
    if (
      targetVersion !== undefined &&
      targetVersion !== 0 &&
      !applied.has(targetVersion)
    ) {
      throw planError(
        backend,
        `cannot migrate down to version ${targetVersion} because it is not applied`
      );
    }
    selected = migrations
      .filter(
        ({ version }) =>
          applied.has(version) &&
          (targetVersion === undefined || version > targetVersion)
      )
      .reverse();
    // Down migrations remove one version at a time unless a target says how
    // far to go, because reverting drops tables and their data.
    defaultMaxSteps = targetVersion === undefined ? 1 : undefined;
  }

  const limit = maxSteps ?? defaultMaxSteps;
  return limit === undefined ? selected : selected.slice(0, limit);
}

/** River's main migration line, bundled with this package. */
export const MIGRATION_LINE_MAIN = "main";

// Version that adds `river_migration.line`. Bookkeeping for main-line versions
// below it must not reference the column.
const LINE_COLUMN_VERSION = 5;

/** How to update `river_migration` after one migration step. */
export type VersionRecord =
  | { readonly kind: "delete"; readonly line: string; readonly version: number }
  | { readonly kind: "delete_without_line"; readonly version: number }
  | { readonly kind: "insert"; readonly line: string; readonly version: number }
  | { readonly kind: "insert_without_line"; readonly version: number }
  | { readonly kind: "none" };

/**
 * Describe the `river_migration` change that accompanies one step.
 *
 * Main-line versions before 5 predate the `line` column, and reverting main
 * version 1 drops the table itself, so neither may use the column.
 */
export function versionRecord(
  direction: MigrationDirection,
  line: string,
  version: number
): VersionRecord {
  const isMain = line === MIGRATION_LINE_MAIN;
  if (direction === "down") {
    if (isMain && version === 1) return { kind: "none" };
    return isMain && version <= LINE_COLUMN_VERSION
      ? { kind: "delete_without_line", version }
      : { kind: "delete", line, version };
  }
  return isMain && version < LINE_COLUMN_VERSION
    ? { kind: "insert_without_line", version }
    : { kind: "insert", line, version };
}

/**
 * Throw if a non-main line is used before the main line created a
 * `river_migration` table with a `line` column.
 */
export function requireLineStorage(
  backend: MigrationBackend,
  line: string,
  storage: { readonly hasLineColumn: boolean; readonly tableExists: boolean }
): void {
  if (line === MIGRATION_LINE_MAIN) return;
  if (!storage.tableExists || !storage.hasLineColumn) {
    throw new MigrationError(
      `cannot migrate line ${JSON.stringify(line)} until the main line is ` +
        "migrated to version 5 or later; migrate the main line and try again",
      { backend, operation: "read_versions" }
    );
  }
}

function planError(backend: MigrationBackend, message: string): MigrationError {
  return new MigrationError(message, { backend, operation: "plan" });
}

/** Whether `version` is already applied (up) or already reverted (down). */
export function isInTargetState(
  direction: MigrationDirection,
  applied: readonly number[],
  version: number
): boolean {
  return applied.includes(version) === (direction === "up");
}
