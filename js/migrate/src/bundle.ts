import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";

import { MigrationError } from "riverqueue";

/** Database backend that a migration bundle targets. */
export type MigrationBackend = "postgres" | "sqlite";

/** One River migration version with SQL for both directions. */
export interface Migration {
  readonly downSql: string;
  /** Short name derived from the migration file name, such as `bulk_unique`. */
  readonly name: string;
  readonly upSql: string;
  /** Positive version number. A line starts at 1 and increases by 1. */
  readonly version: number;
}

interface ManifestEntry {
  file: string;
  sha256: string;
}

interface MigrationManifest {
  backends: Record<MigrationBackend, ManifestEntry[]>;
  format: number;
}

const MIGRATION_FILE_RE =
  /^(?<version>\d{3})_(?<name>.+)\.(?<direction>up|down)\.sql$/;

/**
 * Load River's bundled main migration line for a backend, ordered by version.
 *
 * The SQL files ship with this package and are verified against recorded
 * checksums on every load. Postgres SQL contains a schema placeholder that
 * a migrator fills in, so run migrations through {@link createMigrator}
 * instead of executing this SQL directly.
 *
 * @throws {@link MigrationError} if the bundled files are missing or altered.
 */
export function loadMigrations(
  backend: MigrationBackend
): readonly Migration[] {
  if (!["postgres", "sqlite"].includes(backend)) {
    throw new MigrationError(
      `unsupported migration backend: ${JSON.stringify(backend)}`,
      { backend, operation: "load" }
    );
  }
  const fail = (message: string, cause?: unknown): never => {
    throw new MigrationError(message, {
      backend,
      operation: "load",
      ...(cause === undefined ? {} : { cause }),
    });
  };
  const root = new URL("../migrations/", import.meta.url);
  const read = (path: string) => {
    try {
      return readFileSync(new URL(path, root));
    } catch (error: unknown) {
      return fail(`failed to read bundled migration file ${path}`, error);
    }
  };

  const manifest = JSON.parse(
    read("manifest.json").toString("utf8")
  ) as MigrationManifest;
  if (manifest.format !== 1) {
    fail(`unsupported River migration manifest format: ${manifest.format}`);
  }

  const partial = new Map<
    number,
    { downSql?: string; name: string; upSql?: string }
  >();
  for (const { file, sha256 } of manifest.backends[backend]) {
    const groups = MIGRATION_FILE_RE.exec(file)?.groups;
    const direction = groups?.direction;
    const name = groups?.name;
    const versionText = groups?.version;
    if (
      (direction !== "down" && direction !== "up") ||
      name === undefined ||
      versionText === undefined
    ) {
      return fail(`invalid bundled migration file name: ${file}`);
    }

    const version = Number.parseInt(versionText, 10);
    const entry = partial.get(version) ?? { name };
    if (entry.name !== name) {
      fail(`migration ${version} has mismatched up and down names`);
    }
    const contents = read(`${backend}/main/${file}`);
    const actualHash = createHash("sha256").update(contents).digest("hex");
    if (actualHash !== sha256) {
      fail(`bundled migration checksum mismatch for ${backend}/${file}`);
    }
    entry[`${direction}Sql`] = contents.toString("utf8");
    partial.set(version, entry);
  }

  return [...partial.entries()]
    .sort(([left], [right]) => left - right)
    .map(([version, migration]) => {
      if (migration.downSql === undefined || migration.upSql === undefined) {
        return fail(`migration ${version} is missing an up or down direction`);
      }
      return Object.freeze({
        downSql: migration.downSql,
        name: migration.name,
        upSql: migration.upSql,
        version,
      });
    });
}
