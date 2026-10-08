import { randomUUID } from "node:crypto";

import { PgDriver } from "@riverqueue/driver-pg";
import pg from "pg";
import { afterEach, beforeEach, describe, expect, it } from "vitest";

import { createMigrator, type Migration } from "./index.js";

const TEST_DATABASE_URL =
  process.env.TEST_DATABASE_URL ??
  "postgres://localhost:5432/river_test?sslmode=disable";

const ALL_VERSIONS = [1, 2, 3, 4, 5, 6, 7, 8];

function versions(result: {
  versions: readonly { version: number }[];
}): number[] {
  return result.versions.map(({ version }) => version);
}

function quote(identifier: string): string {
  return `"${identifier.replaceAll('"', '""')}"`;
}

describe("Postgres migrator", () => {
  let pool: pg.Pool;
  let schema: string;
  let schemas: string[];

  async function createSchema(name: string): Promise<string> {
    schemas.push(name);
    await pool.query(`CREATE SCHEMA ${quote(name)}`);
    return name;
  }

  beforeEach(async () => {
    pool = new pg.Pool({ connectionString: TEST_DATABASE_URL });
    schemas = [];
    schema = await createSchema(
      `river_migrate_${randomUUID().replaceAll("-", "")}`
    );
  });

  afterEach(async () => {
    for (const name of schemas) {
      await pool.query(`DROP SCHEMA IF EXISTS ${quote(name)} CASCADE`);
    }
    await pool.end();
  });

  it("migrates up, down by steps and targets, and back to 0", async () => {
    // Mixed case, spaces, and a quote exercise identifier quoting.
    const schema = await createSchema(
      `River "Migrate" ${randomUUID().slice(0, 8)}`
    );
    const migrator = createMigrator({ pool, schema });

    const dryRun = await migrator.migrateUp({ dryRun: true });
    expect(versions(dryRun)).toEqual(ALL_VERSIONS);
    await expect(migrator.existingVersions()).resolves.toEqual([]);

    expect(versions(await migrator.migrateUp({ maxSteps: 3 }))).toEqual([
      1, 2, 3,
    ]);
    expect(versions(await migrator.migrateUp())).toEqual([4, 5, 6, 7, 8]);
    await expect(migrator.validate()).resolves.toEqual({
      messages: [],
      ok: true,
    });
    const tables = await pool.query<{ table_name: string }>(
      "SELECT table_name FROM information_schema.tables " +
        "WHERE table_schema = $1 AND table_name = 'river_job'",
      [schema]
    );
    expect(tables.rows).toEqual([{ table_name: "river_job" }]);

    expect(versions(await migrator.migrateDown())).toEqual([8]);
    expect(versions(await migrator.migrateDown({ targetVersion: 4 }))).toEqual([
      7, 6, 5,
    ]);
    await expect(migrator.existingVersions()).resolves.toEqual([1, 2, 3, 4]);
    expect(versions(await migrator.migrateDown({ targetVersion: 0 }))).toEqual([
      4, 3, 2, 1,
    ]);
    await expect(migrator.existingVersions()).resolves.toEqual([]);
    const remaining = await pool.query(
      "SELECT 1 FROM information_schema.tables WHERE table_schema = $1",
      [schema]
    );
    expect(remaining.rows).toEqual([]);
  });

  it("preserves existing job and queue rows across a down-up migration", async () => {
    const migrator = createMigrator({ pool, schema });
    await migrator.migrateUp();
    const inserted = await pool.query<{ id: string }>(
      `INSERT INTO ${quote(schema)}.river_job ` +
        "(args, kind, max_attempts, metadata, queue) " +
        "VALUES ($1, $2, $3, $4, $5) RETURNING id::text AS id",
      [
        { account: 42 },
        "migration_existing_job",
        10,
        { source: "old" },
        "saved",
      ]
    );
    await pool.query(
      `INSERT INTO ${quote(schema)}.river_queue (name, metadata) VALUES ($1, $2)`,
      ["saved", { source: "old" }]
    );

    expect(versions(await migrator.migrateDown())).toEqual([8]);
    expect(versions(await migrator.migrateUp())).toEqual([8]);
    await expect(migrator.validate()).resolves.toEqual({
      messages: [],
      ok: true,
    });

    const jobs = await pool.query<{
      args: { account: number };
      id: string;
      metadata: { source: string };
      queue: string;
    }>(
      `SELECT id::text AS id, args, metadata, queue FROM ${quote(schema)}.river_job`
    );
    const queues = await pool.query<{
      metadata: { source: string };
      name: string;
    }>(`SELECT name, metadata FROM ${quote(schema)}.river_queue`);
    expect(jobs.rows).toEqual([
      {
        args: { account: 42 },
        id: inserted.rows[0]?.id,
        metadata: { source: "old" },
        queue: "saved",
      },
    ]);
    expect(queues.rows).toEqual([
      { metadata: { source: "old" }, name: "saved" },
    ]);
  });

  it("migrates the database and schema of a PgDriver", async () => {
    const migrator = createMigrator(new PgDriver(pool, { schema }));

    expect(versions(await migrator.migrateUp())).toEqual(ALL_VERSIONS);
    await expect(
      createMigrator({ pool, schema }).existingVersions()
    ).resolves.toEqual(ALL_VERSIONS);
  });

  it("migrates the client and schema of a PgDriver of one client", async () => {
    const client = new pg.Client({ connectionString: TEST_DATABASE_URL });
    await client.connect();
    try {
      const migrator = createMigrator(new PgDriver(client, { schema }));

      expect(versions(await migrator.migrateUp())).toEqual(ALL_VERSIONS);
      await expect(
        createMigrator({ pool, schema }).existingVersions()
      ).resolves.toEqual(ALL_VERSIONS);
    } finally {
      await client.end();
    }
  });

  it("migrates through a dedicated client", async () => {
    const client = new pg.Client({ connectionString: TEST_DATABASE_URL });
    await client.connect();
    try {
      const migrator = createMigrator({ client, schema });

      expect(versions(await migrator.migrateUp())).toEqual(ALL_VERSIONS);
      expect(
        versions(await migrator.migrateDown({ targetVersion: 0 }))
      ).toEqual([...ALL_VERSIONS].reverse());
    } finally {
      await client.end();
    }
  });

  it("ignores applied versions newer than this package", async () => {
    const migrator = createMigrator({ pool, schema });
    await migrator.migrateUp();
    await pool.query(
      `INSERT INTO ${quote(schema)}.river_migration (line, version) VALUES ('main', 9)`
    );

    await expect(migrator.existingVersions()).resolves.toEqual([
      ...ALL_VERSIONS,
      9,
    ]);
    expect(versions(await migrator.migrateUp())).toEqual([]);
    await expect(migrator.validate()).resolves.toMatchObject({ ok: true });
    expect(versions(await migrator.migrateDown())).toEqual([8]);
  });

  it("migrates an additional line and rolls back a failed version", async () => {
    const extraMigrations: readonly Migration[] = [
      {
        downSql: "DROP TABLE /* TEMPLATE: schema */extra_widget;",
        name: "create_widget",
        upSql: "CREATE TABLE /* TEMPLATE: schema */extra_widget (id bigint);",
        version: 1,
      },
      {
        downSql: "SELECT 1;",
        name: "broken",
        upSql:
          "ALTER TABLE /* TEMPLATE: schema */extra_widget ADD COLUMN name text; " +
          "SELECT 1 / 0;",
        version: 2,
      },
    ];
    const main = createMigrator({ pool, schema });
    const extra = createMigrator(
      { pool, schema },
      { line: "extra", migrations: extraMigrations }
    );

    await expect(extra.migrateUp()).rejects.toThrow(
      'cannot migrate line "extra" until the main line is migrated'
    );
    await main.migrateUp();
    const failure = await extra.migrateUp().catch((error: unknown) => error);

    expect(failure).toMatchObject({
      backend: "postgres",
      message: 'failed to apply up migration 2 (broken) on line "extra"',
      operation: "apply",
    });
    await expect(extra.existingVersions()).resolves.toEqual([1]);
    const columns = await pool.query(
      "SELECT column_name FROM information_schema.columns " +
        "WHERE table_schema = $1 AND table_name = 'extra_widget'",
      [schema]
    );
    expect(columns.rows).toEqual([{ column_name: "id" }]);
    await expect(main.migrateDown({ targetVersion: 4 })).rejects.toThrow(
      "failed to apply down migration 5"
    );
    expect(versions(await extra.migrateDown({ targetVersion: 0 }))).toEqual([
      1,
    ]);
  });

  it("records an additional line's version 1 until it is reverted", async () => {
    const extraMigrations: readonly Migration[] = [
      {
        downSql: "DROP TABLE /* TEMPLATE: schema */extra_widget;",
        name: "create_widget",
        upSql: "CREATE TABLE /* TEMPLATE: schema */extra_widget (id bigint);",
        version: 1,
      },
      {
        downSql:
          "ALTER TABLE /* TEMPLATE: schema */extra_widget DROP COLUMN name;",
        name: "add_widget_name",
        upSql:
          "ALTER TABLE /* TEMPLATE: schema */extra_widget ADD COLUMN name text;",
        version: 2,
      },
    ];
    const main = createMigrator({ pool, schema });
    await main.migrateUp();
    const extra = createMigrator(
      { pool, schema },
      { line: "extra", migrations: extraMigrations }
    );
    const recorded = async (): Promise<number[]> =>
      (
        await pool.query<{ version: number | string }>(
          `SELECT version FROM ${quote(schema)}.river_migration ` +
            "WHERE line = 'extra' ORDER BY version"
        )
      ).rows.map(({ version }) => Number(version));
    const hasWidgetTable = async (): Promise<boolean> =>
      (
        await pool.query(
          "SELECT 1 FROM information_schema.tables " +
            "WHERE table_schema = $1 AND table_name = 'extra_widget'",
          [schema]
        )
      ).rows.length > 0;

    await extra.migrateUp();
    // Targeting version 1 stops before reverting it, so its row stays.
    expect(versions(await extra.migrateDown({ targetVersion: 1 }))).toEqual([
      2,
    ]);
    await expect(recorded()).resolves.toEqual([1]);
    await expect(hasWidgetTable()).resolves.toBe(true);

    // Stepping down from version 1 applies its down migration and removes
    // its row, unlike main version 1, whose down migration drops the table.
    expect(versions(await extra.migrateDown())).toEqual([1]);
    await expect(recorded()).resolves.toEqual([]);
    await expect(hasWidgetTable()).resolves.toBe(false);

    await extra.migrateUp();
    expect(versions(await extra.migrateDown({ targetVersion: 0 }))).toEqual([
      2, 1,
    ]);
    await expect(recorded()).resolves.toEqual([]);
    await expect(hasWidgetTable()).resolves.toBe(false);
    await expect(main.existingVersions()).resolves.toEqual(ALL_VERSIONS);
  });

  it("serializes concurrent migrators", async () => {
    const first = createMigrator({ pool, schema });
    const second = createMigrator({ pool, schema });

    const up = await Promise.all([first.migrateUp(), second.migrateUp()]);
    expect(up.flatMap(versions).sort((a, b) => a - b)).toEqual(ALL_VERSIONS);

    const down = await Promise.all([
      first.migrateDown({ targetVersion: 0 }),
      second.migrateDown({ targetVersion: 0 }),
    ]);
    expect(down.flatMap(versions).sort((a, b) => b - a)).toEqual(
      [...ALL_VERSIONS].reverse()
    );
    await expect(first.existingVersions()).resolves.toEqual([]);
  });

  it("locks by resolved schema, not by how the schema is spelled", async () => {
    const searchPathPool = new pg.Pool({
      connectionString: TEST_DATABASE_URL,
      options: `-c search_path=${schema}`,
    });
    const holder = await pool.connect();
    try {
      // Hold the lock that a migrator with an explicit schema would take.
      await holder.query("BEGIN");
      await holder.query(
        "SELECT pg_advisory_xact_lock(hashtext(current_database()::text), " +
          "hashtext('river_migration:' || $1::text))",
        [schema]
      );

      // This migrator names no schema, so it resolves it from search_path.
      const migrating = createMigrator({ pool: searchPathPool }).migrateUp({
        maxSteps: 1,
      });
      await waitForAdvisoryLockWaiter(pool);
      await expect(
        createMigrator({ pool, schema }).existingVersions()
      ).resolves.toEqual([]);

      await holder.query("COMMIT");
      expect(versions(await migrating)).toEqual([1]);
      await expect(
        createMigrator({ pool, schema }).existingVersions()
      ).resolves.toEqual([1]);
    } finally {
      await holder.query("ROLLBACK").catch(() => undefined);
      holder.release();
      await searchPathPool.end();
    }
  });
});

async function waitForAdvisoryLockWaiter(pool: pg.Pool): Promise<void> {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    const result = await pool.query(
      "SELECT 1 FROM pg_locks WHERE locktype = 'advisory' AND NOT granted " +
        "AND database = (SELECT oid FROM pg_database WHERE datname = current_database())"
    );
    if (result.rows.length > 0) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error("migrator never waited for the advisory lock");
}
