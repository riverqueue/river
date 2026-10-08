import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";

import { MigrationError, type ClientDriver } from "riverqueue";
import {
  registerDriver,
  type DriverMigrationTarget,
} from "riverqueue/unstable-driver";
import { afterEach, describe, expect, it } from "vitest";

import {
  createMigrator,
  loadMigrations,
  type Migration,
  type PgMigrationClient,
  type PgMigrationPool,
} from "./index.js";

const EXTRA_MIGRATIONS: readonly Migration[] = [
  {
    downSql: "DROP TABLE extra_widget;",
    name: "create_widget",
    upSql: "CREATE TABLE extra_widget (id INTEGER PRIMARY KEY);",
    version: 1,
  },
  {
    downSql: "ALTER TABLE extra_widget DROP COLUMN name;",
    name: "add_widget_name",
    upSql: "ALTER TABLE extra_widget ADD COLUMN name TEXT;",
    version: 2,
  },
];

function versions(result: {
  versions: readonly { version: number }[];
}): number[] {
  return result.versions.map(({ version }) => version);
}

/** A registered insert-only driver that migrates `migration`, if given. */
function registeredDriver(
  migration: DriverMigrationTarget | undefined
): ClientDriver<unknown, "insert"> {
  const operations = {
    jobInsert: () => Promise.reject(new Error("not used")),
    jobInsertMany: () => Promise.reject(new Error("not used")),
  };
  const driver: ClientDriver<unknown, "insert"> = Object.freeze(
    Object.create(null) as object
  );
  registerDriver(driver, {
    backend: "fake",
    capability: "insert",
    operations,
    ...(migration === undefined ? {} : { migration }),
  });
  return driver;
}

describe("createMigrator", () => {
  it("migrates a SQLite database", async () => {
    const database = new DatabaseSync(":memory:");
    try {
      const migrator = createMigrator({ database });

      expect(migrator.backend).toBe("sqlite");
      expect(migrator.line).toBe("main");
      expect(migrator.migrations).toEqual(loadMigrations("sqlite"));
      expect(versions(await migrator.migrateUp())).toHaveLength(8);
      await expect(
        createMigrator({ database }).existingVersions()
      ).resolves.toHaveLength(8);
    } finally {
      database.close();
    }
  });

  it("migrates the connection a River driver registered", async () => {
    const database = new DatabaseSync(":memory:");
    try {
      const driver = registeredDriver({ database });
      const migrator = createMigrator(driver);

      expect(migrator.backend).toBe("sqlite");
      expect(versions(await migrator.migrateUp())).toHaveLength(8);
      await expect(
        createMigrator({ database }).existingVersions()
      ).resolves.toHaveLength(8);
    } finally {
      database.close();
    }
  });

  it("rejects a River driver that can't migrate", () => {
    const driver = registeredDriver(undefined);

    expect(() => createMigrator(driver)).toThrow(
      "createMigrator requires a River driver that supports migrations"
    );
  });

  it.each([
    [null, "createMigrator requires a River driver"],
    [{}, "createMigrator requires a River driver"],
    [{ query: () => undefined }, "createMigrator requires a River driver"],
  ])("rejects an invalid source %#", (source, message) => {
    expect(() => createMigrator(source as never)).toThrow(MigrationError);
    expect(() => createMigrator(source as never)).toThrow(message);
  });

  it("rejects invalid Postgres schemas", () => {
    const pool = {} as PgMigrationPool;

    expect(() => createMigrator({ pool, schema: "" })).toThrow("non-empty");
    expect(() => createMigrator({ pool, schema: "bad\0schema" })).toThrow(
      "NUL"
    );
    expect(() => createMigrator({ pool, schema: "x".repeat(64) })).toThrow(
      "63 bytes"
    );
  });

  it("rejects invalid lines and migration sets", () => {
    const database = new DatabaseSync(":memory:");
    try {
      expect(() => createMigrator({ database }, { line: "" })).toThrow(
        "migration line must be a string of 1 to 127 characters"
      );
      expect(() => createMigrator({ database }, { line: "extra" })).toThrow(
        'migration line "extra" is not bundled'
      );
      expect(() =>
        createMigrator(
          { database },
          { line: "extra", migrations: EXTRA_MIGRATIONS.slice(1) }
        )
      ).toThrow("expected 1, received 2");
      expect(() =>
        createMigrator(
          { database },
          {
            line: "extra",
            migrations: [{ ...EXTRA_MIGRATIONS[0]!, upSql: " " }],
          }
        )
      ).toThrow("migration 1 must have a non-empty upSql");
    } finally {
      database.close();
    }
  });
});

describe("Postgres migrator", () => {
  it("renders a quoted custom schema for a dry run without connecting", async () => {
    const queries: { text: string; values?: readonly unknown[] }[] = [];
    const pool: PgMigrationPool = {
      async connect() {
        throw new Error("a dry run must not check out a connection");
      },
      // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-parameters -- implements the generic pool query
      async query<TRow>(text: string, values?: readonly unknown[]) {
        queries.push({ text, ...(values === undefined ? {} : { values }) });
        return {
          rows: [{ has_line_column: false, table_exists: false }] as TRow[],
        };
      },
    };
    const schema = 'tenant"one; DROP TABLE users; --';
    const migrator = createMigrator({ pool, schema });

    const result = await migrator.migrateUp({
      dryRun: true,
      targetVersion: 1,
    });

    expect(versions(result)).toEqual([1]);
    expect(result.versions[0]?.sql).toContain(
      'CREATE TABLE "tenant""one; DROP TABLE users; --".river_migration'
    );
    expect(result.versions[0]?.sql).not.toContain("/* TEMPLATE: schema */");
    expect(queries).toHaveLength(1);
    expect(queries[0]?.values).toEqual([
      '"tenant""one; DROP TABLE users; --"."river_migration"',
    ]);
  });

  for (const [server, product, locks] of [
    ["Postgres", "PostgreSQL 17.4 on aarch64-apple-darwin", true],
    [
      "YugabyteDB",
      "PostgreSQL 15.12-YB-2025.2.1.0-b1 on x86_64-pc-linux-gnu",
      false,
    ],
  ] as const) {
    it(`${locks ? "locks" : "doesn't lock"} each step on ${server}, like Go on YugabyteDB`, async () => {
      const queries: string[] = [];
      const client: PgMigrationClient = {
        // eslint-disable-next-line @typescript-eslint/no-unnecessary-type-parameters -- implements the generic client query
        async query<TRow>(text: string) {
          queries.push(text);
          const rows = text.startsWith("SELECT version()")
            ? [{ product }]
            : text.startsWith("SELECT to_regclass")
              ? [{ has_line_column: false, table_exists: false }]
              : [];
          return { rows: rows as TRow[] };
        },
      };

      await createMigrator({ client, schema: undefined }).migrateUp({
        targetVersion: 2,
      });

      expect(
        queries.filter((text) => text.includes("pg_advisory_xact_lock"))
      ).toHaveLength(locks ? 2 : 0);
      expect(
        queries.filter((text) => text.startsWith("SELECT version()"))
      ).toHaveLength(1);
    });
  }

  it("wraps read failures in MigrationError", async () => {
    const cause = new Error("connection refused");
    const pool: PgMigrationPool = {
      async connect() {
        throw cause;
      },
      async query() {
        throw cause;
      },
    };
    const migrator = createMigrator({ pool });

    await expect(migrator.existingVersions()).rejects.toMatchObject({
      backend: "postgres",
      cause,
      operation: "read_versions",
    });
    await expect(migrator.migrateUp()).rejects.toMatchObject({
      backend: "postgres",
      cause,
      operation: "connect",
    });
  });
});

describe("SQLite migrator", () => {
  let database: DatabaseSync | undefined;

  afterEach(() => {
    database?.close();
    database = undefined;
  });

  function openMigrator() {
    database = new DatabaseSync(":memory:");
    return { database, migrator: createMigrator({ database }) };
  }

  it("migrates up, validates, and reverts one version by default", async () => {
    const { migrator } = openMigrator();

    await expect(migrator.existingVersions()).resolves.toEqual([]);
    await expect(migrator.validate()).resolves.toEqual({
      messages: ["unapplied migrations: 1, 2, 3, 4, 5, 6, 7, 8"],
      ok: false,
    });

    const up = await migrator.migrateUp();
    expect(up.direction).toBe("up");
    expect(versions(up)).toEqual([1, 2, 3, 4, 5, 6, 7, 8]);
    await expect(migrator.validate()).resolves.toEqual({
      messages: [],
      ok: true,
    });
    await expect(migrator.migrateUp()).resolves.toEqual({
      direction: "up",
      versions: [],
    });

    const down = await migrator.migrateDown();
    expect(versions(down)).toEqual([8]);
    await expect(migrator.existingVersions()).resolves.toEqual([
      1, 2, 3, 4, 5, 6, 7,
    ]);
    await expect(migrator.validate({ targetVersion: 7 })).resolves.toEqual({
      messages: [],
      ok: true,
    });
  });

  it("supports targets, step limits, dry runs, and reverting to 0", async () => {
    const { migrator } = openMigrator();

    const dryRun = await migrator.migrateUp({ dryRun: true, targetVersion: 2 });
    expect(
      dryRun.versions.map(({ duration, version }) => ({
        duration: duration.toString(),
        version,
      }))
    ).toEqual([
      { duration: "PT0S", version: 1 },
      { duration: "PT0S", version: 2 },
    ]);
    await expect(migrator.existingVersions()).resolves.toEqual([]);
    expect(dryRun.versions[0]?.sql).toMatch(/^CREATE TABLE river_migration/m);
    expect(dryRun.versions[0]?.sql).not.toContain("TEMPLATE");

    expect(versions(await migrator.migrateUp({ maxSteps: 2 }))).toEqual([1, 2]);
    expect(versions(await migrator.migrateUp({ targetVersion: 5 }))).toEqual([
      3, 4, 5,
    ]);
    expect(versions(await migrator.migrateUp({ maxSteps: 0 }))).toEqual([]);
    expect(
      versions(await migrator.migrateDown({ dryRun: true, targetVersion: 0 }))
    ).toEqual([5, 4, 3, 2, 1]);
    await expect(migrator.existingVersions()).resolves.toEqual([1, 2, 3, 4, 5]);
    expect(versions(await migrator.migrateDown({ targetVersion: 3 }))).toEqual([
      5, 4,
    ]);
    expect(versions(await migrator.migrateDown({ targetVersion: 0 }))).toEqual([
      3, 2, 1,
    ]);
    await expect(migrator.existingVersions()).resolves.toEqual([]);
    await expect(migrator.migrateDown({ targetVersion: 0 })).resolves.toEqual({
      direction: "down",
      versions: [],
    });
  });

  it("rejects invalid options before touching the database", async () => {
    const { migrator } = openMigrator();

    await expect(migrator.migrateUp({ targetVersion: 0 })).rejects.toThrow(
      "targetVersion 0 is only valid when migrating down"
    );
    await expect(migrator.migrateDown({ targetVersion: 3 })).rejects.toThrow(
      "cannot migrate down to version 3 because it is not applied"
    );
    await expect(migrator.migrateUp({ maxSteps: -1 })).rejects.toMatchObject({
      operation: "plan",
    });
    await expect(migrator.validate({ targetVersion: 99 })).rejects.toThrow(
      "version 99 is not a migration version"
    );
  });

  it("ignores applied versions newer than this package", async () => {
    const { database, migrator } = openMigrator();
    await migrator.migrateUp();
    database
      .prepare("INSERT INTO river_migration (line, version) VALUES (?, ?)")
      .run("main", 9);

    await expect(migrator.existingVersions()).resolves.toEqual([
      1, 2, 3, 4, 5, 6, 7, 8, 9,
    ]);
    await expect(migrator.migrateUp()).resolves.toEqual({
      direction: "up",
      versions: [],
    });
    await expect(migrator.validate()).resolves.toEqual({
      messages: [],
      ok: true,
    });
    expect(versions(await migrator.migrateDown())).toEqual([8]);
    await expect(migrator.existingVersions()).resolves.toEqual([
      1, 2, 3, 4, 5, 6, 7, 9,
    ]);
  });

  it("never reuses a deleted job's ID after version 8", async () => {
    const { database, migrator } = openMigrator();
    await migrator.migrateUp({ targetVersion: 7 });
    const insertJob = (): bigint => {
      const statement = database.prepare(
        "INSERT INTO river_job (kind, max_attempts) VALUES ('id_reuse', 25) RETURNING id"
      );
      statement.setReadBigInts(true);
      return statement.get()!.id as bigint;
    };
    const beforeUpgrade = insertJob();

    expect(versions(await migrator.migrateUp())).toEqual([8]);
    expect(
      database.prepare("SELECT count(*) AS count FROM river_job").get()
    ).toEqual({ count: 1 });
    database.prepare("DELETE FROM river_job").run();

    expect(insertJob()).toBeGreaterThan(beforeUpgrade);
  });

  it.each(
    [
      "CREATE INDEX river_job_workflow_scheduling ON river_job (state)",
      "CREATE TABLE river_job_sequence (id integer PRIMARY KEY, key text)",
      "CREATE TABLE river_workflow (id text PRIMARY KEY)",
    ].flatMap((sql) => [["up", sql] as const, ["down", sql] as const])
  )("refuses version 8 %s once %s ran, like Go", async (direction, sql) => {
    const { database, migrator } = openMigrator();
    const version = direction === "up" ? 7 : 8;
    await migrator.migrateUp({ targetVersion: version });
    database.exec(sql);
    database.exec("ALTER TABLE river_job ADD COLUMN extension_column text");

    const migrate =
      direction === "up"
        ? migrator.migrateUp({ maxSteps: 1 })
        : migrator.migrateDown({ maxSteps: 1 });
    await expect(migrate).rejects.toMatchObject({
      cause: expect.objectContaining({
        message: expect.stringContaining(
          "River SQLite migration 008 cannot run"
        ),
      }),
    });
    await expect(migrator.existingVersions()).resolves.toHaveLength(version);
    expect(
      database
        .prepare(
          "SELECT count(*) AS count FROM pragma_table_info('river_job') WHERE name = 'extension_column'"
        )
        .get()
    ).toEqual({ count: 1 });
  });

  it("migrates an additional line once the main line is migrated", async () => {
    const { database, migrator: main } = openMigrator();
    const extra = createMigrator(
      { database },
      { line: "extra", migrations: EXTRA_MIGRATIONS }
    );

    await expect(extra.migrateUp()).rejects.toThrow(
      'cannot migrate line "extra" until the main line is migrated'
    );
    await main.migrateUp({ targetVersion: 4 });
    await expect(extra.existingVersions()).rejects.toMatchObject({
      operation: "read_versions",
    });
    await main.migrateUp();

    expect(versions(await extra.migrateUp())).toEqual([1, 2]);
    await expect(extra.existingVersions()).resolves.toEqual([1, 2]);
    await expect(main.existingVersions()).resolves.toEqual([
      1, 2, 3, 4, 5, 6, 7, 8,
    ]);
    await expect(main.migrateDown({ targetVersion: 4 })).rejects.toThrow(
      "main migration 5 cannot be reverted while other migration lines"
    );
    await expect(main.existingVersions()).resolves.toEqual([1, 2, 3, 4, 5]);

    expect(versions(await extra.migrateDown({ targetVersion: 0 }))).toEqual([
      2, 1,
    ]);
    await expect(extra.existingVersions()).resolves.toEqual([]);
    expect(versions(await main.migrateDown({ targetVersion: 0 }))).toEqual([
      5, 4, 3, 2, 1,
    ]);
  });

  it("records an additional line's version 1 until it is reverted", async () => {
    const { database, migrator: main } = openMigrator();
    await main.migrateUp();
    const extra = createMigrator(
      { database },
      { line: "extra", migrations: EXTRA_MIGRATIONS }
    );
    const recorded = (): unknown[] =>
      database
        .prepare(
          "SELECT version FROM river_migration WHERE line = ? ORDER BY version"
        )
        .all("extra")
        .map((row) => row.version);
    const hasWidgetTable = (): boolean =>
      database
        .prepare(
          "SELECT 1 FROM sqlite_master WHERE type = 'table' AND name = 'extra_widget'"
        )
        .get() !== undefined;

    await extra.migrateUp();
    // Targeting version 1 stops before reverting it, so its row stays.
    expect(versions(await extra.migrateDown({ targetVersion: 1 }))).toEqual([
      2,
    ]);
    expect(recorded()).toEqual([1]);
    expect(hasWidgetTable()).toBe(true);

    // Stepping down from version 1 applies its down migration and removes
    // its row, unlike main version 1, whose down migration drops the table.
    expect(versions(await extra.migrateDown())).toEqual([1]);
    expect(recorded()).toEqual([]);
    expect(hasWidgetTable()).toBe(false);

    await extra.migrateUp();
    expect(versions(await extra.migrateDown({ targetVersion: 0 }))).toEqual([
      2, 1,
    ]);
    expect(recorded()).toEqual([]);
    expect(hasWidgetTable()).toBe(false);
    await expect(main.existingVersions()).resolves.toEqual([
      1, 2, 3, 4, 5, 6, 7, 8,
    ]);
  });

  it("serializes concurrent migrators and applies each version once", async () => {
    const directory = mkdtempSync(join(tmpdir(), "river-js-migrate-"));
    const path = join(directory, "river.sqlite3");
    const firstDatabase = new DatabaseSync(path);
    const secondDatabase = new DatabaseSync(path);
    try {
      firstDatabase.exec("PRAGMA busy_timeout = 5000");
      secondDatabase.exec("PRAGMA busy_timeout = 5000");
      const first = createMigrator({ database: firstDatabase });
      const second = createMigrator({ database: secondDatabase });

      const up = await Promise.all([first.migrateUp(), second.migrateUp()]);
      expect(up.flatMap(versions).sort((a, b) => a - b)).toEqual([
        1, 2, 3, 4, 5, 6, 7, 8,
      ]);

      const down = await Promise.all([
        first.migrateDown({ targetVersion: 0 }),
        second.migrateDown({ targetVersion: 0 }),
      ]);
      expect(down.flatMap(versions).sort((a, b) => b - a)).toEqual([
        8, 7, 6, 5, 4, 3, 2, 1,
      ]);
      await expect(first.existingVersions()).resolves.toEqual([]);
      await expect(second.existingVersions()).resolves.toEqual([]);
    } finally {
      firstDatabase.close();
      secondDatabase.close();
      rmSync(directory, { force: true, recursive: true });
    }
  });
});
