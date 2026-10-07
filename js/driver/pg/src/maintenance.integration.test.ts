import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import pg from "pg";
import { Client, defineJob, type RiverEvent, Workers } from "riverqueue";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";

import { PgDriver } from "./driver.js";

const TEST_DATABASE_URL =
  process.env.TEST_DATABASE_URL ??
  "postgres://localhost:5432/river_test?sslmode=disable";
const filePrefix = `js_pgmaint_${Math.random().toString(36).slice(2, 10)}`;
const migrationDirectory = fileURLToPath(
  new URL("../../../migrate/migrations/postgres/main/", import.meta.url)
);

async function migrateSchema(pool: pg.Pool, schema: string): Promise<void> {
  const migrationFiles = (await readdir(migrationDirectory))
    .filter((name) => name.endsWith(".up.sql"))
    .sort();
  for (const migrationFile of migrationFiles) {
    const migration = await readFile(
      join(migrationDirectory, migrationFile),
      "utf8"
    );
    await pool.query(
      migration.replaceAll("/* TEMPLATE: schema */", `"${schema}".`)
    );
  }
}

// Leader-owned maintenance acts on every row and contends for the one
// leadership row, so this file owns a schema of its own.
describe("Postgres maintenance", () => {
  const schema = `${filePrefix}_schema`;
  const idle = defineJob({ kind: `${filePrefix}_idle` });
  let pool: pg.Pool;

  beforeAll(async () => {
    pool = new pg.Pool({ connectionString: TEST_DATABASE_URL });
    await pool.query(`CREATE SCHEMA "${schema}"`);
    await migrateSchema(pool, schema);
  });

  afterAll(async () => {
    await pool.query(`DROP SCHEMA "${schema}" CASCADE`);
    await pool.end();
  });

  afterEach(async () => {
    await pool.query(
      `TRUNCATE "${schema}".river_job, "${schema}".river_leader, "${schema}".river_queue`
    );
  });

  function client(options: ConstructorParameters<typeof Client>[1] = {}) {
    return new Client(new PgDriver(pool, { schema }), {
      queues: { [`${filePrefix}_idle`]: { maxWorkers: 1 } },
      workers: new Workers().add(idle, () => undefined),
      ...options,
    });
  }

  it("keeps renewing the same term while the job cleaner waits on a row lock", async () => {
    const inserted = await pool.query<{ id: string }>(
      `INSERT INTO "${schema}".river_job
         (args, finalized_at, kind, max_attempts, queue, state)
       VALUES ('{}', now() - interval '2 days', $1, 25, 'default', 'completed')
       RETURNING id`,
      [`${filePrefix}_finalized`]
    );
    const jobId = inserted.rows[0]!.id;
    const blocker = await pool.connect();
    await blocker.query("BEGIN");
    await blocker.query(
      `SELECT id FROM "${schema}".river_job WHERE id = $1 FOR UPDATE`,
      [jobId]
    );
    let blocking = true;
    const run = await client({
      maintenance: {
        electionInterval: { milliseconds: 200 },
        jobCleanerTimeout: null,
      },
    }).start();
    try {
      await waitFor(() => run.diagnostics.maintenance?.isLeader === true);
      // The cleaner's delete waits on the locked row inside the leader's
      // fenced transaction.
      await waitFor(async () => {
        const waiting = await pool.query(
          `SELECT 1 FROM pg_stat_activity
           WHERE wait_event_type = 'Lock' AND query LIKE $1`,
          [`%DELETE FROM "${schema}"."river_job"%`]
        );
        return (waiting.rowCount ?? 0) > 0;
      }, 5_000);

      const terms = new Map<string, Set<string>>();
      await waitFor(async () => {
        const leader = await pool.query<{
          elected_at: string;
          expires_at: string;
        }>(
          `SELECT elected_at::text AS elected_at, expires_at::text AS expires_at
           FROM "${schema}".river_leader`
        );
        const row = leader.rows[0];
        if (row !== undefined) {
          const expiries = terms.get(row.elected_at) ?? new Set<string>();
          expiries.add(row.expires_at);
          terms.set(row.elected_at, expiries);
        }
        // The first expiry plus at least two renewals.
        return (terms.values().next().value?.size ?? 0) >= 3;
      }, 5_000);

      // One term throughout, renewed while the cleaner stayed blocked.
      expect(terms.size).toBe(1);
      expect(run.diagnostics.maintenance?.isLeader).toBe(true);
      expect(run.diagnostics.maintenance?.runs.job_cleaner).toBe(0);
      expect(await jobExists(pool, schema, jobId)).toBe(true);

      await blocker.query("ROLLBACK");
      blocking = false;
      await waitFor(async () => !(await jobExists(pool, schema, jobId)));
      expect(run.diagnostics.maintenance?.runs.job_cleaner).toBe(1);
    } finally {
      if (blocking) await blocker.query("ROLLBACK");
      blocker.release();
      await run.stop();
    }
  }, 20_000);

  it("reindexer skips a missing index and one with a leftover artifact", async () => {
    const artifact = "river_job_prioritized_fetching_index_ccnew1";
    await pool.query(
      `CREATE INDEX "${artifact}" ON "${schema}".river_job (id)`
    );
    const rebuilt = [
      "river_job_kind",
      "river_job_state_and_finalized_at_index",
    ];
    const skipped = "river_job_prioritized_fetching_index";
    const before = await relfilenodes(pool, schema);
    const events: RiverEvent[] = [];
    let schedules = 0;
    const run = await client({
      hooks: {
        onEvent: (event) => {
          events.push(event);
        },
      },
      maintenance: {
        reindexerIndexNames: ["river_job_does_not_exist", ...rebuilt, skipped],
        // Once soon after the term starts, then not again during the test.
        reindexerSchedule: (after) =>
          after.add(schedules++ === 0 ? { milliseconds: 50 } : { hours: 24 }),
      },
    }).start();
    try {
      await waitFor(
        () => (run.diagnostics.maintenance?.runs.reindexer ?? 0) >= 1,
        10_000
      );
    } finally {
      await run.stop();
    }

    expect(events).toContainEqual(
      expect.objectContaining({
        count: rebuilt.length,
        kind: "maintenance_succeeded",
        service: "reindexer",
      })
    );
    const after = await relfilenodes(pool, schema);
    for (const index of rebuilt) {
      expect(after.get(index)).toBeDefined();
      expect(after.get(index)).not.toBe(before.get(index));
    }
    expect(after.get(skipped)).toBe(before.get(skipped));
    // The artifact is left for an operator, like Go's reindexer.
    expect(after.has(artifact)).toBe(true);
    expect(after.has("river_job_does_not_exist")).toBe(false);
    await pool.query(`DROP INDEX "${schema}"."${artifact}"`);
  });
});

async function jobExists(
  pool: pg.Pool,
  schema: string,
  id: string
): Promise<boolean> {
  const result = await pool.query(
    `SELECT 1 FROM "${schema}".river_job WHERE id = $1`,
    [id]
  );
  return (result.rowCount ?? 0) > 0;
}

async function relfilenodes(
  pool: pg.Pool,
  schema: string
): Promise<Map<string, string>> {
  const result = await pool.query<{ relfilenode: string; relname: string }>(
    `SELECT c.relname::text AS relname, c.relfilenode::text AS relfilenode
     FROM pg_catalog.pg_class c
     JOIN pg_catalog.pg_namespace n ON n.oid = c.relnamespace
     WHERE n.nspname = $1 AND c.relkind = 'i'`,
    [schema]
  );
  return new Map(result.rows.map((row) => [row.relname, row.relfilenode]));
}

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs = 2_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await predicate())) {
    if (Date.now() > deadline) throw new Error("condition was not reached");
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}
