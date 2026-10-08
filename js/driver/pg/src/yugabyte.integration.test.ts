/**
 * Postgres-compatible servers without `xmax` or `LISTEN`/`NOTIFY`, like
 * YugabyteDB, simulated on Postgres the way River for Go's tests do.
 *
 * A test schema shadows `version()` and `current_setting(text, boolean)`
 * ahead of `pg_catalog` on the connections' `search_path`, so River detects
 * a Yugabyte version and notification setting. When notifications are off it
 * also shadows `pg_notify` with a function that raises, so any notification
 * fails the statement that sends it. River's tables live in that schema as
 * the connections' current schema. This exercises detection and River's
 * fallbacks, not Yugabyte's storage or transaction semantics.
 */
import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterEach, describe, expect, it } from "vitest";
import pg from "pg";
import { Client, defineJob, type Logger, Workers } from "riverqueue";
import { PgDriver, testPgDriver } from "./driver.js";

const TEST_DATABASE_URL =
  process.env.TEST_DATABASE_URL ??
  "postgres://localhost:5432/river_test?sslmode=disable";
const migrationDirectory = fileURLToPath(
  new URL("../../../migrate/migrations/postgres/main/", import.meta.url)
);

/** Which server a test schema simulates. */
type Server =
  /** Postgres 17, before `RETURNING OLD`. */
  | "postgres17"
  /** YugabyteDB before 2025.2.3, without `yb_enable_listen_notify`. */
  | "yugabyte_unavailable"
  /** YugabyteDB with `yb_enable_listen_notify` off. */
  | "yugabyte_disabled"
  /** YugabyteDB with `yb_enable_listen_notify` on. */
  | "yugabyte_enabled";

const SERVERS: readonly Server[] = [
  "postgres17",
  "yugabyte_unavailable",
  "yugabyte_disabled",
  "yugabyte_enabled",
];

function listenNotify(server: Server): boolean {
  return server === "postgres17" || server === "yugabyte_enabled";
}

describe("Postgres servers like YugabyteDB, simulated", () => {
  const cleanups: (() => Promise<void>)[] = [];

  afterEach(async () => {
    for (const cleanup of cleanups.splice(0).reverse()) await cleanup();
  });

  /**
   * Create a schema simulating `server` with River migrated into it, and a
   * pool whose connections search it ahead of `pg_catalog`.
   */
  async function simulate(server: Server): Promise<{
    admin: pg.Pool;
    pool: pg.Pool;
    schema: string;
  }> {
    const schema = `js_yb_${Math.random().toString(36).slice(2, 10)}`;
    const quoted = `"${schema}"`;
    const admin = new pg.Pool({ connectionString: TEST_DATABASE_URL, max: 2 });
    cleanups.push(async () => {
      await admin.query(`DROP SCHEMA IF EXISTS ${quoted} CASCADE`);
      await admin.end();
    });
    await admin.query(`CREATE SCHEMA ${quoted}`);
    if (server === "postgres17") {
      await admin.query(`
        CREATE FUNCTION ${quoted}.current_setting(setting_name text)
        RETURNS text LANGUAGE sql AS $$
          SELECT CASE WHEN setting_name = 'server_version_num' THEN '170004'
          ELSE pg_catalog.current_setting(setting_name) END
        $$
      `);
    } else {
      const [version, setting] =
        server === "yugabyte_unavailable"
          ? ["2025.2.1.0", "NULL::text"]
          : server === "yugabyte_disabled"
            ? ["2025.2.3.0", "'off'::text"]
            : ["2025.2.3.0", "'on'::text"];
      await admin.query(`
        CREATE FUNCTION ${quoted}.version() RETURNS text LANGUAGE sql AS $$
          SELECT 'PostgreSQL 15.12-YB-${version}-b1'::text
        $$;
        CREATE FUNCTION ${quoted}.current_setting(
          setting_name text, missing_ok boolean
        ) RETURNS text LANGUAGE sql AS $$
          SELECT CASE WHEN setting_name = 'yb_enable_listen_notify'
            THEN ${setting}
            ELSE pg_catalog.current_setting(setting_name, missing_ok) END
        $$
      `);
    }
    if (!listenNotify(server)) {
      await admin.query(`
        CREATE FUNCTION ${quoted}.pg_notify(text, text) RETURNS void
        LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'LISTEN/NOTIFY is unavailable'; END
        $$
      `);
    }
    for (const file of (await readdir(migrationDirectory))
      .filter((name) => name.endsWith(".up.sql"))
      .sort()) {
      const migration = await readFile(join(migrationDirectory, file), "utf8");
      await admin.query(
        migration.replaceAll("/* TEMPLATE: schema */", `${quoted}.`)
      );
    }

    const url = new URL(TEST_DATABASE_URL);
    url.searchParams.set("options", `-c search_path=${schema},pg_catalog`);
    const pool = new pg.Pool({ connectionString: url.toString(), max: 4 });
    cleanups.push(() => pool.end());
    return { admin, pool, schema };
  }

  it.each(SERVERS)(
    "detects whether %s delivers notifications and inserts unique jobs",
    async (server) => {
      const { admin, pool, schema } = await simulate(server);
      const driver = testPgDriver(pool);
      await expect(driver.runtimeDeliversNotifications()).resolves.toBe(
        listenNotify(server)
      );

      const client = new Client(new PgDriver(pool));
      const job = defineJob<{ value: number }>()({ kind: "yugabyte_unique" });
      const first = await client.insert(
        job,
        { value: 1 },
        { unique: { byArgs: true } }
      );
      const second = await client.insert(
        job,
        { value: 1 },
        { unique: { byArgs: true } }
      );
      const other = await client.insert(job, { value: 2 });

      expect(first.status).toBe("inserted");
      expect(second.status).toBe("duplicate");
      expect(second.job.id).toBe(first.job.id);
      expect(other.status).toBe("inserted");
      // Without `xmax`, a row carries a nonce like SQLite's.
      const nonces = await admin.query<{ has_nonce: boolean }>(
        `SELECT metadata ? 'river:unique_nonce' AS has_nonce
         FROM "${schema}".river_job ORDER BY id`
      );
      expect(nonces.rows.map(({ has_nonce }) => has_nonce)).toEqual(
        server === "postgres17" ? [false, false] : [true, true]
      );
    }
  );

  it("sends no notification when the server has no LISTEN/NOTIFY", async () => {
    const { pool } = await simulate("yugabyte_unavailable");
    const driver = testPgDriver(pool);
    const client = new Client(new PgDriver(pool));
    const job = defineJob({ kind: "yugabyte_quiet" });

    // Each of these notifies on Postgres; the shadowed pg_notify raises.
    const { job: inserted } = await client.insert(job, {});
    await expect(client.jobs.cancel(inserted.id)).resolves.toMatchObject({
      state: "cancelled",
    });
    await driver.runtimeQueueUpsert("quiet", Temporal.Now.instant());
    await client.queues.pause("quiet");
    await client.queues.resume("*");
    await client.queues.update("quiet", { metadata: { owner: "workers" } });
    await client.requestLeadershipResignation();
    const leader = await driver.maintenanceLeaderAcquire(
      "yugabyte_leader",
      Temporal.Now.instant(),
      30_000,
      null
    );
    expect(leader).not.toBeNull();
    await expect(driver.maintenanceLeaderResign(leader!)).resolves.toBe(true);
  });

  it("polls for a running job's cancellation without LISTEN/NOTIFY", async () => {
    const { pool } = await simulate("yugabyte_unavailable");
    const job = defineJob({ kind: "yugabyte_cancel" });
    const logs: string[] = [];
    let started!: () => void;
    const running = new Promise<void>((resolve) => {
      started = resolve;
    });
    const client = new Client(new PgDriver(pool), {
      leaderElectionDisabled: true,
      logger: recordingLogger(logs),
      queues: {
        default: {
          fetchCooldown: { milliseconds: 10 },
          maxWorkers: 1,
          pollInterval: { milliseconds: 50 },
        },
      },
      workers: new Workers().add(job, ({ signal }) => {
        started();
        return new Promise((_, reject) => {
          signal.addEventListener("abort", () => reject(signal.reason), {
            once: true,
          });
        });
      }),
    });
    const run = await client.start();
    try {
      const { job: inserted } = await client.insert(job, {});
      await running;
      // Cancel through another client, so only polling can tell this one.
      const cancelledAt = Date.now();
      await new Client(new PgDriver(pool)).jobs.cancel(inserted.id);
      await waitFor(
        async () => (await client.jobs.get(inserted.id))?.state === "cancelled",
        6_000
      );
      expect(Date.now() - cancelledAt).toBeLessThan(6_000);
      expect(logs).toContain(
        "River's database does not support LISTEN/NOTIFY; polling instead"
      );
    } finally {
      await run.stop();
    }
    // Schema setup, migrations, and up to six seconds of polling outlast
    // vitest's default five second timeout on a loaded database.
  }, 20_000);

  it("polls for a running job's cancellation when poll-only", async () => {
    const { pool } = await simulate("postgres17");
    const job = defineJob({ kind: "poll_only_cancel" });
    let started!: () => void;
    const running = new Promise<void>((resolve) => {
      started = resolve;
    });
    const client = new Client(new PgDriver(pool), {
      leaderElectionDisabled: true,
      pollOnly: true,
      queues: {
        default: {
          fetchCooldown: { milliseconds: 10 },
          maxWorkers: 1,
          pollInterval: { milliseconds: 50 },
        },
      },
      workers: new Workers().add(job, ({ signal }) => {
        started();
        return new Promise((_, reject) => {
          signal.addEventListener("abort", () => reject(signal.reason), {
            once: true,
          });
        });
      }),
    });
    const run = await client.start();
    try {
      const { job: inserted } = await client.insert(job, {});
      await running;
      await new Client(new PgDriver(pool)).jobs.cancel(inserted.id);
      await waitFor(
        async () => (await client.jobs.get(inserted.id))?.state === "cancelled",
        6_000
      );
    } finally {
      await run.stop();
    }
  }, 20_000);
});

function recordingLogger(messages: string[]): Logger {
  const log = (_attributes: unknown, message: string) => {
    messages.push(message);
  };
  return { debug: log, error: log, info: log, warn: log };
}

async function waitFor(
  predicate: () => boolean | Promise<boolean>,
  timeoutMs: number
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!(await predicate())) {
    if (Date.now() > deadline) throw new Error("condition was not reached");
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
}
