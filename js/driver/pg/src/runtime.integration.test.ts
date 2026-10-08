import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import pg from "pg";
import { Client, defineJob, type Logger, Workers } from "riverqueue";
import { PgDriver } from "./driver.js";

const TEST_DATABASE_URL =
  process.env.TEST_DATABASE_URL ??
  "postgres://localhost:5432/river_test?sslmode=disable";
const filePrefix = `js_pgrt_${Math.random().toString(36).slice(2, 10)}`;
const migrationDirectory = fileURLToPath(
  new URL("../../../migrate/migrations/postgres/main/", import.meta.url)
);

interface LogEntry {
  readonly attributes: Readonly<Record<string, unknown>> | undefined;
  readonly level: string;
  readonly message: string;
}

function recordingLogger(entries: LogEntry[]): Logger {
  const log =
    (level: string) =>
    (attributes: Readonly<Record<string, unknown>>, message: string) => {
      entries.push({ attributes, level, message });
    };
  return {
    debug: log("debug"),
    error: log("error"),
    info: log("info"),
    warn: log("warn"),
  };
}

describe("Postgres runtime resilience", () => {
  let admin: pg.Pool;

  beforeAll(async () => {
    admin = new pg.Pool({ connectionString: TEST_DATABASE_URL });
    await admin.query("SELECT 1");
  });

  afterAll(async () => {
    await admin.end();
  });

  afterEach(async () => {
    await admin.query("DELETE FROM river_job WHERE kind LIKE $1", [
      `${filePrefix}%`,
    ]);
    await admin.query("DELETE FROM river_queue WHERE name LIKE $1", [
      `${filePrefix}%`,
    ]);
  });

  it("fails over within a second of a leader's resignation, like Go", async () => {
    // A schema of its own, so no other test's client contends for leadership.
    const schema = `${filePrefix}_failover`;
    await admin.query(`CREATE SCHEMA "${schema}"`);
    try {
      for (const file of (await readdir(migrationDirectory))
        .filter((name) => name.endsWith(".up.sql"))
        .sort()) {
        const migration = await readFile(
          join(migrationDirectory, file),
          "utf8"
        );
        await admin.query(
          migration.replaceAll("/* TEMPLATE: schema */", `"${schema}".`)
        );
      }
      const job = defineJob({ kind: `${filePrefix}_failover` });
      const client = (clientId: string) =>
        new Client(new PgDriver(admin, { schema }), {
          clientId,
          queues: { [`${filePrefix}_failover`]: { maxWorkers: 1 } },
          workers: new Workers().add(job, () => undefined),
        });
      const first = await client("first").start();
      const second = await client("second").start();
      try {
        await waitFor(() => first.diagnostics.maintenance?.isLeader === true);
        expect(second.diagnostics.maintenance?.isLeader).toBe(false);

        // With the default five second election interval, only the
        // resignation notification explains a prompt failover.
        const stopped = Date.now();
        await first.stop();
        await waitFor(
          () => second.diagnostics.maintenance?.isLeader === true,
          1_000
        );
        expect(Date.now() - stopped).toBeLessThan(1_000);
      } finally {
        await first.stop();
        await second.stop();
      }
    } finally {
      await admin.query(`DROP SCHEMA "${schema}" CASCADE`);
    }
  });

  it("rejects a non-ISO DateStyle with a configuration error", async () => {
    const url = new URL(TEST_DATABASE_URL);
    url.searchParams.set("options", "-c DateStyle=SQL,DMY");
    const pool = new pg.Pool({ connectionString: url.toString(), max: 1 });
    try {
      const client = new Client(new PgDriver(pool));
      const job = defineJob({ kind: `${filePrefix}_date_style` });

      await expect(client.insert(job, {})).rejects.toMatchObject({
        message: expect.stringContaining(
          'River needs Postgres\'s DateStyle to be ISO, not "SQL, DMY"'
        ),
        name: "ConfigurationError",
      });
    } finally {
      await pool.end();
    }
  });

  it("fails to start without claiming when LISTEN fails, like Go", async () => {
    const queue = `${filePrefix}_listen_fails`;
    const job = defineJob({ kind: `${filePrefix}_listen_fails` });
    // A pooler or server that rejects LISTEN, as some proxies do.
    const pool = new pg.Pool({ connectionString: TEST_DATABASE_URL });
    pool.on("connect", (connection) => {
      const query = connection.query.bind(connection) as (
        ...args: unknown[]
      ) => unknown;
      Object.assign(connection, {
        query: (...args: unknown[]) =>
          typeof args[0] === "string" && args[0].startsWith("LISTEN ")
            ? Promise.reject(
                Object.assign(new Error("LISTEN is not supported"), {
                  code: "0A000",
                })
              )
            : query(...args),
      });
    });
    try {
      const client = new Client(new PgDriver(pool), {
        leaderElectionDisabled: true,
        queues: {
          [queue]: {
            fetchCooldown: { milliseconds: 1 },
            maxWorkers: 1,
            pollInterval: { milliseconds: 5 },
          },
        },
        workers: new Workers().add(job, () => undefined),
      });
      const inserted = await client.insert(job, {}, { queue });

      await expect(client.start()).rejects.toThrow("LISTEN is not supported");
      expect(await jobState(admin, inserted.job.id)).toBe("available");
    } finally {
      await pool.end();
    }
  });

  it("keeps a committed transactional completion after a handler error", async () => {
    const queue = `${filePrefix}_tx_commit`;
    const job = defineJob({ kind: `${filePrefix}_tx_commit` });
    const events: string[] = [];
    let errorHandlerCalls = 0;
    const client = new Client(new PgDriver(admin), {
      completionBatchSize: 1,
      errorHandler: () => {
        errorHandlerCalls++;
      },
      hooks: {
        onEvent: ({ kind }) => {
          events.push(kind);
        },
      },
      leaderElectionDisabled: true,
      queues: {
        [queue]: {
          fetchCooldown: { milliseconds: 1 },
          maxWorkers: 1,
          pollInterval: { milliseconds: 5 },
        },
      },
      workers: new Workers().add(job, async ({ completeTx }) => {
        const tx = await admin.connect();
        try {
          await tx.query("BEGIN");
          await completeTx(tx, { output: { committed: true } });
          await tx.query("COMMIT");
        } finally {
          tx.release();
        }
        throw new Error("after commit");
      }),
    });
    const inserted = await client.insert(job, {}, { queue });
    const run = await client.start();
    try {
      await waitFor(
        async () => (await jobState(admin, inserted.job.id)) === "completed"
      );
      await waitFor(() => events.includes("job_completed"));
      expect((await client.jobs.get(inserted.job.id))?.metadata.output).toEqual(
        {
          committed: true,
        }
      );
      expect(errorHandlerCalls).toBe(1);
      expect(events.filter((kind) => kind === "job_completed")).toHaveLength(1);
      expect(events).not.toContain("job_failed");
      expect(events).not.toContain("job_race");
    } finally {
      await run.stop();
    }
  });

  it("falls back to normal completion after transactional rollback", async () => {
    const queue = `${filePrefix}_tx_rollback`;
    const job = defineJob({ kind: `${filePrefix}_tx_rollback` });
    const events: string[] = [];
    const client = new Client(new PgDriver(admin), {
      completionBatchSize: 1,
      hooks: {
        onEvent: ({ kind }) => {
          events.push(kind);
        },
      },
      leaderElectionDisabled: true,
      queues: {
        [queue]: {
          fetchCooldown: { milliseconds: 1 },
          maxWorkers: 1,
          pollInterval: { milliseconds: 5 },
        },
      },
      workers: new Workers().add(job, async ({ completeTx }) => {
        const tx = await admin.connect();
        try {
          await tx.query("BEGIN");
          await completeTx(tx, { output: { rolledBack: true } });
          await tx.query("ROLLBACK");
        } finally {
          tx.release();
        }
      }),
    });
    const inserted = await client.insert(job, {}, { queue });
    const run = await client.start();
    try {
      await waitFor(
        async () => (await jobState(admin, inserted.job.id)) === "completed"
      );
      await waitFor(() => events.includes("job_completed"));
      expect(
        (await client.jobs.get(inserted.job.id))?.metadata
      ).not.toHaveProperty("output");
      expect(events.filter((kind) => kind === "job_completed")).toHaveLength(1);
      expect(events).not.toContain("job_race");
    } finally {
      await run.stop();
    }
  });

  it("bounds concurrent work to the caller-owned Postgres pool", async () => {
    const applicationName = `${filePrefix}_pool`;
    const pool = new pg.Pool({
      application_name: applicationName,
      connectionString: TEST_DATABASE_URL,
      max: 4,
    });
    const queue = `${filePrefix}_pool`;
    const job = defineJob({ kind: `${filePrefix}_pool` });
    const client = new Client(new PgDriver(pool), {
      completionBatchSize: 1,
      leaderElectionDisabled: true,
      queues: {
        [queue]: {
          fetchCooldown: { milliseconds: 1 },
          maxWorkers: 4,
          pollInterval: { milliseconds: 5 },
        },
      },
      workers: new Workers().add(job, async () => {
        await new Promise((resolve) => setTimeout(resolve, 20));
      }),
    });
    const run = await client.start();
    try {
      await Promise.all(
        Array.from({ length: 20 }, () => client.insert(job, {}, { queue }))
      );
      let maximumConnections = 0;
      await waitFor(async () => {
        const connections = await admin.query<{ count: string }>(
          "SELECT count(*)::text AS count FROM pg_stat_activity " +
            "WHERE application_name = $1",
          [applicationName]
        );
        const count = Number(connections.rows[0]?.count);
        maximumConnections = Math.max(maximumConnections, count);
        expect(count).toBeLessThanOrEqual(4);
        const completed = await admin.query<{ count: string }>(
          "SELECT count(*)::text AS count FROM river_job WHERE kind = $1 AND state = 'completed'",
          [job.kind]
        );
        return Number(completed.rows[0]?.count) === 20;
      }, 5_000);
      expect(maximumConnections).toBeGreaterThan(0);
    } finally {
      await run.stop();
      await pool.end();
    }
  });

  it("keeps working when a row lock outlasts the statement timeout", async () => {
    // A DBA guardrail such as `ALTER ROLE ... SET statement_timeout` plus a
    // row lock held by another session (an admin transaction, a UI, or
    // another engine's update) makes the first completion attempt fail with
    // SQLSTATE 57014. The runtime must retry instead of failing.
    const pool = new pg.Pool({
      connectionString: TEST_DATABASE_URL,
      options: "-c statement_timeout=200",
    });
    const queue = `${filePrefix}_lock`;
    const job = defineJob({ kind: `${filePrefix}_lock` });
    const started: bigint[] = [];
    const logs: LogEntry[] = [];
    const client = new Client(new PgDriver(pool), {
      completionFlushInterval: { milliseconds: 1 },
      logger: recordingLogger(logs),
      leaderElectionDisabled: true,
      queues: {
        [queue]: {
          fetchCooldown: { milliseconds: 10 },
          maxWorkers: 2,
          pollInterval: { milliseconds: 50 },
        },
      },
      workers: new Workers().add(job, async ({ job: row }) => {
        started.push(row.id);
        await new Promise((resolve) => setTimeout(resolve, 100));
      }),
    });
    const run = await client.start();
    try {
      const first = await client.insert(job, {}, { queue });
      await waitFor(() => started.includes(first.job.id));

      const locker = await admin.connect();
      try {
        await locker.query("BEGIN");
        await locker.query(
          "SELECT id FROM river_job WHERE id = $1 FOR UPDATE",
          [first.job.id.toString(10)]
        );
        // Hold the lock past the handler and the statement timeout.
        await waitFor(() =>
          logs.some(({ attributes }) =>
            String(attributes?.error).includes("57014")
          )
        );
      } finally {
        await locker.query("ROLLBACK");
        locker.release();
      }

      await waitFor(
        async () => (await jobState(admin, first.job.id)) === "completed",
        5_000
      );
      expect(run.state).toBe("running");

      const second = await client.insert(job, {}, { queue });
      await waitFor(
        async () => (await jobState(admin, second.job.id)) === "completed",
        5_000
      );
      expect(logs).toContainEqual(
        expect.objectContaining({
          attributes: expect.objectContaining({ attempt: 1, retryable: true }),
          level: "warn",
          message: "River completion persistence attempt failed",
        })
      );
    } finally {
      await run.stop({ timeout: { milliseconds: 5_000 } });
      await pool.end();
    }
    expect(run.state).toBe("stopped");
  });
});

async function jobState(pool: pg.Pool, id: bigint): Promise<string> {
  const result = await pool.query<{ state: string }>(
    "SELECT state::text AS state FROM river_job WHERE id = $1",
    [id.toString(10)]
  );
  return result.rows[0]?.state ?? "missing";
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
