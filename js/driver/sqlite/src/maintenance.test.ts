import type { DatabaseSync } from "node:sqlite";

import {
  Client,
  Workers,
  defineJob,
  periodicJob,
  type RiverEvent,
} from "riverqueue";
import { describe, expect, onTestFinished, test } from "vitest";

import { sqliteTimestamp } from "./codecs.js";
import {
  SQLITE_DRIVER_TEST_HOOKS,
  type SqliteRuntime,
  testSqliteMemory,
} from "./driver.js";
import type { SqliteDriverOptions } from "./types.js";

/** River's own tests fail any lock window that crosses the event loop. */
const STRICT = {
  [SQLITE_DRIVER_TEST_HOOKS]: { strictLockWindow: true },
} as SqliteDriverOptions;

/** Queue settings that claim promptly in tests. */
const FAST_QUEUE = {
  fetchCooldown: { milliseconds: 1 },
  maxWorkers: 2,
  pollInterval: { milliseconds: 5 },
} as const;

/** Work that keeps a client running without touching the tested rows. */
const IDLE_WORK = {
  queues: { sqlite_idle: FAST_QUEUE },
  workers: new Workers().add(
    defineJob({ kind: "sqlite_idle" }),
    () => undefined
  ),
};

describe("SqliteDriver maintenance", () => {
  test("job cleaner deletes finalized jobs by per-state retention, like Go", async () => {
    const { database, driver } = await setup();
    const now = Temporal.Now.instant();
    const insert = (
      state: string,
      finalizedAgo: Temporal.DurationLike | null
    ) =>
      insertJob(database, {
        finalizedAt: finalizedAgo === null ? null : now.subtract(finalizedAgo),
        kind: "sqlite_cleaner",
        state,
      });
    // Default cancelled retention is 24 hours and discarded 7 days. A null
    // completed retention (Go's -1) keeps completed jobs forever.
    const cancelledOld = insert("cancelled", { hours: 25 });
    const cancelledRecent = insert("cancelled", { hours: 1 });
    const discardedOld = insert("discarded", { hours: 8 * 24 });
    const discardedRecent = insert("discarded", { hours: 6 * 24 });
    const completedOld = insert("completed", { hours: 30 * 24 });
    const running = insert("running", null);
    const available = insert("available", null);

    const client = new Client(driver, {
      maintenance: { completedJobRetention: null },
      ...IDLE_WORK,
    });
    const run = await client.start();
    try {
      await waitUntil(
        () => (run.diagnostics.maintenance?.runs.job_cleaner ?? 0) >= 1
      );
    } finally {
      await run.stop();
    }

    expect(jobState(database, cancelledOld)).toBeNull();
    expect(jobState(database, discardedOld)).toBeNull();
    expect(jobState(database, cancelledRecent)).toBe("cancelled");
    expect(jobState(database, discardedRecent)).toBe("discarded");
    expect(jobState(database, completedOld)).toBe("completed");
    expect(jobState(database, running)).toBe("running");
    expect(jobState(database, available)).toBe("available");
  });

  test("leader loses a term another process replaced under its client ID, then wins a fresh term", async () => {
    const { database, driver } = await setup();
    const job = defineJob({ kind: "sqlite_same_client_id_periodic" });
    const events: RiverEvent[] = [];
    let periodicStarts = 0;
    const client = new Client(driver, {
      clientId: "sqlite-same-client-id",
      hooks: {
        onEvent: (event) => {
          events.push(event);
        },
        onPeriodicJobsStart: () => {
          periodicStarts++;
        },
      },
      maintenance: { electionInterval: { milliseconds: 50 } },
      periodicJobs: [
        periodicJob({ args: {}, every: { hours: 1 }, job, runOnStart: true }),
      ],
      queues: { default: FAST_QUEUE },
      workers: new Workers().add(job, () => undefined),
    });
    const run = await client.start();
    try {
      await waitUntil(() => run.diagnostics.maintenance?.isLeader === true);
      const original = run.diagnostics.maintenance!.leader!;
      await waitUntil(() => countJobs(database, job.kind, "completed") === 1);

      // Another process with the same client ID (a restarted deployment,
      // say) replaces the row with a newer term of its own.
      const replacedElectedAt = Temporal.Now.instant();
      const replacedExpiresAt = replacedElectedAt.add({ milliseconds: 1_500 });
      database
        .prepare(
          `UPDATE river_leader SET elected_at = ?, expires_at = ?, leader_id = ?`
        )
        .run(
          sqliteTimestamp(replacedElectedAt),
          sqliteTimestamp(replacedExpiresAt),
          original.leaderId
        );

      // Like Go's elector, the failed renewal ends the original term at
      // once, well before the newer term expires, without renewing it.
      await waitUntil(
        () => run.diagnostics.maintenance?.isLeader === false,
        1_000
      );
      expect(
        Temporal.Instant.compare(Temporal.Now.instant(), replacedExpiresAt)
      ).toBe(-1);
      expect(events).toContainEqual(
        expect.objectContaining({
          kind: "leader_lost",
          leader: expect.objectContaining({ electedAt: original.electedAt }),
        })
      );
      const replaced = await driver.leaderGet();
      expect(replaced?.electedAt.equals(original.electedAt)).toBe(false);
      expect(
        replaced?.expiresAt.equals(
          replacedExpiresAt.round({
            roundingMode: "halfExpand",
            smallestUnit: "millisecond",
          })
        )
      ).toBe(true);

      // Once the newer term expires, the client wins a fresh term of its own
      // and inserts its run-on-start job again.
      await waitUntil(
        () => run.diagnostics.maintenance?.isLeader === true,
        5_000
      );
      const fresh = run.diagnostics.maintenance!.leader!;
      expect(fresh.leaderId).toBe(original.leaderId);
      expect(fresh.electedAt.equals(original.electedAt)).toBe(false);
      expect(
        Temporal.Instant.compare(fresh.electedAt, replacedExpiresAt)
      ).toBeGreaterThanOrEqual(0);
      await waitUntil(() => countJobs(database, job.kind, "completed") === 2);
      await sleep(200);
    } finally {
      await run.stop();
    }

    expect(countJobs(database, job.kind)).toBe(2);
    expect(periodicStarts).toBe(2);
    expect(
      events.filter(({ kind }) => kind === "leader_acquired")
    ).toHaveLength(2);
  });

  test("queue cleaner deletes expired queues and keeps recent ones", async () => {
    const { database, driver } = await setup();
    const now = Temporal.Now.instant();
    insertQueue(database, "sqlite_queue_now", now);
    insertQueue(database, "sqlite_queue_23h", now.subtract({ hours: 23 }));
    insertQueue(database, "sqlite_queue_25h", now.subtract({ hours: 25 }));
    insertQueue(database, "sqlite_queue_48h", now.subtract({ hours: 48 }));

    const client = new Client(driver, IDLE_WORK);
    const run = await client.start();
    try {
      await waitUntil(
        () => (run.diagnostics.maintenance?.runs.queue_cleaner ?? 0) >= 1
      );
    } finally {
      await run.stop();
    }

    expect(queueNames(database)).toEqual([
      // The client's own queue, reported as it starts.
      "sqlite_idle",
      "sqlite_queue_23h",
      "sqlite_queue_now",
    ]);
  });

  test("queue cleaner keeps a queue this client actively works", async () => {
    const { database, driver } = await setup();
    insertQueue(database, "sqlite_queue_idle", Temporal.Now.instant());

    // Retention far shorter than the run: only the client's heartbeats keep
    // its queue's row fresh.
    const client = new Client(driver, {
      maintenance: {
        queueCleanerInterval: { milliseconds: 50 },
        queueRetention: { seconds: 1 },
      },
      queueControlPollInterval: { milliseconds: 50 },
      queueHeartbeatInterval: { milliseconds: 50 },
      queues: { sqlite_queue_active: FAST_QUEUE },
      workers: new Workers().add(
        defineJob({ kind: "sqlite_queue_active" }),
        () => undefined
      ),
    });
    const run = await client.start();
    try {
      await waitUntil(() =>
        queueNames(database).includes("sqlite_queue_active")
      );
      const createdAt = queueCreatedAt(database, "sqlite_queue_active");
      await waitUntil(
        () => !queueNames(database).includes("sqlite_queue_idle")
      );
      const runs = run.diagnostics.maintenance!.runs.queue_cleaner;
      await waitUntil(
        () => run.diagnostics.maintenance!.runs.queue_cleaner >= runs + 10
      );
      expect(queueNames(database)).toEqual(["sqlite_queue_active"]);
      // Kept all along rather than deleted and recreated.
      expect(queueCreatedAt(database, "sqlite_queue_active")).toBe(createdAt);
    } finally {
      await run.stop();
    }
  });

  test("rescuer pages past a full batch of jobs without timeouts to rescue an eligible job", async () => {
    const { database, driver } = await setup();
    const noTimeout = defineJob({ kind: "sqlite_rescuer_no_timeout" });
    const attemptedAt = sqliteTimestamp(
      Temporal.Now.instant().subtract({ hours: 24 })
    );
    // A full default batch (10,000) of stuck jobs whose kind disables its
    // timeout, which the rescuer never rescues.
    database
      .prepare(
        `WITH RECURSIVE n(i) AS (SELECT 1 UNION ALL SELECT i + 1 FROM n WHERE i < 10000)
         INSERT INTO river_job (attempt, attempted_at, attempted_by, kind, max_attempts, state)
         SELECT 1, ?, jsonb('["elsewhere"]'), ?, 5, 'running' FROM n`
      )
      .run(attemptedAt, noTimeout.kind);
    const eligible = insertJob(database, {
      attemptedAt: Temporal.Now.instant().subtract({ hours: 2 }),
      kind: "sqlite_rescuer_unregistered",
      state: "running",
    });

    const client = new Client(driver, {
      queues: { sqlite_rescuer_idle: FAST_QUEUE },
      workers: new Workers().add(noTimeout, () => undefined, {
        timeout: null,
      }),
    });
    const run = await client.start();
    try {
      await waitUntil(
        () => (run.diagnostics.maintenance?.runs.rescuer ?? 0) >= 1,
        10_000
      );
    } finally {
      await run.stop();
    }

    // An unregistered kind is discarded, like Go's rescuer.
    expect(jobState(database, eligible)).toBe("discarded");
    expect(countJobs(database, noTimeout.kind, "running")).toBe(10_000);
  }, 30_000);

  test("inserts a run-on-start periodic job once per term and runs the start hook once", async () => {
    const { database, driver } = await setup();
    const job = defineJob({ kind: "sqlite_run_on_start" });
    let periodicStarts = 0;
    const client = new Client(driver, {
      hooks: {
        onPeriodicJobsStart: () => {
          periodicStarts++;
        },
      },
      maintenance: { electionInterval: { milliseconds: 20 } },
      periodicJobs: [
        periodicJob({ args: {}, every: { hours: 1 }, job, runOnStart: true }),
      ],
      queues: { default: FAST_QUEUE },
      workers: new Workers().add(job, () => undefined),
    });
    const run = await client.start();
    try {
      await waitUntil(() => countJobs(database, job.kind, "completed") === 1);
      const term = run.diagnostics.maintenance!.leader!;
      // Many renewals of the same term insert nothing more.
      await sleep(300);
      expect(
        run.diagnostics.maintenance!.leader!.electedAt.equals(term.electedAt)
      ).toBe(true);
    } finally {
      await run.stop();
    }

    expect(countJobs(database, job.kind)).toBe(1);
    expect(periodicStarts).toBe(1);
  });

  test("scheduler makes a job scheduled shortly ahead available and it completes", async () => {
    const { database, driver } = await setup();
    const scheduled = defineJob({ kind: "sqlite_scheduler_scheduled" });
    const onStart = defineJob({ kind: "sqlite_scheduler_run_on_start" });
    const client = new Client(driver, {
      maintenance: { schedulerInterval: { milliseconds: 50 } },
      periodicJobs: [
        periodicJob({
          args: {},
          every: { hours: 1 },
          job: onStart,
          runOnStart: true,
        }),
      ],
      queues: { default: FAST_QUEUE },
      workers: new Workers()
        .add(scheduled, () => undefined)
        .add(onStart, () => undefined),
    });
    const scheduledAt = Temporal.Now.instant().add({ milliseconds: 150 });
    const inserted = await client.insert(scheduled, {}, { scheduledAt });
    expect(inserted.job.state).toBe("scheduled");

    const run = await client.start();
    try {
      await waitUntil(
        () => jobState(database, inserted.job.id) === "completed"
      );
      await waitUntil(
        () => countJobs(database, onStart.kind, "completed") === 1
      );
    } finally {
      await run.stop();
    }

    const completed = await client.jobs.get(inserted.job.id);
    // Never worked before its stored (millisecond) scheduled time.
    expect(
      Temporal.Instant.compare(
        completed!.attemptedAt!,
        inserted.job.scheduledAt
      )
    ).toBeGreaterThanOrEqual(0);
  });
});

async function setup(): Promise<{
  database: DatabaseSync;
  driver: SqliteRuntime;
}> {
  const driver = testSqliteMemory(STRICT);
  const database = driver.connect();
  onTestFinished(() => {
    database.close();
    driver.close();
  });
  await migrate(database);
  return { database, driver };
}

async function migrate(database: DatabaseSync): Promise<void> {
  const moduleUrl = new URL("../../../migrate/dist/index.js", import.meta.url);
  const migrationModule = (await import(moduleUrl.href)) as {
    createMigrator(target: { database: DatabaseSync }): {
      migrateUp(): Promise<unknown>;
    };
  };
  await migrationModule.createMigrator({ database }).migrateUp();
}

function insertJob(
  database: DatabaseSync,
  {
    attemptedAt = null,
    finalizedAt = null,
    kind,
    state,
  }: {
    attemptedAt?: Temporal.Instant | null;
    finalizedAt?: Temporal.Instant | null;
    kind: string;
    state: string;
  }
): bigint {
  const row = database
    .prepare(
      `INSERT INTO river_job (attempt, attempted_at, finalized_at, kind, max_attempts, state)
       VALUES (?, ?, ?, ?, 5, ?) RETURNING id`
    )
    .get(
      attemptedAt === null ? 0 : 1,
      attemptedAt === null ? null : sqliteTimestamp(attemptedAt),
      finalizedAt === null ? null : sqliteTimestamp(finalizedAt),
      kind,
      state
    ) as { id: number };
  return BigInt(row.id);
}

function insertQueue(
  database: DatabaseSync,
  name: string,
  updatedAt: Temporal.Instant
): void {
  database
    .prepare(
      "INSERT INTO river_queue (created_at, name, updated_at) VALUES (?, ?, ?)"
    )
    .run(sqliteTimestamp(updatedAt), name, sqliteTimestamp(updatedAt));
}

function jobState(database: DatabaseSync, id: bigint): string | null {
  const row = database
    .prepare("SELECT state FROM river_job WHERE id = ?")
    .get(id) as { state: string } | undefined;
  return row?.state ?? null;
}

function countJobs(database: DatabaseSync, kind: string, state?: string) {
  const row = database
    .prepare(
      `SELECT count(*) AS count FROM river_job
       WHERE kind = ? AND (? IS NULL OR state = ?)`
    )
    .get(kind, state ?? null, state ?? null) as { count: number };
  return row.count;
}

function queueNames(database: DatabaseSync): string[] {
  return (
    database.prepare("SELECT name FROM river_queue ORDER BY name").all() as {
      name: string;
    }[]
  ).map(({ name }) => name);
}

function queueCreatedAt(database: DatabaseSync, name: string): string {
  const row = database
    .prepare("SELECT created_at FROM river_queue WHERE name = ?")
    .get(name) as { created_at: string };
  return row.created_at;
}

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function waitUntil(
  condition: () => boolean,
  timeoutMs = 5_000
): Promise<void> {
  const deadline = Date.now() + timeoutMs;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error("condition was not reached");
    await sleep(5);
  }
}
