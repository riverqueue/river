import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";

import {
  Client,
  Workers,
  complete,
  defineJob,
  type JsonObject,
} from "riverqueue";
import { describe, expect, onTestFinished, test } from "vitest";
import { z } from "zod";

import {
  SQLITE_DRIVER_TEST_HOOKS,
  type SqliteRuntime,
  testSqliteDriver,
  testSqliteMemory,
} from "./driver.js";
import { transaction } from "./scope.js";
import type { SqliteDriverOptions } from "./types.js";

/** River's own tests fail any lock window that crosses the event loop. */
const STRICT = {
  [SQLITE_DRIVER_TEST_HOOKS]: { strictLockWindow: true },
} as SqliteDriverOptions;

describe("SqliteDriver runtime", () => {
  test("runs the testing guide's whole-runtime example", async () => {
    // Mirrors "The whole runtime" in docs/testing.md, which the snippet check
    // only typechecks; keep the two in sync.
    const chargeCard = defineJob<{ amountCents: number }>()({
      kind: "charge_card",
    });
    const workers = new Workers().add(chargeCard, () => undefined);
    using driver = testSqliteMemory();
    await migrate(driver.database);
    const client = new Client(driver, {
      leaderElectionDisabled: true,
      queues: {
        default: {
          fetchCooldown: { milliseconds: 20 },
          maxWorkers: 1,
          pollInterval: { milliseconds: 50 },
        },
      },
      workers,
    });
    const { job } = await client.insert(chargeCard, { amountCents: 500 });

    using events = client.subscribe({ kinds: ["job_completed"] });
    await using run = await client.start();
    const { value: event } = await events.next();
    expect(event?.kind === "job_completed" && event.job.id).toBe(job.id);
    await run.stop();
  });

  test("completes an exact attempt in a caller transaction", async () => {
    const { database, driver } = await setup();
    const definition = defineJob({ kind: "sqlite_runtime_complete_tx" });
    const observed: string[] = [];
    const workers = new Workers().add(definition, async (context) => {
      await transaction(database, async (tx) => {
        await context.completeTx(tx, { output: { committed: true } });
      });
    });
    const client = new Client(driver, {
      clientId: "sqlite-runtime-tx-test",
      completionBatchSize: 1,
      hooks: {
        onEvent: ({ kind }) => {
          observed.push(kind);
        },
      },
      queues: {
        default: {
          fetchCooldown: { milliseconds: 1 },
          maxWorkers: 1,
          pollInterval: { milliseconds: 5 },
        },
      },
      workers,
    });
    const events = client.subscribe({ kinds: ["job_completed"] });
    const inserted = await client.insert(definition, {});
    const run = await client.start();

    const completed = await waitForJob(client, inserted.job.id, "completed");
    expect(completed.metadata.output).toEqual({ committed: true });
    expect((await events.next()).value).toMatchObject({
      job: { id: inserted.job.id, state: "completed" },
      kind: "job_completed",
    });
    await waitUntil(() => observed.includes("job_completed"));

    await run.stop();
    events.close();
    expect(observed.filter((kind) => kind === "job_completed")).toHaveLength(1);
    expect(observed).not.toContain("job_race");
  });

  test("a committed transactional completion overrides a later handler error", async () => {
    const { database, driver } = await setup();
    const definition = defineJob({ kind: "sqlite_runtime_complete_tx_error" });
    const observed: string[] = [];
    const workStatuses: string[] = [];
    let errorHandlerCalls = 0;
    const workers = new Workers().add(definition, async (context) => {
      await transaction(database, async (tx) => {
        await context.completeTx(tx, { output: { committed: true } });
      });
      throw new Error("after commit");
    });
    const client = new Client(driver, {
      clientId: "sqlite-runtime-tx-error-test",
      completionBatchSize: 1,
      errorHandler: () => {
        errorHandlerCalls++;
      },
      hooks: {
        onEvent: ({ kind }) => {
          observed.push(kind);
        },
        afterWork: (_context, result) => {
          workStatuses.push(result.status);
        },
      },
      queues: {
        default: {
          fetchCooldown: { milliseconds: 1 },
          maxWorkers: 1,
          pollInterval: { milliseconds: 5 },
        },
      },
      workers,
    });
    const inserted = await client.insert(definition, {});
    const run = await client.start();

    const completed = await waitForJob(client, inserted.job.id, "completed");
    expect(completed.metadata.output).toEqual({ committed: true });
    await waitUntil(() => observed.includes("job_completed"));

    await run.stop();
    expect(errorHandlerCalls).toBe(1);
    expect(workStatuses).toEqual(["failed"]);
    expect(observed.filter((kind) => kind === "job_completed")).toHaveLength(1);
    expect(observed).not.toContain("job_failed");
    expect(observed).not.toContain("job_race");
  });

  test("a rolled-back transactional completion falls back to normal completion", async () => {
    const { database, driver } = await setup();
    const definition = defineJob({
      kind: "sqlite_runtime_complete_tx_rollback",
    });
    const observed: string[] = [];
    const rollback = new Error("roll back");
    const workers = new Workers().add(definition, async (context) => {
      try {
        await transaction(database, async (tx) => {
          await context.completeTx(tx, { output: { rolledBack: true } });
          throw rollback;
        });
      } catch (error: unknown) {
        if (error !== rollback) throw error;
      }
    });
    const client = new Client(driver, {
      clientId: "sqlite-runtime-tx-rollback-test",
      completionBatchSize: 1,
      hooks: {
        onEvent: ({ kind }) => {
          observed.push(kind);
        },
      },
      queues: {
        default: {
          fetchCooldown: { milliseconds: 1 },
          maxWorkers: 1,
          pollInterval: { milliseconds: 5 },
        },
      },
      workers,
    });
    const inserted = await client.insert(definition, {});
    const run = await client.start();

    const completed = await waitForJob(client, inserted.job.id, "completed");
    expect(completed.metadata).not.toHaveProperty("output");
    await waitUntil(() => observed.includes("job_completed"));

    await run.stop();
    expect(observed.filter((kind) => kind === "job_completed")).toHaveLength(1);
    expect(observed).not.toContain("job_race");
  });

  test("works and completes an exact job through the common runtime", async () => {
    const { driver } = await setup();
    const definition = defineJob({
      kind: "sqlite_runtime_complete",
      schema: z.object({ value: z.number() }),
    });
    const workers = new Workers().add(definition, ({ job }) =>
      complete({ output: { doubled: job.args.value * 2 } })
    );
    const client = new Client(driver, {
      clientId: "sqlite-runtime-test",
      completionFlushInterval: { milliseconds: 1 },
      queues: {
        default: {
          fetchCooldown: { milliseconds: 1 },
          maxWorkers: 1,
          pollInterval: { milliseconds: 5 },
        },
      },
      workers,
    });
    const run = await client.start();
    const inserted = await client.insert(definition, { value: 21 });

    const completed = await waitForJob(client, inserted.job.id, "completed");
    expect(completed.attempt).toBe(1);
    expect(completed.attemptedBy).toEqual(["sqlite-runtime-test"]);
    expect(completed.metadata.output).toEqual({ doubled: 42 });

    await run.stop();
    await expect(run.completed).resolves.toBeUndefined();
    expect(run.state).toBe("stopped");
  });

  test.each([
    ["with notifications", false],
    ["poll only", true],
  ])("works jobs without leader election, %s", async (_name, pollOnly) => {
    const { driver } = await setup();
    const definition = defineJob({ kind: "sqlite_runtime_no_leader" });
    const client = new Client(driver, {
      clientId: "sqlite-runtime-no-leader",
      completionFlushInterval: { milliseconds: 1 },
      leaderElectionDisabled: true,
      pollOnly,
      queues: {
        default: {
          fetchCooldown: { milliseconds: 1 },
          maxWorkers: 1,
          pollInterval: { milliseconds: 5 },
        },
      },
      workers: new Workers().add(definition, () => undefined),
    });
    const run = await client.start();
    const inserted = await client.insert(definition, {});

    const completed = await waitForJob(client, inserted.job.id, "completed");
    expect(completed.attemptedBy).toEqual(["sqlite-runtime-no-leader"]);
    await expect(driver.leaderGet()).resolves.toBeNull();
    expect(run.diagnostics.maintenance).toBeNull();

    await run.stop();
    expect(run.state).toBe("stopped");
    await expect(driver.leaderGet()).resolves.toBeNull();
  });

  test("runs extensions, resumable steps, and subscriptions on SQLite", async () => {
    const { driver } = await setup();
    const definition = defineJob({
      kind: "sqlite_runtime_extensions",
      schema: z.object({ value: z.number() }),
    });
    const order: string[] = [];
    const client = new Client(driver, {
      clientId: "sqlite-runtime-extensions-test",
      completionBatchSize: 1,
      hooks: {
        afterWork: () => {
          order.push("after");
        },
        beforeWork: () => {
          order.push("before");
        },
      },
      middleware: [
        async (_context, next) => {
          order.push("middleware-before");
          const result = await next();
          order.push("middleware-after");
          return result;
        },
      ],
      queues: {
        default: {
          fetchCooldown: { milliseconds: 1 },
          maxWorkers: 1,
          pollInterval: { milliseconds: 5 },
        },
      },
      workers: new Workers().add(definition, async ({ job, resumable }) => {
        await resumable.step("first", () => {
          order.push("first");
        });
        await resumable.step("second", () => {
          order.push("second");
        });
        return complete({ output: { doubled: job.args.value * 2 } });
      }),
    });
    const events = client.subscribe({
      kinds: ["job_started", "job_completed"],
    });
    const inserted = await client.insert(definition, { value: 21 });
    const run = await client.start();
    try {
      const completed = await waitForJob(client, inserted.job.id, "completed");
      expect(completed.metadata.output).toEqual({ doubled: 42 });
      expect((await events.next()).value.kind).toBe("job_started");
      expect((await events.next()).value.kind).toBe("job_completed");
      expect(order).toEqual([
        "middleware-before",
        "before",
        "first",
        "second",
        "after",
        "middleware-after",
      ]);
    } finally {
      await run.stop();
      events.close();
    }
  });

  test("persists resumable checkpoints on failed SQLite attempts", async () => {
    const { driver } = await setup();
    const definition = defineJob({ kind: "sqlite_runtime_resumable_retry" });
    const order: string[] = [];
    const client = new Client(driver, {
      clientId: "sqlite-runtime-resumable-test",
      completionBatchSize: 1,
      leaderElectionDisabled: true,
      queues: {
        default: {
          fetchCooldown: { milliseconds: 1 },
          maxWorkers: 1,
          pollInterval: { milliseconds: 5 },
        },
      },
      retryPolicy: (_job, now) => now.add({ hours: 1 }),
      workers: new Workers().add(definition, async ({ resumable }) => {
        await resumable.step("first", () => {
          order.push("first");
        });
        await resumable.step("second", () => {
          order.push("second");
          throw new Error("retry after checkpoint");
        });
      }),
    });
    const inserted = await client.insert(definition, {});
    const run = await client.start();
    try {
      const retryable = await waitForJob(client, inserted.job.id, "retryable");
      expect(retryable.metadata["river:resumable_step"]).toBe("first");
      expect(order).toEqual(["first", "second"]);
    } finally {
      await run.stop();
    }
  });

  test("fails jobs with invalid JSON columns without stalling the queue, like Go", async () => {
    const { database, driver } = await setup();
    const job = defineJob({ kind: "sqlite_runtime_invalid_json" });
    const worked: bigint[] = [];
    const client = new Client(driver, {
      leaderElectionDisabled: true,
      queues: {
        default: {
          fetchCooldown: { milliseconds: 1 },
          maxWorkers: 2,
          pollInterval: { milliseconds: 5 },
        },
      },
      // Far enough out that a failed attempt leaves its job retryable, not
      // available again at once with its errors value repaired.
      retryPolicy: () => Temporal.Instant.from("2100-01-01T00:00:00Z"),
      workers: new Workers().add(job, ({ job: { id } }) => {
        worked.push(id);
      }),
    });
    // Ordinary jobs flank the invalid ones in claim order.
    const first = await client.insert(job, {});
    const columns = ["args", "attempted_by", "errors", "metadata", "tags"];
    const invalid = new Map<string, bigint>();
    for (const column of columns) {
      const { job: inserted } = await client.insert(job, {});
      database
        .prepare(`UPDATE river_job SET ${column} = '[not json' WHERE id = ?`)
        .run(inserted.id);
      invalid.set(column, inserted.id);
    }
    const last = await client.insert(job, {});

    const run = await client.start();
    try {
      await waitForJob(client, first.job.id, "completed");
      await waitForJob(client, last.job.id, "completed");
      await waitUntilAsync(() =>
        Promise.resolve(
          [...invalid.values()].every(
            (id) =>
              database
                .prepare("SELECT state FROM river_job WHERE id = ?")
                .get(id)?.state === "retryable"
          )
        )
      );
    } finally {
      await run.stop();
    }

    // Only the ordinary jobs were worked. Each invalid value is left in
    // place, except that invalid errors text becomes a string in a new
    // array with the attempt's decode error appended.
    expect(worked.sort()).toEqual([first.job.id, last.job.id]);
    for (const [column, id] of invalid) {
      const { value } = database
        .prepare(
          `SELECT CASE WHEN typeof(${column}) = 'text' THEN ${column}
             ELSE json(${column}) END AS value
           FROM river_job WHERE id = ?`
        )
        .get(id) as { value: string };
      if (column === "errors") {
        expect(JSON.parse(value)).toEqual([
          "[not json",
          expect.objectContaining({
            attempt: 1,
            error: expect.stringContaining("job row couldn't be decoded"),
          }),
        ]);
      } else {
        expect(value).toBe("[not json");
      }
    }
  });

  test("keeps a live runtime healthy while another connection holds the write lock", async () => {
    const directory = mkdtempSync(join(tmpdir(), "river-sqlite-runtime-"));
    const path = join(directory, "river.db");
    const database = new DatabaseSync(path);
    const other = new DatabaseSync(path);
    onTestFinished(() => {
      if (other.isTransaction) other.exec("ROLLBACK");
      other.close();
      database.close();
      rmSync(directory, { force: true, recursive: true });
    });
    await migrate(database);
    using driver = testSqliteDriver(database, STRICT);
    const definition = defineJob({ kind: "sqlite_runtime_busy" });
    const client = new Client(driver, {
      clientId: "sqlite-runtime-busy-test",
      completionBatchSize: 1,
      queues: {
        default: {
          fetchCooldown: { milliseconds: 1 },
          maxWorkers: 1,
          pollInterval: { milliseconds: 1 },
        },
      },
      workers: new Workers().add(definition, () => undefined),
    });
    const run = await client.start();

    // Another process (for example a Go client) holds SQLite's write lock.
    other.exec("BEGIN IMMEDIATE");
    let ticks = 0;
    const interval = setInterval(() => {
      ticks++;
    }, 1);
    onTestFinished(() => clearInterval(interval));
    const inserted = client.insert(definition, {});
    await waitUntil(() => ticks >= 50);
    expect(run.state).toBe("running");

    other.exec("COMMIT");
    const { job } = await inserted;
    await waitForJob(client, job.id, "completed");
    await run.stop();
    expect(run.state).toBe("stopped");
  });

  test("keeps a live runtime healthy across a long caller transaction", async () => {
    const { database, driver } = await setup();
    const definition = defineJob({ kind: "sqlite_runtime_long_tx" });
    const client = new Client(driver, {
      clientId: "sqlite-runtime-long-tx-test",
      completionBatchSize: 1,
      queues: {
        default: {
          fetchCooldown: { milliseconds: 1 },
          maxWorkers: 1,
          pollInterval: { milliseconds: 1 },
        },
      },
      workers: new Workers().add(definition, () => undefined),
    });
    const run = await client.start();
    let release!: () => void;
    let transactionStarted!: () => void;
    const held = new Promise<void>((resolve) => {
      release = resolve;
    });
    const started = new Promise<void>((resolve) => {
      transactionStarted = resolve;
    });
    let insertedId = 0n;

    const pending = transaction(database, async (tx) => {
      insertedId = (await client.insert(definition, {}, { tx })).job.id;
      transactionStarted();
      await held;
    });
    await started;
    await new Promise((resolve) => setTimeout(resolve, 20));

    expect(run.state).toBe("running");
    expect(run.diagnostics.activeAttempts).toBe(0);

    release();
    await pending;
    await waitForJob(client, insertedId, "completed");
    await run.stop();

    expect(run.state).toBe("stopped");
  });

  test("a poll-only client polls for its running jobs' cancellations, like Go", async () => {
    const { driver } = await setup();
    const job = defineJob({ kind: "sqlite_runtime_poll_only_cancel" });
    const client = new Client(driver, {
      leaderElectionDisabled: true,
      pollOnly: true,
      queues: {
        default: {
          fetchCooldown: { milliseconds: 1 },
          maxWorkers: 1,
          pollInterval: { milliseconds: 5 },
        },
      },
      workers: new Workers().add(
        job,
        ({ signal }) =>
          new Promise((_, reject) => {
            signal.addEventListener("abort", () => reject(signal.reason), {
              once: true,
            });
          })
      ),
    });
    const inserted = await client.insert(job, {});
    const run = await client.start();
    try {
      await waitUntilAsync(
        async () =>
          (await client.jobs.get(inserted.job.id))?.state === "running"
      );
      // Another client's cancellation reaches this one only by polling.
      await new Client(driver).jobs.cancel(inserted.job.id);
      await waitUntilAsync(
        async () =>
          (await client.jobs.get(inserted.job.id))?.state === "cancelled"
      );
    } finally {
      await run.stop();
    }
  });

  test("claims only kinds with workers when fetchOnlyKnownKinds is set", async () => {
    const { driver } = await setup();
    const known = defineJob({ kind: "sqlite_runtime_known_kind" });
    const other = defineJob({ kind: "sqlite_runtime_other_kind" });
    const queues = {
      default: {
        fetchCooldown: { milliseconds: 1 },
        maxWorkers: 2,
        pollInterval: { milliseconds: 5 },
      },
    };
    const client = new Client(driver, {
      fetchOnlyKnownKinds: true,
      leaderElectionDisabled: true,
      queues,
      workers: new Workers().add(known, () => undefined),
    });
    // The other kind comes first in claim order.
    const otherJob = await client.insert(other, {});
    const knownJob = await client.insert(known, {});

    const run = await client.start();
    await waitForJob(client, knownJob.job.id, "completed");
    await run.stop();

    // Left available without using an attempt, for a client that knows it.
    expect(await client.jobs.get(otherJob.job.id)).toMatchObject({
      attempt: 0,
      errors: [],
      state: "available",
    });
    const otherClient = new Client(driver, {
      leaderElectionDisabled: true,
      queues,
      workers: new Workers().add(other, () => undefined),
    });
    const otherRun = await otherClient.start();
    await waitForJob(otherClient, otherJob.job.id, "completed");
    await otherRun.stop();
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

async function waitForJob(
  client: Client,
  id: bigint,
  state: string
): Promise<
  Awaited<ReturnType<Client["jobs"]["get"]>> & { metadata: JsonObject }
> {
  for (let index = 0; index < 1_000; index++) {
    const job = await client.jobs.get(id);
    if (job?.state === state) return job;
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error(`job ${id} did not reach ${state}`);
}

/** Wait up to six seconds, covering a few two-second cancellation polls. */
async function waitUntilAsync(
  condition: () => Promise<boolean>
): Promise<void> {
  const deadline = Date.now() + 6_000;
  while (!(await condition())) {
    if (Date.now() > deadline) throw new Error("condition was not reached");
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
}

async function waitUntil(condition: () => boolean): Promise<void> {
  for (let index = 0; index < 1_000; index++) {
    if (condition()) return;
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error("condition was not reached");
}
