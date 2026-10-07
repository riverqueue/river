import type { DatabaseSync } from "node:sqlite";

import {
  Client,
  type JsonObject,
  Workers,
  defineJob,
  snooze,
} from "riverqueue";
import { describe, expect, onTestFinished, test } from "vitest";

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

/** Fast queue settings so the runtime claims promptly. */
function fastQueue(maxWorkers = 1): {
  fetchCooldown: { milliseconds: number };
  maxWorkers: number;
  pollInterval: { milliseconds: number };
} {
  return {
    fetchCooldown: { milliseconds: 1 },
    maxWorkers,
    pollInterval: { milliseconds: 5 },
  };
}

/** A worker body that settles only by rejecting with its signal's reason. */
function untilAborted(signal: AbortSignal): Promise<never> {
  return new Promise((_resolve, reject) => {
    if (signal.aborted) {
      reject(signal.reason as Error);
      return;
    }
    signal.addEventListener(
      "abort",
      () => {
        reject(signal.reason as Error);
      },
      { once: true }
    );
  });
}

describe("SqliteDriver lifecycle", () => {
  test("adds, reconfigures, and removes a queue on a running client", async () => {
    const { driver } = await setup();
    const job = defineJob({ kind: "sqlite_lifecycle_dynamic_queue" });
    const marker = defineJob({ kind: "sqlite_lifecycle_dynamic_marker" });
    const started: bigint[] = [];
    const release = Promise.withResolvers<undefined>();
    const client = new Client(driver, {
      completionBatchSize: 1,
      leaderElectionDisabled: true,
      queues: { default: fastQueue() },
      workers: new Workers()
        .add(job, async ({ job: { id } }) => {
          started.push(id);
          await release.promise;
        })
        .add(marker, () => undefined),
    });
    const run = await client.start();
    try {
      await run.addQueue("dynamic", fastQueue(1));
      const first = await client.insert(job, {}, { queue: "dynamic" });
      const second = await client.insert(job, {}, { queue: "dynamic" });

      // With one worker, only one of the blocked jobs runs.
      await waitUntil(() => started.length === 1);
      await sleep(50);
      expect(started).toHaveLength(1);

      // Reconfiguring the queue with two workers runs both concurrently.
      await run.updateQueue("dynamic", fastQueue(2));
      await waitUntil(() => started.length === 2);
      expect([...started].sort()).toEqual([first.job.id, second.job.id]);
      release.resolve(undefined);
      await waitForJob(client, first.job.id, "completed");
      await waitForJob(client, second.job.id, "completed");

      // A removed queue isn't worked any more, while others still are.
      await expect(run.removeQueue("dynamic")).resolves.toBe(true);
      expect(run.diagnostics.queues.dynamic).toBeUndefined();
      const stranded = await client.insert(job, {}, { queue: "dynamic" });
      const control = await client.insert(marker, {});
      await waitForJob(client, control.job.id, "completed");
      await sleep(50);
      expect(await client.jobs.get(stranded.job.id)).toMatchObject({
        attempt: 0,
        state: "available",
      });
      expect(started).toHaveLength(2);
    } finally {
      release.resolve(undefined);
      await run.stop();
    }
  });

  test("an error handler's cancel decision cancels a failing job", async () => {
    const { driver } = await setup();
    const job = defineJob({ kind: "sqlite_lifecycle_error_handler_cancel" });
    const handled: unknown[] = [];
    const client = new Client(driver, {
      completionBatchSize: 1,
      errorHandler: (_context, error) => {
        handled.push(error);
        return { cancel: true };
      },
      leaderElectionDisabled: true,
      queues: { default: fastQueue() },
      workers: new Workers().add(job, () => {
        throw new Error("handler cancels this");
      }),
    });
    using events = client.subscribe({ kinds: ["job_cancelled"] });
    const inserted = await client.insert(job, {}, { maxAttempts: 3 });
    const run = await client.start();
    try {
      const cancelled = await waitForJob(client, inserted.job.id, "cancelled");
      expect(cancelled.attempt).toBe(1);
      expect(cancelled.finalizedAt).not.toBeNull();
      expect(cancelled.errors).toHaveLength(1);
      expect(cancelled.errors[0]).toMatchObject({
        attempt: 1,
        error: expect.stringContaining("handler cancels this"),
      });
      const { value: event } = await events.next();
      expect(event).toMatchObject({
        job: { id: inserted.job.id, state: "cancelled" },
        kind: "job_cancelled",
      });
    } finally {
      await run.stop();
    }
    expect(handled).toHaveLength(1);
    expect((handled[0] as Error).message).toBe("handler cancels this");
  });

  test("insert middleware wraps insert hooks and work middleware wraps work hooks", async () => {
    const { driver } = await setup();
    const job = defineJob({ kind: "sqlite_lifecycle_extension_order" });
    const order: string[] = [];
    const client = new Client(driver, {
      completionBatchSize: 1,
      hooks: {
        afterWork: () => {
          order.push("work_hook_after");
        },
        beforeInsert: () => {
          order.push("insert_hook");
        },
        beforeWork: () => {
          order.push("work_hook_before");
        },
      },
      insertMiddleware: [
        async (_context, next) => {
          order.push("insert_middleware_before");
          const results = await next();
          order.push("insert_middleware_after");
          return results;
        },
      ],
      leaderElectionDisabled: true,
      middleware: [
        async (_context, next) => {
          order.push("work_middleware_before");
          const result = await next();
          order.push("work_middleware_after");
          return result;
        },
      ],
      queues: { default: fastQueue() },
      workers: new Workers().add(job, () => {
        order.push("worker");
      }),
    });
    const run = await client.start();
    try {
      const inserted = await client.insert(job, {});
      await waitForJob(client, inserted.job.id, "completed");
    } finally {
      await run.stop();
    }
    expect(order).toEqual([
      "insert_middleware_before",
      "insert_hook",
      "insert_middleware_after",
      "work_middleware_before",
      "work_hook_before",
      "worker",
      "work_hook_after",
      "work_middleware_after",
    ]);
  });

  test("cancels a snoozed job remotely during its refetched attempt", async () => {
    const { driver } = await setup();
    const job = defineJob({ kind: "sqlite_lifecycle_snooze_then_cancel" });
    const attempts: number[] = [];
    const snoozesSeen: unknown[] = [];
    let observedAbort: unknown;
    const client = new Client(driver, {
      completionBatchSize: 1,
      leaderElectionDisabled: true,
      queues: { default: fastQueue() },
      workers: new Workers().add(job, async ({ job: row, signal }) => {
        attempts.push(row.attempt);
        snoozesSeen.push(row.metadata.snoozes);
        if (attempts.length === 1) return snooze({ milliseconds: 1 });
        try {
          return await untilAborted(signal);
        } catch (error: unknown) {
          observedAbort = error;
          throw error;
        }
      }),
    });
    const inserted = await client.insert(job, {});
    const run = await client.start();
    try {
      await waitUntilAsync(async () => {
        const row = await client.jobs.get(inserted.job.id);
        return row?.state === "running" && attempts.length === 2;
      });
      expect(snoozesSeen).toEqual([undefined, 1]);
      // Like Go, a snooze gives its attempt back, so the refetched attempt
      // is the job's first again.
      expect(attempts).toEqual([1, 1]);
      const running = await client.jobs.get(inserted.job.id);
      expect(running?.metadata.snoozes).toBe(1);

      await new Client(driver).jobs.cancel(inserted.job.id);
      const cancelled = await waitForJob(
        client,
        inserted.job.id,
        "cancelled",
        6_000
      );
      expect(cancelled.finalizedAt).not.toBeNull();
      expect(observedAbort).toBeDefined();
      expect((observedAbort as Error).name).toBe("JobCancelledError");
    } finally {
      await run.stop();
    }
  });

  test("skips a completed resumable step on retry", async () => {
    const { driver } = await setup();
    const job = defineJob({ kind: "sqlite_lifecycle_resumable_retry" });
    const ran: string[] = [];
    const storedSteps: unknown[] = [];
    const client = new Client(driver, {
      completionBatchSize: 1,
      leaderElectionDisabled: true,
      queues: { default: fastQueue() },
      retryPolicy: (_job, now) => now,
      workers: new Workers().add(job, async ({ job: row, resumable }) => {
        storedSteps.push(row.metadata["river:resumable_step"]);
        await resumable.step("first", () => {
          ran.push("first");
        });
        await resumable.step("second", () => {
          ran.push("second");
          if (row.attempt === 1) throw new Error("second failed");
        });
      }),
    });
    const inserted = await client.insert(job, {});
    const run = await client.start();
    try {
      const completed = await waitForJob(client, inserted.job.id, "completed");
      expect(completed.attempt).toBe(2);
      expect(completed.errors).toHaveLength(1);
      // Like River for Go, the step's own error is recorded.
      expect(completed.errors[0]?.error).toContain("second failed");
    } finally {
      await run.stop();
    }
    expect(ran).toEqual(["first", "second", "second"]);
    // The failed attempt stored its last completed step for the retry.
    expect(storedSteps).toEqual([undefined, "first"]);
  });

  describe("resumable validation", () => {
    test.each([
      ["without a stored step", undefined, ["first"]],
      ["skipping to a stored later step", "third", [] as string[]],
    ])(
      "fails a job reusing a step name, %s",
      async (_name, storedStep, expectedRan) => {
        const { driver } = await setup();
        const job = defineJob({ kind: "sqlite_lifecycle_resumable_duplicate" });
        const ran: string[] = [];
        const client = new Client(driver, {
          completionBatchSize: 1,
          leaderElectionDisabled: true,
          queues: { default: fastQueue() },
          workers: new Workers().add(job, async ({ resumable }) => {
            // Like Go's ResumableStep, which reports through the
            // middleware, the duplicate fails the job even when the worker
            // swallows it.
            try {
              await resumable.step("first", () => {
                ran.push("first");
              });
              await resumable.step("first", () => {
                ran.push("duplicate");
              });
              await resumable.step("third", () => {
                ran.push("third");
              });
            } catch {
              // Ignored on purpose.
            }
          }),
        });
        const inserted = await client.insert(
          job,
          {},
          {
            maxAttempts: 1,
            ...(storedStep === undefined
              ? {}
              : { metadata: { "river:resumable_step": storedStep } }),
          }
        );
        const run = await client.start();
        try {
          const discarded = await waitForJob(
            client,
            inserted.job.id,
            "discarded"
          );
          expect(discarded.errors).toHaveLength(1);
          expect(discarded.errors[0]?.error).toContain(
            'duplicate resumable step name "first"'
          );
        } finally {
          await run.stop();
        }
        expect(ran).toEqual(expectedRan);
      }
    );

    test("restarts from the first step when the stored step is empty", async () => {
      const { driver } = await setup();
      const job = defineJob({ kind: "sqlite_lifecycle_resumable_empty" });
      const ran: string[] = [];
      const client = new Client(driver, {
        completionBatchSize: 1,
        leaderElectionDisabled: true,
        queues: { default: fastQueue() },
        workers: new Workers().add(job, async ({ resumable }) => {
          await resumable.step("first", () => {
            ran.push("first");
          });
          await resumable.step("second", () => {
            ran.push("second");
          });
        }),
      });
      const inserted = await client.insert(
        job,
        {},
        { metadata: { "river:resumable_step": "" } }
      );
      const run = await client.start();
      try {
        await waitForJob(client, inserted.job.id, "completed");
      } finally {
        await run.stop();
      }
      expect(ran).toEqual(["first", "second"]);
    });

    test("fails an attempt with an invalid cursor before work runs", async () => {
      const { driver } = await setup();
      const job = defineJob({ kind: "sqlite_lifecycle_resumable_bad_cursor" });
      let worked = false;
      const client = new Client(driver, {
        completionBatchSize: 1,
        leaderElectionDisabled: true,
        queues: { default: fastQueue() },
        workers: new Workers().add(job, async ({ resumable }) => {
          worked = true;
          await resumable.stepWithCursor("first", () => undefined);
        }),
      });
      const inserted = await client.insert(
        job,
        {},
        {
          maxAttempts: 1,
          metadata: { "river:resumable_cursor": ["not", "an", "object"] },
        }
      );
      const run = await client.start();
      try {
        const discarded = await waitForJob(
          client,
          inserted.job.id,
          "discarded"
        );
        expect(discarded.attempt).toBe(1);
        expect(discarded.errors).toHaveLength(1);
        expect(discarded.errors[0]?.error).toContain("river:resumable_cursor");
      } finally {
        await run.stop();
      }
      expect(worked).toBe(false);
    });
  });

  test("a job timeout cancels a cooperative worker", async () => {
    const { driver } = await setup();
    const job = defineJob({ kind: "sqlite_lifecycle_timeout" });
    const timeoutMs = 50;
    const client = new Client(driver, {
      completionBatchSize: 1,
      leaderElectionDisabled: true,
      queues: { default: fastQueue() },
      workers: new Workers().add(job, ({ signal }) => untilAborted(signal), {
        timeout: { milliseconds: timeoutMs },
      }),
    });
    const inserted = await client.insert(job, {}, { maxAttempts: 1 });
    const run = await client.start();
    try {
      const discarded = await waitForJob(client, inserted.job.id, "discarded");
      expect(discarded.attempt).toBe(1);
      expect(discarded.errors).toHaveLength(1);
      expect(discarded.errors[0]?.error).toContain("timeout");
      const { attemptedAt, finalizedAt } = discarded;
      if (attemptedAt === null || finalizedAt === null) {
        throw new Error("expected attempted and finalized times");
      }
      // SQLite stores millisecond timestamps, so truncating both ends can
      // shave up to a millisecond off the measured attempt.
      expect(
        Number(finalizedAt.epochNanoseconds - attemptedAt.epochNanoseconds) /
          1e6
      ).toBeGreaterThanOrEqual(timeoutMs - 1);
    } finally {
      await run.stop();
    }
  });

  test("a hard stop gives back cooperatively stopped attempts but not failures", async () => {
    const { driver } = await setup();
    const cooperative = defineJob({
      kind: "sqlite_lifecycle_stop_cooperative",
    });
    const ordinary = defineJob({ kind: "sqlite_lifecycle_stop_ordinary" });
    const fault = defineJob({ kind: "sqlite_lifecycle_stop_fault" });
    let running = 0;
    const throwAfterAbort =
      (makeError: () => Error) =>
      async ({ signal }: { signal: AbortSignal }): Promise<void> => {
        running++;
        await untilAborted(signal).catch(() => undefined);
        throw makeError();
      };
    const client = new Client(driver, {
      completionBatchSize: 1,
      leaderElectionDisabled: true,
      queues: { default: fastQueue(3) },
      workers: new Workers()
        .add(cooperative, async ({ signal }) => {
          running++;
          await untilAborted(signal);
        })
        .add(
          ordinary,
          throwAfterAbort(() => new Error("cleanup failed"))
        )
        .add(
          fault,
          throwAfterAbort(() => new TypeError("cleanup faulted"))
        ),
    });
    const cooperativeJob = await client.insert(cooperative, {});
    const ordinaryJob = await client.insert(ordinary, {});
    const faultJob = await client.insert(fault, {});
    const run = await client.start();
    await waitUntil(() => running === 3);
    const attemptedAt = (await client.jobs.get(cooperativeJob.job.id))
      ?.attemptedAt;
    expect(attemptedAt).not.toBeNull();

    await run.stop({ mode: "cancel" });

    const stopped = await client.jobs.get(cooperativeJob.job.id);
    expect(stopped).toMatchObject({
      attempt: 0,
      errors: [],
      finalizedAt: null,
      state: "available",
    });
    expect(stopped?.attemptedAt?.equals(attemptedAt!)).toBe(true);
    for (const [inserted, message] of [
      [ordinaryJob, "cleanup failed"],
      [faultJob, "cleanup faulted"],
    ] as const) {
      const failed = await client.jobs.get(inserted.job.id);
      expect(failed?.attempt).toBe(1);
      expect(failed?.errors).toHaveLength(1);
      expect(failed?.errors[0]?.error).toContain(message);
      expect(failed?.finalizedAt).toBeNull();
      expect(["available", "retryable"]).toContain(failed?.state);
    }
  });

  test("a hard stop cancels a job whose cancellation never arrived", async () => {
    const { database, driver } = await setup();
    const job = defineJob({ kind: "sqlite_lifecycle_stop_cancel_attempted" });
    let running = false;
    const client = new Client(driver, {
      completionBatchSize: 1,
      leaderElectionDisabled: true,
      queues: { default: fastQueue() },
      workers: new Workers().add(job, async ({ signal }) => {
        running = true;
        await untilAborted(signal);
      }),
    });
    const inserted = await client.insert(job, {});
    const run = await client.start();
    await waitUntil(() => running);

    // Request cancellation without notifying, like a cancellation whose
    // notification was lost.
    database
      .prepare(
        `UPDATE river_job
         SET metadata = jsonb_set(metadata, '$.cancel_attempted_at', ?)
         WHERE id = ?`
      )
      .run(Temporal.Now.instant().toString(), inserted.job.id);
    await run.stop({ mode: "cancel" });

    const cancelled = await client.jobs.get(inserted.job.id);
    expect(cancelled?.state).toBe("cancelled");
    expect(cancelled?.finalizedAt).not.toBeNull();
    expect(cancelled?.attempt).toBe(1);
  });

  test("a graceful stop waits for a running job to complete", async () => {
    const { driver } = await setup();
    const job = defineJob({ kind: "sqlite_lifecycle_graceful_stop" });
    let running = false;
    let finished = false;
    const client = new Client(driver, {
      completionBatchSize: 1,
      leaderElectionDisabled: true,
      queues: { default: fastQueue() },
      workers: new Workers().add(job, async ({ signal }) => {
        running = true;
        await sleep(150);
        expect(signal.aborted).toBe(false);
        finished = true;
      }),
    });
    const inserted = await client.insert(job, {});
    const run = await client.start();
    await waitUntil(() => running);

    await run.stop();

    expect(finished).toBe(true);
    expect(run.state).toBe("stopped");
    const completed = await client.jobs.get(inserted.job.id);
    expect(completed).toMatchObject({ attempt: 1, state: "completed" });
  });

  test("leaves a paused dynamic queue's jobs until it resumes", async () => {
    const { driver } = await setup();
    const job = defineJob({ kind: "sqlite_lifecycle_paused_queue" });
    const marker = defineJob({ kind: "sqlite_lifecycle_paused_marker" });
    const client = new Client(driver, {
      completionBatchSize: 1,
      leaderElectionDisabled: true,
      queues: { default: fastQueue() },
      workers: new Workers()
        .add(job, () => undefined)
        .add(marker, () => undefined),
    });
    const run = await client.start();
    try {
      await run.addQueue("dynamic_paused", fastQueue());
      const paused = await client.queues.pause("dynamic_paused");
      expect(paused?.pausedAt).not.toBeNull();
      const pending = await client.insert(job, {}, { queue: "dynamic_paused" });
      const control = await client.insert(marker, {});
      await waitForJob(client, control.job.id, "completed");
      await sleep(50);
      expect(await client.jobs.get(pending.job.id)).toMatchObject({
        attempt: 0,
        state: "available",
      });

      const resumed = await client.queues.resume("dynamic_paused");
      expect(resumed?.pausedAt).toBeNull();
      const completed = await waitForJob(client, pending.job.id, "completed");
      const attemptedAt = completed.attemptedAt;
      if (attemptedAt === null || resumed === null) {
        throw new Error("expected an attempt and a resumed queue");
      }
      expect(
        Temporal.Instant.compare(attemptedAt, resumed.updatedAt)
      ).toBeGreaterThanOrEqual(0);
      await expect(run.removeQueue("dynamic_paused")).resolves.toBe(true);
    } finally {
      await run.stop();
    }
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

function sleep(milliseconds: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, milliseconds));
}

async function waitForJob(
  client: Client,
  id: bigint,
  state: string,
  timeoutMs = 3_000
): Promise<
  Awaited<ReturnType<Client["jobs"]["get"]>> & { metadata: JsonObject }
> {
  const deadline = Date.now() + timeoutMs;
  for (;;) {
    const job = await client.jobs.get(id);
    if (job?.state === state) return job;
    if (Date.now() > deadline) {
      throw new Error(
        `job ${id} did not reach ${state}; last state ${job?.state}`
      );
    }
    await sleep(2);
  }
}

/** Wait up to six seconds, covering a few two-second cancellation polls. */
async function waitUntilAsync(
  condition: () => Promise<boolean>
): Promise<void> {
  const deadline = Date.now() + 6_000;
  while (!(await condition())) {
    if (Date.now() > deadline) throw new Error("condition was not reached");
    await sleep(5);
  }
}

async function waitUntil(condition: () => boolean): Promise<void> {
  const deadline = Date.now() + 3_000;
  while (!condition()) {
    if (Date.now() > deadline) throw new Error("condition was not reached");
    await sleep(1);
  }
}
