import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { SqliteDriver } from "@riverqueue/driver-sqlite";
import { createMigrator } from "@riverqueue/migrate";
import {
  Client,
  exactJsonNumber,
  isExactJsonNumber,
  LifecycleError,
  Workers,
} from "riverqueue";
import type { JobRow, JsonObject, WorkContext } from "riverqueue";

import { WorkerThreads } from "./index.js";
import type { WorkerThreadModule } from "./index.js";
import type * as testHandlers from "./testdata/handlers.js";
import { testJob } from "./testdata/jobs.js";

const handlers: WorkerThreadModule<typeof testHandlers> = new URL(
  "./testdata/handlers.js",
  import.meta.url
);

// A thread failure must never surface as a host-process failure.
const hostFailures: unknown[] = [];
const recordHostFailure = (reason: unknown) => hostFailures.push(reason);
beforeAll(() => {
  process.on("uncaughtException", recordHostFailure);
  process.on("unhandledRejection", recordHostFailure);
});
afterAll(() => {
  process.off("uncaughtException", recordHostFailure);
  process.off("unhandledRejection", recordHostFailure);
  expect(hostFailures).toEqual([]);
});

describe("WorkerThreads with River clients", () => {
  interface TestClient {
    readonly client: Client;
    readonly driver: SqliteDriver;
  }

  const setup = async (
    executor: WorkerThreads,
    exportName: "complete" | "cooperate" | "echoExact" | "spin" = "complete",
    jobStuckThreshold?: Temporal.DurationLike
  ): Promise<TestClient> => {
    const driver = SqliteDriver.memory({
      // The SQLite driver's private strict lock-window check.
      [Symbol.for("riverqueue.sqlite.driver.test_hooks")]: {
        strictLockWindow: true,
      },
    });
    await createMigrator(driver).migrateUp();
    const workers = new Workers().addExecutor(
      testJob,
      executor.handler(testJob, { exportName, module: handlers })
    );
    const client = new Client(driver, {
      ...(jobStuckThreshold === undefined ? {} : { jobStuckThreshold }),
      queues: {
        default: {
          fetchCooldown: { milliseconds: 1 },
          maxWorkers: 2,
          pollInterval: { milliseconds: 10 },
        },
      },
      workers,
    });
    return { client, driver };
  };

  it("closes an executor at the end of an await using scope", async () => {
    let closed: WorkerThreads | undefined;
    {
      await using executor = new WorkerThreads({ maxThreads: 1 });
      closed = executor;
      const target = executor.handler(testJob, {
        exportName: "complete",
        module: handlers,
      });
      await expect(
        executor.start(workContext({ value: "scoped" }), target.handler).result
      ).resolves.toEqual({ output: { value: "scoped" }, type: "complete" });
      expect(executor.diagnostics()).toMatchObject({ totalThreads: 1 });
    }

    expect(closed.diagnostics()).toMatchObject({ totalThreads: 0 });
    const target = closed.handler(testJob, {
      exportName: "complete",
      module: handlers,
    });
    await expect(
      closed.start(workContext({ value: "late" }), target.handler).result
    ).rejects.toThrow(LifecycleError);
  });

  it("cancels a cooperative handler and reuses its thread", async () => {
    await using executor = new WorkerThreads({ maxThreads: 1 });
    const { client, driver } = await setup(executor, "cooperate");
    const run = await client.start();

    const inserted = await client.insert(testJob, {});
    await waitFor(() => executor.diagnostics().activeThreads === 1);
    await client.jobs.cancel(inserted.job.id);

    await expect(
      waitForFinalized(client, inserted.job.id)
    ).resolves.toMatchObject({ state: "cancelled" });
    expect(executor.diagnostics()).toMatchObject({
      crashedThreads: 0,
      idleThreads: 1,
      totalThreads: 1,
    });
    await run.stop();
    driver.close();
  });

  it("persists an ignored cancellation only after terminating its thread", async () => {
    await using executor = new WorkerThreads({ maxThreads: 1 });
    const { client, driver } = await setup(executor, "spin", {
      milliseconds: 10,
    });
    const run = await client.start();

    const inserted = await client.insert(testJob, {});
    await waitFor(() => executor.diagnostics().activeThreads === 1);
    await client.jobs.cancel(inserted.job.id);

    await expect(
      waitForFinalized(client, inserted.job.id)
    ).resolves.toMatchObject({ state: "cancelled" });
    // The thread had exited before River persisted the cancellation.
    expect(executor.diagnostics()).toMatchObject({
      crashedThreads: 0,
      totalThreads: 0,
    });
    await run.stop();
    driver.close();
  });

  it("fails an attempt whose thread ignored a stop's abort, after the stuck threshold", async () => {
    await using executor = new WorkerThreads({ maxThreads: 1 });
    const { client, driver } = await setup(executor, "spin", {
      milliseconds: 200,
    });
    const run = await client.start();

    const spinning = await client.insert(testJob, {});
    await waitFor(() => executor.diagnostics().activeThreads === 1);
    // The client works two jobs, so this one waits for the busy thread.
    const waiting = await client.insert(testJob, {});
    await waitFor(() => executor.diagnostics().pendingTasks === 1);
    const stopping = Date.now();
    await run.stop({ mode: "cancel" });

    // The stop waited for the whole threshold before terminating it.
    expect(Date.now() - stopping).toBeGreaterThanOrEqual(200);
    const spun = await client.jobs.get(spinning.job.id);
    expect(spun).toMatchObject({
      attempt: 1,
      errors: [
        { attempt: 1, error: "job aborted after ignoring cancellation" },
      ],
    });
    expect(["available", "retryable"]).toContain(spun?.state);
    // The waiting attempt never ran, so it's interrupted without using an
    // attempt.
    await expect(client.jobs.get(waiting.job.id)).resolves.toMatchObject({
      attempt: 0,
      errors: [],
      state: "available",
    });
    driver.close();
  });

  it("interrupts a handler that stops because of a stop's abort without using its attempt", async () => {
    await using executor = new WorkerThreads({ maxThreads: 1 });
    const { client, driver } = await setup(executor, "cooperate");
    const run = await client.start();

    const inserted = await client.insert(testJob, {});
    await waitFor(() => executor.diagnostics().activeThreads === 1);
    await run.stop({ mode: "cancel" });

    await expect(client.jobs.get(inserted.job.id)).resolves.toMatchObject({
      attempt: 0,
      errors: [],
      state: "available",
    });
    expect(executor.diagnostics()).toMatchObject({ totalThreads: 1 });
    driver.close();
  });

  it("keeps a shared executor open when one of its clients stops", async () => {
    await using executor = new WorkerThreads({ maxThreads: 1 });
    const first = await setup(executor);
    const second = await setup(executor);
    const firstRun = await first.client.start();
    const secondRun = await second.client.start();

    await expect(work(first.client, { value: "first" })).resolves.toMatchObject(
      { metadata: { output: { value: "first" } }, state: "completed" }
    );
    await firstRun.stop();

    await expect(
      work(second.client, { value: "second" })
    ).resolves.toMatchObject({
      metadata: { output: { value: "second" } },
      state: "completed",
    });
    await secondRun.stop();
    expect(executor.diagnostics()).toMatchObject({
      idleThreads: 1,
      totalThreads: 1,
    });
    first.driver.close();
    second.driver.close();
  });

  it("serves a client started after another client stopped", async () => {
    await using executor = new WorkerThreads({ maxThreads: 1 });
    const first = await setup(executor);
    const firstRun = await first.client.start();
    await expect(
      work(first.client, { value: "before" })
    ).resolves.toMatchObject({ state: "completed" });
    await firstRun.stop();

    const second = await setup(executor);
    const secondRun = await second.client.start();
    await expect(
      work(second.client, { value: "after" })
    ).resolves.toMatchObject({
      metadata: { output: { value: "after" } },
      state: "completed",
    });
    await secondRun.stop();
    first.driver.close();
    second.driver.close();
  });

  it("works a persisted exact int64 through a single thread", async () => {
    await using executor = new WorkerThreads({ maxThreads: 1 });
    const { client, driver } = await setup(executor, "echoExact");
    const run = await client.start();

    const worked = await work(client, {
      id: exactJsonNumber("9007199254740993"),
    });
    expect(worked.state).toBe("completed");
    const output = worked.metadata["output"] as JsonObject;
    expect(
      isExactJsonNumber(output["id"]) ? output["id"].rawJSON : output["id"]
    ).toBe("9007199254740993");

    await expect(
      work(client, { id: exactJsonNumber("9007199254740995") })
    ).resolves.toMatchObject({ state: "completed" });
    await run.stop();
    driver.close();
  });
});

/** Insert one job and poll until River finalizes it. */
async function work(client: Client, args: JsonObject): Promise<JobRow> {
  const inserted = await client.insert(testJob, args);
  return waitForFinalized(client, inserted.job.id);
}

/** Poll between event-loop turns until a condition holds. */
async function waitFor(predicate: () => boolean): Promise<void> {
  const deadline = Date.now() + 5_000;
  while (!predicate()) {
    if (Date.now() > deadline) throw new Error("condition was not reached");
    await new Promise<void>((resolve) => setTimeout(resolve, 1));
  }
}

async function waitForFinalized(client: Client, id: bigint): Promise<JobRow> {
  const deadline = Date.now() + 10_000;
  for (;;) {
    const job = await client.jobs.get(id);
    if (job !== null && job.finalizedAt !== null) return job;
    if (Date.now() > deadline) throw new Error("job was not finalized");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

function workContext(args: JsonObject): WorkContext {
  const now = Temporal.Now.instant();
  return {
    client: {} as WorkContext["client"],
    completeTx: () =>
      Promise.reject(new Error("test context has no transaction")),
    execution: { attemptedBy: "worker-thread-test", startedAt: now },
    job: {
      args,
      attempt: 1,
      attemptedAt: now,
      attemptedBy: ["worker-thread-test"],
      createdAt: now,
      errors: [],
      finalizedAt: null,
      id: 1n,
      kind: testJob.kind,
      maxAttempts: 3,
      metadata: {},
      priority: 1,
      queue: "default",
      rawArgs: args,
      scheduledAt: now,
      state: "running",
      tags: [],
      uniqueKey: null,
      uniqueStates: null,
    },
    logger: {
      debug: () => undefined,
      error: () => undefined,
      info: () => undefined,
      warn: () => undefined,
    },
    recordOutput: () => undefined,
    resumable: {} as WorkContext["resumable"],
    setMetadata: () => undefined,
    signal: new AbortController().signal,
  };
}
