import assert from "node:assert/strict";
import { describe, it } from "node:test";
import { URL } from "node:url";

import { SqliteDriver } from "@riverqueue/driver-sqlite";
import { createMigrator } from "@riverqueue/migrate";
import { WorkerThreads } from "@riverqueue/worker-threads";
import {
  Client,
  complete,
  DatabaseOperationError,
  defineJob,
  exactJsonNumber,
  isExactJsonNumber,
  jsonNumberToBigInt,
  RiverError,
  Workers,
} from "riverqueue";

import { workUntilFinalized } from "./helpers.mjs";

const INT8_MAX = 9_223_372_036_854_775_807n;

const accountJob = defineJob({
  kind: "packed_account",
  decode(value) {
    if (
      !isExactJsonNumber(value.accountId) &&
      typeof value.accountId !== "number"
    ) {
      throw new TypeError("accountId must be a JSON number");
    }
    return { accountId: value.accountId };
  },
});
const threadJob = defineJob({
  kind: "packed_thread_account",
  decode: (value) => value,
});

async function migratedDriver() {
  const driver = SqliteDriver.memory();
  const migrator = createMigrator(driver);
  const applied = await migrator.migrateUp();
  assert.ok(applied.versions.length > 0, "migrations were applied");
  return driver;
}

describe("packed SQLite runtime", () => {
  it("inserts, works, and completes jobs with exact int64 args", async () => {
    const driver = await migratedDriver();
    try {
      const seen = [];
      const workers = new Workers().add(accountJob, ({ job, recordOutput }) => {
        seen.push(job.args.accountId);
        recordOutput({ accountId: job.args.accountId });
      });
      const client = new Client(driver, {
        queues: { default: { maxWorkers: 2 } },
        workers,
      });
      const accountIds = [INT8_MAX, -INT8_MAX - 1n, 9_007_199_254_740_993n];
      const inserted = await client.insertMany(
        accountIds.map((accountId) => ({
          args: { accountId: exactJsonNumber(accountId.toString()) },
          job: accountJob,
        }))
      );
      const ids = inserted.map((result) => result.job.id);
      assert.ok(ids.every((id) => typeof id === "bigint"));

      const events = await workUntilFinalized(client, ids);
      for (const [index, id] of ids.entries()) {
        assert.equal(events.get(id)?.kind, "job_completed");
        const row = await client.jobs.get(id);
        assert.equal(row?.state, "completed");
        assert.equal(jsonNumberToBigInt(row.args.accountId), accountIds[index]);
        assert.equal(
          jsonNumberToBigInt(row.metadata.output.accountId),
          accountIds[index]
        );
      }
      assert.deepEqual(
        seen.map((value) => jsonNumberToBigInt(value)).sort(),
        [...accountIds].sort()
      );
    } finally {
      driver.close();
    }
  });

  it("runs a handler on a worker thread from the packed module", async () => {
    const driver = await migratedDriver();
    const executor = new WorkerThreads({ maxThreads: 1 });
    try {
      const workers = new Workers().addExecutor(
        threadJob,
        executor.handler(threadJob, {
          exportName: "echoAccount",
          module: new URL("./fixtures/thread-handlers.mjs", import.meta.url),
        })
      );
      const client = new Client(driver, {
        queues: { default: { maxWorkers: 1 } },
        workers,
      });
      const { job } = await client.insert(threadJob, {
        accountId: exactJsonNumber(INT8_MAX.toString()),
      });
      const events = await workUntilFinalized(client, [job.id]);
      assert.equal(events.get(job.id)?.kind, "job_completed");
      const row = await client.jobs.get(job.id);
      assert.equal(row?.metadata.output.thread, true);
      assert.equal(jsonNumberToBigInt(row.metadata.output.accountId), INT8_MAX);
    } finally {
      await executor.close();
      driver.close();
    }
  });

  it("reports an unmigrated database as a River error", async () => {
    const driver = SqliteDriver.memory();
    try {
      const client = new Client(driver);
      await assert.rejects(
        client.insert(accountJob, { accountId: 1 }),
        (error) =>
          error instanceof DatabaseOperationError && error instanceof RiverError
      );
    } finally {
      driver.close();
    }
  });

  it("completes a job through the documented outcome helpers", async () => {
    const driver = await migratedDriver();
    try {
      const workers = new Workers().add(accountJob, () =>
        complete({ output: { done: true } })
      );
      const client = new Client(driver, {
        queues: { default: { maxWorkers: 1 } },
        workers,
      });
      const { job } = await client.insert(accountJob, { accountId: 7 });
      await workUntilFinalized(client, [job.id]);
      assert.deepEqual(
        { ...(await client.jobs.get(job.id))?.metadata.output },
        { done: true }
      );
    } finally {
      driver.close();
    }
  });
});
