import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";

import {
  Client,
  DatabaseOperationError,
  ValidationError,
  Workers,
  cancel,
  complete,
  defineJob,
  discard,
  type JobRow,
  type JsonObject,
} from "riverqueue";
import { describe, expect, onTestFinished, test } from "vitest";

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

const QUEUES = {
  default: {
    fetchCooldown: { milliseconds: 1 },
    maxWorkers: 1,
    pollInterval: { milliseconds: 5 },
  },
};

describe("SqliteDriver job and queue operations", () => {
  describe("bulk_delete_safety", () => {
    test("deleteMany by an explicit ID set deletes exactly those jobs", async () => {
      const { driver } = await setup();
      const client = new Client(driver);
      const definition = defineJob({ kind: "sqlite_ops_delete_many" });
      const ids: bigint[] = [];
      for (let index = 0; index < 4; index++) {
        ids.push((await client.insert(definition, {})).job.id);
      }
      const [first, second, third, fourth] = ids as [
        bigint,
        bigint,
        bigint,
        bigint,
      ];

      const deleted = await client.jobs.deleteMany({ ids: [first, third] });
      expect(deleted.map(({ id }) => id).sort()).toEqual([first, third]);
      expect(deleted.every(({ kind }) => kind === definition.kind)).toBe(true);

      await expect(client.jobs.get(first)).resolves.toBeNull();
      await expect(client.jobs.get(third)).resolves.toBeNull();
      await expect(client.jobs.get(second)).resolves.toMatchObject({
        id: second,
        state: "available",
      });
      await expect(client.jobs.get(fourth)).resolves.toMatchObject({
        id: fourth,
        state: "available",
      });
    });

    test("an unfiltered deleteMany is rejected and deletes nothing", async () => {
      const { driver } = await setup();
      const client = new Client(driver);
      const definition = defineJob({ kind: "sqlite_ops_delete_many_all" });
      const { job } = await client.insert(definition, {});

      await expect(client.jobs.deleteMany({})).rejects.toBeInstanceOf(
        ValidationError
      );
      await expect(client.jobs.deleteMany({ ids: [] })).rejects.toBeInstanceOf(
        ValidationError
      );

      const { jobs } = await client.jobs.list();
      expect(jobs.map(({ id }) => id)).toEqual([job.id]);
    });
  });

  describe("job_update_and_delete", () => {
    test("update sets metadata.output and returns the row", async () => {
      const { driver } = await setup();
      const client = new Client(driver);
      const definition = defineJob({ kind: "sqlite_ops_update" });
      const { job } = await client.insert(definition, {});

      const updated = await client.jobs.update(job.id, {
        output: { answer: 42 },
      });
      expect(updated).toMatchObject({ id: job.id, state: "available" });
      expect(updated?.metadata.output).toEqual({ answer: 42 });
      expect((await client.jobs.get(job.id))?.metadata.output).toEqual({
        answer: 42,
      });
    });

    test("delete returns the deleted row and the job is gone", async () => {
      const { driver } = await setup();
      const client = new Client(driver);
      const definition = defineJob({ kind: "sqlite_ops_delete" });
      const { job } = await client.insert(definition, {});

      const deleted = await client.jobs.delete(job.id);
      expect(deleted).toMatchObject({
        id: job.id,
        kind: definition.kind,
        state: "available",
      });
      await expect(client.jobs.get(job.id)).resolves.toBeNull();
      await expect(client.jobs.delete(job.id)).resolves.toBeNull();
    });
  });

  describe("panic_attempt_trace", () => {
    test("a runtime fault discards at max attempts with its value and a trace", async () => {
      const { driver } = await setup();
      const definition = defineJob({ kind: "sqlite_ops_panic" });
      const client = new Client(driver, {
        completionBatchSize: 1,
        leaderElectionDisabled: true,
        queues: QUEUES,
        workers: new Workers().add(definition, () => {
          // A genuine runtime fault, River's analog of a Go panic.
          const value: unknown = null;
          return complete({
            output: (value as { field: { length: number } }).field.length,
          });
        }),
      });
      const { job } = await client.insert(definition, {}, { maxAttempts: 1 });
      const run = await client.start();
      let discarded: JobRow;
      try {
        discarded = await waitForJob(client, job.id, "discarded");
      } finally {
        await run.stop();
      }

      expect(discarded.attempt).toBe(1);
      expect(discarded.finalizedAt).not.toBeNull();
      expect(discarded.errors).toHaveLength(1);
      const [attemptError] = discarded.errors;
      expect(attemptError?.attempt).toBe(1);
      expect(attemptError?.error).toContain("reading 'field'");
      expect(attemptError?.trace).not.toBe("");
      expect(attemptError?.trace).toContain("TypeError");
    });
  });

  describe("single_implementation_worker_outcomes", () => {
    test.each([
      ["cancel", "cancelled", () => cancel({ reason: "stop now" }), "stop now"],
      ["discard", "discarded", () => discard({ reason: "give up" }), "give up"],
      [
        "thrown error",
        "discarded",
        () => {
          throw new Error("plain failure");
        },
        "plain failure",
      ],
    ] as const)(
      "a %s outcome finalizes the job as %s after one attempt",
      async (name, state, handler, errorText) => {
        const { driver } = await setup();
        const definition = defineJob({
          kind: `sqlite_ops_outcome_${name.replace(" ", "_")}`,
        });
        const client = new Client(driver, {
          completionBatchSize: 1,
          leaderElectionDisabled: true,
          queues: QUEUES,
          workers: new Workers().add(definition, handler),
        });
        const { job } = await client.insert(definition, {}, { maxAttempts: 1 });
        const run = await client.start();
        let finalized: JobRow;
        try {
          finalized = await waitForJob(client, job.id, state);
        } finally {
          await run.stop();
        }

        expect(finalized.attempt).toBe(1);
        expect(finalized.finalizedAt).not.toBeNull();
        expect(finalized.errors).toHaveLength(1);
        expect(finalized.errors[0]?.attempt).toBe(1);
        expect(finalized.errors[0]?.error).toContain(errorText);
      }
    );

    test("recordOutput completes the job with metadata.output", async () => {
      const { driver } = await setup();
      const definition = defineJob({ kind: "sqlite_ops_record_output" });
      const client = new Client(driver, {
        completionBatchSize: 1,
        leaderElectionDisabled: true,
        queues: QUEUES,
        workers: new Workers().add(definition, ({ recordOutput }) => {
          recordOutput({ recorded: [1, 2, 3] });
        }),
      });
      const { job } = await client.insert(definition, {});
      const run = await client.start();
      let completed: JobRow;
      try {
        completed = await waitForJob(client, job.id, "completed");
      } finally {
        await run.stop();
      }

      expect(completed.attempt).toBe(1);
      expect(completed.errors).toEqual([]);
      expect(completed.finalizedAt).not.toBeNull();
      expect(completed.metadata.output).toEqual({ recorded: [1, 2, 3] });
    });
  });

  describe("transaction_abort_rollback_visibility", () => {
    // Unlike Postgres, SQLite rolls back only the failed statement and
    // keeps the transaction open after most errors. The failures that abort
    // a whole transaction, as every failure does in Postgres, are the
    // ROLLBACK conflict resolution and RAISE(ROLLBACK). After one, the
    // transaction can't commit and its job is never visible, as in Go.

    test("a caller transaction aborted by a failed statement can't commit its job", async () => {
      const { database, driver } = await setup();
      const client = new Client(driver);
      const definition = defineJob({ kind: "sqlite_ops_tx_abort" });
      database.exec("CREATE TABLE app_rows (id INTEGER PRIMARY KEY)");

      database.exec("BEGIN IMMEDIATE");
      const { job } = await client.insert(definition, {}, { tx: database });
      await expect(
        client.jobs.get(job.id, { tx: database })
      ).resolves.toMatchObject({ id: job.id });
      database.exec("INSERT INTO app_rows (id) VALUES (1)");
      expect(() => {
        database.exec("INSERT OR ROLLBACK INTO app_rows (id) VALUES (1)");
      }).toThrow(/UNIQUE constraint failed/);

      expect(database.isTransaction).toBe(false);
      expect(() => {
        database.exec("COMMIT");
      }).toThrow(/no transaction is active/);
      await expect(client.jobs.get(job.id)).resolves.toBeNull();
      await expect(client.jobs.list()).resolves.toMatchObject({ jobs: [] });
    });

    test("transaction() fails to commit after a failed statement aborts it", async () => {
      const { database, driver } = await setup();
      const client = new Client(driver);
      const definition = defineJob({ kind: "sqlite_ops_tx_abort_helper" });
      database.exec("CREATE TABLE app_rows (id INTEGER PRIMARY KEY)");
      let insertedId = 0n;

      // The callback swallows the failed statement and resolves, so the
      // helper tries to commit.
      await expect(
        transaction(database, async (tx) => {
          insertedId = (await client.insert(definition, {}, { tx })).job.id;
          tx.exec("INSERT INTO app_rows (id) VALUES (1)");
          try {
            tx.exec("INSERT OR ROLLBACK INTO app_rows (id) VALUES (1)");
          } catch {
            // Ignored, like a caller that commits after an error.
          }
        })
      ).rejects.toBeInstanceOf(DatabaseOperationError);

      expect(insertedId).not.toBe(0n);
      expect(database.isTransaction).toBe(false);
      await expect(client.jobs.get(insertedId)).resolves.toBeNull();
      expect(
        database.prepare("SELECT count(*) AS n FROM app_rows").get()
      ).toEqual({ n: 0 });
    });
  });

  describe("transactional_completion", () => {
    test("completeTx completes the job with no errors and persists work metadata", async () => {
      const { database, driver } = await setup();
      const definition = defineJob({ kind: "sqlite_ops_complete_tx" });
      const client = new Client(driver, {
        completionBatchSize: 1,
        leaderElectionDisabled: true,
        queues: QUEUES,
        workers: new Workers().add(definition, async (context) => {
          context.setMetadata("progress", { step: "done" });
          await transaction(database, async (tx) => {
            await context.completeTx(tx);
          });
        }),
      });
      const { job } = await client.insert(definition, {});
      const run = await client.start();
      let completed: JobRow;
      try {
        completed = await waitForJob(client, job.id, "completed");
      } finally {
        await run.stop();
      }

      expect(completed.attempt).toBe(1);
      expect(completed.errors).toEqual([]);
      expect(completed.finalizedAt).not.toBeNull();
      expect(completed.metadata.progress).toEqual({ step: "done" });
    });
  });

  describe("transactional_update_delete", () => {
    // A file database in WAL mode, so River's own connection reads the last
    // committed state while the caller's transaction holds the write lock.
    // In-memory databases lock readers out of an open write transaction.
    test("update in a caller transaction is visible to others only after commit", async () => {
      const { database, driver } = await setupFile();
      const client = new Client(driver);
      const definition = defineJob({ kind: "sqlite_ops_tx_update" });
      const { job } = await client.insert(definition, {});

      database.exec("BEGIN IMMEDIATE");
      const updated = await client.jobs.update(
        job.id,
        { output: { inTx: true } },
        { tx: database }
      );
      expect(updated?.metadata.output).toEqual({ inTx: true });
      expect((await client.jobs.get(job.id))?.metadata).not.toHaveProperty(
        "output"
      );
      database.exec("COMMIT");

      expect((await client.jobs.get(job.id))?.metadata.output).toEqual({
        inTx: true,
      });
    });

    test("delete in a caller transaction is visible only after commit and undone by rollback", async () => {
      const { database, driver } = await setupFile();
      const client = new Client(driver);
      const definition = defineJob({ kind: "sqlite_ops_tx_delete" });
      const { job } = await client.insert(definition, {});

      database.exec("BEGIN IMMEDIATE");
      await expect(
        client.jobs.delete(job.id, { tx: database })
      ).resolves.toMatchObject({ id: job.id });
      await expect(
        client.jobs.get(job.id, { tx: database })
      ).resolves.toBeNull();
      await expect(client.jobs.get(job.id)).resolves.toMatchObject({
        id: job.id,
      });
      database.exec("ROLLBACK");
      await expect(client.jobs.get(job.id)).resolves.toMatchObject({
        id: job.id,
        state: "available",
      });

      database.exec("BEGIN IMMEDIATE");
      await expect(
        client.jobs.delete(job.id, { tx: database })
      ).resolves.toMatchObject({ id: job.id });
      await expect(client.jobs.get(job.id)).resolves.not.toBeNull();
      database.exec("COMMIT");
      await expect(client.jobs.get(job.id)).resolves.toBeNull();
    });

    test("deleteMany by IDs in a caller transaction is visible only after commit", async () => {
      const { database, driver } = await setupFile();
      const client = new Client(driver);
      const definition = defineJob({ kind: "sqlite_ops_tx_delete_many" });
      const first = (await client.insert(definition, {})).job.id;
      const second = (await client.insert(definition, {})).job.id;
      const kept = (await client.insert(definition, {})).job.id;

      database.exec("BEGIN IMMEDIATE");
      const deleted = await client.jobs.deleteMany({
        ids: [first, second],
        tx: database,
      });
      expect(deleted.map(({ id }) => id).sort()).toEqual([first, second]);
      expect(await listIds(client)).toEqual([first, second, kept]);
      database.exec("COMMIT");

      expect(await listIds(client)).toEqual([kept]);
    });
  });

  describe("queue_get_list", () => {
    test("get and list return the queue row and reflect metadata updates", async () => {
      const { driver } = await setup();
      const definition = defineJob({ kind: "sqlite_ops_queue" });
      const client = new Client(driver, {
        leaderElectionDisabled: true,
        queues: QUEUES,
        workers: new Workers().add(definition, () => undefined),
      });
      const run = await client.start();
      try {
        await waitUntilAsync(
          async () => (await client.queues.get("default")) !== null
        );
      } finally {
        await run.stop();
      }

      const queue = await client.queues.get("default");
      expect(queue).toMatchObject({
        metadata: {},
        name: "default",
        pausedAt: null,
      });
      const listed = await client.queues.list();
      expect(listed.queues).toEqual([queue]);
      expect(listed.nextCursor).toBeNull();
      await expect(client.queues.get("missing")).resolves.toBeNull();

      const updated = await client.queues.update("default", {
        metadata: { region: "us-east" },
      });
      expect(updated).toMatchObject({
        metadata: { region: "us-east" },
        name: "default",
        pausedAt: null,
      });
      await expect(client.queues.get("default")).resolves.toEqual(updated);
      expect((await client.queues.list()).queues).toEqual([updated]);
    });
  });
});

async function listIds(client: Client): Promise<bigint[]> {
  const { jobs } = await client.jobs.list();
  return jobs.map(({ id }) => id).sort();
}

async function setup(): Promise<{
  database: DatabaseSync;
  driver: SqliteRuntime;
}> {
  const driver = testSqliteMemory(STRICT);
  const database = driver.connect();
  onTestFinished(() => {
    if (database.isOpen && database.isTransaction) database.exec("ROLLBACK");
    database.close();
    driver.close();
  });
  await migrate(database);
  return { database, driver };
}

/** A file database in WAL mode, with the application handle the driver uses. */
async function setupFile(): Promise<{
  database: DatabaseSync;
  driver: SqliteRuntime;
}> {
  const directory = mkdtempSync(join(tmpdir(), "river-sqlite-operations-"));
  const database = new DatabaseSync(join(directory, "river.db"));
  const driver = testSqliteDriver(database, STRICT);
  onTestFinished(() => {
    if (database.isOpen && database.isTransaction) database.exec("ROLLBACK");
    driver.close();
    database.close();
    rmSync(directory, { force: true, recursive: true });
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
): Promise<JobRow & { metadata: JsonObject }> {
  for (let index = 0; index < 2_000; index++) {
    const job = await client.jobs.get(id);
    if (job?.state === state) return job;
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
  throw new Error(`job ${id} did not reach ${state}`);
}

async function waitUntilAsync(
  condition: () => Promise<boolean>
): Promise<void> {
  const deadline = Date.now() + 6_000;
  while (!(await condition())) {
    if (Date.now() > deadline) throw new Error("condition was not reached");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}
