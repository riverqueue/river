import { mkdtempSync, rmSync } from "node:fs";
import { stat } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";

import {
  BackendMismatchError,
  Client,
  ConfigurationError,
  DatabaseOperationError,
  defineJob,
  ExtensionError,
  JobCancelledError,
  LifecycleError,
  periodicJob,
  ValidationError,
  Workers,
  type ClientOptions,
  type JobRow,
  type QueueConfig,
} from "riverqueue";
import {
  createJobArgsTransformPlugin,
  createJobInsertMetadataTransformPlugin,
  PilotClient,
  type PreparedInsertParams,
  type Pilot,
  type PilotDatabase,
  type PilotHost,
} from "riverqueue/unstable-driver";
import { describe, expect, onTestFinished, test } from "vitest";

import {
  SQLITE_DRIVER_TEST_HOOKS,
  testSqliteDriver,
  testSqliteMemory,
} from "./driver.js";
import { transaction } from "./scope.js";
import type { SqliteDriverOptions } from "./types.js";

type Transaction = DatabaseSync;

/** River's own tests fail any lock window that crosses the event loop. */
const STRICT = {
  [SQLITE_DRIVER_TEST_HOOKS]: { strictLockWindow: true },
} as SqliteDriverOptions;

const job = defineJob({ kind: "pilot_job" });

/** A companion's queue configuration: River's, plus one key it owns. */
interface CompanionQueueConfig extends QueueConfig {
  readonly limit?: number;
}

/** A companion client, as a first-party package would build one. */
class CompanionClient extends PilotClient<Transaction, CompanionQueueConfig> {}

/** River's transactions reach a pilot as native handles. */
function handle(tx: Transaction): DatabaseSync {
  if (!(tx instanceof DatabaseSync)) {
    throw new Error("expected a native SQLite handle");
  }
  return tx;
}

/** Record a companion effect in the scratch table, in `tx`. */
function note(tx: Transaction, text: string, jobId: bigint | null = null) {
  handle(tx)
    .prepare("INSERT INTO pilot_companion (job_id, note) VALUES (?, ?)")
    .run(jobId, text);
}

async function setup(
  createPilot: (
    database: PilotDatabase<Transaction>
  ) => Pilot<Transaction> = () => ({}),
  options:
    | ClientOptions<Transaction>
    | ((database: DatabaseSync) => ClientOptions<Transaction>) = {},
  driverOptions: SqliteDriverOptions = {}
) {
  const directory = mkdtempSync(join(tmpdir(), "river-sqlite-pilot-"));
  const database = new DatabaseSync(join(directory, "river.db"), {
    timeout: 0,
  });
  await migrate(database);
  database.exec(
    "CREATE TABLE pilot_companion (id INTEGER PRIMARY KEY, job_id INTEGER, note TEXT NOT NULL)"
  );
  const driver = testSqliteDriver(database, { ...STRICT, ...driverOptions });
  let pilotDatabase: PilotDatabase<Transaction> | undefined;
  let host: PilotHost<Transaction> | undefined;
  const client = new CompanionClient(
    driver,
    typeof options === "function" ? options(database) : options,
    (db) => {
      pilotDatabase = db;
      const pilot = createPilot(db);
      const init = pilot.init;
      return {
        ...pilot,
        init(pilotHost) {
          host = pilotHost;
          init?.call(this, pilotHost);
        },
      };
    }
  );
  onTestFinished(() => {
    driver.close();
    database.close();
    rmSync(directory, { force: true, recursive: true });
  });
  const rows = <Row>(sql: string): Row[] =>
    database.prepare(sql).all() as Row[];
  const notes = (): string[] =>
    rows<{ note: string }>("SELECT note FROM pilot_companion ORDER BY id").map(
      (row) => row.note
    );
  const count = (table: string): number =>
    Number(
      database.prepare(`SELECT count(*) AS count FROM ${table}`).get()?.count
    );
  return {
    client,
    count,
    database,
    driver,
    host: host as unknown as PilotHost<Transaction>,
    notes,
    pilotDatabase: pilotDatabase as unknown as PilotDatabase<Transaction>,
    rows,
  };
}

describe("SQLite pilot database", () => {
  test("commits a pilot transaction on River's connection", async () => {
    const { client, count, notes, pilotDatabase } = await setup();

    const inserted = await pilotDatabase.transaction(async (tx) => {
      note(tx, "companion");
      // River's connection stands for the transaction inside it.
      return client.insert(job, {}, { tx });
    });

    expect(inserted.status).toBe("inserted");
    expect(notes()).toEqual(["companion"]);
    expect(count("river_job")).toBe(1);
  });

  test("rolls a pilot transaction back when its callback rejects", async () => {
    const { client, count, notes, pilotDatabase } = await setup();
    const failure = new Error("companion failed");

    await expect(
      pilotDatabase.transaction(async (tx) => {
        note(tx, "companion");
        await client.insert(job, {}, { tx });
        throw failure;
      })
    ).rejects.toBe(failure);

    expect(notes()).toEqual([]);
    expect(count("river_job")).toBe(0);
  });

  test("rolls back instead of committing once the signal aborted", async () => {
    const { notes, pilotDatabase } = await setup();
    const controller = new AbortController();
    const reason = new Error("stopped");

    await expect(
      pilotDatabase.transaction(
        (tx) => {
          note(tx, "companion");
          controller.abort(reason);
        },
        { signal: controller.signal }
      )
    ).rejects.toBe(reason);
    await expect(
      pilotDatabase.transaction(
        () => {
          throw new Error("never runs");
        },
        { signal: controller.signal }
      )
    ).rejects.toBe(reason);

    expect(notes()).toEqual([]);
  });

  test("never runs the callback when River can't begin", async () => {
    const { database, pilotDatabase } = await setup(
      () => ({}),
      {},
      {
        busyTimeout: { milliseconds: 20 },
      }
    );
    let calls = 0;
    database.exec("BEGIN IMMEDIATE");
    try {
      await expect(
        pilotDatabase.transaction(() => {
          calls++;
        })
      ).rejects.toBeInstanceOf(DatabaseOperationError);
    } finally {
      database.exec("ROLLBACK");
    }

    expect(calls).toBe(0);
  });

  test("runs directly in a supplied application transaction", async () => {
    const { client, count, database, notes, pilotDatabase } = await setup();

    const rollback = new Error("roll back");
    await expect(
      transaction(database, async (tx) => {
        note(tx, "application");
        await pilotDatabase.transaction(
          async (inner) => {
            expect(inner).toBe(tx);
            note(inner, "kept");
            await client.insert(job, {}, { tx: inner });
          },
          { tx }
        );
        await expect(
          pilotDatabase.transaction(
            async (inner) => {
              note(inner, "failed");
              await client.insert(job, {}, { tx: inner });
              throw new Error("callback failed");
            },
            { tx }
          )
        ).rejects.toThrow("callback failed");
        // Like River for Go, River opens no savepoint: the failed
        // callback's writes stay in the caller's transaction until the
        // caller rolls it back.
        expect(tx.isTransaction).toBe(true);
        expect(notes()).toEqual(["application", "kept", "failed"]);
        expect(count("river_job")).toBe(2);
        throw rollback;
      })
    ).rejects.toBe(rollback);

    expect(notes()).toEqual([]);
    expect(count("river_job")).toBe(0);
  });

  test("rejects River's connection as { tx } outside a pilot transaction", async () => {
    const { client, pilotDatabase } = await setup();
    let riverConnection: Transaction | undefined;
    await pilotDatabase.transaction((tx) => {
      riverConnection = tx;
    });

    await expect(
      client.insert(job, {}, { tx: riverConnection as Transaction })
    ).rejects.toBeInstanceOf(BackendMismatchError);
    await expect(
      pilotDatabase.transaction(() => undefined, {
        tx: riverConnection as Transaction,
      })
    ).rejects.toBeInstanceOf(BackendMismatchError);

    // Not even while a pilot transaction holds it, from code outside it.
    const gate = Promise.withResolvers<undefined>();
    const outside = gate.promise.then(() =>
      client.insert(job, {}, { tx: riverConnection as Transaction })
    );
    await pilotDatabase.transaction(async () => {
      gate.resolve(undefined);
      await expect(outside).rejects.toBeInstanceOf(BackendMismatchError);
    });
  });

  test("rejects River's connection as { tx } in River's own transaction", async () => {
    let riverConnection: Transaction | undefined;
    let lookup: unknown;
    const { client, count, pilotDatabase } = await setup(
      () => ({}),
      () => ({
        insertMiddleware: [
          async (_context, next) => {
            const results = await next();
            lookup = await client.jobs
              .get(1n, { tx: riverConnection as Transaction })
              .catch((error: unknown) => error);
            return results;
          },
        ],
      })
    );
    await pilotDatabase.transaction((tx) => {
      riverConnection = tx;
    });

    await client.insert(job, {});

    expect(lookup).toBeInstanceOf(BackendMismatchError);
    expect(count("river_job")).toBe(1);
  });

  test("fails a pilot transaction that awaits I/O", async () => {
    const { notes, pilotDatabase } = await setup();

    await expect(
      pilotDatabase.transaction(async (tx) => {
        note(tx, "companion");
        await stat(tmpdir());
      })
    ).rejects.toMatchObject({
      message: expect.stringContaining(
        "a companion's transaction or operation interceptor awaited I/O"
      ) as unknown,
      reason: "event_loop_turn",
    });

    expect(notes()).toEqual([]);
  });

  test("borrows River's connection outside any transaction", async () => {
    const { client, notes, pilotDatabase } = await setup();

    await pilotDatabase.connection((connection) => {
      note(connection, "autocommit");
    });
    await expect(
      pilotDatabase.connection(async () => {
        await client.insert(job, {});
      })
    ).rejects.toMatchObject({ reason: "reentrant" });
    await expect(
      pilotDatabase.connection((connection) => {
        handle(connection).exec("BEGIN");
        note(connection, "left open");
      })
    ).rejects.toMatchObject({ reason: "nested" });

    expect(notes()).toEqual(["autocommit"]);
    await expect(client.insert(job, {})).resolves.toMatchObject({
      status: "inserted",
    });
  });

  test("claims and loads jobs in a pilot transaction", async () => {
    const { client, driver, pilotDatabase, rows } = await setup();
    await client.insertMany([
      { args: {}, job },
      { args: {}, job },
    ]);
    const claim = (tx: Transaction) =>
      driver.jobClaim(
        {
          attemptedBy: "pilot-client",
          kinds: [],
          queues: [{ limit: 2, name: "default" }],
        },
        { tx }
      );

    await expect(
      pilotDatabase.transaction(async (tx) => {
        const claimed = await claim(tx);
        expect(claimed.jobs).toHaveLength(2);
        throw new Error("roll the claim back");
      })
    ).rejects.toThrow("roll the claim back");
    expect(
      rows<{ state: string }>("SELECT state FROM river_job").map(
        ({ state }) => state
      )
    ).toEqual(["available", "available"]);

    const [claimed, loaded] = await pilotDatabase.transaction(async (tx) => {
      const result = await claim(tx);
      const ids = result.jobs.map(({ id }) => id).reverse();
      return [result, await pilotDatabase.loadClaimed(ids, { tx })] as const;
    });
    expect(loaded.jobs.map(({ id }) => id)).toEqual(
      claimed.jobs.map(({ id }) => id).reverse()
    );
    expect(loaded.jobs.every(({ state }) => state === "running")).toBe(true);
    expect(loaded.decodeErrors?.size ?? 0).toBe(0);

    const id = claimed.jobs[0]?.id ?? 0n;
    await pilotDatabase.transaction(async (tx) => {
      await expect(
        pilotDatabase.loadClaimed([id, id], { tx })
      ).rejects.toBeInstanceOf(ValidationError);
      await expect(
        pilotDatabase.loadClaimed([id + 100n], { tx })
      ).rejects.toThrow("has no row");
    });
    await expect(
      pilotDatabase.loadClaimed([id], {} as { tx: Transaction })
    ).rejects.toMatchObject({ reason: "no_transaction" });
  });

  test("writes notifications that commit and roll back with the transaction", async () => {
    const { pilotDatabase, rows } = await setup();

    await pilotDatabase.transaction((tx) =>
      pilotDatabase.notify("control", ['{"kept":true}'], { tx })
    );
    await expect(
      pilotDatabase.transaction(async (tx) => {
        await pilotDatabase.notify("insert", ['{"queue":"x"}'], { tx });
        throw new Error("rolled back");
      })
    ).rejects.toThrow("rolled back");
    await expect(
      pilotDatabase.transaction((tx) =>
        pilotDatabase.notify("leadership" as "control", ["{}"], { tx })
      )
    ).rejects.toBeInstanceOf(ValidationError);

    expect(
      rows<{ payload: string; topic: string }>(
        "SELECT payload, topic FROM river_notification"
      )
    ).toEqual([{ payload: '{"kept":true}', topic: "river_control" }]);
  });

  test("deletes finalized jobs by state cutoff, lowest IDs first", async () => {
    const { client, count, database, pilotDatabase, rows } = await setup();
    const insertFinalized = async (
      state: string | null,
      finalizedAt = "2000-01-01 00:00:00.000"
    ): Promise<bigint> => {
      const { job: row } = await client.insert(job, {});
      if (state !== null) {
        database
          .prepare(
            "UPDATE river_job SET state = ?, finalized_at = ? WHERE id = ?"
          )
          .run(state, finalizedAt, row.id);
      }
      return row.id;
    };
    await insertFinalized("cancelled");
    const cancelledLater = await insertFinalized(
      "cancelled",
      "2000-01-03 00:00:00.000"
    );
    const completed = await insertFinalized("completed");
    await insertFinalized("discarded");
    const available = await insertFinalized(null);
    const discardedLast = await insertFinalized("discarded");
    const cutoff = Temporal.Instant.from("2000-01-02T00:00:00Z");
    const params = {
      cancelledBefore: cutoff,
      completedBefore: null,
      discardedBefore: cutoff,
      limit: 2,
    };
    const remaining = () =>
      rows<{ id: number }>("SELECT id FROM river_job ORDER BY id").map((row) =>
        BigInt(row.id)
      );

    expect(await pilotDatabase.deleteFinalizedJobs(params)).toBe(2);
    expect(remaining()).toEqual([
      cancelledLater,
      completed,
      available,
      discardedLast,
    ]);
    expect(await pilotDatabase.deleteFinalizedJobs(params)).toBe(1);
    expect(await pilotDatabase.deleteFinalizedJobs(params)).toBe(0);
    expect(remaining()).toEqual([cancelledLater, completed, available]);
    // It needs no leadership term.
    expect(count("river_leader")).toBe(0);
  });

  // Like River for Go's `QueuesFilteredBeforeLimit` driver cases: jobs in
  // `kept1` and `kept2` hold the lowest IDs, so a batch limiting candidates
  // before filtering queues would select only them and stall.
  for (const testCase of [
    {
      // `kept1` is in both lists; exclusion wins.
      batches: [2, 2, 1, 0],
      deletedQueues: ["deleted1", "deleted2"],
      name: "both lists",
      queuesExcluded: ["kept1", "kept2"],
      queuesIncluded: ["deleted1", "deleted2", "kept1"],
    },
    {
      batches: [0],
      deletedQueues: [],
      name: "an empty included list",
      queuesIncluded: [],
    },
    {
      batches: [2, 2, 1, 0],
      deletedQueues: ["deleted1", "deleted2"],
      name: "excluded queues",
      queuesExcluded: ["kept1", "kept2"],
    },
    {
      batches: [2, 2, 1, 0],
      deletedQueues: ["deleted1", "deleted2"],
      name: "included queues",
      queuesIncluded: ["deleted1", "deleted2"],
    },
    {
      batches: [2, 2, 2, 2, 2, 1, 0],
      deletedQueues: ["deleted1", "deleted2", "kept1", "kept2"],
      name: "a null included list",
      queuesIncluded: null,
    },
  ] as const) {
    test(`filters finalized job deletion by queue before the limit: ${testCase.name}`, async () => {
      const { client, database, pilotDatabase, rows } = await setup();
      const states = ["cancelled", "completed", "discarded"];
      const queues = [
        ...["kept1", "kept2", "kept1", "kept2", "kept1", "kept2"],
        ...["deleted1", "deleted2", "deleted1", "deleted2", "deleted1"],
      ];
      const allIds: bigint[] = [];
      const eligibleIds: bigint[] = [];
      for (const [index, queue] of queues.entries()) {
        const { job: row } = await client.insert(job, {}, { queue });
        database
          .prepare(
            "UPDATE river_job SET state = ?, finalized_at = '2000-01-01 00:00:00.000' WHERE id = ?"
          )
          .run(states[index % states.length]!, row.id);
        allIds.push(row.id);
        if ((testCase.deletedQueues as readonly string[]).includes(queue)) {
          eligibleIds.push(row.id);
        }
      }
      const before = Temporal.Now.instant();

      let deletedTotal = 0;
      for (const wantDeleted of testCase.batches) {
        const deleted = await pilotDatabase.deleteFinalizedJobs({
          cancelledBefore: before,
          completedBefore: before,
          discardedBefore: before,
          limit: 2,
          ...("queuesExcluded" in testCase
            ? { queuesExcluded: testCase.queuesExcluded }
            : {}),
          ...("queuesIncluded" in testCase
            ? { queuesIncluded: testCase.queuesIncluded }
            : {}),
        });
        expect(deleted).toBe(wantDeleted);
        deletedTotal += deleted;
        const gone = eligibleIds.slice(0, deletedTotal);
        expect(
          rows<{ id: number }>("SELECT id FROM river_job ORDER BY id").map(
            (row) => BigInt(row.id)
          )
        ).toEqual(allIds.filter((id) => !gone.includes(id)));
      }
      expect(deletedTotal).toBe(eligibleIds.length);
    });
  }

  test("deletes finalized jobs in a pilot transaction, rolling back with it", async () => {
    const { client, count, database, pilotDatabase } = await setup();
    for (let index = 0; index < 3; index++) {
      const { job: row } = await client.insert(job, {});
      database
        .prepare(
          "UPDATE river_job SET state = 'completed', finalized_at = '2000-01-01 00:00:00.000' WHERE id = ?"
        )
        .run(row.id);
    }
    const params = {
      cancelledBefore: null,
      completedBefore: Temporal.Now.instant(),
      discardedBefore: null,
      limit: 2,
    };

    await expect(
      pilotDatabase.transaction(async (tx) => {
        expect(await pilotDatabase.deleteFinalizedJobs(params, { tx })).toBe(2);
        throw new Error("rolled back");
      })
    ).rejects.toThrow("rolled back");
    expect(count("river_job")).toBe(3);

    expect(
      await pilotDatabase.transaction((tx) =>
        pilotDatabase.deleteFinalizedJobs(params, { tx })
      )
    ).toBe(2);
    expect(count("river_job")).toBe(1);
  });

  test("rescues in a pilot transaction, fenced by the leader", async () => {
    const { client, driver, pilotDatabase, rows } = await setup();
    const inserted = await client.insert(job, {});
    await driver.jobClaim({
      attemptedBy: "stuck-client",
      kinds: [],
      queues: [{ limit: 1, name: "default" }],
    });
    const now = Temporal.Now.instant();
    const leader = await driver.maintenanceLeaderAcquire(
      "leader",
      now,
      30_000,
      null
    );
    if (leader === null) throw new Error("expected leadership");
    const rescue = {
      error: { at: now, attempt: 1, error: "stuck", trace: "" },
      finalizedAt: null,
      id: inserted.job.id,
      scheduledAt: now,
      state: "retryable" as const,
    };
    const before = now.add({ seconds: 1 });

    await expect(
      pilotDatabase.transaction(async (tx) => {
        expect(
          await driver.maintenanceRescue(leader, before, [rescue], { tx })
        ).toBe(1);
        throw new Error("roll the rescue back");
      })
    ).rejects.toThrow("roll the rescue back");
    expect(rows<{ state: string }>("SELECT state FROM river_job")).toEqual([
      { state: "running" },
    ]);

    await pilotDatabase.transaction((tx) =>
      driver.maintenanceRescue(leader, before, [rescue], { tx })
    );
    expect(rows<{ state: string }>("SELECT state FROM river_job")).toEqual([
      { state: "retryable" },
    ]);
  });
});

describe("SQLite pilot interception", () => {
  const recordingPilot = (
    failAfterNext: () => boolean = () => false
  ): Pilot<Transaction> => ({
    intercept: {
      async cancel(context, next) {
        const job = await next();
        note(context.tx, "cancel", job?.id ?? null);
        if (failAfterNext()) throw new Error("cancel companion failed");
        return job;
      },
      async insert(context, next) {
        note(context.tx, `before ${context.operation}`);
        const results = await next();
        for (const result of results) {
          note(context.tx, `inserted ${result.status}`, result.job.id);
        }
        if (failAfterNext()) throw new Error("insert companion failed");
        return results;
      },
      async retry(context, next) {
        const job = await next();
        note(context.tx, "retry", job?.id ?? null);
        if (failAfterNext()) throw new Error("retry companion failed");
        return job;
      },
    },
  });

  test("commits companion writes with the standard insertion", async () => {
    const { client, count, notes } = await setup(() => recordingPilot());

    await client.insert(job, {});
    await client.insertMany([
      { args: {}, job },
      { args: {}, job },
    ]);

    expect(notes()).toEqual([
      "before insert",
      "inserted inserted",
      "before insertMany",
      "inserted inserted",
      "inserted inserted",
    ]);
    expect(count("river_job")).toBe(3);
  });

  test("leaves a failed interception's writes in the caller transaction until it rolls back", async () => {
    let fail = true;
    const { client, count, database, notes } = await setup(() =>
      recordingPilot(() => fail)
    );
    const rollback = new Error("roll back");

    // Without { tx }, River's own transaction rolls the whole insertion
    // back.
    await expect(client.insert(job, {})).rejects.toThrow(
      "insert companion failed"
    );
    expect(notes()).toEqual([]);
    expect(count("river_job")).toBe(0);

    // In a caller's transaction River opens no savepoint, like River for
    // Go: the failed insertions and their interceptor's writes stay in it,
    // next to the caller's own work, until the caller rolls back.
    await expect(
      transaction(database, async (tx) => {
        note(tx, "application");
        await expect(client.insert(job, {}, { tx })).rejects.toThrow(
          "insert companion failed"
        );
        await expect(
          client.insertMany(
            [
              { args: {}, job },
              { args: {}, job },
            ],
            { tx }
          )
        ).rejects.toThrow("insert companion failed");
        expect(notes()).toEqual([
          "application",
          "before insert",
          "inserted inserted",
          "before insertMany",
          "inserted inserted",
          "inserted inserted",
        ]);
        expect(count("river_job")).toBe(3);
        throw rollback;
      })
    ).rejects.toBe(rollback);
    expect(notes()).toEqual([]);
    expect(count("river_job")).toBe(0);

    fail = false;
    const available = await client.insert(job, {});
    const scheduled = await client.insert(
      job,
      {},
      { scheduledAt: Temporal.Now.instant().add({ hours: 1 }) }
    );
    const before = notes();
    fail = true;
    await expect(
      transaction(database, async (tx) => {
        await expect(
          client.jobs.cancel(available.job.id, { tx })
        ).rejects.toThrow("cancel companion failed");
        await expect(
          client.jobs.retry(scheduled.job.id, { tx })
        ).rejects.toThrow("retry companion failed");
        expect((await client.jobs.get(available.job.id, { tx }))?.state).toBe(
          "cancelled"
        );
        expect((await client.jobs.get(scheduled.job.id, { tx }))?.state).toBe(
          "available"
        );
        expect(notes().slice(before.length)).toEqual(["cancel", "retry"]);
        throw rollback;
      })
    ).rejects.toBe(rollback);
    expect((await client.jobs.get(available.job.id))?.state).toBe("available");
    expect((await client.jobs.get(scheduled.job.id))?.state).toBe("scheduled");
    expect(notes()).toEqual(before);
  });

  test("runs cancel and retry interceptors in the operation's transaction", async () => {
    const { client, notes } = await setup(() => recordingPilot());
    const inserted = await client.insert(job, {});

    await client.jobs.cancel(inserted.job.id);
    await client.jobs.retry(inserted.job.id);

    expect(notes().slice(2)).toEqual(["cancel", "retry"]);
  });

  test("runs overlapping operations on one caller transaction in turn", async () => {
    const { client, count, database, notes } = await setup(() =>
      recordingPilot()
    );

    await transaction(database, async (tx) => {
      const results = await Promise.allSettled([
        client.insert(job, { n: 1 }, { tx }),
        client.insert(job, { n: 2 }, { tx }),
        client.insertMany([{ args: { n: 3 }, job }], { tx }),
      ]);
      expect(results.map(({ status }) => status)).toEqual([
        "fulfilled",
        "fulfilled",
        "fulfilled",
      ]);
    });

    expect(count("river_job")).toBe(3);
    expect(notes().filter((text) => text.startsWith("before"))).toEqual([
      "before insert",
      "before insert",
      "before insertMany",
    ]);
  });

  test("queues ordinary operations on a caller transaction behind an intercepted one", async () => {
    const reached = Promise.withResolvers<undefined>();
    const proceed = Promise.withResolvers<undefined>();
    let fail = false;
    const { client, database } = await setup(() => ({
      intercept: {
        async insert(_context, next) {
          const results = await next();
          if (fail) {
            reached.resolve(undefined);
            await proceed.promise;
            throw new Error("insert companion failed");
          }
          return results;
        },
      },
    }));
    const other = await client.insert(job, {});
    fail = true;

    await transaction(database, async (tx) => {
      const failing = client.insert(job, {}, { tx });
      await reached.promise;
      const cancel = client.jobs.cancel(other.job.id, { tx });
      // Without waiting its turn, the cancellation would run now,
      // interleaved with the failing insertion's statements.
      await new Promise((resolve) => setImmediate(resolve));
      proceed.resolve(undefined);
      await expect(failing).rejects.toThrow("insert companion failed");
      await expect(cancel).resolves.toMatchObject({ state: "cancelled" });
    });

    expect((await client.jobs.get(other.job.id))?.state).toBe("cancelled");
  });

  test("runs operations nested in an interceptor on its transaction", async () => {
    const nestedJob = defineJob({ kind: "pilot_nested" });
    const { client, count, database, host, notes } = await setup(() => ({
      intercept: {
        async insert(context, next) {
          const results = await next();
          if (context.params[0]?.kind === job.kind) {
            const { args, ...stored } = context.params[0];
            void args;
            const prepared = { ...stored, kind: nestedJob.kind };
            // Both nested insertions queue inside this one, not behind it.
            await Promise.all([
              host.insertPrepared([prepared], { tx: context.tx }),
              host.insertPrepared([prepared], { tx: context.tx }),
              client.jobs.get(1n, { tx: context.tx }),
            ]);
            note(context.tx, "nested");
          }
          return results;
        },
      },
    }));

    await transaction(database, (tx) => client.insert(job, {}, { tx }));
    await client.insert(job, {});

    expect(count("river_job")).toBe(6);
    expect(notes()).toEqual(["nested", "nested"]);
  });

  test("runs the rescuer's reads and updates through the pilot", async () => {
    const calls: string[] = [];
    const { client, database, driver, rows } = await setup(
      () => ({
        intercept: {
          async getStuck(context, next) {
            const jobs = await next();
            calls.push(`getStuck ${jobs.length} in ${context.timeoutMs} ms`);
            return jobs;
          },
          async rescue(context, next) {
            const rescued = await next();
            note(context.tx, "rescued");
            calls.push(`rescue ${rescued}`);
            return rescued;
          },
        },
      }),
      {
        jobTimeout: { seconds: 1 },
        maintenance: {
          electionInterval: { milliseconds: 50 },
          rescueAfter: { seconds: 1 },
          rescuerInterval: { milliseconds: 20 },
        },
        pollOnly: true,
        queues: { other: { maxWorkers: 1 } },
        workers: new Workers().add(job, () => undefined),
      }
    );
    await client.insert(job, {});
    await driver.jobClaim({
      attemptedBy: "gone",
      kinds: [],
      queues: [{ limit: 1, name: "default" }],
    });
    database.exec(
      "UPDATE river_job SET attempted_at = '2020-01-01 00:00:00.000'"
    );

    const run = await client.start();
    try {
      await waitFor(() => calls.includes("rescue 1"));
    } finally {
      await run.stop();
    }

    // The rescuer's batch timeout, River's default of 30 s.
    expect(calls).toContain("getStuck 1 in 30000 ms");
    expect(rows<{ state: string }>("SELECT state FROM river_job")).toEqual([
      { state: "retryable" },
    ]);
    expect(rows<{ note: string }>("SELECT note FROM pilot_companion")).toEqual([
      { note: "rescued" },
    ]);
  });

  test("intercepts background and transactional completion alike", async () => {
    const txJob = defineJob({ kind: "pilot_tx_complete" });
    const fabricateJob = defineJob({ kind: "pilot_fabricate" });
    let fabricate = false;
    let secondCompletion: unknown;
    let fabricated: unknown;
    const { client, count, notes, rows } = await setup(
      () => ({
        intercept: {
          async complete(context, next) {
            const results = await next();
            for (const result of results) {
              if (result.status === "applied" && result.job !== null) {
                note(
                  context.tx,
                  `completed ${result.job.kind} ${context.commands.length}`,
                  result.job.id
                );
              }
            }
            return fabricate
              ? results.map((result) => ({ ...result }))
              : results;
          },
        },
      }),
      (database) => ({
        completionBatchSize: 1,
        leaderElectionDisabled: true,
        pollOnly: true,
        queues: {
          default: {
            fetchCooldown: { milliseconds: 1 },
            maxWorkers: 1,
            pollInterval: { milliseconds: 5 },
          },
        },
        workers: new Workers()
          .add(job, () => undefined)
          .add(txJob, async ({ completeTx }) => {
            const application = new DatabaseSync(database.location() ?? "", {
              timeout: 0,
            });
            try {
              await transaction(application, async (tx) => {
                await completeTx(tx);
                secondCompletion = await completeTx(tx).catch(
                  (error: unknown) => error
                );
              });
            } finally {
              application.close();
            }
          })
          .add(fabricateJob, async ({ completeTx }) => {
            const application = new DatabaseSync(database.location() ?? "", {
              timeout: 0,
            });
            try {
              fabricate = true;
              await transaction(application, async (tx) => {
                fabricated = await completeTx(tx).catch(
                  (error: unknown) => error
                );
              });
            } finally {
              fabricate = false;
              application.close();
            }
          }),
      })
    );

    const run = await client.start();
    try {
      await client.insert(job, {});
      await client.insert(txJob, {});
      await client.insert(fabricateJob, {});
      await waitFor(
        () =>
          count("river_job") ===
          rows<{ n: number }>(
            "SELECT count(*) AS n FROM river_job WHERE state = 'completed'"
          )[0]?.n
      );
    } finally {
      await run.stop();
    }

    // The ordinary completion of the job completed in a transaction was
    // stale, so it had no companion effect; the fabricated result was
    // rejected, and the job completed through the background batch.
    expect(secondCompletion).toBeInstanceOf(LifecycleError);
    expect(fabricated).toBeInstanceOf(ExtensionError);
    expect(notes()).toEqual([
      "completed pilot_job 1",
      "completed pilot_tx_complete 1",
      "completed pilot_fabricate 1",
    ]);
  });

  test("shows the insert interceptor each row's arguments from before the argument transforms", async () => {
    const seen: { original: readonly string[]; stored: readonly string[] }[] =
      [];
    const { client, database, host, rows } = await setup(
      () => ({
        intercept: {
          async insert(context, next) {
            seen.push({
              original: context.originalEncodedArgs,
              stored: context.params.map(({ encodedArgs }) => encodedArgs),
            });
            return next();
          },
        },
      }),
      {
        plugins: [
          createJobArgsTransformPlugin({
            name: "wrap",
            // Rewrites the arguments, as an encrypting transform would.
            onInsert: ({ encodedArgs }) => ({
              args: { wrapped: encodedArgs },
              encodedArgs: JSON.stringify({ wrapped: encodedArgs }),
            }),
            onRead: ({ args }) =>
              JSON.parse(args.wrapped as string) as Record<string, number>,
          }),
        ],
      }
    );
    const wrapped = (encodedArgs: string) =>
      JSON.stringify({ wrapped: encodedArgs });

    await client.insert(job, { n: 1 });
    await client.insertMany([
      { args: { n: 2 }, job },
      { args: { n: 3 }, job },
    ]);
    await transaction(database, async (tx) => {
      await client.insert(job, { n: 4 }, { tx });
    });
    await host.insertPrepared([
      {
        encodedArgs: '{"n":5}',
        kind: job.kind,
        maxAttempts: 25,
        metadata: {},
        priority: 1,
        queue: "default",
        state: "available",
        tags: [],
        uniqueKey: null,
        uniqueStates: null,
      },
    ]);

    expect(seen).toEqual([
      { original: ['{"n":1}'], stored: [wrapped('{"n":1}')] },
      {
        original: ['{"n":2}', '{"n":3}'],
        stored: [wrapped('{"n":2}'), wrapped('{"n":3}')],
      },
      { original: ['{"n":4}'], stored: [wrapped('{"n":4}')] },
      { original: ['{"n":5}'], stored: [wrapped('{"n":5}')] },
    ]);
    // River stores the transformed arguments, as without an interceptor.
    expect(
      rows<{ args: string }>(
        "SELECT json(args) AS args FROM river_job ORDER BY id"
      ).map(({ args }) => args)
    ).toEqual([1, 2, 3, 4, 5].map((n) => wrapped(`{"n":${n}}`)));
  });
});

// Like River for Go and Rust, River opens no savepoint or nested
// transaction in a caller's transaction: an operation's statements run
// directly in it, and when the operation fails after writing, its writes
// stay there until the caller rolls back. Without a caller transaction,
// River's own transaction rolls the whole operation back.
describe("SQLite caller transactions", () => {
  const rollback = new Error("roll back");

  /** Run `callback` in an application transaction, then roll it back. */
  const rolledBack = async (
    database: DatabaseSync,
    callback: (tx: DatabaseSync) => Promise<void>
  ): Promise<void> => {
    await expect(
      transaction(database, async (tx) => {
        await callback(tx);
        throw rollback;
      })
    ).rejects.toBe(rollback);
  };

  test("leaves an insertion that fails after its write in the caller transaction", async () => {
    let failMiddleware = false;
    let failHook = false;
    const { client, count, database } = await setup(() => ({}), {
      hooks: {
        afterInsert() {
          if (failHook) throw new Error("hook failed");
        },
      },
      insertMiddleware: [
        async (_context, next) => {
          const results = await next();
          if (failMiddleware) throw new Error("middleware failed");
          return results;
        },
      ],
    });

    failMiddleware = true;
    await expect(client.insert(job, {})).rejects.toThrow("middleware failed");
    failMiddleware = false;
    failHook = true;
    await expect(client.insert(job, {})).rejects.toThrow("hook failed");
    expect(count("river_job")).toBe(0);

    await rolledBack(database, async (tx) => {
      failMiddleware = true;
      failHook = false;
      await expect(client.insert(job, {}, { tx })).rejects.toThrow(
        "middleware failed"
      );
      failMiddleware = false;
      failHook = true;
      await expect(
        client.insertMany(
          [
            { args: {}, job },
            { args: {}, job },
          ],
          { tx }
        )
      ).rejects.toThrow("hook failed");
      expect(count("river_job")).toBe(3);
    });
    expect(count("river_job")).toBe(0);
  });

  test("validates an insertion before writing any of it", async () => {
    const { client, count, database, notes } = await setup();

    await transaction(database, async (tx) => {
      note(tx, "application");
      await expect(
        client.insertMany(
          [
            { args: {}, job },
            { args: {}, job, options: { priority: 99 } },
          ],
          { tx }
        )
      ).rejects.toBeInstanceOf(ValidationError);
      expect(count("river_job")).toBe(0);
    });
    expect(notes()).toEqual(["application"]);
  });

  test("keeps the caller transaction usable after a statement SQLite rejects", async () => {
    const failing = defineJob({ kind: "pilot_failing" });
    const { client, count, database, notes } = await setup();
    database.exec(
      `CREATE TRIGGER fail_insert BEFORE INSERT ON river_job
       WHEN NEW.kind = 'pilot_failing'
       BEGIN SELECT RAISE(ABORT, 'insert failed'); END`
    );

    // Unlike Postgres, SQLite undoes only the failed statement, so the
    // caller's transaction keeps River's earlier write and its own work,
    // and the caller still rolls back on the error.
    await rolledBack(database, async (tx) => {
      note(tx, "application");
      await client.insert(job, {}, { tx });
      await expect(client.insert(failing, {}, { tx })).rejects.toBeInstanceOf(
        DatabaseOperationError
      );
      expect(notes()).toEqual(["application"]);
      expect(count("river_job")).toBe(1);
    });
    expect(notes()).toEqual([]);
    expect(count("river_job")).toBe(0);
  });

  test("leaves a failed transactional completion in the caller transaction until it rolls back", async () => {
    const txJob = defineJob({ kind: "pilot_tx_complete" });
    let fail = true;
    let failure: unknown;
    let inTransaction: string | undefined;
    let notesInTransaction: unknown;
    const { client, notes, rows } = await setup(
      () => ({
        intercept: {
          async complete(context, next) {
            const results = await next();
            note(context.tx, `completed ${context.commands.length}`);
            if (fail) throw new Error("complete companion failed");
            return results;
          },
        },
      }),
      (database) => ({
        completionBatchSize: 1,
        leaderElectionDisabled: true,
        pollOnly: true,
        queues: {
          default: {
            fetchCooldown: { milliseconds: 1 },
            maxWorkers: 1,
            pollInterval: { milliseconds: 5 },
          },
        },
        workers: new Workers().add(txJob, async ({ completeTx, job: row }) => {
          const application = new DatabaseSync(database.location() ?? "", {
            timeout: 0,
          });
          try {
            await expect(
              transaction(application, async (tx) => {
                failure = await completeTx(tx).catch((error: unknown) => error);
                inTransaction = (
                  tx
                    .prepare("SELECT state FROM river_job WHERE id = ?")
                    .get(row.id) as { state: string } | undefined
                )?.state;
                notesInTransaction = tx
                  .prepare("SELECT note FROM pilot_companion ORDER BY id")
                  .all()
                  .map((note) => note.note);
                throw rollback;
              })
            ).rejects.toBe(rollback);
          } finally {
            application.close();
          }
          // The rolled-back completion left the job running, so River
          // completes it once the handler returns.
          fail = false;
        }),
      })
    );

    const run = await client.start();
    try {
      await client.insert(txJob, {});
      await waitFor(
        () =>
          rows<{ state: string }>("SELECT state FROM river_job")[0]?.state ===
          "completed"
      );
    } finally {
      await run.stop();
    }

    expect(failure).toMatchObject({
      message: expect.stringContaining("complete companion failed"),
    });
    expect(inTransaction).toBe("completed");
    expect(notesInTransaction).toEqual(["completed 1"]);
    expect(notes()).toEqual(["completed 1"]);
  });
});

describe("SQLite prepared insertion", () => {
  test("inserts stored rows like an ordinary insertion of them", async () => {
    const calls: string[] = [];
    const { host, rows } = await setup(
      () => ({
        intercept: {
          async insert(context, next) {
            calls.push(`pilot ${context.operation}`);
            return next();
          },
        },
      }),
      {
        hooks: {
          afterInsert: () => {
            calls.push("afterInsert");
          },
          beforeInsert: (context) => {
            calls.push(
              `beforeInsert ${context.operation} ${
                context.requests[0]?.definition === undefined
                  ? "without definition"
                  : "with definition"
              }`
            );
          },
        },
        insertMiddleware: [
          async (_context, next) => {
            calls.push("middleware");
            return next();
          },
        ],
        plugins: [
          createJobArgsTransformPlugin({
            name: "args",
            onInsert: ({ args, definition, encodedArgs }) => {
              calls.push(
                `args transform ${encodedArgs} ${definition === undefined ? "without definition" : "with definition"}`
              );
              // Keeping the stored arguments keeps their bytes.
              return { args, encodedArgs };
            },
            onRead: ({ args }) => args,
          }),
          createJobInsertMetadataTransformPlugin({
            name: "metadata",
            onInsert: ({ args, metadata }) => {
              calls.push(`metadata transform ${JSON.stringify(args)}`);
              return { metadata };
            },
          }),
        ],
      }
    );
    const createdAt = Temporal.Instant.from("2020-01-02T03:04:05.678Z");
    const uniqueKey = new Uint8Array(32).fill(7);
    const prepared: PreparedInsertParams = {
      createdAt,
      // Stored as given, in its own key order.
      encodedArgs: '{"b":1,"a":2}',
      kind: "stored_job",
      maxAttempts: 3,
      metadata: { stored: true },
      priority: 2,
      queue: "default",
      scheduledAt: createdAt,
      state: "available",
      tags: ["kept"],
      uniqueKey,
      uniqueStates: ["available", "running"],
    };

    const [result] = await host.insertPrepared([prepared]);

    // Everything an ordinary insertion runs, once each, on the stored
    // arguments.
    expect(calls).toEqual([
      'metadata transform {"b":1,"a":2}',
      'args transform {"b":1,"a":2} without definition',
      "middleware",
      "beforeInsert insertMany without definition",
      "pilot insertMany",
      "afterInsert",
    ]);
    expect(result?.status).toBe("inserted");
    const [row] = rows<Record<string, unknown>>(
      `SELECT json(args) AS args, created_at, json(metadata) AS metadata,
              max_attempts, priority, unique_key
       FROM river_job`
    );
    expect(row?.args).toBe('{"b":1,"a":2}');
    expect(row?.created_at).toBe("2020-01-02 03:04:05.678");
    expect(row?.max_attempts).toBe(3);
    expect(row?.priority).toBe(2);
    expect(new Uint8Array(row?.unique_key as Uint8Array)).toEqual(uniqueKey);
    const metadata = JSON.parse(row?.metadata as string) as Record<
      string,
      unknown
    >;
    delete metadata["river:unique_nonce"];
    expect(metadata).toEqual({ stored: true });
    expect(
      rows<{ payload: string }>("SELECT payload FROM river_notification")
    ).toEqual([{ payload: '{"queue": "default"}' }]);

    // The same unique key is now a duplicate.
    const [duplicate] = await host.insertPrepared([prepared]);
    expect(duplicate?.status).toBe("duplicate");
  });

  test("stores what the insert transforms return for a stored row", async () => {
    const { host, rows } = await setup(() => ({}), {
      plugins: [
        createJobArgsTransformPlugin({
          name: "wrap",
          // Wraps plain arguments, and keeps arguments it already wrapped.
          onInsert: ({ args, encodedArgs }) =>
            "wrapped" in args
              ? { args, encodedArgs }
              : {
                  args: { wrapped: args },
                  encodedArgs: `{"wrapped":${encodedArgs}}`,
                },
          onRead: ({ args }) => args,
        }),
        createJobInsertMetadataTransformPlugin({
          name: "metadata",
          onInsert: ({ metadata }) => ({
            metadata: { ...metadata, reinserted: true },
          }),
        }),
      ],
    });
    const stored = (
      encodedArgs: string,
      kind: string
    ): PreparedInsertParams => ({
      encodedArgs,
      kind,
      maxAttempts: 25,
      metadata: { stored: true },
      priority: 1,
      queue: "default",
      state: "available",
      tags: [],
      uniqueKey: null,
      uniqueStates: null,
    });

    await host.insertPrepared([
      stored('{"wrapped":{"y":1,"x":1}}', "already_wrapped"),
      stored('{"x":2}', "plain"),
    ]);

    const inserted = rows<{ args: string; kind: string; metadata: string }>(
      "SELECT json(args) AS args, kind, json(metadata) AS metadata FROM river_job ORDER BY id"
    );
    expect(inserted.map(({ args, kind }) => ({ args, kind }))).toEqual([
      // As stored, keys in their stored order.
      { args: '{"wrapped":{"y":1,"x":1}}', kind: "already_wrapped" },
      { args: '{"wrapped":{"x":2}}', kind: "plain" },
    ]);
    for (const { metadata } of inserted) {
      expect(JSON.parse(metadata)).toMatchObject({
        reinserted: true,
        stored: true,
      });
    }
  });

  test("keeps stored arguments that aren't an object, and pending states", async () => {
    const argsTransformed: string[] = [];
    const metadataSeen: string[] = [];
    const hooked: string[] = [];
    const { host, rows } = await setup(() => ({}), {
      hooks: {
        beforeInsert: (context) => {
          for (const request of context.requests) {
            hooked.push(`${request.kind} ${JSON.stringify(request.args)}`);
          }
        },
      },
      plugins: [
        createJobArgsTransformPlugin({
          name: "args",
          onInsert: ({ args, encodedArgs, kind }) => {
            argsTransformed.push(kind);
            return { args, encodedArgs };
          },
          onRead: ({ args }) => args,
        }),
        createJobInsertMetadataTransformPlugin({
          name: "metadata",
          onInsert: ({ args, kind, metadata, pending }) => {
            metadataSeen.push(`${kind} ${JSON.stringify(args)} ${pending}`);
            return kind === "made_pending"
              ? { metadata, pending: true }
              : { metadata };
          },
        }),
      ],
    });
    const stored = (
      kind: string,
      encodedArgs: string,
      state: PreparedInsertParams["state"] = "available"
    ): PreparedInsertParams => ({
      encodedArgs,
      kind,
      maxAttempts: 25,
      metadata: {},
      priority: 1,
      queue: "default",
      state,
      tags: [],
      uniqueKey: null,
      uniqueStates: null,
    });

    await host.insertPrepared([
      // Another River client's job whose arguments are a JSON array.
      stored("array_args", '[1,"a"]'),
      stored("stored_pending", "{}", "pending"),
      stored("made_pending", "{}"),
    ]);

    expect(
      rows<{ args: string; kind: string; state: string }>(
        "SELECT json(args) AS args, kind, state FROM river_job ORDER BY id"
      )
    ).toEqual([
      { args: '[1,"a"]', kind: "array_args", state: "available" },
      { args: "{}", kind: "stored_pending", state: "pending" },
      { args: "{}", kind: "made_pending", state: "pending" },
    ]);
    // Argument transforms take only objects; the rest see empty arguments.
    expect(argsTransformed).toEqual(["stored_pending", "made_pending"]);
    expect(metadataSeen).toEqual([
      "array_args {} false",
      "stored_pending {} true",
      "made_pending {} false",
    ]);
    expect(hooked).toEqual([
      "array_args {}",
      "stored_pending {}",
      "made_pending {}",
    ]);
  });

  test("validates prepared rows and joins a supplied transaction", async () => {
    const { count, database, host } = await setup();
    const prepared: PreparedInsertParams = {
      encodedArgs: "{}",
      kind: "stored_job",
      maxAttempts: 25,
      metadata: {},
      priority: 1,
      queue: "default",
      scheduledAt: Temporal.Now.instant(),
      state: "available",
      tags: [],
      uniqueKey: null,
      uniqueStates: null,
    };

    for (const [invalid, message] of [
      [{ ...prepared, priority: 0 }, "priority must be an integer from 1 to 4"],
      [
        { ...prepared, encodedArgs: "{not json" },
        "encodedArgs must be JSON text",
      ],
      // Arguments come from `encodedArgs` alone.
      [{ ...prepared, args: {} }, "args must be omitted"],
    ] as const) {
      await expect(
        host.insertPrepared([invalid as PreparedInsertParams])
      ).rejects.toThrow(`prepared job 0 ${message}`);
    }
    await expect(host.insertPrepared([])).resolves.toEqual([]);
    await expect(
      transaction(database, async (tx) => {
        await host.insertPrepared([prepared], { tx });
        throw new Error("roll back");
      })
    ).rejects.toThrow("roll back");
    expect(count("river_job")).toBe(0);
  });
});

describe("SQLite producer configuration", () => {
  test("hands the session its queue metadata as stored", async () => {
    const texts: string[] = [];
    const { client, database } = await setup(
      () => ({
        startProducer: (context) => {
          texts.push(`start ${context.metadataText}`);
          return Promise.resolve({
            configurationChanged: (configuration) => {
              texts.push(`changed ${configuration.metadataText}`);
            },
          });
        },
      }),
      {
        leaderElectionDisabled: true,
        pollOnly: true,
        queueControlPollInterval: { milliseconds: 10 },
        queues: { default: { maxWorkers: 1 } },
        workers: new Workers().add(job, () => undefined),
      }
    );
    // River stamps its own writes with the client's clock, so a change a
    // few milliseconds later by SQLite's clock could read as older.
    const store = (metadata: string, updatedAt = "+0 seconds") =>
      database
        .prepare(
          `INSERT INTO river_queue (name, metadata, created_at, updated_at)
           VALUES ('default', jsonb(?), datetime('now', 'subsec'),
             datetime('now', ?, 'subsec'))
           ON CONFLICT (name) DO UPDATE SET metadata = excluded.metadata,
             updated_at = excluded.updated_at`
        )
        .run(metadata, updatedAt);
    // Number literals River's parsed metadata folds into plain numbers.
    store('{"retries":1.0,"scale":1e2}');

    const run = await client.start();
    await waitFor(() => texts.length === 1);
    // The same values, written differently, are offered too.
    store('{"retries":1,"scale":100}', "+1 seconds");
    await waitFor(() => texts.length === 2);
    await run.stop();

    expect(texts).toEqual([
      'start {"retries":1.0,"scale":1e2}',
      'changed {"retries":1,"scale":100}',
    ]);
  });
});

describe("SQLite producer metadata notifications", () => {
  test("offers a queue's metadata change as its notification arrives", async () => {
    const texts: string[] = [];
    const { client, database } = await setup(
      () => ({
        startProducer: () =>
          Promise.resolve({
            configurationChanged: (configuration) => {
              texts.push(configuration.metadataText);
            },
          }),
      }),
      {
        leaderElectionDisabled: true,
        // Only the notification can deliver the change in time.
        queueControlPollInterval: { hours: 1 },
        queues: { default: { maxWorkers: 1 } },
        workers: new Workers().add(job, () => undefined),
      }
    );
    const run = await client.start();
    // Another client changes the queue's metadata.
    const other = testSqliteDriver(database, STRICT);
    onTestFinished(() => other.close());
    await new Client(other).queues.update("default", {
      metadata: { retries: 2 },
    });

    await waitFor(() => texts.length === 1);
    await run.stop();

    expect(texts).toEqual(['{"retries":2}']);
  });
});

describe("SQLite producer sessions", () => {
  test("claims through River's claim or its own statement in a pilot transaction", async () => {
    const finished: bigint[] = [];
    const calls: string[] = [];
    const worked: bigint[] = [];
    const { client, count, rows } = await setup(
      (database) => ({
        startProducer: (context) => {
          calls.push(`start ${context.queue.name}`);
          return Promise.resolve({
            claim: async (claimContext, next) => {
              const own = claimContext.queue === "own";
              const result = await database.transaction(async (tx) => {
                if (!own) return next({ tx });
                // Select and claim like a companion would, then read the
                // claimed rows back with River's decoder.
                const ids = handle(tx)
                  .prepare(
                    `UPDATE river_job
                     SET state = 'running', attempt = attempt + 1,
                       attempted_at = datetime('now', 'subsec'),
                       attempted_by = jsonb(json_array(?))
                     WHERE id IN (
                       SELECT id FROM river_job
                       WHERE queue = 'own' AND state = 'available'
                       ORDER BY id LIMIT ?
                     )
                     RETURNING id`
                  )
                  .all(claimContext.attemptedBy, claimContext.limit)
                  .map((row) => BigInt(row.id as number));
                note(tx, `claimed ${ids.length}`);
                return database.loadClaimed(ids, { tx });
              });
              return result;
            },
            jobFinished: (row) => {
              finished.push(row.id);
            },
            shutdown: () => {
              calls.push(`shutdown ${context.queue.name}`);
              return Promise.resolve();
            },
          });
        },
      }),
      {
        leaderElectionDisabled: true,
        pollOnly: true,
        queues: {
          default: {
            fetchCooldown: { milliseconds: 1 },
            maxWorkers: 2,
            pollInterval: { milliseconds: 5 },
          },
          own: {
            fetchCooldown: { milliseconds: 1 },
            maxWorkers: 2,
            pollInterval: { milliseconds: 5 },
          },
        },
        workers: new Workers().add(job, ({ job: row }) => {
          worked.push(row.id);
        }),
      }
    );
    const inserted = await client.insertMany([
      { args: {}, job },
      { args: {}, job },
      { args: {}, job, options: { queue: "own" } },
      { args: {}, job, options: { queue: "own" } },
    ]);

    const run = await client.start();
    await waitFor(() => finished.length === 4);
    await run.stop();

    const ids = inserted.map(({ job: row }) => row.id).sort();
    expect([...worked].sort()).toEqual(ids);
    expect([...finished].sort()).toEqual(ids);
    expect(calls.sort()).toEqual([
      "shutdown default",
      "shutdown own",
      "start default",
      "start own",
    ]);
    expect(count("river_job")).toBe(4);
    expect(
      rows<{ n: number }>(
        "SELECT count(*) AS n FROM pilot_companion WHERE note LIKE 'claimed %'"
      )[0]?.n
    ).toBeGreaterThan(0);
  });
});

describe("SQLite claim-time cancellation", () => {
  test("cancels a job whose cancellation arrives while its claim is in flight", async () => {
    const claimed = Promise.withResolvers<undefined>();
    const release = Promise.withResolvers<undefined>();
    // Like River for Go, the worker starts with its cancellation applied.
    let startedCancelled: boolean | undefined;
    const { client, rows } = await setup(
      (database) => ({
        startProducer: () =>
          Promise.resolve({
            claim: async (_context, next) => {
              const result = await database.transaction((tx) => next({ tx }));
              if (result.jobs.length > 0) {
                // Committed, but not yet handed to River.
                claimed.resolve(undefined);
                await release.promise;
              }
              return result;
            },
          }),
      }),
      {
        pollOnly: true,
        queues: {
          default: {
            fetchCooldown: { milliseconds: 1 },
            maxWorkers: 1,
            pollInterval: { milliseconds: 5 },
          },
        },
        workers: new Workers().add(job, ({ signal }) => {
          startedCancelled = signal.aborted;
          signal.throwIfAborted();
        }),
      }
    );
    const inserted = await client.insert(job, {});

    const run = await client.start();
    await claimed.promise;
    await client.jobs.cancel(inserted.job.id);
    release.resolve(undefined);
    await waitFor(
      () =>
        rows<{ state: string }>("SELECT state FROM river_job")[0]?.state ===
        "cancelled"
    );
    await run.stop();

    expect(startedCancelled).toBe(true);
  });
});

describe("SQLite peer attempts", () => {
  test("claims peers with a companion statement and completes each once", async () => {
    let attempts: PilotHost<Transaction>["attempts"] | undefined;
    let database: PilotDatabase<Transaction> | undefined;
    const rejected: string[] = [];
    const worked: bigint[] = [];
    /** Claim every available job of the `peers` queue in `tx`. */
    const claimPeers = (tx: Transaction, attemptedBy: string) => {
      const ids = handle(tx)
        .prepare(
          `UPDATE river_job
           SET state = 'running', attempt = attempt + 1,
             attempted_at = datetime('now', 'subsec'),
             attempted_by = jsonb(json_array(?))
           WHERE id IN (
             SELECT id FROM river_job
             WHERE queue = 'peers' AND state = 'available'
             ORDER BY id
           )
           RETURNING id`
        )
        .all(attemptedBy)
        .map((row) => BigInt(row.id as number));
      note(tx, `claimed ${ids.length}`);
      return ids.toSorted((left, right) => (left < right ? -1 : 1));
    };
    const { client, notes, rows } = await setup(
      (pilotDatabase) => {
        database = pilotDatabase;
        return {
          init(host) {
            attempts = host.attempts;
          },
        };
      },
      {
        pollOnly: true,
        queues: {
          default: {
            fetchCooldown: { milliseconds: 1 },
            maxWorkers: 1,
            pollInterval: { milliseconds: 5 },
          },
        },
        workers: new Workers().add(job, async (context) => {
          if (attempts === undefined || database === undefined) {
            throw new Error("the pilot wasn't initialized");
          }
          const peerDatabase = database;
          worked.push(context.job.id);
          // A claim returning the attempt's own job rolls back entirely.
          await attempts
            .claim(context, async ({ tx }) => {
              const ids = claimPeers(tx, context.execution.attemptedBy);
              return peerDatabase.loadClaimed([...ids, context.job.id], {
                tx,
              });
            })
            .catch((error: unknown) => {
              expect(error).toBeInstanceOf(ExtensionError);
              rejected.push((error as Error).message);
            });
          const peers = await attempts.claim(context, async ({ tx }) =>
            peerDatabase.loadClaimed(
              claimPeers(tx, context.execution.attemptedBy),
              { tx }
            )
          );
          // The first peer completes; the attempt leaves the second without
          // an outcome.
          await attempts.complete(context, [
            { job: peers[0] as JobRow, result: { status: "succeeded" } },
          ]);
        }),
      }
    );
    const inserted = await client.insertMany([
      { args: {}, job, options: { queue: "peers" } },
      { args: {}, job, options: { queue: "peers" } },
    ]);
    const [first, second] = inserted.map(({ job: row }) => row.id);

    const run = await client.start();
    const coordinator = await client.insert(job, {});
    await waitFor(
      () =>
        rows<{ state: string }>(
          `SELECT state FROM river_job WHERE id = ${coordinator.job.id}`
        )[0]?.state === "completed"
    );
    await run.stop();

    expect(worked).toEqual([coordinator.job.id]);
    expect(rejected).toEqual([
      `a peer claim returned job ${coordinator.job.id}, the claiming attempt's own job`,
    ]);
    // Only the committed claim's note remains.
    expect(notes()).toEqual(["claimed 2"]);
    const state = (id: bigint | undefined) =>
      rows<{ attempt: number; errors: string | null; state: string }>(
        `SELECT attempt, json(errors) AS errors, state FROM river_job WHERE id = ${id}`
      )[0];
    expect(state(first)).toMatchObject({ attempt: 1, state: "completed" });
    // Failed, and due again at once, so available.
    expect(state(second)).toMatchObject({ attempt: 1, state: "available" });
    expect(state(second)?.errors).toContain(
      "ended without an outcome for this job"
    );
  });

  /**
   * Claim every available job of the `peers` queue for `attemptedBy`, in
   * `tx`, with the pilot database's `loadClaimed`.
   */
  const claimAllPeers = (
    database: PilotDatabase<Transaction>,
    tx: Transaction,
    attemptedBy: string
  ) => {
    const ids = handle(tx)
      .prepare(
        `UPDATE river_job
         SET state = 'running', attempt = attempt + 1,
           attempted_at = datetime('now', 'subsec'),
           attempted_by = jsonb(json_array(?))
         WHERE queue = 'peers' AND state = 'available'
         RETURNING id`
      )
      .all(attemptedBy)
      .map((row) => BigInt(row.id as number));
    return database.loadClaimed(ids, { tx });
  };

  test("keeps a coordinator's peer claims open through a graceful stop", async () => {
    let attempts: PilotHost<Transaction>["attempts"] | undefined;
    let database: PilotDatabase<Transaction> | undefined;
    const running = Promise.withResolvers<undefined>();
    const gate = Promise.withResolvers<undefined>();
    const { client, rows } = await setup(
      (pilotDatabase) => {
        database = pilotDatabase;
        return {
          init(host) {
            attempts = host.attempts;
          },
        };
      },
      {
        pollOnly: true,
        queues: {
          default: {
            fetchCooldown: { milliseconds: 1 },
            maxWorkers: 1,
            pollInterval: { milliseconds: 5 },
          },
        },
        workers: new Workers().add(job, async (context) => {
          if (attempts === undefined || database === undefined) {
            throw new Error("the pilot wasn't initialized");
          }
          const peerDatabase = database;
          running.resolve(undefined);
          await gate.promise;
          const peers = await attempts.claim(context, async ({ tx }) =>
            claimAllPeers(peerDatabase, tx, context.execution.attemptedBy)
          );
          await attempts.complete(
            context,
            peers.map((peer) => ({
              job: peer,
              result: { status: "succeeded" },
            }))
          );
        }),
      }
    );
    await client.insertMany([
      { args: {}, job, options: { queue: "peers" } },
      { args: {}, job, options: { queue: "peers" } },
    ]);

    const run = await client.start();
    await client.insert(job, {});
    await running.promise;
    // The producer stops claiming at once; only then does the coordinator
    // claim its peers.
    const stopping = run.stop();
    gate.resolve(undefined);
    await stopping;

    // The stop resolved only after the peers and the coordinator persisted.
    expect(
      rows<{ attempt: number; queue: string; state: string }>(
        "SELECT attempt, queue, state FROM river_job ORDER BY id"
      )
    ).toEqual([
      { attempt: 1, queue: "peers", state: "completed" },
      { attempt: 1, queue: "peers", state: "completed" },
      { attempt: 1, queue: "default", state: "completed" },
    ]);
  });

  test("refuses peer claims once a stop or cancellation cancels the coordinator", async () => {
    for (const cancellation of ["cancelling stop", "job cancellation"]) {
      let attempts: PilotHost<Transaction>["attempts"] | undefined;
      let database: PilotDatabase<Transaction> | undefined;
      const running = Promise.withResolvers<undefined>();
      const refused = Promise.withResolvers<unknown>();
      const { client, rows } = await setup(
        (pilotDatabase) => {
          database = pilotDatabase;
          return {
            init(host) {
              attempts = host.attempts;
            },
          };
        },
        {
          pollOnly: true,
          queues: {
            default: {
              fetchCooldown: { milliseconds: 1 },
              maxWorkers: 1,
              pollInterval: { milliseconds: 5 },
            },
          },
          workers: new Workers().add(job, async (context) => {
            if (attempts === undefined || database === undefined) {
              throw new Error("the pilot wasn't initialized");
            }
            const peerDatabase = database;
            running.resolve(undefined);
            await new Promise((resolve) => {
              context.signal.addEventListener("abort", resolve, { once: true });
            });
            refused.resolve(
              await attempts
                .claim(context, async ({ tx }) =>
                  claimAllPeers(peerDatabase, tx, context.execution.attemptedBy)
                )
                .then(
                  () => undefined,
                  (error: unknown) => error
                )
            );
          }),
        }
      );
      await client.insert(job, {}, { queue: "peers" });

      const run = await client.start();
      const coordinator = await client.insert(job, {});
      await running.promise;
      if (cancellation === "cancelling stop") {
        await run.stop({ mode: "cancel" });
        await expect(refused.promise).resolves.toBeInstanceOf(LifecycleError);
      } else {
        await client.jobs.cancel(coordinator.job.id);
        await expect(refused.promise).resolves.toBeInstanceOf(
          JobCancelledError
        );
        await run.stop();
      }

      // The refused claim left the peer untouched.
      expect(
        rows<{ attempt: number; state: string }>(
          "SELECT attempt, state FROM river_job WHERE queue = 'peers'"
        )
      ).toEqual([{ attempt: 0, state: "available" }]);
    }
  });
});

describe("SQLite pilot services", () => {
  test("runs services, term services, and the pilot's periodic job store", async () => {
    const calls: string[] = [];
    const upserted: string[] = [];
    const { client, count, notes } = await setup(
      () => ({
        maintenanceServices: () => [
          {
            name: "leader_only",
            run: ({ signal, term }) => {
              calls.push(`term ${term.leaderId}`);
              return new Promise<void>((resolve) => {
                signal.addEventListener("abort", () => {
                  calls.push("term ended");
                  resolve();
                });
              });
            },
          },
        ],
        periodicJobs: {
          getAll: () => {
            calls.push("periodic getAll");
            return Promise.resolve([]);
          },
          keepAliveAndReap: () => Promise.resolve(),
          upsertMany: (tx, jobs) => {
            // A native handle in the transaction inserting the jobs.
            note(tx, "periodic upsert");
            upserted.push(...jobs.map(({ id }) => id));
            return Promise.resolve();
          },
        },
        services: () => [
          {
            name: "always",
            run: ({ signal }) => {
              calls.push("service");
              return new Promise<void>((resolve) => {
                signal.addEventListener("abort", () => {
                  calls.push("service ended");
                  resolve();
                });
              });
            },
          },
        ],
      }),
      {
        clientId: "services-leader",
        maintenance: { electionInterval: { milliseconds: 20 } },
        periodicJobs: [
          periodicJob({
            args: {},
            every: { hours: 1 },
            id: "hourly",
            job,
            runOnStart: true,
          }),
        ],
        pollOnly: true,
        queues: { default: { maxWorkers: 1 } },
        workers: new Workers().add(job, () => undefined),
      }
    );

    const run = await client.start();
    await waitFor(
      () =>
        calls.includes("term services-leader") &&
        calls.includes("periodic getAll") &&
        upserted.includes("hourly")
    );
    await run.stop();

    expect(notes()).toContain("periodic upsert");
    expect(count("river_job")).toBeGreaterThan(0);
    expect(calls[0]).toBe("service");
    expect(calls).toContain("term ended");
    expect(calls).toContain("service ended");
  });

  test("commits a durable periodic batch with its next-run times, or neither", async () => {
    let failUpsert = true;
    const { client, count, database } = await setup(
      () => ({
        periodicJobs: {
          getAll: () => Promise.resolve([]),
          keepAliveAndReap: () => Promise.resolve(),
          upsertMany: async (tx, jobs) => {
            await Promise.resolve();
            for (const periodic of jobs) {
              handle(tx)
                .prepare(
                  "INSERT OR REPLACE INTO periodic_state (id, next_run_at) VALUES (?, ?)"
                )
                .run(periodic.id, periodic.nextRunAt.toString());
            }
            if (failUpsert) {
              failUpsert = false;
              throw new Error("store failed after its write");
            }
          },
        },
      }),
      {
        periodicJobs: [
          periodicJob({
            args: {},
            every: { seconds: 1 },
            id: "durable",
            job,
            runOnStart: true,
          }),
        ],
        queues: { default: { maxWorkers: 1 } },
        workers: new Workers().add(job, () => undefined),
      }
    );
    database.exec(
      "CREATE TABLE periodic_state (id text PRIMARY KEY, next_run_at text)"
    );

    const run = await client.start();
    onTestFinished(() => run.stop());
    await waitFor(() => !failUpsert);
    // Well before the next occurrence, the failed batch has left neither its
    // job nor its next run behind.
    await new Promise((resolve) => setTimeout(resolve, 100));
    expect(count("river_job")).toBe(0);
    expect(count("periodic_state")).toBe(0);

    await waitFor(() => count("river_job") > 0);
    expect(count("river_job")).toBe(1);
    expect(count("periodic_state")).toBe(1);
    await run.stop();
  });
});

describe("PilotClient", () => {
  test("is a Client whose pilot sees the final client after construction", async () => {
    let initClient: unknown;
    let initInsert: Promise<unknown> | undefined;
    const { client, host } = await setup(() => ({
      init(pilotHost) {
        initClient = pilotHost.client;
        initInsert = pilotHost.client
          .insert(job, {})
          .catch((error: unknown) => error);
      },
    }));

    expect(client).toBeInstanceOf(Client);
    expect(initClient).toBe(client);
    expect(host.client).toBe(client);
    expect(host.clientId).toMatch(/^riverqueue-js-/);
    expect(host.workerKinds).toEqual([]);
    expect(host.producerReportInterval.total("seconds")).toBe(30);
    await expect(initInsert).resolves.toBeInstanceOf(LifecycleError);
    await expect(client.insert(job, {})).resolves.toMatchObject({
      status: "inserted",
    });
  });

  test("types the host's client and queue options as the companion's", () => {
    const driver = testSqliteMemory(STRICT);
    onTestFinished(() => {
      driver.close();
    });
    class TypedClient extends PilotClient<Transaction, CompanionQueueConfig> {
      readonly companion = "companion";
    }
    let captured: PilotHost<Transaction, TypedClient> | undefined;
    const pilot = (): Pilot<Transaction, unknown, TypedClient> => ({
      init(host) {
        captured = host;
      },
      queueOptions: { keys: ["limit"], parse: (_queue, config) => config },
    });

    const client = new TypedClient(
      driver,
      { queues: { limited: { limit: 1, maxWorkers: 1 } } },
      pilot
    );
    expect(
      () =>
        new TypedClient(
          driver,
          // @ts-expect-error A queue key neither River nor the pilot owns.
          { queues: { limited: { maxWorkers: 1, other: 1 } } },
          pilot
        )
    ).toThrow(ValidationError);

    expect(captured?.client).toBe(client);
    expect(captured?.client.companion).toBe("companion");
  });

  test("can't construct PilotClient itself", () => {
    const driver = testSqliteMemory(STRICT);
    onTestFinished(() => {
      driver.close();
    });
    const Abstract = PilotClient as unknown as new (
      ...args: unknown[]
    ) => unknown;

    expect(() => new Abstract(driver, {}, () => ({}))).toThrow(TypeError);
  });

  test("ignores an extra constructor argument on a plain Client", () => {
    const driver = testSqliteMemory(STRICT);
    onTestFinished(() => {
      driver.close();
    });
    let created = false;
    const client = new (
      Client as unknown as new (...args: unknown[]) => Client<Transaction>
    )(driver, {}, () => {
      created = true;
      return {};
    });

    expect(client).toBeInstanceOf(Client);
    expect(created).toBe(false);
  });

  test("rejects pilots River can't attach", async () => {
    const driver = testSqliteMemory(STRICT);
    onTestFinished(() => {
      driver.close();
    });
    const construct = (
      createPilot: (database: PilotDatabase<Transaction>) => unknown,
      options: ClientOptions<Transaction> = {}
    ) =>
      new CompanionClient(
        driver,
        options,
        createPilot as () => Pilot<Transaction>
      );
    const shared: Pilot<Transaction> = {};
    construct(() => shared);

    for (const createPilot of [
      () => shared,
      () => Promise.resolve({}),
      () => null,
      () => ({ init: () => Promise.resolve() }),
      () => ({ intercept: { claim: () => undefined } }),
      () => ({ intercept: { insert: true } }),
      () => ({ queueOptions: { keys: ["maxWorkers"], parse: () => 1 } }),
      () => ({ queueOptions: { keys: ["a", "a"], parse: () => 1 } }),
      () => ({ queueOptions: { keys: [""], parse: () => 1 } }),
      () => ({ queueOptions: { keys: ["limitMs"], parse: () => 1 } }),
    ]) {
      expect(() => construct(createPilot)).toThrow(ConfigurationError);
    }
    expect(
      () =>
        new CompanionClient(
          {
            jobInsert: () => undefined,
            jobInsertMany: () => undefined,
          } as never,
          {},
          () => ({})
        )
    ).toThrow(ConfigurationError);
  });

  test("rejects queue keys nothing owns", async () => {
    const driver = testSqliteMemory(STRICT);
    onTestFinished(() => {
      driver.close();
    });

    expect(
      () =>
        new Client(driver, {
          queues: { default: { limit: 1, maxWorkers: 1 } as QueueConfig },
        })
    ).toThrow(ValidationError);
    expect(
      () =>
        new CompanionClient(
          driver,
          { queues: { default: { other: 1, maxWorkers: 1 } as QueueConfig } },
          () => ({ queueOptions: { keys: ["limit"], parse: () => 1 } })
        )
    ).toThrow('queue "default" has no option "other"');
  });

  test("rejects queue keys River doesn't know when adding or updating a queue", async () => {
    const { driver } = await setup();
    // An ordinary client, without a pilot.
    const client = new Client(driver, {
      leaderElectionDisabled: true,
      pollOnly: true,
      queues: { default: { maxWorkers: 1 } },
      workers: new Workers().add(job, () => undefined),
    });
    const run = await client.start();
    try {
      await expect(
        run.addQueue("added", { maxWorkers: 1, other: 1 } as QueueConfig)
      ).rejects.toThrow('queue "added" has no option "other"');
      await expect(
        run.updateQueue("default", { maxWorkers: 2, other: 1 } as QueueConfig)
      ).rejects.toThrow('queue "default" has no option "other"');
      expect(Object.keys(run.diagnostics.queues)).toEqual(["default"]);
      expect(run.diagnostics.queues.default?.maxWorkers).toBe(1);
    } finally {
      await run.stop();
    }
  });

  test("parses pilot-owned queue keys before River changes anything", async () => {
    const parsed: [string, Readonly<Record<string, unknown>>][] = [];
    const { client } = await setup(
      () => ({
        queueOptions: {
          keys: ["limit"],
          parse(queue: string, config: Readonly<Record<string, unknown>>) {
            parsed.push([queue, config]);
            if (config.limit === -1) throw new ValidationError("bad limit");
            if (config.limit === -2) return Promise.resolve(1);
            return { limit: config.limit ?? null };
          },
        },
      }),
      {
        leaderElectionDisabled: true,
        pollOnly: true,
        queues: {
          default: { limit: 2, maxWorkers: 2 } as QueueConfig,
          other: { maxWorkers: 1 },
        },
        workers: new Workers().add(job, () => undefined),
      }
    );
    expect(parsed).toEqual([
      ["default", { limit: 2 }],
      ["other", {}],
    ]);

    const run = await client.start();
    try {
      await run.addQueue("added", { limit: 3, maxWorkers: 1 });
      await expect(
        run.addQueue("rejected", { limit: -1, maxWorkers: 1 })
      ).rejects.toThrow("bad limit");
      await expect(
        run.addQueue("async", { limit: -2, maxWorkers: 1 })
      ).rejects.toBeInstanceOf(ConfigurationError);
      await expect(
        run.addQueue("unknown", {
          maxWorkers: 1,
          other: 1,
        } as CompanionQueueConfig)
      ).rejects.toBeInstanceOf(ValidationError);
      await expect(
        run.updateQueue("default", { limit: -1, maxWorkers: 7 })
      ).rejects.toThrow("bad limit");

      expect(Object.keys(run.diagnostics.queues).sort()).toEqual([
        "added",
        "default",
        "other",
      ]);
      expect(run.diagnostics.queues.default?.maxWorkers).toBe(2);
      await run.updateQueue("default", { limit: 4, maxWorkers: 7 });
      expect(run.diagnostics.queues.default?.maxWorkers).toBe(7);
    } finally {
      await run.stop();
    }
    expect(parsed.slice(2)).toEqual([
      ["added", { limit: 3 }],
      ["rejected", { limit: -1 }],
      ["async", { limit: -2 }],
      ["default", { limit: -1 }],
      ["default", { limit: 4 }],
    ]);
  });

  test("wakes producers for jobs another owner committed", async () => {
    const worked: bigint[] = [];
    const emptyClaim = Promise.withResolvers<undefined>();
    const { client, host } = await setup(() => ({}), {
      hooks: {
        onMetric: (metric) => {
          if (metric.name === "job_get_available_count" && metric.count === 0) {
            emptyClaim.resolve(undefined);
          }
        },
      },
      leaderElectionDisabled: true,
      pollOnly: true,
      queues: {
        default: { maxWorkers: 1, pollInterval: { minutes: 1 } },
      },
      workers: new Workers().add(job, ({ job: row }) => {
        worked.push(row.id);
      }),
    });
    host.notifyCommitted([]);
    const run = await client.start();
    try {
      // Once the producer found the queue empty it waits a minute to poll.
      await emptyClaim.promise;
      const inserted = await client.insert(job, {});
      host.notifyCommitted([inserted]);
      await waitFor(() => worked.includes(inserted.job.id));
    } finally {
      await run.stop();
    }
  });
});

async function migrate(database: DatabaseSync): Promise<void> {
  const moduleUrl = new URL("../../../migrate/dist/index.js", import.meta.url);
  const migrationModule = (await import(moduleUrl.href)) as {
    createMigrator(target: { database: DatabaseSync }): {
      migrateUp(): Promise<unknown>;
    };
  };
  await migrationModule.createMigrator({ database }).migrateUp();
}

async function waitFor(condition: () => boolean): Promise<void> {
  const deadline = performance.now() + 3_000;
  while (!condition()) {
    if (performance.now() > deadline) {
      throw new Error("condition was not reached");
    }
    await new Promise((resolve) => setTimeout(resolve, 1));
  }
}
