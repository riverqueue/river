import pg from "pg";
import type { ClientBase } from "pg";
import {
  Client,
  DatabaseOperationError,
  defineJob,
  ExtensionError,
  JobCancelledError,
  LifecycleError,
  UnsupportedCapabilityError,
  ValidationError,
  Workers,
  type ClientOptions,
  type JobRow,
  type WorkContext,
} from "riverqueue";
import {
  createJobArgsTransformPlugin,
  PilotClient,
  type PreparedInsertParams,
  type Pilot,
  type PilotDatabase,
  type PilotHost,
} from "riverqueue/unstable-driver";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";

import { PgDriver, testPgDriver } from "./driver.js";

const TEST_DATABASE_URL =
  process.env.TEST_DATABASE_URL ??
  "postgres://localhost:5432/river_test?sslmode=disable";
const prefix = `js_pilot_${Math.random().toString(36).slice(2, 10)}`;
const companionTable = `${prefix}_companion`;

class CompanionClient extends PilotClient<ClientBase> {}

describe("PostgreSQL pilot", () => {
  let admin: pg.Pool;

  beforeAll(async () => {
    admin = new pg.Pool({ connectionString: TEST_DATABASE_URL });
    await admin.query(
      `CREATE TABLE ${companionTable} (id bigserial PRIMARY KEY, job_id bigint, note text NOT NULL)`
    );
  });

  afterAll(async () => {
    await admin.query(`DROP TABLE IF EXISTS ${companionTable}`);
    await admin.end();
  });

  afterEach(async () => {
    await admin.query(`DELETE FROM ${companionTable}`);
    await admin.query("DELETE FROM river_job WHERE kind LIKE $1", [
      `${prefix}%`,
    ]);
    await admin.query("DELETE FROM river_queue WHERE name LIKE $1", [
      `${prefix}%`,
    ]);
  });

  const job = defineJob({ kind: `${prefix}_job` });

  const note = async (
    tx: ClientBase,
    text: string,
    jobId: bigint | null = null
  ): Promise<void> => {
    await tx.query(
      `INSERT INTO ${companionTable} (job_id, note) VALUES ($1, $2)`,
      [jobId?.toString(10) ?? null, text]
    );
  };

  const notes = async (): Promise<string[]> =>
    (
      await admin.query<{ note: string }>(
        `SELECT note FROM ${companionTable} ORDER BY id`
      )
    ).rows.map((row) => row.note);

  const jobCount = async (): Promise<number> =>
    Number(
      (
        await admin.query<{ count: string }>(
          "SELECT count(*) AS count FROM river_job WHERE kind LIKE $1",
          [`${prefix}%`]
        )
      ).rows[0]?.count
    );

  const setup = (
    createPilot: (
      database: PilotDatabase<ClientBase>
    ) => Pilot<ClientBase> = () => ({}),
    options: ClientOptions<ClientBase> = {},
    pool: pg.Pool = admin
  ) => {
    let database: PilotDatabase<ClientBase> | undefined;
    let host: PilotHost<ClientBase> | undefined;
    const client = new CompanionClient(new PgDriver(pool), options, (db) => {
      database = db;
      const pilot = createPilot(db);
      return {
        ...pilot,
        init(pilotHost) {
          host = pilotHost;
        },
      };
    });
    return {
      client,
      database: database as unknown as PilotDatabase<ClientBase>,
      host: host as unknown as PilotHost<ClientBase>,
    };
  };

  it("commits and rolls back pilot transactions with River's writes", async () => {
    const { client, database } = setup();

    expect(database.backend).toBe("postgres");
    await database.transaction(async (tx) => {
      await note(tx, "committed");
      await client.insert(job, {}, { tx });
    });
    await expect(
      database.transaction(async (tx) => {
        await note(tx, "rolled back");
        await client.insert(job, {}, { tx });
        throw new Error("companion failed");
      })
    ).rejects.toThrow("companion failed");
    const controller = new AbortController();
    await expect(
      database.transaction(
        async (tx) => {
          await note(tx, "aborted");
          controller.abort(new Error("stopped"));
        },
        { signal: controller.signal }
      )
    ).rejects.toThrow("stopped");

    expect(await notes()).toEqual(["committed"]);
    expect(await jobCount()).toBe(1);
  });

  it("runs directly in a caller's transaction and never ends it", async () => {
    const { client, database } = setup();
    const tx = await admin.connect();
    try {
      await tx.query("BEGIN");
      await note(tx, "application");
      await database.transaction(
        async (inner) => {
          expect(inner).toBe(tx);
          await note(inner, "kept");
        },
        { tx }
      );
      await expect(
        database.transaction(
          async (inner) => {
            await client.insert(job, {}, { tx: inner });
            await inner.query("SELECT 1/0");
          },
          { tx }
        )
      ).rejects.toThrow();
      // Like River for Go, River opens no savepoint, so the failed
      // statement aborts the caller's transaction, which can only roll
      // back, along with its earlier work.
      await expect(tx.query("SELECT 1")).rejects.toMatchObject({
        code: "25P02",
      });
      await tx.query("ROLLBACK");
    } finally {
      tx.release();
    }

    expect(await notes()).toEqual([]);
    expect(await jobCount()).toBe(0);
  });

  it("never runs the callback when no connection can be leased", async () => {
    const pool = new pg.Pool({ connectionString: TEST_DATABASE_URL, max: 1 });
    try {
      const { database } = setup(() => ({}), {}, pool);
      const held = await pool.connect();
      let calls = 0;
      try {
        const controller = new AbortController();
        setTimeout(() => {
          controller.abort(new Error("gave up"));
        }, 20);
        await expect(
          database.transaction(
            () => {
              calls++;
            },
            { signal: controller.signal }
          )
        ).rejects.toThrow("gave up");
        await expect(
          database.connection(
            () => {
              calls++;
            },
            { signal: AbortSignal.abort(new Error("gave up")) }
          )
        ).rejects.toThrow("gave up");
      } finally {
        held.release();
      }
      expect(calls).toBe(0);
      await expect(
        database.connection(async (handle) =>
          Number((await handle.query("SELECT 1 AS one")).rows[0].one)
        )
      ).resolves.toBe(1);
    } finally {
      await pool.end();
    }
  });

  it("rolls back and rejects a connection callback that leaves a transaction open", async () => {
    const pool = new pg.Pool({ connectionString: TEST_DATABASE_URL, max: 1 });
    try {
      const { client, database } = setup(() => ({}), {}, pool);
      await expect(
        database.connection(async (handle) => {
          await handle.query("BEGIN");
          await note(handle, "leaked");
        })
      ).rejects.toMatchObject({ reason: "nested" });
      await expect(
        database.connection(async (handle) => {
          await handle.query("BEGIN");
          await handle.query("SELECT 1/0");
        })
      ).rejects.toThrow();
      // The one pooled connection is usable and outside any transaction.
      await client.insert(job, {});
      await database.connection(async (handle) => {
        await note(handle, "autocommit");
      });
    } finally {
      await pool.end();
    }

    expect(await notes()).toEqual(["autocommit"]);
    expect(await jobCount()).toBe(1);
  });

  it("claims and loads jobs in a pilot transaction", async () => {
    const queue = `${prefix}_claim`;
    const driver = testPgDriver(admin);
    const { client, database } = setup();
    await client.insertMany([
      { args: {}, job, options: { queue } },
      { args: {}, job, options: { queue } },
    ]);
    const claim = (tx: ClientBase) =>
      driver.jobClaim(
        {
          attemptedBy: "pilot-client",
          kinds: [],
          queues: [{ limit: 2, name: queue }],
        },
        { tx }
      );

    await expect(
      database.transaction(async (tx) => {
        expect((await claim(tx)).jobs).toHaveLength(2);
        throw new Error("roll the claim back");
      })
    ).rejects.toThrow("roll the claim back");
    const states = await admin.query<{ state: string }>(
      "SELECT state::text AS state FROM river_job WHERE queue = $1",
      [queue]
    );
    expect(states.rows.map(({ state }) => state)).toEqual([
      "available",
      "available",
    ]);

    const [claimed, loaded] = await database.transaction(async (tx) => {
      const result = await claim(tx);
      const ids = result.jobs.map(({ id }) => id).reverse();
      return [result, await database.loadClaimed(ids, { tx })] as const;
    });
    expect(loaded.jobs.map(({ id }) => id)).toEqual(
      claimed.jobs.map(({ id }) => id).reverse()
    );
    expect(loaded.jobs.every(({ state }) => state === "running")).toBe(true);

    const id = claimed.jobs[0]?.id ?? 0n;
    await database.transaction(async (tx) => {
      await expect(
        database.loadClaimed([id, id], { tx })
      ).rejects.toBeInstanceOf(ValidationError);
      await expect(
        database.loadClaimed([id + 1_000_000n], { tx })
      ).rejects.toThrow("has no row");
    });
  });

  it("sends notifications only when the transaction commits", async () => {
    const { database } = setup();
    const listener = new pg.Client({ connectionString: TEST_DATABASE_URL });
    await listener.connect();
    try {
      const schema = (
        await listener.query<{ schema: string }>(
          "SELECT current_schema() AS schema"
        )
      ).rows[0]?.schema;
      const received: string[] = [];
      listener.on("notification", (message) => {
        received.push(message.payload ?? "");
      });
      await listener.query(`LISTEN "${schema}.river_control"`);

      await expect(
        database.transaction(async (tx) => {
          await database.notify("control", ['{"rolled":"back"}'], { tx });
          throw new Error("rolled back");
        })
      ).rejects.toThrow("rolled back");
      await database.transaction((tx) =>
        database.notify("control", [`{"${prefix}":1}`], { tx })
      );
      await waitFor(() => received.length > 0);

      expect(received).toEqual([`{"${prefix}":1}`]);
    } finally {
      await listener.end();
    }
  });

  it("deletes finalized jobs by state cutoff and queue, lowest IDs first", async () => {
    const { client, database } = setup();
    const deleted1 = `${prefix}_deleted1`;
    const deleted2 = `${prefix}_deleted2`;
    const kept = `${prefix}_kept`;
    const ids: bigint[] = [];
    // Jobs in `kept` hold the lowest IDs, so a batch limiting candidates
    // before filtering queues would select only them and stall.
    for (const [queue, state, finalizedAt] of [
      [kept, "cancelled", "1990-01-01T00:00:00Z"],
      [kept, "discarded", "1990-01-01T00:00:00Z"],
      [deleted1, "cancelled", "1990-01-01T00:00:00Z"],
      [deleted2, "completed", "1990-01-01T00:00:00Z"],
      [deleted1, "discarded", "1990-01-03T00:00:00Z"],
      [deleted2, "available", null],
      [deleted1, "discarded", "1990-01-01T00:00:00Z"],
      [deleted2, "cancelled", "1990-01-01T00:00:00Z"],
    ] as const) {
      const { job: row } = await client.insert(job, {}, { queue });
      await admin.query(
        "UPDATE river_job SET state = $1, finalized_at = $2 WHERE id = $3",
        [state, finalizedAt, row.id.toString(10)]
      );
      ids.push(row.id);
    }
    const cutoff = Temporal.Instant.from("1990-01-02T00:00:00Z");
    const params = {
      cancelledBefore: cutoff,
      completedBefore: null,
      discardedBefore: cutoff,
      limit: 2,
      // `kept` is in both lists; exclusion wins.
      queuesExcluded: [kept],
      queuesIncluded: [deleted1, deleted2, kept],
    };
    const remaining = async (): Promise<bigint[]> =>
      (
        await admin.query<{ id: string }>(
          "SELECT id FROM river_job WHERE kind = $1 ORDER BY id",
          [job.kind]
        )
      ).rows.map((row) => BigInt(row.id));

    expect(await database.deleteFinalizedJobs(params)).toBe(2);
    expect(await remaining()).toEqual([
      ids[0],
      ids[1],
      ids[3],
      ids[4],
      ids[5],
      ids[7],
    ]);
    expect(
      await database.deleteFinalizedJobs({ ...params, queuesIncluded: [] })
    ).toBe(0);
    expect(await database.deleteFinalizedJobs(params)).toBe(1);
    expect(await database.deleteFinalizedJobs(params)).toBe(0);
    expect(await remaining()).toEqual([ids[0], ids[1], ids[3], ids[4], ids[5]]);
  });

  it("deletes finalized jobs in a pilot transaction, rolling back with it", async () => {
    const { client, database } = setup();
    const queue = `${prefix}_cleaned`;
    for (let index = 0; index < 3; index++) {
      const { job: row } = await client.insert(job, {}, { queue });
      await admin.query(
        "UPDATE river_job SET state = 'completed', finalized_at = '1990-01-01T00:00:00Z' WHERE id = $1",
        [row.id.toString(10)]
      );
    }
    const params = {
      cancelledBefore: null,
      completedBefore: Temporal.Instant.from("1990-01-02T00:00:00Z"),
      discardedBefore: null,
      limit: 2,
      queuesIncluded: [queue],
    };

    await expect(
      database.transaction(async (tx) => {
        expect(await database.deleteFinalizedJobs(params, { tx })).toBe(2);
        throw new Error("rolled back");
      })
    ).rejects.toThrow("rolled back");
    expect(await jobCount()).toBe(3);

    expect(
      await database.transaction((tx) =>
        database.deleteFinalizedJobs(params, { tx })
      )
    ).toBe(2);
    expect(await jobCount()).toBe(1);
  });

  it("leaves an interceptor's failed writes in the caller transaction until it rolls back", async () => {
    let fail = true;
    const { client } = setup(() => ({
      intercept: {
        async cancel(context, next) {
          const row = await next();
          await note(context.tx, "cancel", row?.id ?? null);
          if (fail) throw new Error("cancel companion failed");
          return row;
        },
        async insert(context, next) {
          const results = await next();
          await note(context.tx, "insert", results[0]?.job.id ?? null);
          if (fail) throw new Error("insert companion failed");
          return results;
        },
        async retry(context, next) {
          const row = await next();
          await note(context.tx, "retry", row?.id ?? null);
          if (fail) throw new Error("retry companion failed");
          return row;
        },
      },
    }));
    const notesIn = async (tx: ClientBase): Promise<string[]> =>
      (
        await tx.query<{ note: string }>(
          `SELECT note FROM ${companionTable} ORDER BY id`
        )
      ).rows.map((row) => row.note);
    const stateIn = async (tx: ClientBase, id: bigint): Promise<string> =>
      (
        await tx.query<{ state: string }>(
          "SELECT state FROM river_job WHERE id = $1",
          [id.toString(10)]
        )
      ).rows[0]?.state ?? "missing";

    // Without a caller transaction, River's own transaction rolls the
    // whole insertion back.
    await expect(client.insert(job, {})).rejects.toThrow(
      "insert companion failed"
    );
    expect(await notes()).toEqual([]);
    expect(await jobCount()).toBe(0);

    // In a caller's transaction River opens no savepoint, like River for
    // Go: the failed insertions and their interceptor's writes stay in it,
    // next to the caller's own work, until the caller rolls back.
    let tx = await admin.connect();
    try {
      await tx.query("BEGIN");
      await note(tx, "application");
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
      expect(await notesIn(tx)).toEqual(["application", "insert", "insert"]);
      expect(
        Number(
          (
            await tx.query<{ count: string }>(
              "SELECT count(*) AS count FROM river_job WHERE kind LIKE $1",
              [`${prefix}%`]
            )
          ).rows[0]?.count
        )
      ).toBe(3);
      await tx.query("ROLLBACK");
    } finally {
      tx.release();
    }
    expect(await notes()).toEqual([]);
    expect(await jobCount()).toBe(0);

    fail = false;
    const available = await client.insert(job, {});
    const scheduled = await client.insert(
      job,
      {},
      { scheduledAt: Temporal.Now.instant().add({ hours: 1 }) }
    );
    await admin.query(`DELETE FROM ${companionTable}`);
    fail = true;
    tx = await admin.connect();
    try {
      await tx.query("BEGIN");
      await expect(
        client.jobs.cancel(available.job.id, { tx })
      ).rejects.toThrow("cancel companion failed");
      await expect(client.jobs.retry(scheduled.job.id, { tx })).rejects.toThrow(
        "retry companion failed"
      );
      expect(await stateIn(tx, available.job.id)).toBe("cancelled");
      expect(await stateIn(tx, scheduled.job.id)).toBe("available");
      expect(await notesIn(tx)).toEqual(["cancel", "retry"]);
      await tx.query("ROLLBACK");
    } finally {
      tx.release();
    }
    expect((await client.jobs.get(available.job.id))?.state).toBe("available");
    expect((await client.jobs.get(scheduled.job.id))?.state).toBe("scheduled");
    expect(await notes()).toEqual([]);
  });

  it("intercepts a caller's transaction on a one-connection pool without deadlock", async () => {
    const pool = new pg.Pool({ connectionString: TEST_DATABASE_URL, max: 1 });
    try {
      const { client } = setup(
        () => ({
          intercept: {
            async cancel(context, next) {
              const row = await next();
              await note(context.tx, "cancel", row?.id ?? null);
              return row;
            },
            async insert(context, next) {
              const results = await next();
              await note(context.tx, "insert");
              return results;
            },
            async retry(context, next) {
              const row = await next();
              await note(context.tx, "retry", row?.id ?? null);
              return row;
            },
          },
        }),
        {},
        pool
      );
      const tx = await pool.connect();
      try {
        await tx.query("BEGIN");
        const inserted = await client.insert(job, {}, { tx });
        await client.jobs.cancel(inserted.job.id, { tx });
        await client.jobs.retry(inserted.job.id, { tx });
        await tx.query("COMMIT");
      } finally {
        tx.release();
      }
    } finally {
      await pool.end();
    }
    expect(await notes()).toEqual(["insert", "cancel", "retry"]);
  });

  it("runs overlapping operations on one caller transaction in turn", async () => {
    const { client } = setup(() => ({
      intercept: {
        async insert(context, next) {
          const results = await next();
          await note(context.tx, "insert");
          return results;
        },
      },
    }));
    const tx = await admin.connect();
    try {
      await tx.query("BEGIN");
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
      expect((await tx.query("COMMIT")).command).toBe("COMMIT");
    } finally {
      tx.release();
    }

    expect(await jobCount()).toBe(3);
    expect(await notes()).toEqual(["insert", "insert", "insert"]);
  });

  it("queues ordinary operations on a caller transaction behind an intercepted one", async () => {
    const reached = Promise.withResolvers<undefined>();
    const proceed = Promise.withResolvers<undefined>();
    let fail = false;
    const { client } = setup(() => ({
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
    const tx = await admin.connect();
    try {
      await tx.query("BEGIN");
      const failing = client.insert(job, {}, { tx });
      await reached.promise;
      const cancel = client.jobs.cancel(other.job.id, { tx });
      // Without waiting its turn, the cancellation would run now,
      // interleaved with the failing insertion's statements.
      await new Promise((resolve) => setTimeout(resolve, 20));
      proceed.resolve(undefined);
      await expect(failing).rejects.toThrow("insert companion failed");
      await expect(cancel).resolves.toMatchObject({ state: "cancelled" });
      await tx.query("COMMIT");
    } finally {
      tx.release();
    }

    expect((await client.jobs.get(other.job.id))?.state).toBe("cancelled");
  });

  it("intercepts background and transactional completion alike", async () => {
    const queue = `${prefix}_complete`;
    const txJob = defineJob({ kind: `${prefix}_tx_complete` });
    const fabricateJob = defineJob({ kind: `${prefix}_fabricate` });
    let fabricate = false;
    let secondCompletion: unknown;
    let fabricated: unknown;
    const inTransaction = async (
      run: (tx: ClientBase) => Promise<void>
    ): Promise<void> => {
      const tx = await admin.connect();
      try {
        await tx.query("BEGIN");
        await run(tx);
        await tx.query("COMMIT");
      } finally {
        tx.release();
      }
    };
    const { client } = setup(
      () => ({
        intercept: {
          async complete(context, next) {
            const results = await next();
            for (const result of results) {
              if (result.status === "applied" && result.job !== null) {
                await note(context.tx, `completed ${result.job.kind}`);
              }
            }
            return fabricate
              ? results.map((result) => ({ ...result }))
              : results;
          },
        },
      }),
      {
        completionBatchSize: 1,
        leaderElectionDisabled: true,
        queues: {
          [queue]: {
            fetchCooldown: { milliseconds: 1 },
            maxWorkers: 1,
            pollInterval: { milliseconds: 5 },
          },
        },
        workers: new Workers()
          .add(job, () => undefined)
          .add(txJob, async ({ completeTx }) => {
            await inTransaction(async (tx) => {
              await completeTx(tx);
              secondCompletion = await completeTx(tx).catch(
                (error: unknown) => error
              );
            });
          })
          .add(fabricateJob, async ({ completeTx }) => {
            fabricate = true;
            try {
              await inTransaction(async (tx) => {
                fabricated = await completeTx(tx).catch(
                  (error: unknown) => error
                );
              });
            } finally {
              fabricate = false;
            }
          }),
      }
    );
    const run = await client.start();
    try {
      await client.insert(job, {}, { queue });
      await client.insert(txJob, {}, { queue });
      await client.insert(fabricateJob, {}, { queue });
      await waitFor(async () => {
        const result = await admin.query<{ count: string }>(
          "SELECT count(*) AS count FROM river_job WHERE queue = $1 AND state = 'completed'",
          [queue]
        );
        return Number(result.rows[0]?.count) === 3;
      }, 10_000);
    } finally {
      await run.stop();
    }

    expect(secondCompletion).toBeInstanceOf(LifecycleError);
    expect(fabricated).toBeInstanceOf(ExtensionError);
    expect(await notes()).toEqual([
      `completed ${prefix}_job`,
      `completed ${prefix}_tx_complete`,
      `completed ${prefix}_fabricate`,
    ]);
  });

  it("inserts prepared rows like an ordinary insertion of them", async () => {
    const calls: string[] = [];
    const { host } = setup(
      () => ({
        intercept: {
          insert(context, next) {
            calls.push(`pilot ${context.operation}`);
            return next();
          },
        },
      }),
      {
        insertMiddleware: [
          (_context, next) => {
            calls.push("middleware");
            return next();
          },
        ],
        plugins: [
          createJobArgsTransformPlugin({
            name: "args",
            onInsert: ({ args, encodedArgs }) => {
              calls.push(`args transform ${encodedArgs}`);
              return { args, encodedArgs };
            },
            onRead: ({ args }) => args,
          }),
        ],
      }
    );
    const createdAt = Temporal.Instant.from("2020-01-02T03:04:05.678901Z");
    const uniqueKey = new Uint8Array(32).fill(9);
    const prepared: PreparedInsertParams = {
      createdAt,
      encodedArgs: '{"stored":"encoded"}',
      kind: `${prefix}_stored`,
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

    expect(calls).toEqual([
      'args transform {"stored":"encoded"}',
      "middleware",
      "pilot insertMany",
    ]);
    expect(result?.status).toBe("inserted");
    const row = (
      await admin.query<{
        args: unknown;
        created_us: string;
        metadata: unknown;
        unique_key: Buffer;
      }>(
        `SELECT args, (extract(epoch FROM created_at) * 1000000)::bigint::text AS created_us,
                metadata, unique_key
         FROM river_job WHERE kind = $1`,
        [`${prefix}_stored`]
      )
    ).rows[0];
    expect(row?.args).toEqual({ stored: "encoded" });
    expect(row?.metadata).toEqual({ stored: true });
    expect(new Uint8Array(row?.unique_key ?? [])).toEqual(uniqueKey);
    expect(row?.created_us).toBe(
      (createdAt.epochNanoseconds / 1000n).toString()
    );
    const [duplicate] = await host.insertPrepared([prepared]);
    expect(duplicate?.status).toBe("duplicate");

    // Arguments that aren't a JSON object, as another River client may
    // store, go back in as they are.
    const [array] = await host.insertPrepared([
      {
        ...prepared,
        encodedArgs: '[1,"a"]',
        kind: `${prefix}_array`,
        uniqueKey: null,
        uniqueStates: null,
      },
    ]);
    expect(array?.status).toBe("inserted");
    const stored = await admin.query<{ args: unknown }>(
      "SELECT args FROM river_job WHERE kind = $1",
      [`${prefix}_array`]
    );
    expect(stored.rows[0]?.args).toEqual([1, "a"]);
  });

  it("rescues in a pilot transaction, fenced by the leader", async () => {
    const queue = `${prefix}_rescue`;
    const driver = testPgDriver(admin);
    const { client, database } = setup();
    const inserted = await client.insert(job, {}, { queue });
    await driver.jobClaim({
      attemptedBy: "stuck-client",
      kinds: [],
      queues: [{ limit: 1, name: queue }],
    });
    await admin.query("DELETE FROM river_leader");
    const now = Temporal.Now.instant();
    const leader = await driver.maintenanceLeaderAcquire(
      `${prefix}_leader`,
      now,
      30_000,
      null
    );
    if (leader === null) throw new Error("expected leadership");
    try {
      const rescue = {
        error: { at: now, attempt: 1, error: "stuck", trace: "" },
        finalizedAt: null,
        id: inserted.job.id,
        scheduledAt: now,
        state: "retryable" as const,
      };
      const before = Temporal.Now.instant().add({ seconds: 1 });

      await expect(
        database.transaction(async (tx) => {
          expect(
            await driver.maintenanceRescue(leader, before, [rescue], { tx })
          ).toBe(1);
          throw new Error("roll the rescue back");
        })
      ).rejects.toThrow("roll the rescue back");
      expect((await client.jobs.get(inserted.job.id))?.state).toBe("running");
      await database.transaction((tx) =>
        driver.maintenanceRescue(leader, before, [rescue], { tx })
      );
      expect((await client.jobs.get(inserted.job.id))?.state).toBe("retryable");
    } finally {
      await driver.maintenanceLeaderResign(leader);
    }
  });

  it("runs producer sessions and services through PostgreSQL transactions", async () => {
    const own = `${prefix}_own_claim`;
    const standard = `${prefix}_standard_claim`;
    const finished = new Map<bigint, number>();
    const log: string[] = [];
    const { client } = setup(
      () => ({
        maintenanceServices: () => [
          {
            name: "term",
            run: ({ signal, term }) =>
              new Promise<void>((resolve) => {
                log.push(`term started ${term.leaderId}`);
                signal.addEventListener("abort", () => {
                  log.push("term ended");
                  resolve();
                });
              }),
          },
        ],
        services: () => [
          {
            name: "runtime",
            run: ({ signal }) =>
              new Promise<void>((resolve) => {
                log.push("service started");
                signal.addEventListener("abort", () => {
                  log.push("service ended");
                  resolve();
                });
              }),
          },
        ],
        startProducer: (context) =>
          Promise.resolve({
            async claim(claim, next) {
              if (claim.queue === standard) {
                return claim.database.transaction(async (tx) => {
                  const result = await next({ tx });
                  for (const row of result.jobs) {
                    await note(tx, `claimed ${claim.queue}`, row.id);
                  }
                  return result;
                });
              }
              // Claim at most two jobs in the session's own transaction
              // and load the rows River works.
              return claim.database.transaction(async (tx) => {
                const claimed = await tx.query<{ id: string }>(
                  `UPDATE river_job
                   SET attempt = attempt + 1,
                     attempted_at = now(),
                     attempted_by = array_append(attempted_by, $1),
                     state = 'running'
                   WHERE id IN (
                     SELECT id FROM river_job
                     WHERE queue = $2 AND state = 'available'
                       AND scheduled_at <= now()
                     ORDER BY priority, scheduled_at, id
                     LIMIT $3
                     FOR UPDATE SKIP LOCKED
                   )
                   RETURNING id`,
                  [claim.attemptedBy, claim.queue, Math.min(claim.limit, 2)]
                );
                const ids = claimed.rows.map(({ id }) => BigInt(id));
                for (const id of ids) {
                  await note(tx, `claimed ${claim.queue}`, id);
                }
                return claim.database.loadClaimed(ids, { tx });
              });
            },
            jobFinished(row) {
              finished.set(row.id, (finished.get(row.id) ?? 0) + 1);
            },
            shutdown() {
              log.push(`shutdown ${context.queue.name}`);
              return Promise.resolve();
            },
          }),
      }),
      {
        clientId: `${prefix}_sessions`,
        maintenance: { electionInterval: { milliseconds: 50 } },
        queues: {
          [own]: {
            fetchCooldown: { milliseconds: 1 },
            maxWorkers: 5,
            pollInterval: { milliseconds: 20 },
          },
          [standard]: {
            fetchCooldown: { milliseconds: 1 },
            maxWorkers: 5,
            pollInterval: { milliseconds: 20 },
          },
        },
        workers: new Workers().add(job, () => undefined),
      }
    );
    const ids: bigint[] = [];
    for (let index = 0; index < 10; index++) {
      const inserted = await client.insert(
        job,
        {},
        { queue: index % 2 === 0 ? own : standard }
      );
      ids.push(inserted.job.id);
    }

    const run = await client.start();
    try {
      await waitFor(async () => {
        const result = await admin.query<{ count: string }>(
          "SELECT count(*) AS count FROM river_job WHERE queue = ANY($1) AND state = 'completed'",
          [[own, standard]]
        );
        return Number(result.rows[0]?.count) === 10;
      }, 10_000);
      await waitFor(
        () => log.includes(`term started ${prefix}_sessions`),
        10_000
      );
    } finally {
      await run.stop();
    }

    expect([...finished.keys()].toSorted((a, b) => (a < b ? -1 : 1))).toEqual(
      ids
    );
    expect([...finished.values()].every((count) => count === 1)).toBe(true);
    expect((await notes()).toSorted()).toEqual(
      [
        ...Array.from({ length: 5 }, () => `claimed ${own}`),
        ...Array.from({ length: 5 }, () => `claimed ${standard}`),
      ].toSorted()
    );
    expect(log.slice(0, 1)).toEqual(["service started"]);
    expect(log).toEqual(
      expect.arrayContaining([
        "term ended",
        `shutdown ${own}`,
        `shutdown ${standard}`,
        "service ended",
      ])
    );
    expect(log.at(-1)).toBe("service ended");
  });

  it("claims peers in a pilot transaction and completes them through the pilot", async () => {
    const coordinatorQueue = `${prefix}_coordinator`;
    const peerQueue = `${prefix}_peers`;
    const rejected: string[] = [];
    /** Claim every available job of the peer queue in `tx`. */
    const claimPeers = async (tx: ClientBase, attemptedBy: string) => {
      const claimed = await tx.query<{ id: string }>(
        `UPDATE river_job
         SET attempt = attempt + 1, attempted_at = now(),
           attempted_by = array_append(attempted_by, $1), state = 'running'
         WHERE id IN (
           SELECT id FROM river_job
           WHERE queue = $2 AND state = 'available'
           ORDER BY id
           FOR UPDATE SKIP LOCKED
         )
         RETURNING id`,
        [attemptedBy, peerQueue]
      );
      await note(tx, `claimed ${claimed.rows.length}`);
      return claimed.rows
        .map(({ id }) => BigInt(id))
        .toSorted((left, right) => (left < right ? -1 : 1));
    };
    const { client, database, host } = setup(
      () => ({
        intercept: {
          async complete(context, next) {
            const results = await next();
            for (const result of results) {
              if (result.job?.queue === peerQueue) {
                await note(context.tx, "completed peer", result.job.id);
              }
            }
            return results;
          },
        },
      }),
      {
        clientId: `${prefix}_peers`,
        leaderElectionDisabled: true,
        queues: {
          [coordinatorQueue]: {
            fetchCooldown: { milliseconds: 1 },
            maxWorkers: 1,
            pollInterval: { milliseconds: 20 },
          },
        },
        workers: new Workers().add(job, async (context) => {
          const peerAttempts = pilot.host.attempts;
          const peerDatabase = pilot.database;
          // A claim returning another client's job rolls back entirely.
          await peerAttempts
            .claim(context, async ({ tx }) =>
              peerDatabase.loadClaimed(
                await claimPeers(tx, `${prefix}_someone_else`),
                { tx }
              )
            )
            .catch((error: unknown) => {
              expect(error).toBeInstanceOf(ExtensionError);
              rejected.push((error as Error).message);
            });
          const peers = await peerAttempts.claim(context, async ({ tx }) =>
            peerDatabase.loadClaimed(
              await claimPeers(tx, context.execution.attemptedBy),
              { tx }
            )
          );
          // The attempt completes the first peer only.
          await peerAttempts.complete(context, [
            { job: peers[0] as JobRow, result: { status: "succeeded" } },
          ]);
        }),
      }
    );
    // The worker reaches the pilot's host once the client is constructed.
    const pilot = { database, host };
    const first = await client.insert(job, {}, { queue: peerQueue });
    const second = await client.insert(job, {}, { queue: peerQueue });
    const coordinator = await client.insert(
      job,
      {},
      { queue: coordinatorQueue }
    );

    const run = await client.start();
    try {
      await waitFor(async () => {
        const result = await admin.query<{ state: string }>(
          "SELECT state FROM river_job WHERE id = $1",
          [coordinator.job.id.toString(10)]
        );
        return result.rows[0]?.state === "completed";
      }, 4_000);
    } finally {
      await run.stop();
    }

    expect(rejected).toEqual([
      `a peer claim returned job ${first.job.id}, which another client claimed`,
    ]);
    // The rolled-back claim left no note.
    expect(await notes()).toEqual([
      "claimed 2",
      "completed peer",
      "completed peer",
    ]);
    const { rows } = await admin.query<{
      attempt: number;
      errors: unknown[] | null;
      id: string;
      state: string;
    }>(
      "SELECT attempt, errors, id, state FROM river_job WHERE id = ANY($1) ORDER BY id",
      [[first.job.id.toString(10), second.job.id.toString(10)]]
    );
    expect(rows.map(({ attempt, state }) => ({ attempt, state }))).toEqual([
      { attempt: 1, state: "completed" },
      { attempt: 1, state: "available" },
    ]);
    expect(JSON.stringify(rows[1]?.errors)).toContain(
      "ended without an outcome for this job"
    );
  });

  describe("peer claims while stopping", () => {
    const coordinatorQueue = `${prefix}_stop_coordinator`;
    const peerQueue = `${prefix}_stop_peers`;

    /** Claim every available job of the peer queue for `attemptedBy`, in `tx`. */
    const claimAllPeers = async (
      database: PilotDatabase<ClientBase>,
      tx: ClientBase,
      attemptedBy: string
    ) => {
      const claimed = await tx.query<{ id: string }>(
        `UPDATE river_job
         SET attempt = attempt + 1, attempted_at = now(),
           attempted_by = array_append(attempted_by, $1), state = 'running'
         WHERE queue = $2 AND state = 'available'
         RETURNING id`,
        [attemptedBy, peerQueue]
      );
      return database.loadClaimed(
        claimed.rows.map(({ id }) => BigInt(id)),
        { tx }
      );
    };

    /** A client working `coordinatorQueue` with `work`, and its pilot. */
    const setupCoordinator = (
      work: (
        context: WorkContext,
        pilot: {
          readonly database: PilotDatabase<ClientBase>;
          readonly host: PilotHost<ClientBase>;
        }
      ) => Promise<void>
    ) => {
      const { client, database, host } = setup(() => ({}), {
        clientId: `${prefix}_stop_peers`,
        leaderElectionDisabled: true,
        queues: {
          [coordinatorQueue]: {
            fetchCooldown: { milliseconds: 1 },
            maxWorkers: 1,
            pollInterval: { milliseconds: 20 },
          },
        },
        workers: new Workers().add(job, (context) => work(context, pilot)),
      });
      // The worker reaches the pilot once the client is constructed.
      const pilot = { database, host };
      return client;
    };

    const peerRows = async () =>
      (
        await admin.query<{ attempt: number; queue: string; state: string }>(
          "SELECT attempt, queue, state FROM river_job WHERE queue = ANY($1) ORDER BY id",
          [[peerQueue, coordinatorQueue]]
        )
      ).rows;

    it("keeps a coordinator's peer claims open through a graceful stop", async () => {
      const running = Promise.withResolvers<undefined>();
      const gate = Promise.withResolvers<undefined>();
      const client = setupCoordinator(async (context, pilot) => {
        running.resolve(undefined);
        await gate.promise;
        const peers = await pilot.host.attempts.claim(context, async ({ tx }) =>
          claimAllPeers(pilot.database, tx, context.execution.attemptedBy)
        );
        await pilot.host.attempts.complete(
          context,
          peers.map((peer) => ({ job: peer, result: { status: "succeeded" } }))
        );
      });
      await client.insert(job, {}, { queue: peerQueue });
      await client.insert(job, {}, { queue: peerQueue });
      await client.insert(job, {}, { queue: coordinatorQueue });

      const run = await client.start();
      await running.promise;
      // The producer stops claiming at once; only then does the coordinator
      // claim its peers.
      const stopping = run.stop();
      gate.resolve(undefined);
      await stopping;

      // The stop resolved only after the peers and the coordinator persisted.
      expect(await peerRows()).toEqual([
        { attempt: 1, queue: peerQueue, state: "completed" },
        { attempt: 1, queue: peerQueue, state: "completed" },
        { attempt: 1, queue: coordinatorQueue, state: "completed" },
      ]);
    });

    it("refuses peer claims once a stop or cancellation cancels the coordinator", async () => {
      for (const cancellation of ["cancelling stop", "job cancellation"]) {
        const running = Promise.withResolvers<undefined>();
        const refused = Promise.withResolvers<unknown>();
        const client = setupCoordinator(async (context, pilot) => {
          running.resolve(undefined);
          await new Promise((resolve) => {
            context.signal.addEventListener("abort", resolve, { once: true });
          });
          refused.resolve(
            await pilot.host.attempts
              .claim(context, async ({ tx }) =>
                claimAllPeers(pilot.database, tx, context.execution.attemptedBy)
              )
              .then(
                () => undefined,
                (error: unknown) => error
              )
          );
        });
        await client.insert(job, {}, { queue: peerQueue });
        const coordinator = await client.insert(
          job,
          {},
          { queue: coordinatorQueue }
        );

        const run = await client.start();
        try {
          await running.promise;
          if (cancellation === "cancelling stop") {
            await run.stop({ mode: "cancel" });
            await expect(refused.promise).resolves.toBeInstanceOf(
              LifecycleError
            );
          } else {
            await client.jobs.cancel(coordinator.job.id);
            await expect(refused.promise).resolves.toBeInstanceOf(
              JobCancelledError
            );
          }
        } finally {
          await run.stop();
        }

        // The refused claim left the peer untouched.
        expect(
          (await peerRows()).filter(({ queue }) => queue === peerQueue)
        ).toEqual([{ attempt: 0, queue: peerQueue, state: "available" }]);
        await admin.query("DELETE FROM river_job WHERE queue = ANY($1)", [
          [peerQueue, coordinatorQueue],
        ]);
      }
    });

    it("settles each peer's own outcome after the coordinator is cancelled remotely", async () => {
      const running = Promise.withResolvers<undefined>();
      const client = setupCoordinator(async (context, pilot) => {
        const peers = await pilot.host.attempts.claim(context, async ({ tx }) =>
          claimAllPeers(pilot.database, tx, context.execution.attemptedBy)
        );
        const cancelled = new Promise((resolve) => {
          context.signal.addEventListener("abort", resolve, { once: true });
        });
        running.resolve(undefined);
        await cancelled;
        // Like River for Go, the cancellation applies to the coordinator
        // alone; each peer keeps its own outcome.
        await pilot.host.attempts.complete(context, [
          { job: peers[0] as JobRow, result: { status: "succeeded" } },
          {
            job: peers[1] as JobRow,
            result: { error: new Error("peer failed"), status: "failed" },
          },
        ]);
        context.signal.throwIfAborted();
      });
      await client.insert(job, {}, { queue: peerQueue });
      await client.insert(job, {}, { queue: peerQueue });
      const coordinator = await client.insert(
        job,
        {},
        { queue: coordinatorQueue }
      );

      const run = await client.start();
      try {
        await running.promise;
        await client.jobs.cancel(coordinator.job.id);
        await waitFor(
          async () => (await peerRows()).at(-1)?.state === "cancelled",
          4_000
        );
      } finally {
        await run.stop();
      }

      const rows = await peerRows();
      expect(rows.map(({ attempt, state }) => ({ attempt, state }))).toEqual([
        { attempt: 1, state: "completed" },
        { attempt: 1, state: expect.stringMatching(/^(available|retryable)$/) },
        { attempt: 1, state: "cancelled" },
      ]);
    });
  });

  it("cancels a job whose cancellation arrives while its claim is in flight", async () => {
    const queue = `${prefix}_claim_cancel`;
    const claimed = Promise.withResolvers<undefined>();
    const release = Promise.withResolvers<undefined>();
    // Like River for Go, the worker starts with its cancellation applied.
    let startedCancelled: boolean | undefined;
    const { client } = setup(
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
        clientId: `${prefix}_claim_cancel`,
        leaderElectionDisabled: true,
        queues: {
          [queue]: {
            fetchCooldown: { milliseconds: 1 },
            maxWorkers: 1,
            pollInterval: { milliseconds: 20 },
          },
        },
        workers: new Workers().add(job, ({ signal }) => {
          startedCancelled = signal.aborted;
          signal.throwIfAborted();
        }),
      }
    );
    const inserted = await client.insert(job, {}, { queue });

    const run = await client.start();
    try {
      await claimed.promise;
      await client.jobs.cancel(inserted.job.id);
      release.resolve(undefined);
      await waitFor(async () => {
        const result = await admin.query<{ state: string }>(
          "SELECT state FROM river_job WHERE id = $1",
          [inserted.job.id.toString(10)]
        );
        return result.rows[0]?.state === "cancelled";
      }, 4_000);
    } finally {
      release.resolve(undefined);
      await run.stop();
    }

    expect(startedCancelled).toBe(true);
  });

  it("hands a producer session its queue metadata as stored", async () => {
    const queue = `${prefix}_metadata_text`;
    const texts: string[] = [];
    await admin.query(
      `INSERT INTO river_queue (name, metadata, created_at, updated_at)
       VALUES ($1, '{"retries": 1.0, "scale": 1e2}'::jsonb, now(), now())`,
      [queue]
    );
    const { client } = setup(
      () => ({
        startProducer: (context) => {
          texts.push(context.metadataText);
          return Promise.resolve({});
        },
      }),
      {
        clientId: `${prefix}_metadata_text`,
        leaderElectionDisabled: true,
        queues: { [queue]: { maxWorkers: 1 } },
        workers: new Workers().add(job, () => undefined),
      }
    );

    const run = await client.start();
    await run.stop();

    // PostgreSQL's rendering of the stored JSONB, which keeps `1.0` and
    // orders keys by length.
    expect(texts).toEqual(['{"scale": 100, "retries": 1.0}']);
  });

  it("offers a queue's metadata change as its notification arrives", async () => {
    const queue = `${prefix}_metadata_notification`;
    const texts: string[] = [];
    const { client } = setup(
      () => ({
        startProducer: () =>
          Promise.resolve({
            configurationChanged: (configuration) => {
              texts.push(configuration.metadataText);
            },
          }),
      }),
      {
        clientId: `${prefix}_metadata_notification`,
        leaderElectionDisabled: true,
        // Only the notification can deliver the change in time.
        queueControlPollInterval: { hours: 1 },
        queues: { [queue]: { maxWorkers: 1 } },
        workers: new Workers().add(job, () => undefined),
      }
    );

    const run = await client.start();
    try {
      // Another client changes the queue's metadata.
      await new Client(new PgDriver(admin)).queues.update(queue, {
        metadata: { retries: 2 },
      });
      await waitFor(() => texts.length === 1, 4_000);
    } finally {
      await run.stop();
    }

    expect(texts).toEqual(['{"retries": 2}']);
  });

  it("requires a pool-backed driver", async () => {
    const single = new pg.Client({ connectionString: TEST_DATABASE_URL });
    expect(
      () => new CompanionClient(new PgDriver(single), {}, () => ({}))
    ).toThrow(UnsupportedCapabilityError);
    expect(new Client(new PgDriver(single))).toBeInstanceOf(Client);
  });

  it("shows the insert interceptor each row's arguments from before the argument transforms", async () => {
    const seen: { original: readonly string[]; stored: readonly string[] }[] =
      [];
    const { client, host } = setup(
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
    const tx = await admin.connect();
    try {
      await tx.query("BEGIN");
      await client.insert(job, { n: 4 }, { tx });
      await tx.query("COMMIT");
    } finally {
      tx.release();
    }
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
    const stored = await admin.query<{ args: unknown }>(
      "SELECT args FROM river_job WHERE kind = $1 ORDER BY id",
      [job.kind]
    );
    expect(stored.rows.map(({ args }) => args)).toEqual(
      [1, 2, 3, 4, 5].map((n) => ({ wrapped: `{"n":${n}}` }))
    );
  });

  // Like River for Go and Rust, River opens no savepoint or nested
  // transaction in a caller's transaction: an operation's statements run
  // directly in it, and when the operation fails after writing, its writes
  // stay there until the caller rolls back. Without a caller transaction,
  // River's own transaction rolls the whole operation back.
  describe("caller transactions", () => {
    /** Run `callback` in a caller's transaction, then roll it back. */
    const rolledBack = async (
      callback: (tx: ClientBase) => Promise<void>
    ): Promise<void> => {
      const tx = await admin.connect();
      try {
        await tx.query("BEGIN");
        await callback(tx);
      } finally {
        await tx.query("ROLLBACK");
        tx.release();
      }
    };
    const jobCountIn = async (tx: ClientBase): Promise<number> =>
      Number(
        (
          await tx.query<{ count: string }>(
            "SELECT count(*) AS count FROM river_job WHERE kind LIKE $1",
            [`${prefix}%`]
          )
        ).rows[0]?.count
      );

    it("leaves an insertion that fails after its write in the caller transaction", async () => {
      let failMiddleware = false;
      let failHook = false;
      const { client } = setup(() => ({}), {
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
      expect(await jobCount()).toBe(0);

      await rolledBack(async (tx) => {
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
        expect(await jobCountIn(tx)).toBe(3);
      });
      expect(await jobCount()).toBe(0);
    });

    it("validates an insertion before writing any of it", async () => {
      const { client } = setup();

      await rolledBack(async (tx) => {
        await expect(
          client.insertMany(
            [
              { args: {}, job },
              { args: {}, job, options: { priority: 99 } },
            ],
            { tx }
          )
        ).rejects.toBeInstanceOf(ValidationError);
        // Nothing failed in the database, so the transaction is usable.
        expect(await jobCountIn(tx)).toBe(0);
      });
    });

    it("lets a database error abort the caller transaction", async () => {
      const failing = defineJob({ kind: `${prefix}_failing` });
      const trigger = `${prefix}_fail_insert`;
      await admin.query(
        `CREATE FUNCTION ${trigger}() RETURNS trigger LANGUAGE plpgsql AS $$
         BEGIN RAISE EXCEPTION 'insert failed'; END $$`
      );
      await admin.query(
        `CREATE TRIGGER ${trigger} BEFORE INSERT ON river_job FOR EACH ROW
         WHEN (NEW.kind = '${prefix}_failing') EXECUTE FUNCTION ${trigger}()`
      );
      try {
        const { client } = setup();

        // River doesn't hide the error behind a savepoint, so the caller's
        // transaction can only be rolled back, with its earlier work.
        await rolledBack(async (tx) => {
          await client.insert(job, {}, { tx });
          await expect(
            client.insert(failing, {}, { tx })
          ).rejects.toBeInstanceOf(DatabaseOperationError);
          await expect(tx.query("SELECT 1")).rejects.toMatchObject({
            code: "25P02",
          });
        });
        expect(await jobCount()).toBe(0);
      } finally {
        await admin.query(`DROP TRIGGER ${trigger} ON river_job`);
        await admin.query(`DROP FUNCTION ${trigger}()`);
      }
    });

    it("leaves a failed transactional completion in the caller transaction until it rolls back", async () => {
      const queue = `${prefix}_caller_complete`;
      const txJob = defineJob({ kind: `${prefix}_tx_complete` });
      let fail = true;
      let failure: unknown;
      let inTransaction: unknown;
      const { client } = setup(
        () => ({
          intercept: {
            async complete(context, next) {
              const results = await next();
              await note(context.tx, `completed ${context.commands.length}`);
              if (fail) throw new Error("complete companion failed");
              return results;
            },
          },
        }),
        {
          completionBatchSize: 1,
          leaderElectionDisabled: true,
          queues: {
            [queue]: {
              fetchCooldown: { milliseconds: 1 },
              maxWorkers: 1,
              pollInterval: { milliseconds: 5 },
            },
          },
          workers: new Workers().add(
            txJob,
            async ({ completeTx, job: row }) => {
              await rolledBack(async (tx) => {
                failure = await completeTx(tx).catch((error: unknown) => error);
                inTransaction = {
                  notes: (
                    await tx.query<{ note: string }>(
                      `SELECT note FROM ${companionTable} ORDER BY id`
                    )
                  ).rows.map(({ note }) => note),
                  state: (
                    await tx.query<{ state: string }>(
                      "SELECT state FROM river_job WHERE id = $1",
                      [row.id.toString(10)]
                    )
                  ).rows[0]?.state,
                };
              });
              // The rolled-back completion left the job running, so River
              // completes it once the handler returns.
              fail = false;
            }
          ),
        }
      );
      const run = await client.start();
      try {
        await client.insert(txJob, {}, { queue });
        await waitFor(async () => {
          const result = await admin.query<{ count: string }>(
            "SELECT count(*) AS count FROM river_job WHERE queue = $1 AND state = 'completed'",
            [queue]
          );
          return Number(result.rows[0]?.count) === 1;
        }, 10_000);
      } finally {
        await run.stop();
      }

      expect(failure).toMatchObject({
        message: expect.stringContaining("complete companion failed"),
      });
      expect(inTransaction).toEqual({
        notes: ["completed 1"],
        state: "completed",
      });
      expect(await notes()).toEqual(["completed 1"]);
    });

    it("writes everything in the caller transaction's own transaction ID", async () => {
      const { client } = setup(() => ({
        intercept: {
          async cancel(context, next) {
            const row = await next();
            await note(context.tx, "cancel", row?.id ?? null);
            return row;
          },
          async insert(context, next) {
            const results = await next();
            await note(context.tx, "insert");
            return results;
          },
          async retry(context, next) {
            const row = await next();
            await note(context.tx, "retry", row?.id ?? null);
            return row;
          },
        },
      }));

      await rolledBack(async (tx) => {
        // A write directly in the caller's transaction, so that even one
        // savepoint around all of River's writes would be detected.
        await note(tx, "application");
        // More than PostgreSQL's cached subtransaction ID limit.
        const ids: bigint[] = [];
        for (let index = 0; index < 70; index++) {
          ids.push((await client.insert(job, {}, { tx })).job.id);
        }
        const many = await client.insertMany(
          Array.from({ length: 5 }, () => ({ args: {}, job })),
          { tx }
        );
        await client.jobs.cancel(ids[0] as bigint, { tx });
        await client.jobs.retry(ids[0] as bigint, { tx });

        const result = await tx.query<{
          rows: string;
          transactions: string;
        }>(
          `SELECT count(*) AS rows, count(DISTINCT xmin::text) AS transactions
           FROM (
             SELECT xmin FROM river_job WHERE kind LIKE $1
             UNION ALL
             SELECT xmin FROM ${companionTable}
           ) AS written`,
          [`${prefix}%`]
        );
        expect(many).toHaveLength(5);
        // 75 jobs, the application's row, an effect row for each of 71
        // intercepted insertions, and the cancellation and retry.
        expect(Number(result.rows[0]?.rows)).toBe(75 + 1 + 71 + 2);
        expect(Number(result.rows[0]?.transactions)).toBe(1);
      });
    });
  });
});

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
