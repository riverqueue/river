import { randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { createPool, type Pool, type RowDataPacket } from "mysql2/promise";
import { afterEach, beforeEach, describe, expect, it } from "vitest";
import {
  Client,
  defineJob,
  Workers,
  parseJson,
  isExactJsonNumber,
  jsonNumberToBigInt,
  stringifyJson,
  type ExactJsonNumber,
  type JobState,
  type JsonObject,
} from "riverqueue";
import type {
  JobInsertParams,
  JobListParams,
  JobCompletionCommand,
} from "riverqueue/unstable-driver";
import { createMigrator } from "../../../migrate/src/index.js";
import { decodeAttemptError } from "../../../src/driver-codecs.js";
import { completionCommand } from "../../../src/runtime/completion-command.js";
import { MySqlDriver, MySqlRuntime } from "./driver.js";

function fixture(name: string) {
  return parseJson(
    readFileSync(
      new URL(`../../../../conformance/testdata/${name}.json`, import.meta.url),
      "utf8"
    )
  );
}

const url = process.env.TEST_MYSQL_URL;
const listing: JobListParams = {
  after: null,
  ids: [],
  kinds: [],
  limit: 100,
  metadata: null,
  priorities: [],
  queues: [],
  sortDirection: "asc",
  sortField: "id",
  states: [],
  tagsAll: [],
  tagsAny: [],
};
function params(overrides: Partial<JobInsertParams> = {}): JobInsertParams {
  return {
    args: { message: "hello" },
    encodedArgs: '{"message":"hello"}',
    kind: "message",
    maxAttempts: 25,
    metadata: {},
    priority: 1,
    queue: "default",
    // Use the claim clock so small client/server clock differences cannot defer a job.
    scheduledAt: Temporal.Now.instant(),
    state: "available",
    tags: [],
    uniqueKey: null,
    uniqueStates: null,
    ...overrides,
  };
}

describe.skipIf(!url)("MySQL driver", () => {
  let pool: Pool;
  let schema: string;
  let driver: MySqlDriver;
  let runtime: MySqlRuntime;
  beforeEach(async () => {
    pool = createPool({ uri: url!, connectionLimit: 12 });
    schema = `js_${randomUUID().replaceAll("-", "")}`;
    driver = new MySqlDriver(pool, { schema });
    runtime = new MySqlRuntime(pool, { schema });
    await createMigrator(driver).migrateUp();
  });
  afterEach(async () => {
    await pool.query(`DROP DATABASE IF EXISTS \`${schema}\``);
    await pool.end();
  });

  it("migrates down and back up", async () => {
    const migrator = createMigrator(driver);
    expect((await migrator.validate()).ok).toBe(true);
    await migrator.migrateDown({ targetVersion: 0 });
    expect(await migrator.existingVersions()).toEqual([]);
    await migrator.migrateUp();
    expect((await migrator.validate()).ok).toBe(true);
  });

  it("preserves large IDs, JSON integers, microseconds, and time zones", async () => {
    const time = Temporal.Instant.from("2026-10-01T02:03:04.123456Z");
    await pool.query(
      `ALTER TABLE \`${schema}\`.river_job AUTO_INCREMENT=9007199254740993`
    );
    const first = await runtime.jobInsert(
      params({
        encodedArgs: '{"n":9007199254740993}',
        args: parseJson('{"n":9007199254740993}') as { n: number },
        createdAt: time,
        scheduledAt: time,
      })
    );
    const second = await runtime.jobInsert(params());
    expect(first.job.id).toBe(9007199254740993n);
    expect(second.job.id).toBe(first.job.id + 1n);
    expect(isExactJsonNumber(first.job.args.n)).toBe(true);
    expect(first.job.createdAt.equals(time)).toBe(true);
    expect(first.job.scheduledAt.equals(time)).toBe(true);
    expect(
      (await runtime.jobList({ ...listing, ids: [second.job.id] })).map(
        (job) => job.id
      )
    ).toEqual([second.job.id]);
  });

  it("joins caller transactions and keeps rollback atomic", async () => {
    const tx = await pool.getConnection();
    try {
      await expect(runtime.jobInsert(params(), { tx })).rejects.toMatchObject({
        code: "transaction_scope",
      });
      await tx.beginTransaction();
      const inserted = await runtime.jobInsert(params(), { tx });
      await runtime.notifyInsert(["default"], { tx });
      expect(await runtime.jobGet(inserted.job.id)).toBeNull();
      await tx.rollback();
      expect(await runtime.jobGet(inserted.job.id)).toBeNull();
      const [rows] = await pool.query(
        `SELECT * FROM \`${schema}\`.river_notification`
      );
      expect(rows).toEqual([]);
    } finally {
      tx.release();
    }
  });

  it("deduplicates concurrent inserts without changing the existing kind", async () => {
    const uniqueKey = new Uint8Array(32).fill(3);
    const inserts = await Promise.all(
      Array.from({ length: 8 }, () =>
        runtime.jobInsert(params({ uniqueKey, uniqueStates: ["available"] }))
      )
    );
    expect(inserts.filter((item) => item.status === "inserted")).toHaveLength(
      1
    );
    expect(new Set(inserts.map((item) => item.job.id)).size).toBe(1);
    const duplicate = await runtime.jobInsert(
      params({ kind: "different", uniqueKey, uniqueStates: ["available"] })
    );
    expect(duplicate.status).toBe("duplicate");
    expect(duplicate.job.kind).toBe("message");
  });

  it("allows repeated batch keys outside the unique states", async () => {
    const uniqueKey = new Uint8Array(32).fill(4);
    const pending = params({
      uniqueKey,
      uniqueStates: ["available"],
      state: "pending",
    });
    const active = params({ uniqueKey, uniqueStates: ["available"] });
    const inserted = await runtime.jobInsertMany([pending, active, pending]);
    expect(inserted.map((item) => item.status)).toEqual([
      "inserted",
      "inserted",
      "inserted",
    ]);
    expect(new Set(inserted.map((item) => item.job.id)).size).toBe(3);
    await expect(
      runtime.jobInsertMany([active, pending, active])
    ).rejects.toThrow("Batch repeats a unique key");
    expect(await runtime.jobList(listing)).toHaveLength(3);
  });

  it("rejects unrelated unique constraints when the incoming key is inactive", async () => {
    await pool.query(
      `CREATE UNIQUE INDEX custom_kind ON \`${schema}\`.river_job(kind)`
    );
    const uniqueKey = new Uint8Array(32).fill(5);
    const original = await runtime.jobInsert(
      params({ uniqueKey, uniqueStates: ["available"] })
    );
    await expect(
      runtime.jobInsert(
        params({ uniqueKey, uniqueStates: ["available"], state: "pending" })
      )
    ).rejects.toMatchObject({
      cause: { message: "Insertion violated an unrelated unique constraint" },
    });
    expect(await runtime.jobGet(original.job.id)).toEqual(original.job);
    expect(await runtime.jobList(listing)).toHaveLength(1);
  });

  it("claims concurrently without duplicate work, and respects paused queues", async () => {
    await runtime.runtimeQueueUpsert("paused", Temporal.Now.instant());
    await runtime.queuePause("paused");
    await runtime.jobInsert(params({ queue: "paused" }));
    await runtime.jobInsertMany(Array.from({ length: 20 }, () => params()));
    const batches = await Promise.all(
      Array.from({ length: 4 }, (_, index) =>
        runtime.jobClaim({
          attemptedBy: `worker${index}`,
          kinds: [],
          queues: [
            { name: "default", limit: 10 },
            { name: "paused", limit: 1 },
          ],
        })
      )
    );
    const ids = batches.flatMap((batch) => batch.jobs.map((job) => job.id));
    // A peer may skip rows while the first claimant holds secondary-index
    // locks. The next poll must claim the remainder, with no duplicate IDs.
    const remaining = await runtime.jobClaim({
      attemptedBy: "next",
      kinds: [],
      queues: [{ name: "default", limit: 20 }],
    });
    ids.push(...remaining.jobs.map((job) => job.id));
    expect(ids).toHaveLength(20);
    expect(new Set(ids).size).toBe(20);
    await runtime.queueResume("paused");
    expect(
      (
        await runtime.jobClaim({
          attemptedBy: "worker",
          kinds: [],
          queues: [{ name: "paused", limit: 1 }],
        })
      ).jobs
    ).toHaveLength(1);
  });

  it("fences completions and makes a cancellation win over a retry", async () => {
    const inserted = await runtime.jobInsert(
      params({ metadata: { keep: true, nested: { a: 1 } } })
    );
    const claimed = await runtime.jobClaim({
      attemptedBy: "worker",
      kinds: [],
      queues: [{ name: "default", limit: 1 }],
    });
    const job = claimed.jobs[0]!;
    const command: JobCompletionCommand = {
      id: job.id,
      attempt: job.attempt,
      attemptedBy: "worker",
      kind: "retry",
      finalizedAt: null,
      scheduledAt: Temporal.Now.instant().add({ hours: 1 }),
      error: { at: Temporal.Now.instant(), error: "retry", trace: "" },
      metadata: { nested: { b: 2 }, present: null },
      output: null,
      outputSet: true,
    };
    expect(
      (await runtime.jobCompleteMany([{ ...command, attempt: 999 }]))[0]?.status
    ).toBe("stale");
    await runtime.jobCancel(inserted.job.id);
    const result = (await runtime.jobCompleteMany([command]))[0]!;
    expect(result.status).toBe("applied");
    expect(result.job?.state).toBe("cancelled");
    expect(result.job?.metadata).toMatchObject({
      keep: true,
      nested: { b: 2 },
      present: null,
      output: null,
    });
    expect(result.job?.metadata.nested).toEqual({ b: 2 });
    expect(result.job?.errors).toHaveLength(1);
  });

  it.each([
    [{ value: { a: 1, b: 2 } }, { value: { a: 1 } }, true],
    [{ value: { a: 1, b: 2 } }, { value: { a: 2 } }, false],
    [{ value: [[1]] }, { value: [1] }, false],
    [{ value: [1] }, { value: 1 }, false],
    [{ value: [{ a: 1, b: 2 }] }, { value: [{ b: 2 }] }, true],
    [{ tenant: { settings: [] } }, { tenant: { settings: {} } }, false],
    [{ tenant: { settings: { a: 1 } } }, { tenant: { settings: {} } }, true],
  ])(
    "preserves nested metadata containment %#",
    async (metadata, wanted, matches) => {
      await runtime.jobInsert(params({ metadata: { unrelated: true } }));
      const inserted = await runtime.jobInsert(params({ metadata }));
      const rows = await runtime.jobList({ ...listing, metadata: wanted });
      expect(rows.map((row) => row.id)).toEqual(
        matches ? [inserted.job.id] : []
      );
    }
  );

  it("updates queues and jobs, retries and deletes", async () => {
    await runtime.runtimeQueueUpsert("default", Temporal.Now.instant());
    expect(
      (await runtime.queueUpdate("default", { metadata: { concurrency: 5 } }))
        ?.metadata
    ).toEqual({ concurrency: 5 });
    expect(
      await runtime.queueList({ nameAfter: null, limit: 10 })
    ).toHaveLength(1);
    const inserted = await runtime.jobInsert(params());
    await runtime.jobUpdate(inserted.job.id, {
      metadata: { nested: { value: true } },
      output: null,
    });
    expect((await runtime.jobCancel(inserted.job.id))?.state).toBe("cancelled");
    expect((await runtime.jobRetry(inserted.job.id))?.state).toBe("available");
    expect((await runtime.jobDelete(inserted.job.id)).status).toBe("deleted");
    await runtime.jobInsertMany([params(), params({ kind: "other" })]);
    expect(
      await runtime.jobDeleteMany({ ...listing, all: false, kinds: ["other"] })
    ).toHaveLength(1);
  });

  it("cleans expired jobs without waiting for locked jobs", async () => {
    const now = Temporal.Now.instant();
    const leader = (await runtime.maintenanceLeaderAcquire(
      "cleaner",
      now,
      60_000,
      null
    ))!;
    const [running, locked, ...expired] = await runtime.jobInsertMany(
      Array.from({ length: 1002 }, () => params())
    );
    await pool.query(
      `UPDATE \`${schema}\`.river_job SET state='completed', finalized_at='1970-01-01' WHERE id>=?`,
      [locked!.job.id.toString()]
    );
    await pool.query(
      `UPDATE \`${schema}\`.river_job SET state='running' WHERE id=?`,
      [running!.job.id.toString()]
    );
    const tx = await pool.getConnection();
    try {
      await tx.beginTransaction();
      for (const id of [running!.job.id, locked!.job.id]) {
        await tx.query(
          `SELECT id FROM \`${schema}\`.river_job WHERE id=? FOR UPDATE`,
          [id.toString()]
        );
      }
      expect(
        await runtime.maintenanceCleanJobs(
          leader,
          {
            cancelledBefore: now,
            completedBefore: now,
            discardedBefore: now,
            limit: 1000,
          },
          null,
          AbortSignal.timeout(2_000)
        )
      ).toBe(1000);
      expect(await runtime.jobGet(expired[0]!.job.id)).toBeNull();
      expect((await runtime.jobGet(locked!.job.id))?.state).toBe("completed");
      expect((await runtime.jobGet(running!.job.id))?.state).toBe("running");
    } finally {
      await tx.rollback();
      tx.release();
    }
  });

  it("schedules, rescues and cleans under an exact leader term", async () => {
    const now = Temporal.Now.instant();
    const leader = (await runtime.maintenanceLeaderAcquire(
      "leader",
      now,
      60_000,
      null
    ))!;
    expect(leader).not.toBeNull();
    expect(
      await runtime.maintenanceLeaderAcquire("follower", now, 60_000, null)
    ).toBeNull();
    const inserted = await runtime.jobInsert(
      params({ state: "scheduled", scheduledAt: now })
    );
    expect(
      await runtime.maintenanceSchedule(leader, {
        now,
        limit: 100,
        notificationHorizon: now,
        scheduledAtHorizon: now,
        allowInsertNotifications: (queues) => queues,
      })
    ).toBe(1);
    await runtime.jobClaim({
      attemptedBy: "worker",
      kinds: [],
      queues: [{ name: "default", limit: 1 }],
    });
    const horizon = Temporal.Now.instant().add({ hours: 1 });
    expect(
      await runtime.maintenanceGetStuck(leader, horizon, 0n, 100)
    ).toHaveLength(1);
    expect(
      await runtime.maintenanceRescue(leader, horizon, [
        {
          id: inserted.job.id,
          state: "discarded",
          finalizedAt: now,
          scheduledAt: now,
          error: { at: now, attempt: 1, error: "stuck", trace: "" },
        },
      ])
    ).toBe(1);
    expect(
      (await runtime.jobGet(inserted.job.id))?.metadata["river:rescue_count"]
    ).toBe(1);
    expect(
      await runtime.maintenanceCleanJobs(
        leader,
        {
          limit: 100,
          cancelledBefore: horizon,
          completedBefore: horizon,
          discardedBefore: horizon,
        },
        null,
        new AbortController().signal
      )
    ).toBe(1);
    expect(await runtime.maintenanceLeaderResign(leader)).toBe(true);
    expect(
      await runtime.maintenanceSchedule(leader, {
        now,
        limit: 100,
        notificationHorizon: now,
        scheduledAtHorizon: now,
        allowInsertNotifications: (queues) => queues,
      })
    ).toBe(0);
  });

  it("delivers request_resign and waits for earlier uncommitted notifications", async () => {
    const abort = new AbortController();
    const messages: string[] = [];
    const ready = Promise.withResolvers<undefined>();
    const listening = (async () => {
      for await (const message of runtime.runtimeNotificationSubscribe(
        ["leadership", "insert"],
        abort.signal,
        () => ready.resolve(undefined)
      ))
        messages.push(message.payload);
    })();
    await ready.promise;
    const first = await pool.getConnection();
    try {
      await first.beginTransaction();
      await runtime.notifyInsert(["first"], { tx: first });
      await runtime.notifyInsert(["second"]);
      await first.commit();
      await runtime.runtimeRequestLeadershipResignation();
      await expect.poll(() => messages.length).toBe(3);
      expect(messages.map((message) => JSON.parse(message))).toEqual([
        { queue: "first" },
        { queue: "second" },
        { action: "request_resign", leader_id: "" },
      ]);
    } finally {
      first.release();
      abort.abort();
      await listening;
    }
  });

  it("interrupts a notification poll blocked on an uncommitted row", async () => {
    const abort = new AbortController();
    const ready = Promise.withResolvers<undefined>();
    const listening = (async () => {
      for await (const message of runtime.runtimeNotificationSubscribe(
        ["insert"],
        abort.signal,
        () => ready.resolve(undefined)
      ))
        expect(message).toBeUndefined();
    })();
    await ready.promise;
    const first = await pool.getConnection();
    try {
      await first.beginTransaction();
      await runtime.notifyInsert(["blocked"], { tx: first });
      await expect
        .poll(async () => {
          const [rows] = await pool.query<
            import("mysql2/promise").RowDataPacket[]
          >(
            "SELECT COUNT(*) AS n FROM performance_schema.data_lock_waits w JOIN performance_schema.data_locks l ON l.ENGINE_LOCK_ID=w.REQUESTING_ENGINE_LOCK_ID AND l.ENGINE=w.ENGINE WHERE l.OBJECT_SCHEMA=? AND l.OBJECT_NAME='river_notification'",
            [schema]
          );
          return Number(rows[0]?.n);
        })
        .toBeGreaterThan(0);
      abort.abort();
      await listening;
      expect(await runtime.jobList(listing)).toEqual([]);
    } finally {
      abort.abort();
      await first.rollback();
      first.release();
      await listening;
    }
  });

  it("matches job kinds and queues exactly, including trailing spaces", async () => {
    const wanted = (await runtime.jobInsert(params())).job;
    await runtime.jobInsert(params({ kind: "message ", queue: "default " }));
    expect(
      (await runtime.jobList({ ...listing, kinds: ["message"] })).map(
        (job) => job.id
      )
    ).toEqual([wanted.id]);
    expect(
      (await runtime.jobList({ ...listing, queues: ["default"] })).map(
        (job) => job.id
      )
    ).toEqual([wanted.id]);
  });

  it("runs the public worker API to completion", async () => {
    const message = defineJob({ kind: "message" });
    const workers = new Workers().add(message, () => undefined);
    const client = new Client(driver, {
      queues: { default: { maxWorkers: 2 } },
      workers,
      completionFlushInterval: { milliseconds: 1 },
    });
    const run = await client.start();
    try {
      const inserted = await client.insert(message, {});
      await expect
        .poll(async () => (await client.jobs.get(inserted.job.id))?.state, {
          timeout: 5000,
        })
        .toBe("completed");
    } finally {
      await run.stop();
    }
  });

  describe("Go conformance", () => {
    if (!url) return;

    const protocol = fixture("protocol_values") as unknown as {
      attempt_error: JsonObject;
      job_states: { state: JobState; unique_bit: number }[];
      metadata_keys: {
        output: string;
        rescue_count: string;
        unique_nonce: string;
      };
      notifications: { name: string; topic: string; payload: JsonObject }[];
    };
    const snoozes = fixture("snooze_counters") as unknown as {
      snooze_counters: {
        name: string;
        metadata: JsonObject;
        expected_snoozes: ExactJsonNumber | number;
      }[];
    };

    it.each(protocol.job_states)(
      "stores $state and its unique bit",
      async ({ state, unique_bit: bit }) => {
        const inserted = await runtime.jobInsert(
          params({
            uniqueKey: new Uint8Array(32).fill(1),
            uniqueStates: [state],
          })
        );
        const [encoded] = await pool.query<RowDataPacket[]>(
          `SELECT unique_states FROM \`${schema}\`.river_job WHERE id=?`,
          [inserted.job.id.toString()]
        );
        expect(encoded[0]?.unique_states).toBe(bit);
        const finalized = ["cancelled", "completed", "discarded"].includes(
          state
        );
        // Seed the Go state/mask independently of the driver's encoder.
        await pool.query(
          `UPDATE \`${schema}\`.river_job SET state=?, finalized_at=${finalized ? "UTC_TIMESTAMP(6)" : "NULL"}, unique_states=? WHERE id=?`,
          [state, bit, inserted.job.id.toString()]
        );
        const stored = await runtime.jobGet(inserted.job.id);
        expect(stored?.state).toBe(state);
        expect(stored?.uniqueStates).toEqual([state]);
        expect(
          await runtime.jobInsert(
            params({
              uniqueKey: inserted.job.uniqueKey,
              uniqueStates: ["available", state],
            })
          )
        ).toMatchObject({ status: "duplicate", job: { id: inserted.job.id } });
        const [rows] = await pool.query<RowDataPacket[]>(
          `SELECT unique_states FROM \`${schema}\`.river_job WHERE id=?`,
          [inserted.job.id.toString()]
        );
        expect(rows[0]?.unique_states).toBe(bit);
      }
    );

    it.each(snoozes.snooze_counters)(
      "persists the $name snooze counter",
      async ({ metadata, expected_snoozes: expected }) => {
        const inserted = await runtime.jobInsert(params({ metadata }));
        const { jobs } = await runtime.jobClaim({
          attemptedBy: "client",
          kinds: [],
          queues: [{ name: "default", limit: 1 }],
        });
        const now = Temporal.Now.instant();
        const command = completionCommand(
          jobs[0]!,
          "client",
          {
            status: "succeeded",
            outcome: {
              type: "snooze",
              duration: Temporal.Duration.from({ hours: 1 }),
            },
          },
          now,
          now,
          now,
          1000
        );
        expect((await runtime.jobCompleteMany([command]))[0]?.status).toBe(
          "applied"
        );
        const stored = (await runtime.jobGet(inserted.job.id))!;
        expect(stored.state).toBe("scheduled");
        expect(stored.attempt).toBe(0);
        expect(
          jsonNumberToBigInt(
            stored.metadata.snoozes as ExactJsonNumber | number
          )
        ).toBe(jsonNumberToBigInt(expected));
      }
    );

    it.each([
      { initial: 2, expected: 3 },
      { initial: 2.5, expected: 3.5 },
      { initial: null, expected: 1 },
      { initial: "2", expected: 1 },
    ])(
      "round trips Go's attempt error and persists rescue metadata from $initial",
      async ({ initial, expected }) => {
        const keys = protocol.metadata_keys;
        const inserted = await runtime.jobInsert(
          params({ metadata: { [keys.rescue_count]: initial } })
        );
        const nonce = inserted.job.metadata[keys.unique_nonce];
        expect(nonce).toEqual(expect.any(String));
        expect(nonce).not.toBe("");
        await pool.query(
          `UPDATE \`${schema}\`.river_job SET state='running', attempt=3, attempted_at=UTC_TIMESTAMP(6), errors=JSON_ARRAY(CAST(? AS JSON)) WHERE id=?`,
          [stringifyJson(protocol.attempt_error), inserted.job.id.toString()]
        );
        const error = decodeAttemptError(stringifyJson(protocol.attempt_error));
        expect((await runtime.jobGet(inserted.job.id))?.errors).toEqual([
          error,
        ]);
        const now = Temporal.Now.instant();
        const leader = (await runtime.maintenanceLeaderAcquire(
          "client",
          now,
          60_000,
          null
        ))!;
        expect(
          await runtime.maintenanceRescue(leader, now.add({ hours: 1 }), [
            {
              id: inserted.job.id,
              state: "retryable",
              scheduledAt: now,
              finalizedAt: null,
              error,
            },
          ])
        ).toBe(1);
        await runtime.maintenanceSchedule(leader, {
          now,
          limit: 1,
          scheduledAtHorizon: now,
          notificationHorizon: now,
          allowInsertNotifications: (queues) => queues,
        });
        const { jobs } = await runtime.jobClaim({
          attemptedBy: "client",
          kinds: [],
          queues: [{ name: "default", limit: 1 }],
        });
        const command = completionCommand(
          jobs[0]!,
          "client",
          {
            status: "succeeded",
            outcome: { type: "complete", output: { ok: true } },
          },
          now,
          now,
          now,
          1000
        );
        expect((await runtime.jobCompleteMany([command]))[0]?.status).toBe(
          "applied"
        );
        const stored = (await runtime.jobGet(inserted.job.id))!;
        expect(stored.metadata).toMatchObject({
          [keys.output]: { ok: true },
          [keys.rescue_count]: expected,
          [keys.unique_nonce]: nonce,
        });
        expect(stored.errors).toEqual([error, error]);
      }
    );

    it.each(protocol.notifications)(
      "emits Go's $name topic and payload",
      async ({ name, topic, payload }) => {
        // Inputs deliberately use API semantics, not the fixture's wire keys.
        switch (name) {
          case "cancel": {
            const inserted = await runtime.jobInsert(
              params({ queue: "priority" })
            );
            await pool.query(
              `UPDATE \`${schema}\`.river_job SET id=42, state='running' WHERE id=?`,
              [inserted.job.id.toString()]
            );
            await runtime.jobCancel(42n);
            break;
          }
          case "insert":
            await new Client(driver).insert(
              defineJob({ kind: "notify" }),
              {},
              { queue: "priority" }
            );
            break;
          case "metadata_changed":
          case "pause":
          case "resume":
            await runtime.runtimeQueueUpsert(
              "priority",
              Temporal.Now.instant()
            );
            if (name === "metadata_changed")
              await runtime.queueUpdate("priority", {
                metadata: { owner: "candidate" },
              });
            else {
              await runtime.queuePause("priority");
              if (name === "resume") await runtime.queueResume("priority");
            }
            break;
          case "request_resign":
            await runtime.runtimeRequestLeadershipResignation();
            break;
          case "resigned": {
            const leader = (await runtime.maintenanceLeaderAcquire(
              "client-1",
              Temporal.Now.instant(),
              60_000,
              null
            ))!;
            expect(await runtime.maintenanceLeaderResign(leader)).toBe(true);
            break;
          }
          default:
            throw new Error(`Unhandled Go notification: ${name}`);
        }
        const [rows] = await pool.query<RowDataPacket[]>(
          `SELECT topic, CAST(payload AS CHAR) AS payload FROM \`${schema}\`.river_notification ORDER BY id DESC LIMIT 1`
        );
        expect(rows[0]?.topic).toBe(topic);
        expect(parseJson(rows[0]?.payload as string)).toEqual(payload);
      }
    );

    it("receives Go's notifications from the MySQL outbox", async () => {
      const abort = new AbortController();
      const ready = Promise.withResolvers<undefined>();
      const received: { topic: string; payload: JsonObject }[] = [];
      const listening = (async () => {
        for await (const message of runtime.runtimeNotificationSubscribe(
          ["control", "insert", "leadership"],
          abort.signal,
          () => ready.resolve(undefined)
        )) {
          received.push({
            topic: message.topic,
            payload: parseJson(message.payload) as JsonObject,
          });
        }
      })();
      try {
        await ready.promise;
        for (const { topic, payload } of protocol.notifications) {
          await pool.query(
            `INSERT INTO \`${schema}\`.river_notification(topic,payload) VALUES (?,?)`,
            [topic, stringifyJson(payload)]
          );
        }
        await expect
          .poll(() => received)
          .toEqual(
            protocol.notifications.map(({ topic, payload }) => ({
              // The runtime exposes logical topics; the outbox uses Go's prefix.
              topic: topic.replace(/^river_/, ""),
              payload,
            }))
          );
      } finally {
        abort.abort();
        await listening;
      }
    });
  });
});
