import { mkdtempSync, rmSync } from "node:fs";
import { readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { DatabaseSync } from "node:sqlite";
import { fileURLToPath } from "node:url";

import { describe, expect, onTestFinished, test } from "vitest";
import {
  Client,
  DatabaseOperationError,
  defineJob,
  exactJsonNumber,
  isExactJsonNumber,
  ValidationError,
} from "riverqueue";
import type { JobListParams } from "riverqueue/unstable-driver";

import {
  SQLITE_DRIVER_TEST_HOOKS,
  SqliteDriver,
  type SqliteRuntime,
  testSqliteDriver,
  testSqliteMemory,
} from "./driver.js";
import { transaction } from "./scope.js";
import type {
  SqliteDriverOptions,
  SqliteJobRow,
  SqliteRiverScope,
} from "./types.js";

/** River Go's protocol goldens, including its notification payloads. */
const PROTOCOL_GOLDENS = new URL(
  "../../../../conformance/testdata/protocol_values.json",
  import.meta.url
);

/**
 * A notification from River Go's protocol goldens: its topic, its payload
 * struct's JSON fields, and a payload Go sends.
 */
interface NotificationGolden {
  readonly fields: readonly { name: string; omitempty: boolean }[];
  readonly name: string;
  readonly payload: Record<string, unknown>;
  readonly topic: string;
}

/** River's own tests fail any lock window that crosses the event loop. */
const STRICT = {
  [SQLITE_DRIVER_TEST_HOOKS]: { strictLockWindow: true },
} as SqliteDriverOptions;

describe("SqliteDriver surface", () => {
  test("exposes only its construction and connection lifecycle", () => {
    using driver = SqliteDriver.memory();

    expect(Reflect.ownKeys(driver)).toEqual([]);
    expect(new Set(Reflect.ownKeys(SqliteDriver.prototype))).toEqual(
      new Set(["close", "connect", "constructor", Symbol.dispose])
    );
    const handle = driver.connect();
    try {
      expect(new Client(driver)).toBeInstanceOf(Client);
    } finally {
      handle.close();
    }
  });
});

describe("SqliteDriver", () => {
  test("preserves exact IDs and rounds SQLite timestamps to milliseconds", async () => {
    const { database, driver } = await setup();
    const exactID = 9_007_199_254_740_999n;
    const instant = Temporal.Instant.from("2026-08-30T17:20:10.123600000Z");

    const result = await driver.jobInsert({
      args: { nested: { accepted: true } },
      createdAt: instant,
      id: exactID,
      kind: "exact_values",
      scheduledAt: instant,
    });

    expect(result.status).toBe("inserted");
    expect(result.job.id).toBe(exactID);
    expect(typeof result.job.id).toBe("bigint");
    expect(result.job.createdAt.toString()).toBe("2026-08-30T17:20:10.124Z");
    expect((await driver.jobGet(exactID))?.id).toBe(exactID);
    const scheduled = await driver.jobInsert({
      args: {},
      createdAt: Temporal.Instant.from("2026-08-30T17:20:10Z"),
      kind: "scheduled_values",
      scheduledAt: Temporal.Instant.from("2026-08-30T18:20:10Z"),
    });
    expect(scheduled.job.state).toBe("scheduled");
    // Insertion alone notifies nobody; the client notifies producers after.
    expect(notificationRows(database)).toEqual([]);
  });

  test("rounds half milliseconds up like Go's time.Round, before 1970 too", async () => {
    const { driver } = await setup();
    const cases = [
      ["2026-08-30T17:20:10.1235Z", "2026-08-30T17:20:10.124Z"],
      ["1969-12-31T23:59:59.1235Z", "1969-12-31T23:59:59.124Z"],
      ["1969-12-31T23:59:59.9995Z", "1970-01-01T00:00:00Z"],
    ] as const;
    for (const [input, stored] of cases) {
      const instant = Temporal.Instant.from(input);
      const { job } = await driver.jobInsert({
        args: {},
        createdAt: instant,
        kind: "rounding",
        scheduledAt: instant,
      });
      expect(job.createdAt.toString()).toBe(stored);
    }
  });

  test("rejects a client batch repeating an active unique key, like Go", async () => {
    const { database, driver } = await setup();
    const job = defineJob<{ value: string }>()({ kind: "batch_unique" });
    const item = {
      args: { value: "same" },
      job,
      options: { unique: { byArgs: true } },
    } as const;

    await expect(
      new Client(driver).insertMany([
        item,
        { args: { value: "other" }, job },
        item,
      ])
    ).rejects.toThrow(
      new ValidationError("unique key appears more than once in batch")
    );
    expect(
      database.prepare("SELECT count(*) AS count FROM river_job").get()
    ).toEqual({ count: 0 });
  });

  test("stores attempt counts wider than 16 bits, like Go", async () => {
    const { driver } = await setup();
    const job = defineJob()({ kind: "wide_attempts" });

    const [{ job: inserted }] = await new Client(driver).insertMany([
      { args: {}, job, options: { maxAttempts: 40_000 } },
    ]);
    expect(inserted.maxAttempts).toBe(40_000);
    expect((await driver.jobGet(inserted.id))?.maxAttempts).toBe(40_000);

    const { job: full } = await driver.jobInsert({
      args: {},
      attempt: 40_000,
      kind: "wide_attempts",
      maxAttempts: 40_001,
    });
    expect([full.attempt, full.maxAttempts]).toEqual([40_000, 40_001]);
  });

  test("round-trips exact JSON numbers from JavaScript and other engines", async () => {
    const { database, driver } = await setup();
    const inserted = await driver.jobInsert({
      args: { integer: exactJsonNumber("9223372036854775807") },
      kind: "exact_json_javascript",
    });
    expect(isExactJsonNumber(inserted.job.args.integer)).toBe(true);
    expect(JSON.stringify(inserted.job.args)).toBe(
      '{"integer":9223372036854775807}'
    );

    const insertExact = database.prepare(
      `INSERT INTO river_job (args, kind, max_attempts, metadata)
       VALUES (jsonb(?), ?, 25, jsonb(?)) RETURNING id`
    );
    insertExact.setReadBigInts(true);
    const raw = insertExact.get(
      '{"decimal":0.1234567890123456789,"integer":9223372036854775807}',
      "exact_json_external",
      '{"underflow":1e-400}'
    );
    expect(typeof raw?.id).toBe("bigint");
    const job = (await driver.jobGet(raw!.id as bigint))!;

    expect(isExactJsonNumber(job.args.decimal)).toBe(true);
    expect(isExactJsonNumber(job.args.integer)).toBe(true);
    expect(isExactJsonNumber(job.metadata.underflow)).toBe(true);
    expect(JSON.stringify(job.args)).toBe(
      '{"decimal":0.1234567890123456789,"integer":9223372036854775807}'
    );
  });

  test("returns a duplicate for a unique key another job holds", async () => {
    const { database, driver } = await setup();
    const uniqueKey = new Uint8Array(32).fill(7);
    const first = await driver.jobInsert({
      args: { sequence: 1 },
      kind: "unique_job",
      uniqueKey,
      uniqueStates: ["available"],
    });
    const second = await driver.jobInsert({
      args: { sequence: 2 },
      kind: "unique_job",
      uniqueKey,
      uniqueStates: ["available"],
    });

    expect(first.status).toBe("inserted");
    expect(second.status).toBe("duplicate");
    expect(second.job.id).toBe(first.job.id);
    expect(second.job.args).toEqual({ sequence: 1 });
    expect(first.job.metadata["river:unique_nonce"]).toMatch(/^[0-9a-f]{16}$/);
    expect(second.job.metadata["river:unique_nonce"]).toBe(
      first.job.metadata["river:unique_nonce"]
    );
    expect(
      (await driver.jobGet(first.job.id))?.metadata["river:unique_nonce"]
    ).toBe(first.job.metadata["river:unique_nonce"]);
    expect(
      resultRow(
        database,
        `SELECT json_extract(metadata, '$."river:unique_nonce"') AS nonce
         FROM river_job`
      )?.nonce
    ).toBe(first.job.metadata["river:unique_nonce"]);
  });

  test("keeps the existing job's kind on a unique skip of another kind", async () => {
    const { database, driver } = await setup();
    const jobA = defineJob<{ value: string }>()({ kind: "unique_kind_a" });
    const jobB = defineJob<{ value: string }>()({ kind: "unique_kind_b" });
    const client = new Client(driver);
    const options = { unique: { byArgs: true, excludeKind: true } } as const;

    const first = await client.insert(jobA, { value: "same" }, options);
    const single = await client.insert(jobB, { value: "same" }, options);
    const [batched] = await client.insertMany([
      { args: { value: "same" }, job: jobB, options },
    ]);

    expect(first.status).toBe("inserted");
    for (const result of [single, batched]) {
      expect(result.status).toBe("duplicate");
      expect(result.job.id).toBe(first.job.id);
      expect(result.job.kind).toBe(jobA.kind);
    }
    expect(database.prepare("SELECT kind FROM river_job").all()).toEqual([
      { kind: jobA.kind },
    ]);
  });

  test("writes insert notifications only for the queues it's given", async () => {
    const { database, driver } = await setup();
    const insertPayloads = () =>
      notificationRows(database)
        .filter(({ topic }) => topic === "river_insert")
        .map(({ payload }) => payload);

    await driver.jobInsertMany([
      { args: {}, kind: "notification_volume", queue: "alpha" },
      { args: {}, kind: "notification_volume", queue: "beta" },
    ]);
    await driver.notifyInsert([]);
    expect(insertPayloads()).toEqual([]);

    await driver.notifyInsert(["alpha", "beta"]);
    expect(insertPayloads()).toEqual([
      '{"queue": "alpha"}',
      '{"queue": "beta"}',
    ]);

    // Notifications written in a transaction roll back with it.
    const rollback = new Error("roll back");
    await expect(
      driver.operationScope(undefined, async (tx) => {
        await driver.notifyInsert(["gamma"], { tx });
        throw rollback;
      })
    ).rejects.toBe(rollback);
    expect(insertPayloads()).toHaveLength(2);
  });

  test("notifies only the scheduled queues the client's limiter allows", async () => {
    const { database, driver } = await setup();
    const now = Temporal.Now.instant();
    const leader = (await driver.maintenanceLeaderAcquire(
      "leader",
      now,
      60_000,
      null
    ))!;
    await driver.jobInsertMany(
      ["alpha", "beta", "alpha", "gamma"].map((queue) => ({
        args: {},
        kind: "scheduled_notification",
        queue,
        scheduledAt: now,
        state: "scheduled" as const,
      }))
    );
    const offered: (readonly string[])[] = [];

    expect(
      await driver.maintenanceSchedule(leader, {
        allowInsertNotifications: (queues) => {
          offered.push([...queues].sort());
          return ["alpha"];
        },
        limit: 10,
        notificationHorizon: now,
        now,
        scheduledAtHorizon: now,
      })
    ).toBe(4);

    expect(offered).toEqual([["alpha", "alpha", "beta", "gamma"]]);
    expect(
      notificationRows(database)
        .filter(({ topic }) => topic === "river_insert")
        .map(({ payload }) => payload)
    ).toEqual(['{"queue": "alpha"}']);
  });

  test("cancels, retries, and protects running jobs from deletion", async () => {
    const { database, driver } = await setup();
    const now = Temporal.Instant.from("2026-08-30T17:30:00.111Z");
    const inserted = (await driver.jobInsert({ args: {}, kind: "control_job" }))
      .job;

    const cancelled = await driver.jobCancelDetailed(inserted.id, { now });
    expect(cancelled.status).toBe("cancelled");
    if (cancelled.status === "cancelled") {
      expect(cancelled.job.state).toBe("cancelled");
      expect(cancelled.job.finalizedAt?.toString()).toBe(
        "2026-08-30T17:30:00.111Z"
      );
      expect(cancelled.job.metadata.cancel_attempted_at).toBe(now.toString());
    }
    expect((await driver.jobCancelDetailed(inserted.id, { now })).status).toBe(
      "unchanged"
    );

    const retried = await driver.jobRetryDetailed(inserted.id, { now });
    expect(retried.status).toBe("retried");
    if (retried.status === "retried")
      expect(retried.job.state).toBe("available");
    expect((await driver.jobRetryDetailed(inserted.id, { now })).status).toBe(
      "unchanged"
    );

    database
      .prepare(
        "UPDATE river_job SET state = 'running', attempt = 1 WHERE id = ?"
      )
      .run(inserted.id);
    expect(await driver.jobDelete(inserted.id)).toMatchObject({
      job: { id: inserted.id },
      status: "running",
    });

    const control = notificationRows(database);
    // Like River for Go, a retry publishes no insert notification.
    expect(control.map(({ topic }) => topic)).toEqual(["river_control"]);
    expect(control[0]?.payload).toBe(
      `{"action":"cancel","job_id":${inserted.id},"queue":"default"}`
    );
  });

  test("notifies a running job's cancellation in the cancelling transaction, like Go", async () => {
    const { database, driver } = await setup();
    const inserted = (await driver.jobInsert({ args: {}, kind: "control_job" }))
      .job;
    database
      .prepare(
        "UPDATE river_job SET state = 'running', attempt = 1 WHERE id = ?"
      )
      .run(inserted.id);
    const cancels = () =>
      notificationRows(database).filter(
        ({ topic }) => topic === "river_control"
      );

    // An application transaction that rolls back leaves no notification.
    await expect(
      transaction(database, async (tx) => {
        await driver.jobCancel(inserted.id, { tx });
        expect(
          tx
            .prepare(
              "SELECT count(*) AS n FROM river_notification WHERE topic = 'river_control'"
            )
            .get()?.n
        ).toBe(1);
        throw new Error("roll back");
      })
    ).rejects.toThrow("roll back");
    expect(cancels()).toEqual([]);

    await transaction(database, async (tx) => {
      await driver.jobCancel(inserted.id, { tx });
    });
    expect(cancels()).toEqual([
      {
        payload: `{"action":"cancel","job_id":${inserted.id},"queue":"default"}`,
        topic: "river_control",
      },
    ]);
  });

  test("manages queues and writes control changes to the durable outbox", async () => {
    const { database, driver } = await setup();
    const first = Temporal.Instant.from("2026-08-30T18:00:00.001Z");
    await driver.queueUpsert("beta", { metadata: { order: 2 }, now: first });
    await driver.queueUpsert("alpha", { metadata: { order: 1 }, now: first });

    expect(
      (await driver.queueList({ limit: 10_000, nameAfter: null })).map(
        ({ name }) => name
      )
    ).toEqual(["alpha", "beta"]);
    expect(await driver.queueGet("missing")).toBeNull();
    await driver.queuePause("*");
    expect(
      (await driver.queueList({ limit: 10_000, nameAfter: null })).every(
        ({ pausedAt }) => pausedAt !== null
      )
    ).toBe(true);
    expect(await driver.queuePause("alpha")).toBeNull();

    const resumed = await driver.queueResume("alpha");
    expect(resumed?.pausedAt).toBeNull();
    expect(await driver.queueResume("alpha")).toBeNull();
    const updated = await driver.queueUpdate("beta", {
      metadata: { a: 1, nested: { enabled: true } },
    });
    expect(updated?.metadata).toEqual({ a: 1, nested: { enabled: true } });
    const touched = await driver.queueUpdate("beta", {});
    expect(touched?.metadata).toEqual({ a: 1, nested: { enabled: true } });
    expect(await driver.queueUpdate("missing", {})).toBeNull();

    expect(notificationRows(database).map(({ payload }) => payload)).toEqual([
      '{"action":"pause","queue":"*"}',
      '{"action":"resume","queue":"alpha"}',
      '{"action":"metadata_changed","metadata":{"a":1,"nested":{"enabled":true}},"queue":"beta"}',
    ]);
  });

  test("finds queue names outside the grammar absent instead of rejecting them", async () => {
    const { driver } = await setup();

    for (const name of ["Not A Queue!", "", "x".repeat(200)]) {
      await expect(driver.queueGet(name)).resolves.toBeNull();
      await expect(driver.queuePause(name)).resolves.toBeNull();
      await expect(driver.queueResume(name)).resolves.toBeNull();
      await expect(
        driver.queueUpdate(name, { metadata: {} })
      ).resolves.toBeNull();
    }
  });

  test("commits application SQL and River writes in an application transaction", async () => {
    const { database, driver } = await setup();
    database.exec("CREATE TABLE application_row (id text PRIMARY KEY)");

    const id = await transaction(database, async (tx) => {
      tx.prepare("INSERT INTO application_row (id) VALUES (?)").run(
        "application-1"
      );
      const inserted = await driver.jobInsert(
        { args: { applicationId: "application-1" }, kind: "transactional" },
        { tx }
      );
      await Promise.resolve();
      return inserted.job.id;
    });

    expect((await driver.jobGet(id))?.args).toEqual({
      applicationId: "application-1",
    });
    expect(resultRow(database, "SELECT id FROM application_row")?.id).toBe(
      "application-1"
    );
  });

  test("rolls back thrown and rejected application transactions", async () => {
    const { database, driver } = await setup();
    database.exec("CREATE TABLE application_row (id text PRIMARY KEY)");
    const syncFailure = new Error("sync failure");
    await expect(
      transaction(database, (tx) => {
        tx.prepare("INSERT INTO application_row (id) VALUES (?)").run(
          "rollback-sync"
        );
        throw syncFailure;
      })
    ).rejects.toBe(syncFailure);
    expect(
      resultRow(database, "SELECT count(*) AS count FROM application_row")
        ?.count
    ).toBe(0n);

    const asyncFailure = new Error("async failure");
    await expect(
      transaction(database, async (tx) => {
        await driver.jobInsert(
          { args: {}, id: 102n, kind: "rollback_async" },
          { tx }
        );
        await Promise.resolve();
        throw asyncFailure;
      })
    ).rejects.toBe(asyncFailure);
    expect(await driver.jobGet(102n)).toBeNull();
    expect(notificationRows(database)).toEqual([]);
  });

  test("validates an ordinary batch before writing any of it in a caller transaction", async () => {
    const { database, driver } = await setup();

    await transaction(database, async (tx) => {
      await driver.jobInsert(
        { args: {}, id: 201n, kind: "before_batch" },
        { tx }
      );
      await expect(
        driver.jobInsertMany(
          [
            { args: {}, id: 202n, kind: "batch_first" },
            { args: {}, id: 203n, kind: "batch_invalid", priority: 99 },
          ],
          { tx }
        )
      ).rejects.toMatchObject({ code: "validation" });
      await driver.jobInsert(
        { args: {}, id: 204n, kind: "after_batch" },
        { tx }
      );
    });

    expect((await driver.jobGet(201n))?.kind).toBe("before_batch");
    expect(await driver.jobGet(202n)).toBeNull();
    expect(await driver.jobGet(203n)).toBeNull();
    expect((await driver.jobGet(204n))?.kind).toBe("after_batch");
  });

  test("accepts only open transactions on its own database as { tx }", async () => {
    const { database, driver } = await setup();
    const other = testSqliteMemory(STRICT);
    onTestFinished(() => other.close());
    const foreign = other.connect();
    onTestFinished(() => foreign.close());

    await expect(driver.jobGet(1n, { tx: database })).rejects.toMatchObject({
      code: "transaction_scope",
      reason: "no_transaction",
    });
    await transaction(foreign, async (tx) => {
      await expect(driver.jobGet(1n, { tx })).rejects.toMatchObject({
        code: "backend_mismatch",
      });
    });
    await expect(
      driver.jobGet(1n, { tx: {} as DatabaseSync })
    ).rejects.toMatchObject({ code: "backend_mismatch" });

    let expired!: DatabaseSync | SqliteRiverScope;
    await driver.operationScope(undefined, async (tx) => {
      expired = tx;
      if (!(tx instanceof DatabaseSync)) {
        // River's own transaction is opaque, not a connection.
        // @ts-expect-error -- SqliteRiverScope has no prepare.
        expect(() => tx.prepare("SELECT 1")).toThrow(TypeError);
      }
      await expect(other.jobGet(1n, { tx })).rejects.toMatchObject({
        code: "backend_mismatch",
      });
      await expect(driver.jobGet(1n, { tx })).resolves.toBeNull();
    });
    await expect(driver.jobGet(1n, { tx: expired })).rejects.toMatchObject({
      code: "backend_mismatch",
    });

    // River's private connection is never a valid { tx }.
    const connection = await driver.execute("leak", {}, (db) => db);
    connection.exec("BEGIN");
    await expect(driver.jobGet(1n, { tx: connection })).rejects.toMatchObject({
      code: "backend_mismatch",
      message: expect.stringContaining("private"),
    });
    connection.exec("ROLLBACK");

    const connected = driver.connect();
    onTestFinished(() => connected.close());
    await transaction(connected, async (tx) => {
      await expect(driver.jobGet(1n, { tx })).resolves.toBeNull();
    });
  });

  test("queues River operations behind its own open transaction", async () => {
    const { driver } = await setup();
    const order: string[] = [];

    const scope = driver.operationScope(undefined, async (tx) => {
      await driver.jobInsert({ args: {}, id: 301n, kind: "first" }, { tx });
      order.push("first:inserted");
      for (let index = 0; index < 10; index++) await Promise.resolve();
      order.push("first:end");
    });
    const outside = driver.jobGet(301n).then((job) => {
      order.push(`outside:${job?.kind ?? "missing"}`);
    });
    await Promise.all([scope, outside]);

    expect(order).toEqual(["first:inserted", "first:end", "outside:first"]);
  });

  test("closes only the connections it opened", async () => {
    const directory = mkdtempSync(join(tmpdir(), "river-sqlite-close-"));
    onTestFinished(() => {
      rmSync(directory, { force: true, recursive: true });
    });
    const database = new DatabaseSync(join(directory, "river.db"));
    onTestFinished(() => database.close());
    await migrate(database);
    const fileDriver = testSqliteDriver(database, STRICT);
    await fileDriver.jobInsert({ args: {}, kind: "ownership" });
    fileDriver.close();
    fileDriver.close();
    expect(database.isOpen).toBe(true);
    expect(
      resultRow(database, "SELECT count(*) AS count FROM river_job")?.count
    ).toBe(1n);
    await expect(fileDriver.jobGet(1n)).rejects.toMatchObject({
      code: "lifecycle",
    });
    expect(() => fileDriver.connect()).toThrow(/closed/);

    const memory = testSqliteMemory(STRICT);
    const connected = memory.connect();
    memory.close();
    expect(memory.database.isOpen).toBe(false);
    expect(connected.isOpen).toBe(true);
    connected.close();
  });

  test("requires a database file River can open its own connection to", () => {
    const database = new DatabaseSync(":memory:");

    expect(() => testSqliteDriver(database)).toThrow(/SqliteDriver.memory/);
    database.close();
    expect(() => testSqliteDriver(database)).toThrow(/open/);
  });

  test("rejects lossy JSON integers before writing", async () => {
    const { database, driver } = await setup();
    await expect(
      driver.jobInsert({
        args: { unsafe: Number.MAX_SAFE_INTEGER + 1 },
        kind: "unsafe_json",
      })
    ).rejects.toThrow(expect.objectContaining({ code: "validation" }));
    expect(
      resultRow(database, "SELECT count(*) AS count FROM river_job")?.count
    ).toBe(0n);
  });

  test("surfaces unknown persisted states as structured row errors", async () => {
    const { database, driver } = await setup();
    const inserted = (await driver.jobInsert({ args: {}, kind: "bad_state" }))
      .job;
    database.exec("PRAGMA ignore_check_constraints = ON");
    database
      .prepare("UPDATE river_job SET state = 'future_state' WHERE id = ?")
      .run(inserted.id);

    await expect(driver.jobGet(inserted.id)).rejects.toThrow(
      DatabaseOperationError
    );
    await expect(driver.jobGet(inserted.id)).rejects.toThrow(
      expect.objectContaining({
        code: "database",
        details: expect.objectContaining({ reason: "invalid_row" }),
      })
    );
  });

  test("acts on rows River can't fully read by ID and lists them like Go", async () => {
    const { database, driver } = await setup();
    const uniqueKey = new Uint8Array(32).fill(11);
    const insert = async (kind: string, extra: object = {}) =>
      (await driver.jobInsert({ args: {}, kind, ...extra })).job;
    const cancelled = await insert("poison_cancel");
    const retried = await insert("poison_retry", {
      finalizedAt: Temporal.Instant.from("2026-01-01T00:00:00Z"),
      state: "discarded",
    });
    const deleted = await insert("poison_delete");
    const duplicate = await insert("poison_unique", {
      uniqueKey,
      uniqueStates: ["available"],
    });
    // Another engine stored array arguments, which Go keeps as raw bytes.
    database.exec(
      "UPDATE river_job SET args = jsonb('[1, 2]') WHERE kind LIKE 'poison_%'"
    );

    await expect(driver.jobGet(cancelled.id)).rejects.toThrow(
      expect.objectContaining({
        details: expect.objectContaining({ reason: "invalid_row" }),
      })
    );
    const listed = await driver.jobList(
      jobListParams({ kinds: ["poison_cancel", "poison_delete"] })
    );
    expect(listed.map(({ args, id }) => [id, args])).toEqual([
      [cancelled.id, {}],
      [deleted.id, {}],
    ]);
    expect((await driver.jobCancel(cancelled.id))?.state).toBe("cancelled");
    expect((await driver.jobRetry(retried.id))?.state).toBe("available");
    expect(await driver.jobDelete(deleted.id)).toMatchObject({
      job: { id: deleted.id },
      status: "deleted",
    });
    const single = await driver.jobInsert({
      args: {},
      kind: "poison_unique",
      uniqueKey,
      uniqueStates: ["available"],
    });
    expect([single.job.id, single.status]).toEqual([duplicate.id, "duplicate"]);
  });

  test("lists and updates jobs with stable keyset filters", async () => {
    const { driver } = await setup();
    const firstAt = Temporal.Instant.from("2026-08-30T18:10:00.001Z");
    const secondAt = Temporal.Instant.from("2026-08-30T18:10:00.002Z");
    const first = (
      await driver.jobInsert({
        args: {},
        id: 9_007_199_254_740_993n,
        kind: "list_job",
        metadata: { tenant: "one" },
        scheduledAt: firstAt,
        tags: ["all", "first"],
      })
    ).job;
    const second = (
      await driver.jobInsert({
        args: {},
        id: 9_007_199_254_740_995n,
        kind: "list_job",
        metadata: { tenant: "one" },
        priority: 2,
        scheduledAt: secondAt,
        tags: ["all", "second"],
      })
    ).job;
    await driver.jobInsert({ args: {}, kind: "other_job" });

    const firstPage = await driver.jobList(
      jobListParams({
        kinds: ["list_job"],
        limit: 1,
        sortField: "scheduledAt",
        tagsAll: ["all"],
      })
    );
    expect(firstPage.map(({ id }) => id)).toEqual([first.id]);
    const secondPage = await driver.jobList(
      jobListParams({
        after: {
          id: first.id,
          kind: first.kind,
          queue: first.queue,
          sortField: "scheduledAt",
          time: first.scheduledAt,
        },
        kinds: ["list_job"],
        sortField: "scheduledAt",
        tagsAny: ["second"],
      })
    );
    expect(secondPage.map(({ id }) => id)).toEqual([second.id]);
    // Like Go, a cursor without a time resumes after its ID alone, even
    // when the list is ordered by a time.
    const timelessPage = await driver.jobList(
      jobListParams({
        after: {
          id: second.id,
          kind: second.kind,
          queue: second.queue,
          sortField: "scheduledAt",
          time: null,
        },
        kinds: ["list_job"],
        sortDirection: "desc",
        sortField: "scheduledAt",
      })
    );
    expect(timelessPage.map(({ id }) => id)).toEqual([first.id]);

    const updated = await driver.jobUpdate(second.id, {
      metadata: { changed: true },
      output: { delivered: true },
    });
    expect(updated?.metadata).toEqual({
      changed: true,
      output: { delivered: true },
      "river:unique_nonce": expect.stringMatching(/^[0-9a-f]{16}$/),
      tenant: "one",
    });
    expect(
      (
        await driver.jobList(
          jobListParams({ metadata: { output: { delivered: true } } })
        )
      ).map(({ id }) => id)
    ).toEqual([second.id]);
  });

  test("matches Go time ordering by using the first requested state", async () => {
    const { database, driver } = await setup();
    const completed = (await driver.jobInsert({ args: {}, kind: "time_order" }))
      .job;
    const scheduled = (await driver.jobInsert({ args: {}, kind: "time_order" }))
      .job;
    const running = (await driver.jobInsert({ args: {}, kind: "time_order" }))
      .job;
    database
      .prepare(
        `UPDATE river_job
         SET state = 'completed', finalized_at = ?, scheduled_at = ?
         WHERE id = ?`
      )
      .run(
        "2026-01-01 00:00:01.000000000+00:00",
        "2026-01-01 00:00:03.000000000+00:00",
        completed.id
      );
    database
      .prepare(
        `UPDATE river_job SET state = 'scheduled', scheduled_at = ? WHERE id = ?`
      )
      .run("2026-01-01 00:00:02.000000000+00:00", scheduled.id);
    database
      .prepare(
        `UPDATE river_job
         SET state = 'running', attempted_at = ?, scheduled_at = ?
         WHERE id = ?`
      )
      .run(
        "2026-01-01 00:00:03.000000000+00:00",
        "2026-01-01 00:00:01.000000000+00:00",
        running.id
      );

    expect(
      (
        await driver.jobList(
          jobListParams({
            kinds: ["time_order"],
            sortField: "time",
            states: ["available", "completed", "running", "scheduled"],
          })
        )
      ).map(({ id }) => id)
    ).toEqual([running.id, scheduled.id, completed.id]);
  });

  test("deletes bounded filtered non-running jobs in ID order", async () => {
    const { driver } = await setup();
    const first = (await driver.jobInsert({ args: {}, kind: "delete_many" }))
      .job;
    const second = (await driver.jobInsert({ args: {}, kind: "delete_many" }))
      .job;
    const last = (await driver.jobInsert({ args: {}, kind: "delete_many" }))
      .job;
    const [running] = (
      await driver.jobClaim({
        attemptedBy: "delete-worker",
        kinds: ["delete_many"],
        queues: [{ limit: 1, name: "default" }],
      })
    ).jobs;
    expect(running).toBeDefined();

    expect(
      (
        await driver.jobDeleteMany({
          all: false,
          ids: [],
          kinds: ["delete_many"],
          limit: 10,
          priorities: [],
          queues: [],
          states: [],
        })
      ).map(({ id }) => id)
    ).toEqual(
      [first, second, last]
        .filter(({ id }) => id !== running!.id)
        .map(({ id }) => id)
    );
    expect((await driver.jobGet(running!.id))?.state).toBe("running");
    await expect(
      driver.jobDeleteMany({
        all: false,
        ids: [],
        kinds: [],
        limit: 10,
        priorities: [],
        queues: [],
        states: [],
      })
    ).rejects.toThrow("requires a filter");
  });

  test("claims due jobs atomically in priority order and honors queue pause", async () => {
    const { database, driver } = await setup();
    const now = Temporal.Now.instant().subtract({ seconds: 1 });
    const low = (
      await driver.jobInsert({
        args: {},
        attemptedBy: ["old-1", "old-2"],
        kind: "claim_job",
        priority: 2,
        scheduledAt: now,
      })
    ).job;
    const high = (
      await driver.jobInsert({
        args: {},
        kind: "claim_job",
        priority: 1,
        scheduledAt: now,
      })
    ).job;
    await driver.jobInsert({
      args: {},
      kind: "claim_job",
      scheduledAt: now.add({ hours: 1 }),
    });

    const claimed = (
      await driver.jobClaim({
        attemptedBy: "sqlite-worker",
        kinds: ["claim_job"],
        queues: [{ limit: 2, name: "default" }],
      })
    ).jobs;
    expect(claimed.map(({ id }) => id)).toEqual([high.id, low.id]);
    expect(
      claimed.every(
        ({ attempt, state }) => attempt === 1 && state === "running"
      )
    ).toBe(true);
    expect((await driver.jobGet(low.id))?.attemptedBy).toEqual([
      "old-1",
      "old-2",
      "sqlite-worker",
    ]);

    const attemptedBy = Array.from(
      { length: 101 },
      (_, index) => `worker-${index.toString().padStart(3, "0")}`
    );
    const longHistory = (
      await driver.jobInsert({
        args: {},
        attemptedBy,
        kind: "claim_history",
        scheduledAt: now,
      })
    ).job;
    await driver.jobClaim({
      attemptedBy: "worker-101",
      kinds: ["claim_history"],
      queues: [{ limit: 1, name: "default" }],
    });
    expect((await driver.jobGet(longHistory.id))?.attemptedBy).toEqual([
      ...attemptedBy.slice(2),
      "worker-101",
    ]);
    expect(
      await driver.jobClaim({
        attemptedBy: "other",
        kinds: ["claim_job"],
        queues: [{ limit: 2, name: "default" }],
      })
    ).toEqual({ jobs: [] });

    await driver.queueUpsert("paused", { now });
    await driver.queuePause("paused");
    const paused = (
      await driver.jobInsert({
        args: {},
        kind: "paused_claim",
        queue: "paused",
        scheduledAt: now,
      })
    ).job;
    expect(
      await driver.jobClaim({
        attemptedBy: "worker",
        kinds: [],
        queues: [{ limit: 1, name: "paused" }],
      })
    ).toEqual({ jobs: [] });
    expect((await driver.jobGet(paused.id))?.state).toBe("available");
    expect(
      resultRow(
        database,
        "SELECT count(*) AS count FROM river_job WHERE state = 'running'"
      )?.count
    ).toBe(3n);
  });

  test("pages sparse metadata lists without blocking the event loop", async () => {
    const { database, driver } = await setup();
    database.exec(`
      WITH RECURSIVE n(x) AS (SELECT 1 UNION ALL SELECT x + 1 FROM n WHERE x < 2500)
      INSERT INTO river_job (kind, queue, state, metadata, scheduled_at)
      SELECT 'sparse_list', 'default', 'available', jsonb(json_object('n', x)),
        '2026-01-01 00:00:00'
      FROM n;
    `);
    const params = jobListParams({ limit: 5, metadata: { n: 2400 } });
    let yielded = false;
    setImmediate(() => {
      yielded = true;
    });

    const listed = await driver.jobList(params);

    expect(yielded).toBe(true);
    expect(listed.map(({ metadata }) => metadata.n)).toEqual([2400]);
    await transaction(database, async (tx) => {
      const inTransaction = await driver.jobList(params, { tx });
      expect(inTransaction.map(({ metadata }) => metadata.n)).toEqual([2400]);
    });
  });

  test("guards completion batches by attempt and applies cancellation races", async () => {
    const { database, driver } = await setup();
    const now = Temporal.Instant.from("2026-08-30T18:30:00Z");
    const jobs = [
      (
        await driver.jobInsert({
          args: {},
          kind: "complete_job",
          scheduledAt: now,
        })
      ).job,
      (
        await driver.jobInsert({
          args: {},
          kind: "complete_job",
          scheduledAt: now,
        })
      ).job,
    ];
    const claimed = (
      await driver.jobClaim({
        attemptedBy: "completion-worker",
        kinds: ["complete_job"],
        queues: [{ limit: 2, name: "default" }],
      })
    ).jobs;
    expect(claimed).toHaveLength(2);
    expect((await driver.jobCancelDetailed(jobs[1]!.id, { now })).status).toBe(
      "cancelled"
    );

    const wrongOwner = await driver.jobCompleteMany([
      {
        attempt: 1,
        attemptedBy: "different-worker",
        error: null,
        id: jobs[0]!.id,
        kind: "complete",
        finalizedAt: Temporal.Now.instant(),
        output: null,
        outputSet: false,
        scheduledAt: null,
      },
    ]);
    expect(wrongOwner[0]).toMatchObject({
      job: { state: "running" },
      status: "stale",
    });

    const completed = await driver.jobCompleteMany([
      {
        attempt: 1,
        attemptedBy: "completion-worker",
        error: null,
        id: jobs[0]!.id,
        kind: "complete",
        finalizedAt: Temporal.Now.instant(),
        output: { ok: true },
        outputSet: true,
        scheduledAt: null,
      },
      {
        attempt: 1,
        attemptedBy: "completion-worker",
        error: null,
        id: jobs[1]!.id,
        kind: "retry",
        finalizedAt: null,
        output: null,
        outputSet: false,
        scheduledAt: now,
      },
    ]);
    expect(completed.map(({ status }) => status)).toEqual([
      "applied",
      "applied",
    ]);
    expect((await driver.jobGet(jobs[0]!.id))?.state).toBe("completed");
    expect((await driver.jobGet(jobs[1]!.id))?.state).toBe("cancelled");

    const interrupted = (
      await driver.jobInsert({
        args: {},
        kind: "interrupt_job",
        scheduledAt: now,
      })
    ).job;
    await driver.jobClaim({
      attemptedBy: "interrupt-worker",
      kinds: ["interrupt_job"],
      queues: [{ limit: 1, name: "default" }],
    });
    expect(
      (
        await driver.jobCompleteMany([
          {
            attempt: 1,
            attemptedBy: "interrupt-worker",
            error: null,
            id: interrupted.id,
            kind: "interrupt",
            finalizedAt: null,
            output: null,
            outputSet: false,
            scheduledAt: now,
          },
        ])
      )[0]
    ).toMatchObject({
      job: { attempt: 0, errors: [], state: "available" },
      status: "applied",
    });

    const stale = await driver.jobCompleteMany([
      {
        attempt: 1,
        attemptedBy: "completion-worker",
        error: null,
        id: jobs[0]!.id,
        kind: "discard",
        finalizedAt: Temporal.Now.instant(),
        output: null,
        outputSet: false,
        scheduledAt: null,
      },
    ]);
    expect(stale[0]).toMatchObject({
      job: { id: jobs[0]!.id, state: "completed" },
      key: `${jobs[0]!.id}:1:completion-worker`,
      status: "stale",
    });

    const metadataMerged = await driver.jobCompleteMany([
      {
        attempt: 1,
        attemptedBy: "completion-worker",
        error: null,
        id: jobs[0]!.id,
        kind: "complete",
        finalizedAt: Temporal.Now.instant(),
        metadata: { checkpoint: "same-attempt" },
        output: { delivered: true },
        outputSet: true,
        scheduledAt: null,
      },
    ]);
    expect(metadataMerged[0]).toMatchObject({
      job: {
        metadata: {
          checkpoint: "same-attempt",
          output: { delivered: true },
        },
        state: "completed",
      },
      status: "stale",
    });

    database
      .prepare(
        "UPDATE river_job SET attempt = 2, attempted_by = jsonb('[\"new-owner\"]'), finalized_at = NULL, state = 'running' WHERE id = ?"
      )
      .run(jobs[0]!.id);
    const newerAttempt = await driver.jobCompleteMany([
      {
        attempt: 1,
        attemptedBy: "completion-worker",
        error: null,
        id: jobs[0]!.id,
        kind: "complete",
        finalizedAt: Temporal.Now.instant(),
        metadata: { must_not_merge: true },
        output: null,
        outputSet: false,
        scheduledAt: null,
      },
    ]);
    expect(newerAttempt[0]).toMatchObject({
      job: { attempt: 2, state: "running" },
      status: "stale",
    });
    expect(
      (await driver.jobGet(jobs[0]!.id))?.metadata.must_not_merge
    ).toBeUndefined();

    const running = (
      await driver.jobInsert({
        args: {},
        kind: "batch_rollback",
        scheduledAt: now,
      })
    ).job;
    await driver.jobClaim({
      attemptedBy: "worker",
      kinds: ["batch_rollback"],
      queues: [{ limit: 1, name: "default" }],
    });
    await expect(
      driver.jobCompleteMany([
        {
          attempt: 1,
          attemptedBy: "worker",
          error: null,
          id: running.id,
          kind: "complete",
          finalizedAt: Temporal.Now.instant(),
          output: null,
          outputSet: false,
          scheduledAt: null,
        },
        {
          attempt: 0,
          attemptedBy: "worker",
          error: null,
          id: running.id,
          kind: "complete",
          finalizedAt: Temporal.Now.instant(),
          output: null,
          outputSet: false,
          scheduledAt: null,
        },
      ])
    ).rejects.toThrow(expect.objectContaining({ code: "validation" }));
    expect((await driver.jobGet(running.id))?.state).toBe("running");
  });

  test("persists captured completion times for every outcome kind", async () => {
    const { driver } = await setup();
    const finish = Temporal.Instant.from("2026-08-30T18:31:00.123600000Z");
    const scheduledAt = Temporal.Instant.from("2026-08-30T19:00:00Z");
    const kinds = [
      "cancel",
      "complete",
      "discard",
      "interrupt",
      "retry",
      "snooze",
    ] as const;
    const jobs: SqliteJobRow[] = [];
    for (const kind of kinds) {
      jobs.push(
        (await driver.jobInsert({ args: {}, kind: `completion_${kind}` })).job
      );
    }
    await driver.jobClaim({
      attemptedBy: "completion-timing-worker",
      kinds: kinds.map((kind) => `completion_${kind}`),
      queues: [{ limit: kinds.length, name: "default" }],
    });

    const results = await driver.jobCompleteMany(
      jobs.map((job, index) => {
        const kind = kinds[index]!;
        const terminal =
          kind === "cancel" || kind === "complete" || kind === "discard";
        return {
          attempt: 1,
          attemptedBy: "completion-timing-worker",
          error: null,
          finalizedAt: terminal ? finish : null,
          id: job.id,
          kind,
          output: null,
          outputSet: false,
          scheduledAt: terminal ? null : scheduledAt,
        };
      })
    );

    expect(results.map(({ job }) => job?.state)).toEqual([
      "cancelled",
      "completed",
      "discarded",
      "available",
      "retryable",
      "scheduled",
    ]);
    expect(
      results.map(({ job }) => job?.finalizedAt?.toString() ?? null)
    ).toEqual([
      "2026-08-30T18:31:00.124Z",
      "2026-08-30T18:31:00.124Z",
      "2026-08-30T18:31:00.124Z",
      null,
      null,
      null,
    ]);
  });

  test("polls the durable outbox and publishes cancellation IDs exactly", async () => {
    const { driver } = await setup();
    const exactID = 9_007_199_254_740_999n;
    await driver.jobInsert({ args: {}, id: exactID, kind: "cancel_stream" });
    const controller = new AbortController();
    const ready = Promise.withResolvers<undefined>();
    const iterator = driver
      .runtimeNotificationSubscribe(["control"], controller.signal, () => {
        ready.resolve(undefined);
      })
      [Symbol.asyncIterator]();
    const next = iterator.next();
    await ready.promise;
    await driver.jobCancel(exactID);
    const notification = await next;
    expect(notification.done).toBe(false);
    expect(notification.value?.topic).toBe("control");
    expect(notification.value?.payload).toContain(
      `"job_id":${exactID.toString(10)}`
    );
    controller.abort();
    await expect(iterator.next()).resolves.toEqual({
      done: true,
      value: undefined,
    });
  });

  test("publishes leadership resignation requests through the durable outbox", async () => {
    const { database, driver } = await setup();

    await driver.runtimeRequestLeadershipResignation();

    expect(notificationRows(database)).toEqual([
      {
        payload: '{"action":"request_resign","leader_id":""}',
        topic: "river_leadership",
      },
    ]);

    const rollback = new Error("roll back resignation");
    await expect(
      transaction(database, async (tx) => {
        await driver.runtimeRequestLeadershipResignation({ tx });
        throw rollback;
      })
    ).rejects.toBe(rollback);
    expect(notificationRows(database)).toHaveLength(1);

    await transaction(database, async (tx) => {
      await driver.runtimeRequestLeadershipResignation({ tx });
    });
    expect(notificationRows(database)).toHaveLength(2);
  });

  test("writes River Go's notification topics and payload fields", async () => {
    const golden = JSON.parse(await readFixture(PROTOCOL_GOLDENS)) as {
      readonly notifications: readonly NotificationGolden[];
    };
    expect(golden.notifications.length).toBeGreaterThan(0);

    for (const notification of golden.notifications) {
      const { database, driver } = await setup();
      const queue = notification.payload.queue as string;
      switch (notification.name) {
        case "cancel":
          await driver.jobInsert({
            args: {},
            id: BigInt(notification.payload.job_id as number),
            kind: "notification_golden",
            queue,
          });
          await driver.jobCancel(BigInt(notification.payload.job_id as number));
          break;
        case "insert":
          await driver.notifyInsert([queue]);
          break;
        case "metadata_changed":
          await driver.queueUpsert(queue);
          await driver.queueUpdate(queue, {
            metadata: notification.payload.metadata as Record<string, string>,
          });
          break;
        case "pause":
          await driver.queueUpsert(queue);
          await driver.queuePause(queue);
          break;
        case "request_resign":
          await driver.runtimeRequestLeadershipResignation();
          break;
        case "resigned": {
          // Like River for Go's SQLite driver, other clients learn of a
          // resignation at their next election attempt, without a
          // notification.
          const leader = (await driver.maintenanceLeaderAcquire(
            notification.payload.leader_id as string,
            Temporal.Now.instant(),
            60_000,
            null
          ))!;
          expect(await driver.maintenanceLeaderResign(leader)).toBe(true);
          expect(notificationRows(database)).toEqual([]);
          continue;
        }
        case "resume":
          await driver.queueUpsert(queue, {
            pausedAt: Temporal.Now.instant(),
          });
          await driver.queueResume(queue);
          break;
        default:
          throw new Error(`unhandled notification golden ${notification.name}`);
      }

      const rows = notificationRows(database);
      expect(
        rows.map(({ topic }) => topic),
        notification.name
      ).toEqual([notification.topic]);
      const payload = JSON.parse(rows[0]!.payload) as Record<string, unknown>;
      expect(payload, notification.name).toEqual(notification.payload);
      const fields = new Map(
        notification.fields.map(({ name, omitempty }) => [name, omitempty])
      );
      for (const key of Object.keys(payload)) {
        expect(fields.has(key), `${notification.name}.${key}`).toBe(true);
      }
      for (const [name, omitempty] of fields) {
        if (!omitempty) {
          expect(
            Object.hasOwn(payload, name),
            `${notification.name}.${name}`
          ).toBe(true);
        }
      }
    }
  });

  test("fences leadership renewals and supports expiry failover", async () => {
    const { driver } = await setup();
    const firstAt = Temporal.Instant.from("2026-08-30T18:40:00Z");
    const first = await driver.leaderElect({
      leaderId: "leader-one",
      now: firstAt,
      ttlMs: 1_000,
    });
    expect(first?.leaderId).toBe("leader-one");
    expect(
      await driver.leaderElect({
        leaderId: "leader-two",
        now: firstAt.add({ milliseconds: 500 }),
        ttlMs: 1_000,
      })
    ).toBeNull();
    const renewed = await driver.leaderReelect(first!, {
      now: firstAt.add({ milliseconds: 500 }),
      ttlMs: 1_000,
    });
    expect(renewed?.expiresAt.toString()).toBe("2026-08-30T18:40:01.5Z");

    const second = await driver.leaderElect({
      leaderId: "leader-two",
      now: firstAt.add({ milliseconds: 1_501 }),
      ttlMs: 1_000,
    });
    expect(second?.leaderId).toBe("leader-two");
    expect(
      await driver.leaderReelect(first!, {
        now: firstAt.add({ milliseconds: 1_600 }),
        ttlMs: 1_000,
      })
    ).toBeNull();
    expect(await driver.leaderResign(first!)).toBe(false);
    expect((await driver.leaderGet())?.leaderId).toBe("leader-two");
    expect(await driver.leaderResign(second!)).toBe(true);
    expect(await driver.leaderGet()).toBeNull();
  });

  test("renews only the held term and never adopts a same-ID term", async () => {
    const { database, driver } = await setup();
    const now = Temporal.Now.instant();
    const first = (await driver.maintenanceLeaderAcquire(
      "leader",
      now,
      60_000,
      null
    ))!;

    // A live term is renewed only by its holder, like Go's elector.
    expect(
      await driver.maintenanceLeaderAcquire("leader", now, 60_000, null)
    ).toBeNull();
    expect(
      (await driver.maintenanceLeaderAcquire("leader", now, 60_000, first))
        ?.electedAt
    ).toEqual(first.electedAt);

    // Another process with the same client ID takes over with a newer term.
    database
      .prepare(
        "UPDATE river_leader SET elected_at = strftime('%Y-%m-%d %H:%M:%f', elected_at, '+1 second')"
      )
      .run();
    expect(
      await driver.maintenanceLeaderAcquire("leader", now, 60_000, first)
    ).toBeNull();
    expect((await driver.leaderGet())?.electedAt).toEqual(
      first.electedAt.add({ seconds: 1 })
    );
  });

  test("cleans finalized jobs except in excluded queues", async () => {
    const { database, driver } = await setup();
    const now = Temporal.Now.instant();
    const leader = (await driver.maintenanceLeaderAcquire(
      "cleaner",
      now,
      60_000,
      null
    ))!;
    const ids: Record<string, bigint> = {};
    for (const queue of ["kept", "cleaned"]) {
      const { job } = await driver.jobInsert({
        args: {},
        kind: "cleanup",
        queue,
      });
      ids[queue] = job.id;
      database
        .prepare(
          "UPDATE river_job SET state = 'completed', finalized_at = '2000-01-01 00:00:00.000' WHERE id = ?"
        )
        .run(job.id);
    }

    expect(
      await driver.maintenanceCleanJobs(
        leader,
        {
          cancelledBefore: now,
          completedBefore: now,
          discardedBefore: now,
          limit: 10,
          queuesExcluded: ["kept"],
        },
        null,
        new AbortController().signal
      )
    ).toBe(1);
    expect(await driver.jobGet(ids.kept as bigint)).not.toBeNull();
    expect(await driver.jobGet(ids.cleaned as bigint)).toBeNull();
  });

  // Like Go's `QueuesFilteredBeforeLimit` driver cases: retained jobs in
  // `kept1`/`kept2` hold the lowest IDs, so a pass limiting candidates
  // before filtering queues would select only them and stall.
  for (const testCase of [
    {
      // `kept1` appears in both lists; exclusion wins.
      batches: [2, 2, 1, 0],
      deletedQueues: ["deleted1", "deleted2"],
      name: "both lists",
      queuesExcluded: ["kept1", "kept2"],
      queuesIncluded: ["deleted1", "deleted2", "kept1"],
    },
    {
      batches: [2, 2, 2, 2, 2, 1, 0],
      deletedQueues: ["deleted1", "deleted2", "kept1", "kept2"],
      name: "an empty excluded list",
      queuesExcluded: [],
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
      batches: [0],
      deletedQueues: [],
      name: "a missing included queue",
      queuesIncluded: ["missing"],
    },
    {
      batches: [2, 2, 2, 2, 2, 1, 0],
      deletedQueues: ["deleted1", "deleted2", "kept1", "kept2"],
      name: "a null included list",
      queuesIncluded: null,
    },
  ] as const) {
    test(`cleans with queue filters before the batch limit: ${testCase.name}`, async () => {
      const { database, driver } = await setup();
      const states = ["cancelled", "completed", "discarded"];
      const queues = [
        "kept1",
        "kept2",
        "kept1",
        "kept2",
        "kept1",
        "kept2",
        "deleted1",
        "deleted2",
        "deleted1",
        "deleted2",
        "deleted1",
      ];
      const allIds: bigint[] = [];
      const eligibleIds: bigint[] = [];
      for (const [index, queue] of queues.entries()) {
        const { job } = await driver.jobInsert({
          args: {},
          kind: "cleanup",
          queue,
        });
        database
          .prepare(
            "UPDATE river_job SET state = ?, finalized_at = '2000-01-01 00:00:00.000' WHERE id = ?"
          )
          .run(states[index % states.length]!, job.id);
        allIds.push(job.id);
        if ((testCase.deletedQueues as readonly string[]).includes(queue)) {
          eligibleIds.push(job.id);
        }
      }

      const before = Temporal.Now.instant();
      let deletedTotal = 0;
      for (const wantDeleted of testCase.batches) {
        const deleted = await driver.jobCleanup({
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

        // Batches delete the oldest eligible jobs first.
        const gone = eligibleIds.slice(0, deletedTotal);
        const remaining = database
          .prepare("SELECT id FROM river_job ORDER BY id")
          .all()
          .map((row) => BigInt(row.id as number | bigint));
        expect(remaining).toEqual(allIds.filter((id) => !gone.includes(id)));
      }
      expect(deletedTotal).toBe(eligibleIds.length);
    });
  }

  test("fences maintenance mutations to the exact current term", async () => {
    const { driver } = await setup();
    const now = Temporal.Now.instant();
    const first = (await driver.maintenanceLeaderAcquire(
      "leader-one",
      now,
      60_000,
      null
    ))!;
    const job = (
      await driver.jobInsert({
        args: {},
        kind: "fenced_scheduler",
        scheduledAt: now,
        state: "scheduled",
      })
    ).job;
    expect(await driver.maintenanceLeaderResign(first)).toBe(true);
    const second = (await driver.maintenanceLeaderAcquire(
      "leader-two",
      now.add({ milliseconds: 1 }),
      60_000,
      null
    ))!;
    const params = {
      allowInsertNotifications: allowEveryQueue,
      limit: 10,
      notificationHorizon: now,
      now,
      scheduledAtHorizon: now,
    };

    expect(await driver.maintenanceSchedule(first, params)).toBe(0);
    expect((await driver.jobGet(job.id))?.state).toBe("scheduled");
    expect(await driver.maintenanceSchedule(second, params)).toBe(1);
    expect((await driver.jobGet(job.id))?.state).toBe("available");
  });

  test("promotes ahead without prematurely notifying workers", async () => {
    const { database, driver } = await setup();
    const now = Temporal.Now.instant();
    const leader = (await driver.maintenanceLeaderAcquire(
      "leader",
      now,
      60_000,
      null
    ))!;
    const scheduledAt = now.add({ milliseconds: 100 });
    const job = (
      await driver.jobInsert({
        args: {},
        kind: "scheduler_lookahead",
        scheduledAt,
        state: "scheduled",
      })
    ).job;

    expect(
      await driver.maintenanceSchedule(leader, {
        allowInsertNotifications: allowEveryQueue,
        limit: 10,
        notificationHorizon: now.add({ milliseconds: 5 }),
        now,
        scheduledAtHorizon: now.add({ seconds: 5 }),
      })
    ).toBe(1);

    expect((await driver.jobGet(job.id))?.state).toBe("available");
    expect(
      notificationRows(database).filter(({ topic }) => topic === "river_insert")
    ).toEqual([]);
  });

  test("schedules unique jobs and runs bounded rescue and cleaners", async () => {
    const { database, driver } = await setup();
    const now = Temporal.Instant.from("2026-08-30T18:50:00Z");
    const uniqueKey = new Uint8Array(32).fill(9);
    await driver.jobInsert({
      args: {},
      kind: "unique_existing",
      uniqueKey,
      uniqueStates: ["available"],
    });
    const conflict = (
      await driver.jobInsert({
        args: {},
        kind: "unique_scheduled",
        scheduledAt: now,
        state: "scheduled",
        uniqueKey,
        uniqueStates: ["available"],
      })
    ).job;
    const ordinary = (
      await driver.jobInsert({
        args: {},
        kind: "ordinary_scheduled",
        scheduledAt: now,
        state: "scheduled",
      })
    ).job;
    const scheduled = await driver.jobSchedule({ now });
    expect(
      scheduled.map(({ conflictDiscarded, job }) => [job.id, conflictDiscarded])
    ).toEqual([
      [conflict.id, true],
      [ordinary.id, false],
    ]);
    expect(
      (await driver.jobGet(conflict.id))?.metadata.unique_key_conflict
    ).toBe("scheduler_discarded");

    const running = (
      await driver.jobInsert({
        args: {},
        kind: "rescue",
        queue: "rescue",
        scheduledAt: now,
      })
    ).job;
    await driver.jobClaim({
      attemptedBy: "worker",
      kinds: [],
      queues: [{ limit: 1, name: "rescue" }],
    });
    const attemptedBefore = Temporal.Now.instant().add({ milliseconds: 1 });
    const stuck = await driver.jobGetStuck({ attemptedBefore });
    expect(stuck.some(({ id }) => id === running.id)).toBe(true);
    const rescued = await driver.jobRescueMany(
      [
        {
          error: { at: now, attempt: 1, error: "stuck", trace: "trace" },
          id: running.id,
          scheduledAt: now,
          state: "retryable",
        },
      ],
      attemptedBefore
    );
    expect(rescued[0]?.metadata["river:rescue_count"]).toBe(1);

    const old = now.subtract({ hours: 1 });
    database
      .prepare(
        "UPDATE river_job SET state = 'completed', finalized_at = ? WHERE id = ?"
      )
      .run("2026-08-30 17:50:00.000", ordinary.id);
    expect(
      await driver.jobCleanup({
        cancelledBefore: old.add({ hours: 2 }),
        completedBefore: old.add({ hours: 2 }),
        discardedBefore: old.add({ hours: 2 }),
      })
    ).toBeGreaterThan(0);
  });
});

/** A manually advanced monotonic clock for the driver's timing decisions. */
class ManualClock {
  #now = 0;

  advance(milliseconds: number): void {
    this.#now += milliseconds;
  }

  now(): number {
    return this.#now;
  }
}

async function setup(options: SqliteDriverOptions = {}): Promise<{
  clock: ManualClock;
  database: DatabaseSync;
  driver: SqliteRuntime;
}> {
  const clock = new ManualClock();
  const driver = testSqliteMemory({
    ...options,
    [SQLITE_DRIVER_TEST_HOOKS]: {
      now: () => clock.now(),
      strictLockWindow: true,
    },
  } as SqliteDriverOptions);
  onTestFinished(() => driver.close());
  const database = driver.database;
  await migrate(database);
  return { clock, database, driver };
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

function jobListParams(overrides: Partial<JobListParams> = {}): JobListParams {
  return {
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
    ...overrides,
  };
}

/**
 * Reads a fixture that `make generate/fixtures` writes from River's Go
 * implementation. A missing fixture fails the test rather than skipping it.
 */
async function readFixture(url: URL): Promise<string> {
  try {
    return await readFile(url, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      throw new Error(
        `missing conformance fixture ${fileURLToPath(url)}; run \`make generate/fixtures\` from the repository root`,
        { cause: error }
      );
    }
    throw error;
  }
}

function notificationRows(
  database: DatabaseSync
): { payload: string; topic: string }[] {
  const statement = database.prepare(
    "SELECT payload, topic FROM river_notification ORDER BY id"
  );
  statement.setReadBigInts(true);
  return statement.all() as { payload: string; topic: string }[];
}

function resultRow(
  database: DatabaseSync,
  sql: string
): Record<string, unknown> | undefined {
  const statement = database.prepare(sql);
  statement.setReadBigInts(true);
  return statement.get();
}

/** Let every queue's insert notification through, with no limiter. */
function allowEveryQueue(queues: readonly string[]): readonly string[] {
  return [...new Set(queues)];
}
