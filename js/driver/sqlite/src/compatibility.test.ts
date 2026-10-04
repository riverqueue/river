import type { DatabaseSync } from "node:sqlite";

import {
  Client,
  DatabaseOperationError,
  ValidationError,
  defineJob,
  exactJsonNumber,
  parseJson,
} from "riverqueue";
import type { JsonObject, JsonValue } from "riverqueue";
import type {
  JobCompletionCommand,
  JobListParams,
  RuntimeJobRescue,
} from "riverqueue/unstable-driver";
import { describe, expect, onTestFinished, test } from "vitest";

import {
  SQLITE_DRIVER_TEST_HOOKS,
  type SqliteRuntime,
  testSqliteMemory,
} from "./driver.js";
import type {
  SqliteDriverOptions,
  SqliteJobRow,
  SqliteJsonObject,
  SqliteRescueJobParams,
} from "./types.js";

/** River's own tests fail any lock window that crosses the event loop. */
const STRICT = {
  [SQLITE_DRIVER_TEST_HOOKS]: { strictLockWindow: true },
} as SqliteDriverOptions;

// Mirrors riverdrivertest's precision test time, rounded to SQLite's
// millisecond precision.
const PRECISION_TEST_TIME = Temporal.Instant.from("2025-04-30T13:26:39.123Z");

describe("SqliteDriver compatibility with Go's riversqlite", () => {
  test("decodes and works rows that only Go's wider decoding accepts", async () => {
    const { database, driver } = await setup();
    database.exec(`
      INSERT INTO river_job (
        id, args, attempt, attempted_by, created_at, errors, kind,
        max_attempts, metadata, scheduled_at, tags, unique_key, unique_states
      ) VALUES (
        1, jsonb('{}'), 39999, jsonb('null'), '2026-01-01 00:00:00',
        jsonb('[null, {"attempt": 2}, {"at": null, "error": "e", "trace": null}]'),
        'go_legal', 40000, jsonb('{}'), '2026-01-01 00:00:00',
        jsonb('null'), 'text-key', 0
      ), (
        2, jsonb('{}'), -5, jsonb('["a", null]'), CURRENT_TIMESTAMP,
        jsonb('null'), 'go_legal', 9223372036854775807, jsonb('{}'),
        '2026-01-01 00:00:01', jsonb('["tag", null]'), NULL, NULL
      )`);

    const zero = Temporal.Instant.from("0001-01-01T00:00:00Z");
    const first = await driver.jobGet(1n);
    expect(first).toMatchObject({
      attempt: 39_999,
      attemptedBy: [],
      errors: [
        { at: zero, attempt: 0, error: "", trace: "" },
        { at: zero, attempt: 2, error: "", trace: "" },
        { at: zero, attempt: 0, error: "e", trace: "" },
      ],
      maxAttempts: 40_000,
      tags: [],
      uniqueKey: new TextEncoder().encode("text-key"),
      uniqueStates: [],
    });
    expect(await driver.jobGet(2n)).toMatchObject({
      attempt: 0,
      attemptedBy: ["a", ""],
      errors: [],
      maxAttempts: Number.MAX_SAFE_INTEGER,
      tags: ["tag", ""],
    });

    const claimed = (
      await driver.jobClaim({
        attemptedBy: "js-worker",
        kinds: ["go_legal"],
        queues: [{ limit: 10, name: "default" }],
      })
    ).jobs;
    expect(claimed.map(({ attempt, id }) => [id, attempt])).toEqual([
      [1n, 40_000],
      // The claim stores -4; decoding clamps it to 0 like Go.
      [2n, 0],
    ]);
    expect(claimed[0]?.attemptedBy).toEqual(["js-worker"]);

    const [completed] = await driver.jobCompleteMany([
      completion({
        attempt: 40_000,
        attemptedBy: "js-worker",
        error: {
          at: PRECISION_TEST_TIME,
          error: "failed",
          trace: "",
        },
        id: 1n,
        kind: "retry",
        scheduledAt: PRECISION_TEST_TIME,
      }),
    ]);
    expect(completed).toMatchObject({ status: "applied" });
    expect(completed?.job?.errors.at(-1)).toMatchObject({
      attempt: 40_000,
      error: "failed",
    });
  });

  test("tolerates JSON columns holding text that isn't valid JSON like Go", async () => {
    const { database, driver } = await setup();
    const scheduledAt = Temporal.Now.instant().subtract({ seconds: 10 });
    const columns = ["args", "attempted_by", "errors", "metadata", "tags"];
    const ids = new Map<string, bigint>();
    for (const [index, column] of columns.entries()) {
      const { job } = await driver.jobInsert({
        args: {},
        kind: "malformed",
        scheduledAt: scheduledAt.add({ milliseconds: index }),
      });
      database
        .prepare(`UPDATE river_job SET ${column} = '[not json' WHERE id = ?`)
        .run(job.id);
      ids.set(column, job.id);
    }
    const healthy = (
      await driver.jobInsert({
        args: {},
        kind: "malformed",
        scheduledAt: scheduledAt.add({ seconds: 1 }),
      })
    ).job;

    // The claim doesn't fail on "malformed JSON": every job is claimed, in
    // claim order, and each malformed one is reported undecodable.
    const claimed = await driver.jobClaim({
      attemptedBy: "js-worker",
      kinds: [],
      queues: [{ limit: 10, name: "default" }],
    });
    expect(claimed.jobs.map(({ id }) => id)).toEqual([
      ...columns.map((column) => ids.get(column)),
      healthy.id,
    ]);
    const undecodable = claimed.jobs.filter(({ id }) =>
      claimed.decodeErrors?.has(id)
    );
    expect(
      new Map(
        [...(claimed.decodeErrors ?? [])].map(([id, error]) => [
          id,
          error.message,
        ])
      )
    ).toEqual(
      new Map(
        columns.map((column) => [
          ids.get(column),
          expect.stringContaining(column) as unknown as string,
        ])
      )
    );

    // Failing their attempts leaves the invalid values in place, except
    // that invalid errors text becomes a string in a new array.
    const failed = await driver.jobCompleteMany(
      undecodable.map((job) =>
        completion({
          attempt: job.attempt,
          attemptedBy: "js-worker",
          error: {
            at: scheduledAt,
            error: "job row couldn't be decoded",
            trace: "",
          },
          id: job.id,
          kind: "retry",
          scheduledAt: scheduledAt.subtract({ seconds: 1 }),
        })
      )
    );
    expect(failed.map(({ status }) => status)).toEqual(
      columns.map(() => "applied")
    );
    for (const column of columns) {
      const text = rawRow(
        database,
        `SELECT CASE WHEN typeof(${column}) = 'text' THEN ${column}
           ELSE json(${column}) END AS value
         FROM river_job WHERE id = ${ids.get(column)}`
      )?.value;
      if (column === "errors") {
        expect(JSON.parse(text as string)).toEqual([
          "[not json",
          expect.objectContaining({ error: "job row couldn't be decoded" }),
        ]);
      } else {
        expect(text).toBe("[not json");
      }
    }
    const errorsJob = await driver.jobGet(ids.get("errors")!);
    expect(errorsJob?.errors.map(({ error }) => error)).toEqual([
      "[not json",
      "job row couldn't be decoded",
    ]);

    // Scheduling the retries and rescuing a stuck one don't fail either.
    const leader = (await driver.maintenanceLeaderAcquire(
      "leader",
      Temporal.Now.instant(),
      60_000,
      null
    ))!;
    await expect(
      driver.maintenanceSchedule(leader, {
        allowInsertNotifications: allowEveryQueue,
        limit: 100,
        now: Temporal.Now.instant(),
        notificationHorizon: Temporal.Now.instant(),
        scheduledAtHorizon: Temporal.Now.instant(),
      })
    ).resolves.toBe(columns.length);
    database
      .prepare(
        "UPDATE river_job SET state = 'running', attempted_at = '2000-01-01 00:00:00.000' WHERE id = ?"
      )
      .run(ids.get("metadata")!);
    const horizon = Temporal.Instant.from("2001-01-01T00:00:00Z");
    const stuck = await driver.maintenanceGetStuck(leader, horizon, 0n, 10);
    expect(stuck.map(({ id }) => id)).toEqual([ids.get("metadata")]);
    await driver.maintenanceRescue(leader, horizon, [
      {
        error: { at: horizon, attempt: 1, error: "stuck", trace: "" },
        finalizedAt: null,
        id: ids.get("metadata")!,
        scheduledAt: horizon,
        state: "retryable",
      },
    ]);
    expect(
      rawRow(
        database,
        `SELECT state, metadata FROM river_job WHERE id = ${ids.get("metadata")}`
      )
    ).toEqual({ metadata: "[not json", state: "retryable" });
  });

  test("returns undecodable claimed rows with their errors and completes them like Go", async () => {
    const { database, driver } = await setup();
    const scheduledAt = Temporal.Now.instant().subtract({ seconds: 10 });
    const corrupt = (
      await driver.jobInsert({ args: {}, kind: "poison", scheduledAt })
    ).job;
    const wrappedErrors = (
      await driver.jobInsert({
        args: {},
        kind: "poison",
        scheduledAt: scheduledAt.add({ seconds: 1 }),
      })
    ).job;
    const healthy = (
      await driver.jobInsert({
        args: {},
        kind: "poison",
        scheduledAt: scheduledAt.add({ seconds: 2 }),
      })
    ).job;
    database
      .prepare("UPDATE river_job SET tags = jsonb('[1]') WHERE id = ?")
      .run(corrupt.id);
    database
      .prepare(
        `UPDATE river_job SET errors = jsonb('{"legacy":true}') WHERE id = ?`
      )
      .run(wrappedErrors.id);

    const claimed = await driver.jobClaim({
      attemptedBy: "js-worker",
      kinds: [],
      queues: [{ limit: 10, name: "default" }],
    });

    // Every row is running and comes back in claim order; the undecodable
    // ones have the bad field empty and their decode errors alongside.
    expect(claimed.jobs.map(({ id }) => id)).toEqual([
      corrupt.id,
      wrappedErrors.id,
      healthy.id,
    ]);
    expect([...(claimed.decodeErrors?.keys() ?? [])]).toEqual([
      corrupt.id,
      wrappedErrors.id,
    ]);
    const undecodable = claimed.jobs.slice(0, 2);
    expect(undecodable[0]).toMatchObject({
      attempt: 1,
      state: "running",
      tags: [],
    });
    expect(claimed.decodeErrors?.get(corrupt.id)?.message).toContain("tags[0]");
    expect(undecodable[1]?.errors).toEqual([]);

    // The runtime fails their attempts. The error is appended without
    // decoding the bad column, which keeps its value; a non-array errors
    // value is wrapped in an array first. Completion still returns the rows.
    const failures = await driver.jobCompleteMany(
      undecodable.map((job) =>
        completion({
          attempt: job.attempt,
          attemptedBy: "js-worker",
          error: {
            at: scheduledAt,
            error: "job row couldn't be decoded",
            trace: "",
          },
          id: job.id,
          kind: "retry",
          scheduledAt: scheduledAt.add({ hours: 1 }),
        })
      )
    );
    expect(failures.map(({ status }) => status)).toEqual([
      "applied",
      "applied",
    ]);
    expect(failures[0]?.job).toMatchObject({ state: "retryable", tags: [] });
    expect(
      rawRow(
        database,
        `SELECT json(tags) AS tags, json_array_length(errors) AS errors
         FROM river_job WHERE id = ${corrupt.id}`
      )
    ).toEqual({ errors: 1n, tags: "[1]" });
    expect(
      rawRow(
        database,
        `SELECT json(errors) AS errors FROM river_job WHERE id = ${wrappedErrors.id}`
      )?.errors
    ).toMatch(
      /^\[\{"legacy":true\},\{"at":.*"error":"job row couldn't be decoded"/
    );

    // The rescuer can read stuck rows that don't fully decode.
    database
      .prepare(
        "UPDATE river_job SET state = 'running', attempted_at = '2000-01-01 00:00:00.000' WHERE id = ?"
      )
      .run(corrupt.id);
    const leader = (await driver.maintenanceLeaderAcquire(
      "leader",
      Temporal.Now.instant(),
      60_000,
      null
    ))!;
    const stuck = await driver.maintenanceGetStuck(
      leader,
      Temporal.Now.instant(),
      0n,
      10
    );
    expect(stuck.map(({ id }) => id)).toContain(corrupt.id);
  });

  test("finds cancellation requests beside metadata that isn't valid JSON like Go", async () => {
    const { database, driver } = await setup();
    const requested = await runningJob(driver, {
      metadata: { cancel_attempted_at: PRECISION_TEST_TIME.toString() },
    });
    const corrupt = await runningJob(driver);
    const plain = await runningJob(driver);
    database
      .prepare("UPDATE river_job SET metadata = '[not json' WHERE id = ?")
      .run(corrupt.id);

    await expect(
      driver.jobGetCancelRequested([plain.id, corrupt.id, requested.id])
    ).resolves.toEqual([requested.id]);
  });

  test("treats a present null cancel_attempted_at as a cancellation like Go", async () => {
    const { driver } = await setup();
    const job = await runningJob(driver, {
      metadata: { cancel_attempted_at: null },
    });

    const [result] = await driver.jobCompleteMany([
      completion({
        attempt: job.attempt,
        attemptedBy: "js-worker",
        id: job.id,
        kind: "retry",
        scheduledAt: PRECISION_TEST_TIME,
      }),
    ]);

    expect(result?.job?.state).toBe("cancelled");
  });

  test("fences leader terms by instant, not by timestamp text", async () => {
    const { database, driver } = await setup();
    database.exec(
      `INSERT INTO river_leader (elected_at, expires_at, leader_id)
       VALUES ('2026-08-30 18:40:00', '2999-01-01 00:00:00.000', 'go-leader')`
    );
    const leader = await driver.leaderGet();
    expect(leader?.electedAt.toString()).toBe("2026-08-30T18:40:00Z");

    const renewed = await driver.leaderReelect(leader!, {
      now: Temporal.Instant.from("2026-08-30T18:40:01Z"),
      ttlMs: 1_000,
    });
    expect(renewed?.leaderId).toBe("go-leader");
    expect(await driver.leaderResign(renewed!)).toBe(true);
    expect(await driver.leaderGet()).toBeNull();
  });

  test("stores an empty unique state set as NULL like Go", async () => {
    const { database, driver } = await setup();
    const inserted = await driver.jobInsert({
      args: {},
      kind: "empty_unique_states",
      uniqueKey: new Uint8Array(32).fill(1),
      uniqueStates: [],
    });

    expect(
      rawRow(
        database,
        `SELECT unique_states FROM river_job WHERE id = ${inserted.job.id}`
      )
    ).toEqual({ unique_states: null });
    expect(inserted.job.uniqueStates).toBeNull();
  });

  test("stores inserted JSON like Go's riversqlite", async () => {
    // The values River for Go's `riversqlite` driver stores for the same
    // inserts and error: Go's `Client.InsertManyFast` for the client insert,
    // the returning `JobInsertFastMany` for the reinsertion, and the
    // completer's `JobSetStateIfRunningMany` for the error.
    const { database, driver } = await setup();
    const stored = (kind: string) => ({
      ...rawRow(
        database,
        `SELECT attempted_by IS NULL AS attempted_by_null,
           errors IS NULL AS errors_null
         FROM river_job WHERE kind = '${kind}'`
      ),
      ...storedJson(database, kind, [
        "args",
        "attempted_by",
        "errors",
        "metadata",
        "tags",
      ]),
    });

    // River's own insert encodes arguments and metadata.
    const escapeJob = defineJob<{
      html: string;
      lines: string;
      nested: Record<string, string>;
    }>()({ kind: "golden_escape" });
    const client = new Client(driver);
    const args = {
      html: '<a href="x">&amp;</a>',
      lines: "one\u2028two\u2029three",
      nested: { "k<": ">v&" },
    };
    const metadata = { note: "<x> & y", sep: "a\u2028b", already: "<" };
    await client.insertMany([
      {
        args,
        job: escapeJob,
        options: { metadata, tags: ["tag_a", "tag-b"] },
      },
    ]);
    expect(stored("golden_escape")).toEqual({
      args,
      attempted_by: null,
      attempted_by_null: 1n,
      errors: null,
      errors_null: 1n,
      metadata,
      tags: ["tag_a", "tag-b"],
    });
    // Like Go's returning insert, River's insertion adds a nonce.
    expect(uniqueNonce(database, "golden_escape")).toMatch(/^[0-9a-f]{16}$/);

    // A reinsertion stores the caller's encoded arguments, not `args`, and
    // keeps its creation time.
    const createdAt = Temporal.Instant.from("2026-01-02T03:04:05.123Z");
    const [reinserted] = await driver.jobInsertMany([
      {
        args: { a: [1, 2.5, 1], u: "\u2028", z: "<b>&c" },
        createdAt,
        encodedArgs:
          '{"z": "<b>&c", "a": [1, 2.50, 123456789012345678901234567890], "u": "\u2028"}',
        kind: "golden_reinsert",
        maxAttempts: 25,
        metadata: { m: ">" },
        priority: 1,
        queue: "default",
        scheduledAt: Temporal.Instant.from("2026-01-02T04:00:00Z"),
        state: "available",
        tags: [],
        uniqueKey: null,
        uniqueStates: null,
      },
    ]);
    expect(stored("golden_reinsert")).toEqual({
      args: {
        a: [1, 2.5, exactJsonNumber("123456789012345678901234567890")],
        u: "\u2028",
        z: "<b>&c",
      },
      attempted_by: null,
      attempted_by_null: 1n,
      errors: null,
      errors_null: 1n,
      metadata: { m: ">" },
      tags: [],
    });
    // Like Go's returning insert, a row without a unique key still stores
    // the nonce, as eight random bytes in lowercase hex.
    expect(uniqueNonce(database, "golden_reinsert")).toMatch(/^[0-9a-f]{16}$/);
    expect(reinserted?.job.metadata["river:unique_nonce"]).toBe(
      uniqueNonce(database, "golden_reinsert")
    );
    expect(reinserted?.job.createdAt).toEqual(createdAt);
    expect(Object.keys(reinserted!.job.args)).toEqual(["z", "a", "u"]);
    expect(reinserted?.job.args.a).toEqual([
      1,
      2.5,
      exactJsonNumber("123456789012345678901234567890"),
    ]);

    // A failed attempt's error and client ID.
    const failing = await driver.jobInsert({
      args: {},
      attempt: 1,
      attemptedAt: createdAt,
      attemptedBy: ["client<1>"],
      createdAt,
      kind: "golden_error",
      scheduledAt: createdAt,
      state: "running",
    });
    await driver.jobCompleteMany([
      completion({
        attempt: 1,
        attemptedBy: "client<1>",
        error: { at: createdAt, error: "<boom> & \u2028", trace: "trace>" },
        id: failing.job.id,
        kind: "retry",
        scheduledAt: Temporal.Instant.from("2026-01-02T04:00:00Z"),
      }),
    ]);
    expect(stored("golden_error")).toMatchObject({
      attempted_by: ["client<1>"],
      errors: [
        {
          at: "2026-01-02T03:04:05.123Z",
          attempt: 1,
          error: "<boom> & \u2028",
          trace: "trace>",
        },
      ],
    });
  });

  test("stores update, cancellation, retry, and output JSON like Go's riversqlite", async () => {
    // The values River for Go's `riversqlite` driver stores for
    // `Client.JobUpdate` with an output, `JobCancel`, `JobRetry` after a
    // cancellation, and the completer's `JobSetStateIfRunningMany`
    // completing with an output and discarding with an error. Every job
    // starts with metadata `{"m":"x"}`.
    const output = { text: "a<b>&c\u2028d\u2029\u00e9", values: [2.5, "<&>"] };
    const now = Temporal.Instant.from("2026-01-02T05:06:07.123456789Z");
    const cancelled = { cancel_attempted_at: now.toString(), m: "x" };
    const { database, driver } = await setup();
    const insert = async (kind: string, state: "available" | "running") =>
      (
        await driver.jobInsert({
          args: {},
          attempt: 1,
          attemptedAt: now,
          attemptedBy: ["client<1>"],
          kind,
          metadata: { m: "x" },
          state,
        })
      ).job.id;
    const stored = (kind: string) => ({
      ...rawRow(
        database,
        `SELECT state, finalized_at, scheduled_at, errors IS NULL AS errors_null
         FROM river_job WHERE kind = '${kind}'`
      ),
      ...storedJson(database, kind, ["errors", "metadata"]),
    });

    await driver.jobUpdate(await insert("golden_update", "available"), {
      output,
    });
    expect(stored("golden_update")).toMatchObject({
      metadata: { m: "x", output },
    });

    await driver.jobCancelDetailed(await insert("golden_cancel", "available"), {
      now,
    });
    expect(stored("golden_cancel")).toEqual({
      errors: null,
      errors_null: 1n,
      finalized_at: "2026-01-02 05:06:07.123",
      metadata: cancelled,
      scheduled_at: expect.stringMatching(
        /^\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{3}$/
      ),
      state: "cancelled",
    });

    const retried = await insert("golden_retry", "available");
    await driver.jobCancelDetailed(retried, { now });
    await driver.jobRetryDetailed(retried, { now: now.add({ hours: 1 }) });
    expect(stored("golden_retry")).toEqual({
      errors: null,
      errors_null: 1n,
      finalized_at: null,
      metadata: cancelled,
      scheduled_at: "2026-01-02 06:06:07.123",
      state: "available",
    });

    const worked = await insert("golden_output", "running");
    await driver.jobCompleteMany([
      completion({
        attempt: 1,
        attemptedBy: "client<1>",
        finalizedAt: now,
        id: worked,
        kind: "complete",
        output,
        outputSet: true,
      }),
    ]);
    expect(stored("golden_output")).toMatchObject({
      finalized_at: "2026-01-02 05:06:07.123",
      metadata: { m: "x", output },
      state: "completed",
    });

    const discarded = await insert("golden_discard", "running");
    await driver.jobCompleteMany([
      completion({
        attempt: 1,
        attemptedBy: "client<1>",
        error: { at: now, error: "<boom> & \u2028", trace: "trace>" },
        finalizedAt: now,
        id: discarded,
        kind: "discard",
      }),
    ]);
    expect(stored("golden_discard")).toMatchObject({
      errors: [
        {
          at: now.toString(),
          attempt: 1,
          error: "<boom> & \u2028",
          trace: "trace>",
        },
      ],
      finalized_at: "2026-01-02 05:06:07.123",
      metadata: { m: "x" },
      state: "discarded",
    });
  });

  test("stores claim, snooze, rescue, scheduler, and queue JSON like Go's riversqlite", async () => {
    // The values River for Go's `riversqlite` driver stores for
    // `JobGetAvailable` for a claim, the completer's
    // `JobSetStateIfRunningMany` for a snooze, a running job's cancellation
    // with an error, and a completion whose job is no longer running
    // (`JobSetMetadataIfNotRunning`), `JobRescueMany`, `JobSchedule` with a
    // unique key conflict, and `QueueCreateOrSetUpdatedAt`/`QueueUpdate`.
    // Jobs start with metadata `{"m":"x"}`.
    const createdAt = Temporal.Instant.from("2026-01-02T03:04:05.123Z");
    const now = Temporal.Instant.from("2026-01-02T05:06:07.123456789Z");
    const { database, driver } = await setup();
    const insert = async (
      kind: string,
      state: "available" | "running",
      options: { metadata?: SqliteJsonObject; queue?: string } = {}
    ) =>
      (
        await driver.jobInsert({
          args: {},
          attempt: 1,
          attemptedAt: createdAt,
          attemptedBy: ["client<1>"],
          createdAt,
          kind,
          metadata: options.metadata ?? { m: "x" },
          queue: options.queue ?? "default",
          scheduledAt: createdAt,
          state,
        })
      ).job.id;
    const stored = (kind: string) => ({
      ...rawRow(database, `SELECT state FROM river_job WHERE kind = '${kind}'`),
      ...storedJson(database, kind, ["attempted_by", "errors", "metadata"]),
    });
    const queueMetadata = (): unknown => {
      const row = rawRow(
        database,
        `SELECT typeof(metadata) AS type, json(metadata) AS metadata
         FROM river_queue WHERE name = 'golden_queue'`
      );
      expect(row?.type).toBe("blob");
      return parseJson(row?.metadata as string);
    };

    // A claim appends the client ID to `attempted_by`.
    await insert("golden_claim", "available", { queue: "golden_claim" });
    await driver.jobClaim({
      attemptedBy: "worker<2>\u2028\u00e9",
      kinds: [],
      queues: [{ limit: 1, name: "golden_claim" }],
    });
    expect(stored("golden_claim")).toMatchObject({
      attempted_by: ["client<1>", "worker<2>\u2028\u00e9"],
      state: "running",
    });

    // A snooze records its count.
    const snoozed = await insert("golden_snooze", "running");
    await driver.jobCompleteMany([
      completion({
        attempt: 1,
        attemptedBy: "client<1>",
        id: snoozed,
        kind: "snooze",
        metadata: { snoozes: 1 },
        scheduledAt: now.add({ hours: 1 }),
      }),
    ]);
    expect(stored("golden_snooze")).toMatchObject({
      metadata: { m: "x", snoozes: 1 },
      state: "scheduled",
    });

    // A running job cancelled with an error.
    const cancelled = await insert("golden_cancel_running", "running");
    await driver.jobCompleteMany([
      completion({
        attempt: 1,
        attemptedBy: "client<1>",
        error: { at: now, error: "cancelled <x> \u2028", trace: "" },
        finalizedAt: now,
        id: cancelled,
        kind: "cancel",
      }),
    ]);
    expect(stored("golden_cancel_running")).toMatchObject({
      errors: [
        {
          at: now.toString(),
          attempt: 1,
          error: "cancelled <x> \u2028",
          trace: "",
        },
      ],
      metadata: { m: "x" },
      state: "cancelled",
    });

    // A completion that finds its job no longer running still merges its
    // output.
    const stale = await insert("golden_stale", "available");
    await driver.jobCompleteMany([
      completion({
        attempt: 1,
        attemptedBy: "client<1>",
        finalizedAt: now,
        id: stale,
        kind: "complete",
        output: { n: 3, text: "<late>\u2028" },
        outputSet: true,
      }),
    ]);
    expect(stored("golden_stale")).toMatchObject({
      metadata: { m: "x", output: { n: 3, text: "<late>\u2028" } },
      state: "available",
    });

    // Rescue adds a rescue count, or increments an earlier one.
    const rescueError = {
      at: now,
      attempt: 1,
      error: "Stuck job rescued by JobRescuer",
      trace: "",
    };
    const storedRescueError = { ...rescueError, at: now.toString() };
    const first = await insert("golden_rescue_first", "running");
    const again = await insert("golden_rescue_again", "running", {
      metadata: { m: "x", "river:rescue_count": 2 },
    });
    await driver.jobRescueMany(
      [
        {
          error: rescueError,
          finalizedAt: null,
          id: first,
          scheduledAt: now,
          state: "retryable",
        },
        {
          error: rescueError,
          finalizedAt: now,
          id: again,
          scheduledAt: now,
          state: "discarded",
        },
      ],
      now
    );
    expect(stored("golden_rescue_first")).toMatchObject({
      errors: [storedRescueError],
      metadata: { m: "x", "river:rescue_count": 1 },
      state: "retryable",
    });
    expect(stored("golden_rescue_again")).toMatchObject({
      errors: [storedRescueError],
      metadata: { m: "x", "river:rescue_count": 3 },
      state: "discarded",
    });

    // The scheduler discards a job whose unique key another job holds.
    for (const state of ["available", "scheduled"] as const) {
      await driver.jobInsert({
        args: {},
        createdAt,
        kind: `golden_schedule_${state}`,
        metadata: { m: "x" },
        scheduledAt: createdAt,
        state,
        uniqueKey: new Uint8Array(32).fill(7),
        uniqueStates: ["available"],
      });
    }
    await driver.jobSchedule({ now });
    expect(stored("golden_schedule_scheduled")).toMatchObject({
      metadata: { m: "x", unique_key_conflict: "scheduler_discarded" },
      state: "discarded",
    });

    // Queue metadata on creation and on update.
    await driver.queueUpsert("golden_queue", {
      metadata: { n: 1.5, note: "<q> & \u2028" },
      now,
    });
    expect(queueMetadata()).toEqual({ n: 1.5, note: "<q> & \u2028" });
    await driver.queueUpdate("golden_queue", {
      metadata: { list: [1, "a"], updated: "<u>" },
    });
    expect(queueMetadata()).toEqual({ list: [1, "a"], updated: "<u>" });
  });

  test("merges completion metadata into stored metadata like Go", async () => {
    const { database, driver } = await setup();
    database.exec(`
      INSERT INTO river_job (
        args, attempt, attempted_at, attempted_by, kind, max_attempts,
        metadata, state
      ) VALUES (
        jsonb('{}'), 1, '2026-01-02 03:04:05.123', jsonb('["client<1>"]'),
        'golden_patch', 25, jsonb('{"b": 1, "1": 2}'), 'running'
      )`);
    const id = rawRow(
      database,
      "SELECT id FROM river_job WHERE kind = 'golden_patch'"
    )?.id as bigint;

    await driver.jobCompleteMany([
      completion({
        attempt: 1,
        attemptedBy: "client<1>",
        finalizedAt: Temporal.Now.instant(),
        id,
        kind: "complete",
        metadata: { "river:resumable_step": "s", "10": 1, "9": 2 },
        output: { b: 1, a: 2 },
        outputSet: true,
      }),
    ]);

    expect(storedJson(database, "golden_patch", ["metadata"])).toEqual({
      metadata: {
        "1": 2,
        "10": 1,
        "9": 2,
        b: 1,
        output: { a: 2, b: 1 },
        "river:resumable_step": "s",
      },
    });
  });

  test("writes NULL for absent client IDs and errors like Go", async () => {
    const { database, driver } = await setup();
    const absent = await driver.jobInsert({ args: {}, kind: "absent" });
    // Go's full insert writes an explicitly empty client ID list as `[]`.
    const empty = await driver.jobInsert({
      args: {},
      attemptedBy: [],
      errors: [],
      kind: "empty",
    });

    const columns = (id: bigint) =>
      rawRow(
        database,
        `SELECT json(attempted_by) AS attempted_by, errors FROM river_job
         WHERE id = ${id}`
      );
    expect(columns(absent.job.id)).toEqual({
      attempted_by: null,
      errors: null,
    });
    expect(columns(empty.job.id)).toEqual({ attempted_by: "[]", errors: null });
    expect(absent.job).toMatchObject({ attemptedBy: [], errors: [] });
  });

  test("persists near-future retries and snoozes as available like Go", async () => {
    const { database, driver } = await setup();
    const snoozing = (await driver.jobInsert({ args: {}, kind: "fast_path" }))
      .job;
    const failing = (await driver.jobInsert({ args: {}, kind: "fast_path" }))
      .job;
    await driver.jobClaim({
      attemptedBy: "js-worker",
      kinds: ["fast_path"],
      queues: [{ limit: 2, name: "default" }],
    });
    const notificationsBefore = rawRow(
      database,
      "SELECT count(*) AS count FROM river_notification"
    )?.count;
    const scheduledAt = Temporal.Now.instant()
      .round({ roundingMode: "ceil", smallestUnit: "millisecond" })
      .add({ seconds: 1 });
    const common = {
      attempt: 1,
      attemptedBy: "js-worker",
      available: true,
      scheduledAt,
    };

    const results = await driver.jobCompleteMany([
      completion({ ...common, id: snoozing.id, kind: "snooze" }),
      completion({
        ...common,
        error: { at: PRECISION_TEST_TIME, error: "boom", trace: "" },
        id: failing.id,
        kind: "retry",
      }),
    ]);

    // Like Go's `JobSetStateSnoozedAvailable`, a snooze refunds the attempt.
    expect(results[0]).toMatchObject({
      job: { attempt: 0, errors: [], scheduledAt, state: "available" },
      status: "applied",
    });
    // Like `JobSetStateErrorAvailable`, an error keeps it.
    expect(results[1]).toMatchObject({
      job: { attempt: 1, errors: [{ error: "boom" }], state: "available" },
      status: "applied",
    });
    // Neither is claimable, nor announced, before its scheduled time.
    await expect(
      driver.jobClaim({
        attemptedBy: "js-worker",
        kinds: ["fast_path"],
        queues: [{ limit: 2, name: "default" }],
      })
    ).resolves.toEqual({ jobs: [] });
    expect(
      rawRow(database, "SELECT count(*) AS count FROM river_notification")
        ?.count
    ).toBe(notificationsBefore);

    const completed = (await driver.jobInsert({ args: {}, kind: "fast_path" }))
      .job;
    await expect(
      driver.jobCompleteMany([
        completion({
          attempt: 1,
          attemptedBy: "js-worker",
          available: true,
          finalizedAt: PRECISION_TEST_TIME,
          id: completed.id,
          kind: "complete",
        }),
      ])
    ).rejects.toBeInstanceOf(ValidationError);
  });

  test("pages list cursors across tied timestamps", async () => {
    const { driver } = await setup();
    const ids: bigint[] = [];
    for (let index = 0; index < 5; index++) {
      ids.push(
        (
          await driver.jobInsert({
            args: {},
            kind: "tied_cursor",
            scheduledAt: PRECISION_TEST_TIME,
          })
        ).job.id
      );
    }

    for (const sortField of ["scheduledAt", "time"] as const) {
      const seen: bigint[] = [];
      let after: JobListParams["after"] = null;
      for (;;) {
        const page = await driver.jobList(
          listParams({
            after,
            kinds: ["tied_cursor"],
            limit: 2,
            sortField,
            states: ["available"],
          })
        );
        seen.push(...page.map(({ id }) => id));
        const last = page.at(-1);
        if (page.length < 2 || last === undefined) break;
        after = {
          id: last.id,
          kind: last.kind,
          queue: last.queue,
          sortField,
          time: last.scheduledAt,
        };
      }
      expect(seen).toEqual(ids);
    }
  });

  test("filters metadata across many pages without missing sparse matches", async () => {
    const { driver } = await setup();
    const matches: bigint[] = [];
    const params = Array.from({ length: 2_500 }, (_, index) => ({
      args: {},
      kind: "sparse_metadata",
      metadata:
        index === 3 || index === 2_400
          ? {
              'quoted"key': true,
              amount: exactJsonNumber("12345678901234567890"),
              tenant: "wanted",
            }
          : { amount: 1, tenant: index % 2 === 0 ? "other" : 7 },
    }));
    const inserted = await driver.jobInsertMany(params);
    matches.push(inserted[3]!.job.id, inserted[2_400]!.job.id);

    const listed = await driver.jobList(
      listParams({
        limit: 10,
        metadata: {
          'quoted"key': true,
          amount: exactJsonNumber("12345678901234567890"),
          tenant: "wanted",
        },
      })
    );
    expect(listed.map(({ id }) => id)).toEqual(matches);

    const firstOnly = await driver.jobList(
      listParams({ limit: 1, metadata: { tenant: "wanted" } })
    );
    expect(firstOnly.map(({ id }) => id)).toEqual([matches[0]]);
    const afterFirst = await driver.jobList(
      listParams({
        after: {
          id: matches[0]!,
          kind: "sparse_metadata",
          queue: "default",
          sortField: "id",
          time: null,
        },
        limit: 5,
        metadata: { tenant: "wanted" },
      })
    );
    expect(afterFirst.map(({ id }) => id)).toEqual([matches[1]]);
  });

  test("wraps only node:sqlite failures and keeps the SQLite message", async () => {
    const { database, driver } = await setup();

    await expect(
      driver.jobInsert({ args: {}, kind: "bad_priority", priority: 9 })
    ).rejects.toBeInstanceOf(ValidationError);
    await expect(
      driver.jobDeleteMany({
        all: false,
        ids: [],
        kinds: [],
        limit: 0,
        priorities: [],
        queues: [],
        states: [],
      })
    ).rejects.toBeInstanceOf(RangeError);

    database.exec("DROP TABLE river_queue");
    const failure = driver.queueGet("missing");
    await expect(failure).rejects.toBeInstanceOf(DatabaseOperationError);
    await expect(failure).rejects.toMatchObject({
      backend: "sqlite",
      cause: expect.objectContaining({ code: "ERR_SQLITE_ERROR" }),
      message: expect.stringContaining("no such table: river_queue"),
      operation: "queue_get",
      retryable: false,
    });
  });
});

describe("SqliteDriver rescue guards against stale snapshots", () => {
  const horizon = PRECISION_TEST_TIME.subtract({ hours: 1 });
  const rescueAt = PRECISION_TEST_TIME.add({ minutes: 1 });

  for (const state of ["cancelled", "discarded", "retryable"] as const) {
    test(`leaves a job completed after the fetch untouched (${state})`, async () => {
      const { driver } = await setup();
      const job = await runningJob(driver, {
        metadata: { "river:rescue_count": 5, something: "else" },
      });
      const stillRunning = await runningJob(driver, {
        metadata: { "river:rescue_count": 5, something: "else" },
      });
      const stuck = await driver.jobGetStuck({ attemptedBefore: horizon });
      expect(stuck.map(({ id }) => id)).toEqual([job.id, stillRunning.id]);

      // The worker completes after the rescuer reads the job but before its
      // rescue write.
      const [completed] = await driver.jobCompleteMany([
        completion({
          attempt: job.attempt,
          attemptedBy: "js-worker",
          finalizedAt: PRECISION_TEST_TIME,
          id: job.id,
          kind: "complete",
          output: { worker: "finished" },
          outputSet: true,
        }),
      ]);
      expect(completed?.job?.state).toBe("completed");
      const completedJob = await driver.jobGet(job.id);

      const finalizedAt = state === "retryable" ? null : rescueAt;
      await driver.jobRescueMany(
        [job, stillRunning].map((row, index) =>
          rescue(row.id, state, index === 0 ? "stale rescue" : "stuck", {
            finalizedAt,
          })
        ),
        horizon
      );

      const rescued = await driver.jobGet(stillRunning.id);
      expect(rescued).toMatchObject({
        finalizedAt,
        metadata: { "river:rescue_count": 6, something: "else" },
        scheduledAt: rescueAt,
        state,
      });
      expect(rescued?.errors.map(({ error }) => error)).toEqual(["stuck"]);
      expect(await driver.jobGet(job.id)).toEqual(completedJob);
    });
  }

  for (const release of ["failed", "interrupted"] as const) {
    test(`leaves a job claimed again after the fetch untouched (${release})`, async () => {
      const { driver } = await setup();
      const job = await runningJob(driver, {
        metadata: { "river:rescue_count": 5 },
      });
      const stuck = await driver.jobGetStuck({ attemptedBefore: horizon });
      expect(stuck.map(({ id }) => id)).toEqual([job.id]);

      // The old worker releases the job and a new worker claims it before the
      // rescuer writes its stale snapshot. The state alone still matches.
      await driver.jobCompleteMany([
        completion({
          attempt: job.attempt,
          attemptedBy: "js-worker",
          error:
            release === "failed"
              ? { at: PRECISION_TEST_TIME, error: "failed", trace: "" }
              : null,
          id: job.id,
          kind: release === "failed" ? "retry" : "interrupt",
          scheduledAt: PRECISION_TEST_TIME,
        }),
      ]);
      if (release === "failed") {
        await driver.jobSchedule({ now: Temporal.Now.instant() });
      }
      const [claimed] = (
        await driver.jobClaim({
          attemptedBy: "new-worker",
          kinds: [],
          queues: [{ limit: 1, name: job.queue }],
        })
      ).jobs;
      expect(claimed?.id).toBe(job.id);
      expect(
        Temporal.Instant.compare(claimed!.attemptedAt!, horizon)
      ).toBeGreaterThan(0);

      await driver.jobRescueMany(
        [rescue(job.id, "retryable", "stale rescue")],
        horizon
      );

      expect(await driver.jobGet(job.id)).toEqual(claimed);
    });
  }

  test("rescues only attempts strictly before the horizon", async () => {
    const { driver } = await setup();
    const jobs: SqliteJobRow[] = [];
    for (const offset of [-1, 0, 1]) {
      jobs.push(
        await runningJob(driver, {
          attemptedAt: horizon.add({ milliseconds: offset }),
        })
      );
    }

    const rescued = await driver.jobRescueMany(
      jobs.map(({ id }) => rescue(id, "retryable", "stuck")),
      horizon
    );

    expect(rescued.map(({ id }) => id)).toEqual([jobs[0]!.id]);
    expect((await driver.jobGet(jobs[0]!.id))?.state).toBe("retryable");
    expect(await driver.jobGet(jobs[1]!.id)).toEqual(jobs[1]);
    expect(await driver.jobGet(jobs[2]!.id)).toEqual(jobs[2]);
  });

  test("applies the guard through the leader-fenced maintenance rescue", async () => {
    const { driver } = await setup();
    const leader = (await driver.maintenanceLeaderAcquire(
      "leader",
      Temporal.Now.instant(),
      60_000,
      null
    ))!;
    const eligible = await runningJob(driver);
    const fresh = await runningJob(driver, {
      attemptedAt: horizon.add({ milliseconds: 1 }),
    });

    await expect(
      driver.maintenanceRescue(leader, horizon, [
        rescue(eligible.id, "retryable", "stuck"),
        rescue(fresh.id, "retryable", "stuck"),
      ])
    ).resolves.toBe(1);
    expect(await driver.jobGet(fresh.id)).toEqual(fresh);
  });

  function rescue(
    id: bigint,
    state: SqliteRescueJobParams["state"],
    error: string,
    options: { finalizedAt?: Temporal.Instant | null } = {}
  ): RuntimeJobRescue {
    return {
      error: { at: PRECISION_TEST_TIME, attempt: 1, error, trace: "" },
      finalizedAt:
        options.finalizedAt === undefined
          ? state === "retryable"
            ? null
            : rescueAt
          : options.finalizedAt,
      id,
      scheduledAt: rescueAt,
      state,
    };
  }
});

function completion(
  overrides: Partial<JobCompletionCommand> &
    Pick<JobCompletionCommand, "attempt" | "attemptedBy" | "id" | "kind">
): JobCompletionCommand {
  return {
    error: null,
    finalizedAt: null,
    output: null,
    outputSet: false,
    scheduledAt: null,
    ...overrides,
  };
}

function listParams(overrides: Partial<JobListParams> = {}): JobListParams {
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

function rawRow(
  database: DatabaseSync,
  sql: string
): Record<string, unknown> | undefined {
  const statement = database.prepare(sql);
  statement.setReadBigInts(true);
  return statement.get();
}

/** Insert a job that a previous worker attempted two hours before the test time. */
async function runningJob(
  driver: SqliteRuntime,
  options: {
    attemptedAt?: Temporal.Instant;
    metadata?: Record<string, unknown>;
  } = {}
): Promise<SqliteJobRow> {
  const attemptedAt =
    options.attemptedAt ?? PRECISION_TEST_TIME.subtract({ hours: 2 });
  return (
    await driver.jobInsert({
      args: {},
      attempt: 1,
      attemptedAt,
      attemptedBy: ["js-worker"],
      kind: "rescue_guard",
      metadata: (options.metadata ?? {}) as SqliteJobRow["metadata"],
      scheduledAt: attemptedAt,
      state: "running",
    })
  ).job;
}

/**
 * Decode `columns` of the `river_job` row of a kind, requiring each one that
 * isn't NULL to be stored as a JSONB blob, as Go's `riversqlite` stores it.
 * Metadata is returned without its random unique nonce.
 */
function storedJson(
  database: DatabaseSync,
  kind: string,
  columns: readonly string[]
): Record<string, JsonValue> {
  const row = rawRow(
    database,
    `SELECT ${columns
      .map(
        (column) =>
          `typeof(${column}) AS "${column}:type", json(${column}) AS "${column}"`
      )
      .join(", ")}
     FROM river_job WHERE kind = '${kind}'`
  );
  if (row === undefined) throw new Error(`no job of kind ${kind}`);
  return Object.fromEntries(
    columns.map((column) => {
      const text = row[column];
      if (text === null) return [column, null];
      expect(row[`${column}:type`], column).toBe("blob");
      const value = parseJson(text as string);
      if (column === "metadata") {
        delete (value as JsonObject)["river:unique_nonce"];
      }
      return [column, value];
    })
  );
}

/** The unique nonce stored in the raw metadata of the job of a kind. */
function uniqueNonce(database: DatabaseSync, kind: string): unknown {
  return rawRow(
    database,
    `SELECT json_extract(metadata, '$."river:unique_nonce"') AS nonce
     FROM river_job WHERE kind = '${kind}'`
  )?.nonce;
}

async function setup(): Promise<{
  database: DatabaseSync;
  driver: SqliteRuntime;
}> {
  const driver = testSqliteMemory(STRICT);
  onTestFinished(() => driver.close());
  const database = driver.database;
  const moduleUrl = new URL("../../../migrate/dist/index.js", import.meta.url);
  const migrationModule = (await import(moduleUrl.href)) as {
    createMigrator(target: { database: DatabaseSync }): {
      migrateUp(): Promise<unknown>;
    };
  };
  await migrationModule.createMigrator({ database }).migrateUp();
  return { database, driver };
}

/** Let every queue's insert notification through, with no limiter. */
function allowEveryQueue(queues: readonly string[]): readonly string[] {
  return [...new Set(queues)];
}
