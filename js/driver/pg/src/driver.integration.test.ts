import { readdir, readFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import pg from "pg";
import {
  Client,
  defineJob,
  isExactJsonNumber,
  type JobRow,
  type JobState,
  type JsonObject,
  periodicJob,
  ValidationError,
  Workers,
} from "riverqueue";
import type {
  JobInsertParams,
  JobListCursorValue,
} from "riverqueue/unstable-driver";
import {
  decodeJobListCursor,
  encodeJobListCursor,
  jobListCursorValue,
} from "riverqueue/unstable-driver";
import { PgDriver, type PgRuntime, testPgDriver } from "./driver.js";
import type { PgJobRescue } from "./types.js";

const TEST_DATABASE_URL =
  process.env.TEST_DATABASE_URL ??
  "postgres://localhost:5432/river_test?sslmode=disable";
const filePrefix = `js_pg_${Math.random().toString(36).slice(2, 10)}`;
const migrationDirectory = fileURLToPath(
  new URL("../../../migrate/migrations/postgres/main/", import.meta.url)
);

async function migrateSchema(pool: pg.Pool, schema: string): Promise<void> {
  const migrationFiles = (await readdir(migrationDirectory))
    .filter((name) => name.endsWith(".up.sql"))
    .sort();
  const schemaPrefix = `"${schema}".`;

  for (const migrationFile of migrationFiles) {
    const migration = await readFile(
      join(migrationDirectory, migrationFile),
      "utf8"
    );
    await pool.query(
      migration.replaceAll("/* TEMPLATE: schema */", schemaPrefix)
    );
  }
}

function insertParams(
  kind: string,
  overrides: Partial<JobInsertParams> = {}
): JobInsertParams {
  return {
    args: { value: kind },
    encodedArgs: JSON.stringify({ value: kind }),
    kind,
    maxAttempts: 25,
    metadata: {},
    priority: 1,
    queue: "default",
    scheduledAt: Temporal.Now.instant(),
    state: "available",
    tags: [],
    uniqueKey: null,
    uniqueStates: null,
    ...overrides,
  };
}

describe("PgDriver integration", () => {
  let driver: PgRuntime;
  let pool: pg.Pool;

  beforeAll(async () => {
    pool = new pg.Pool({ connectionString: TEST_DATABASE_URL });
    driver = testPgDriver(pool);
    // Maintenance operations act on every row, not just this file's kinds,
    // so start from empty River tables whatever ran against the database
    // before (such as the packed Postgres examples).
    await pool.query("TRUNCATE river_job, river_leader, river_queue");
  });

  afterAll(async () => {
    await pool.end();
  });

  afterEach(async () => {
    await pool.query("DELETE FROM river_job WHERE kind LIKE $1", [
      `${filePrefix}%`,
    ]);
    await pool.query("DELETE FROM river_queue WHERE name LIKE $1", [
      `${filePrefix}%`,
    ]);
  });

  it("inserts and decodes exact IDs and microsecond timestamps", async () => {
    const scheduledAt = Temporal.Instant.from("2026-08-30T12:00:00.123456Z");

    const result = await driver.jobInsert(
      insertParams(`${filePrefix}_exact`, {
        metadata: { source: "javascript" },
        scheduledAt,
        state: "scheduled",
      })
    );

    expect(result.status).toBe("inserted");
    expect(typeof result.job.id).toBe("bigint");
    expect(result.job.scheduledAt.toString()).toBe(
      "2026-08-30T12:00:00.123456Z"
    );
    expect(result.job.metadata).toEqual({ source: "javascript" });

    const fetched = await driver.jobGet(result.job.id);
    expect(fetched!.id).toBe(result.job.id);
    expect(fetched!.scheduledAt.toString()).toBe("2026-08-30T12:00:00.123456Z");
  });

  it("leaves an unscheduled job's times to the database like Go", async () => {
    const { scheduledAt: _unused, ...unscheduled } = insertParams(
      `${filePrefix}_db_time`
    );
    void _unused;
    const plain = new Client(driver);

    const [direct, viaClient] = await Promise.all([
      driver.jobInsert(unscheduled),
      plain.insert(defineJob({ kind: `${filePrefix}_db_time_client` }), {}),
    ]);

    for (const { job } of [direct, viaClient]) {
      const row = (
        await pool.query<{ same: boolean }>(
          "SELECT created_at = scheduled_at AS same FROM river_job WHERE id = $1",
          [job.id.toString(10)]
        )
      ).rows[0];
      expect(row?.same).toBe(true);
      expect(job.scheduledAt.equals(job.createdAt)).toBe(true);
    }
  });

  it("truncates sub-microsecond timestamps like Go instead of rounding", async () => {
    // Postgres would round `.0000019` up to `.000002`; Go's pgx truncates.
    const scheduledAt = Temporal.Instant.from("2026-08-30T12:00:00.0000019Z");
    const createdAt = Temporal.Instant.from("2026-08-30T11:00:00.0000015Z");

    const inserted = await driver.jobInsertMany([
      insertParams(`${filePrefix}_truncate`, {
        createdAt,
        scheduledAt,
        state: "scheduled",
      }),
    ]);

    expect(inserted[0]!.job.scheduledAt.toString()).toBe(
      "2026-08-30T12:00:00.000001Z"
    );
    expect(inserted[0]!.job.createdAt.toString()).toBe(
      "2026-08-30T11:00:00.000001Z"
    );
  });

  it("keeps a reinserted job's creation time and encoded arguments", async () => {
    const createdAt = Temporal.Instant.from("2026-01-02T03:04:05.123456Z");
    // Postgres's clock rounds to microseconds.
    const before = Temporal.Now.instant().subtract({ seconds: 1 });

    const params = [
      insertParams(`${filePrefix}_reinserted`, {
        args: { b: 1, a: 2 },
        createdAt,
        encodedArgs: '{"b": 1, "a": 2}',
      }),
      insertParams(`${filePrefix}_new`),
    ];
    const inserted = await driver.jobInsertMany(params);

    for (const results of [inserted]) {
      expect(results[0]!.job.createdAt).toEqual(createdAt);
      expect(results[0]!.job.args).toEqual({ a: 2, b: 1 });
      expect(
        Temporal.Instant.compare(results[1]!.job.createdAt, before)
      ).toBeGreaterThanOrEqual(0);
      expect(results[1]!.job).toMatchObject({ attemptedBy: [], errors: [] });
    }
    const { rows } = await pool.query<{ nulls: boolean }>(
      `SELECT attempted_by IS NULL AND errors IS NULL AS nulls
       FROM river_job WHERE kind LIKE $1`,
      [`${filePrefix}_%`]
    );
    expect(rows.map(({ nulls }) => nulls)).toEqual([true, true]);
  });

  it("gets IDs outside JavaScript's safe-number range exactly", async () => {
    const exactID = 9_007_199_254_740_993n;
    await pool.query(
      `
        INSERT INTO river_job (id, args, kind, max_attempts)
        VALUES ($1::bigint, '{}'::jsonb, $2::text, 25)
      `,
      [exactID.toString(10), `${filePrefix}_large_id`]
    );

    const fetched = await driver.jobGet(exactID);

    expect(fetched!.id).toBe(exactID);
    expect(typeof fetched!.id).toBe("bigint");
  });

  it("preserves exact JSON numbers inserted by other engines", async () => {
    const result = await pool.query<{ id: string }>(
      `
        INSERT INTO river_job (args, kind, max_attempts, metadata)
        VALUES ($1::jsonb, $2::text, 25, $3::jsonb)
        RETURNING id::text AS id
      `,
      [
        '{"decimal":0.1234567890123456789,"integer":9223372036854775807}',
        `${filePrefix}_exact_json`,
        '{"underflow":1e-400}',
      ]
    );
    const fetched = await driver.jobGet(BigInt(result.rows[0]!.id));

    expect(isExactJsonNumber(fetched!.args.decimal)).toBe(true);
    expect(isExactJsonNumber(fetched!.args.integer)).toBe(true);
    expect(isExactJsonNumber(fetched!.metadata.underflow)).toBe(true);
    expect(JSON.stringify(fetched!.args)).toBe(
      '{"decimal":0.1234567890123456789,"integer":9223372036854775807}'
    );
  });

  it("decodes exact timestamps nested in Postgres jsonb arrays", async () => {
    const inserted = await driver.jobInsert(
      insertParams(`${filePrefix}_attempt_error`)
    );
    await pool.query(
      `
        UPDATE river_job
        SET errors = ARRAY[$2::jsonb]
        WHERE id = $1::bigint
      `,
      [
        inserted.job.id.toString(10),
        JSON.stringify({
          at: "2026-08-30T12:00:00.123456Z",
          attempt: 1,
          error: "failed",
          trace: "trace",
        }),
      ]
    );

    const fetched = await driver.jobGet(inserted.job.id);

    expect(fetched!.errors[0]!.at.toString()).toBe(
      "2026-08-30T12:00:00.123456Z"
    );
  });

  it("fails a batch repeating an active unique key without writing it", async () => {
    const uniqueKey = Uint8Array.from([1, 2, 3, 4, 5, 6]);
    const unique = {
      uniqueKey,
      uniqueStates: [
        "available",
        "completed",
        "pending",
        "retryable",
        "running",
        "scheduled",
      ],
    } as const;
    await expect(
      driver.jobInsertMany([
        insertParams(`${filePrefix}_batch_unique`, unique),
        insertParams(`${filePrefix}_other`),
        insertParams(`${filePrefix}_batch_unique`, unique),
      ])
    ).rejects.toMatchObject({
      // Postgres's cardinality_violation: ON CONFLICT DO UPDATE can't
      // affect a row twice, as for River for Go's batch.
      cause: expect.objectContaining({ code: "21000" }),
    });

    const count = await pool.query<{ count: string }>(
      "SELECT count(*) FROM river_job WHERE kind = ANY($1)",
      [[`${filePrefix}_batch_unique`, `${filePrefix}_other`]]
    );
    expect(count.rows[0]!.count).toBe("0");
  });

  it("rejects a client batch repeating an active unique key", async () => {
    const job = defineJob<{ value: string }>()({
      kind: `${filePrefix}_client_batch_unique`,
    });
    const client = new Client(driver);
    const item = {
      args: { value: "same" },
      job,
      options: { unique: { byArgs: true } },
    } as const;

    await expect(client.insertMany([item, item])).rejects.toThrow(
      new ValidationError("unique key appears more than once in batch")
    );

    const count = await pool.query<{ count: string }>(
      "SELECT count(*) FROM river_job WHERE kind = $1",
      [job.kind]
    );
    expect(count.rows[0]!.count).toBe("0");
  });

  it("keeps the existing job's kind on a unique skip of another kind", async () => {
    const jobA = defineJob<{ value: string }>()({
      kind: `${filePrefix}_unique_kind_a`,
    });
    const jobB = defineJob<{ value: string }>()({
      kind: `${filePrefix}_unique_kind_b`,
    });
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
    const stored = await pool.query<{ kind: string }>(
      "SELECT kind FROM river_job WHERE kind = ANY($1)",
      [[jobA.kind, jobB.kind]]
    );
    expect(stored.rows).toEqual([{ kind: jobA.kind }]);
  });

  it("ordinarily inserts beyond Postgres's bind-parameter limit", async () => {
    const params = Array.from({ length: 6_000 }, (_, index) =>
      insertParams(`${filePrefix}_ordinary_bulk`, {
        args: { index },
        encodedArgs: JSON.stringify({ index }),
        tags: [`bulk-${index % 3}`],
      })
    );

    const results = await driver.jobInsertMany(params);

    expect(results).toHaveLength(params.length);
    expect(results.every(({ status }) => status === "inserted")).toBe(true);
    expect(results[5_999]!.job.args).toEqual({ index: 5_999 });
    expect(results[5_999]!.job.tags).toEqual(["bulk-2"]);
    // Inserting and decoding 6,000 rows takes several seconds on a busy CI
    // runner.
  }, 30_000);

  it("does not conflate active and inactive uniqueness masks", async () => {
    const uniqueKey = Uint8Array.from([7, 7, 7, 7]);
    const results = await driver.jobInsertMany([
      insertParams(`${filePrefix}_mixed_unique`, {
        uniqueKey,
        uniqueStates: [
          "available",
          "completed",
          "pending",
          "retryable",
          "running",
          "scheduled",
        ],
      }),
      insertParams(`${filePrefix}_mixed_unique`, {
        uniqueKey,
        uniqueStates: ["scheduled"],
      }),
    ]);

    expect(results.map(({ status }) => status)).toEqual([
      "inserted",
      "inserted",
    ]);
    expect(results[0]!.job.id).not.toBe(results[1]!.job.id);
  });

  it("keeps transaction operations on the caller-owned connection", async () => {
    const transaction = await pool.connect();
    let insertedID: bigint | undefined;
    try {
      await transaction.query("BEGIN");
      const result = await driver.jobInsert(
        insertParams(`${filePrefix}_transaction`),
        { tx: transaction }
      );
      insertedID = result.job.id;

      const inside = await driver.jobGet(result.job.id, { tx: transaction });
      const outside = await driver.jobGet(result.job.id);
      expect(inside).not.toBeNull();
      expect(outside).toBeNull();

      await transaction.query("ROLLBACK");
    } finally {
      transaction.release();
    }

    expect(insertedID).toBeDefined();
    await expect(driver.jobGet(insertedID)).resolves.toBeNull();
    await expect(pool.query("SELECT 1")).resolves.toBeDefined();
  });

  it("rolls a non-transactional insert back when middleware or a hook throws", async () => {
    const definition = defineJob({ kind: `${filePrefix}_scope_rollback` });
    const failure = new Error("fails after the write");
    let failIn: "afterInsert" | "middleware" | null = null;
    const client = new Client(driver, {
      hooks: {
        afterInsert: () => {
          if (failIn === "afterInsert") throw failure;
        },
      },
      insertMiddleware: [
        async (_context, next) => {
          const results = await next();
          if (failIn === "middleware") throw failure;
          return results;
        },
      ],
    });
    const count = async () =>
      (
        await pool.query<{ count: string }>(
          "SELECT count(*)::text AS count FROM river_job WHERE kind = $1",
          [definition.kind]
        )
      ).rows[0]?.count;

    for (const where of ["middleware", "afterInsert"] as const) {
      failIn = where;
      await expect(client.insert(definition, {})).rejects.toBe(failure);
      await expect(
        client.insertMany([{ args: {}, job: definition }])
      ).rejects.toBe(failure);
    }
    expect(await count()).toBe("0");

    failIn = null;
    await client.insert(definition, {});
    expect(await count()).toBe("1");
    await expect(pool.query("SELECT 1")).resolves.toBeDefined();
  });

  it("cancels queued and running jobs with canonical semantics", async () => {
    const queued = await driver.jobInsert(
      insertParams(`${filePrefix}_cancel_queued`)
    );
    const running = await driver.jobInsert(
      insertParams(`${filePrefix}_cancel_running`)
    );
    await pool.query(
      "UPDATE river_job SET state = 'running' WHERE id = $1::bigint",
      [running.job.id.toString(10)]
    );
    const cancelAttemptedAt = Temporal.Instant.from(
      "2026-08-30T12:00:00.123456Z"
    );
    const now = Temporal.Instant.from("2026-08-30T12:01:00.654321Z");

    const cancelled = await driver.jobCancelWithOptions({
      cancelAttemptedAt,
      controlTopic: "river_control",
      id: queued.job.id,
      now,
    });
    const marked = await driver.jobCancelWithOptions({
      cancelAttemptedAt,
      controlTopic: "river_control",
      id: running.job.id,
      now,
    });

    expect(cancelled!.state).toBe("cancelled");
    expect(cancelled!.finalizedAt!.toString()).toBe(
      "2026-08-30T12:01:00.654321Z"
    );
    expect(cancelled!.metadata.cancel_attempted_at).toBe(
      "2026-08-30T12:00:00.123456Z"
    );
    expect(marked!.state).toBe("running");
    expect(marked!.finalizedAt).toBeNull();
    expect(marked!.metadata.cancel_attempted_at).toBe(
      "2026-08-30T12:00:00.123456Z"
    );

    // A client without notifications finds the running one by polling.
    const unmarked = await driver.jobInsert(
      insertParams(`${filePrefix}_cancel_unmarked`)
    );
    await pool.query(
      "UPDATE river_job SET state = 'running' WHERE id = $1::bigint",
      [unmarked.job.id.toString(10)]
    );
    await expect(
      driver.jobGetCancelRequested([
        queued.job.id,
        running.job.id,
        unmarked.job.id,
      ])
    ).resolves.toEqual([running.job.id]);
    await expect(driver.jobGetCancelRequested([])).resolves.toEqual([]);
  });

  it("retries non-running jobs but leaves running jobs unchanged", async () => {
    const scheduledAt = Temporal.Instant.from("2026-08-30T15:00:00Z");
    const now = Temporal.Instant.from("2026-08-30T12:00:00.123456Z");
    const retryable = await driver.jobInsert(
      insertParams(`${filePrefix}_retry`, {
        scheduledAt,
        state: "retryable",
      })
    );
    const running = await driver.jobInsert(
      insertParams(`${filePrefix}_retry_running`)
    );
    await pool.query(
      "UPDATE river_job SET state = 'running' WHERE id = $1::bigint",
      [running.job.id.toString(10)]
    );

    const retried = await driver.jobRetryWithOptions({
      id: retryable.job.id,
      now,
    });
    const unchanged = await driver.jobRetryWithOptions({
      id: running.job.id,
      now,
    });

    expect(retried!.state).toBe("available");
    expect(retried!.scheduledAt.toString()).toBe("2026-08-30T12:00:00.123456Z");
    expect(unchanged!.state).toBe("running");
  });

  it("returns the committed row to the loser of a cancel or retry race", async () => {
    const inserted = await driver.jobInsert(
      insertParams(`${filePrefix}_race`, {
        scheduledAt: Temporal.Now.instant().add({ hours: 1 }),
        state: "scheduled",
      })
    );
    const id = inserted.job.id;
    const observer = new pg.Client({ connectionString: TEST_DATABASE_URL });
    await observer.connect();
    try {
      for (const [operation, lockedCte] of [
        ["jobCancel", "locked_job"],
        ["jobRetry", "job_to_update"],
      ] as const) {
        const winner = await pool.connect();
        try {
          await winner.query("BEGIN");
          const won = await driver[operation](id, { tx: winner });
          // The loser's statement starts while the winner holds the row lock.
          const losing = driver[operation](id);
          await waitFor(async () => {
            const waiting = await observer.query(
              `SELECT 1 FROM pg_stat_activity
               WHERE wait_event_type = 'Lock' AND query LIKE $1`,
              [`%${lockedCte}%`]
            );
            return (waiting.rowCount ?? 0) > 0;
          });
          await winner.query("COMMIT");

          // Like River for Go, the loser returns the winner's committed row,
          // not the row as its statement first saw it.
          const lost = await losing;
          expect(lost).toEqual(won);
          expect(await driver.jobGet(id)).toEqual(won);
        } finally {
          await winner.query("ROLLBACK").catch(() => undefined);
          winner.release();
        }
      }
    } finally {
      await observer.end();
    }
  });

  it("protects running jobs from deletion", async () => {
    const deletable = await driver.jobInsert(
      insertParams(`${filePrefix}_delete`)
    );
    const running = await driver.jobInsert(
      insertParams(`${filePrefix}_delete_running`)
    );
    await pool.query(
      "UPDATE river_job SET state = 'running' WHERE id = $1::bigint",
      [running.job.id.toString(10)]
    );

    await expect(driver.jobDelete(deletable.job.id)).resolves.toMatchObject({
      status: "deleted",
    });
    await expect(driver.jobDelete(running.job.id)).resolves.toMatchObject({
      status: "running",
    });
    await expect(driver.jobDelete(9_223_372_036_854_775_807n)).resolves.toEqual(
      {
        status: "not_found",
      }
    );
  });

  it("acts on rows River can't fully read by ID and lists them like Go", async () => {
    const kind = `${filePrefix}_poison`;
    const uniqueKey = new Uint8Array(32).fill(13);
    const cancelled = await driver.jobInsert(insertParams(kind));
    const retried = await driver.jobInsert(
      insertParams(kind, {
        scheduledAt: Temporal.Now.instant().add({ hours: 1 }),
        state: "retryable",
      })
    );
    const deleted = await driver.jobInsert(insertParams(kind));
    const duplicate = await driver.jobInsert(
      insertParams(kind, { uniqueKey, uniqueStates: ["available"] })
    );
    // Another engine stored array arguments, which Go keeps as raw bytes.
    await pool.query(
      "UPDATE river_job SET args = '[1, 2]'::jsonb WHERE kind = $1",
      [kind]
    );

    await expect(driver.jobGet(cancelled.job.id)).rejects.toThrow("args");
    const listed = await driver.jobList({
      after: null,
      ids: [cancelled.job.id, deleted.job.id],
      kinds: [],
      limit: 10,
      metadata: null,
      priorities: [],
      queues: [],
      sortDirection: "asc",
      sortField: "id",
      states: [],
      tagsAll: [],
      tagsAny: [],
    });
    expect(listed.map(({ args, id }) => [id, args])).toEqual([
      [cancelled.job.id, {}],
      [deleted.job.id, {}],
    ]);
    expect((await driver.jobCancel(cancelled.job.id))?.state).toBe("cancelled");
    expect((await driver.jobRetry(retried.job.id))?.state).toBe("available");
    await expect(driver.jobDelete(deleted.job.id)).resolves.toMatchObject({
      job: { id: deleted.job.id },
      status: "deleted",
    });
    const reinserted = await driver.jobInsertMany([
      insertParams(kind, { uniqueKey, uniqueStates: ["available"] }),
    ]);
    expect(reinserted.map(({ job, status }) => [job.id, status])).toEqual([
      [duplicate.job.id, "duplicate"],
    ]);
  });

  it("bulk deletes matching non-running jobs and preserves running races", async () => {
    const queue = `${filePrefix}_bulk_delete`;
    const matching = await driver.jobInsert(
      insertParams(`${filePrefix}_bulk_kind`, { priority: 1, queue })
    );
    const wrongPriority = await driver.jobInsert(
      insertParams(`${filePrefix}_bulk_kind`, { priority: 2, queue })
    );
    const wrongKind = await driver.jobInsert(
      insertParams(`${filePrefix}_bulk_other`, { priority: 1, queue })
    );
    const running = await driver.jobInsert(
      insertParams(`${filePrefix}_bulk_kind`, { priority: 1, queue })
    );
    await pool.query(
      "UPDATE river_job SET state = 'running', attempt = 1, attempted_at = now(), attempted_by = ARRAY[$2::text] WHERE id = $1::bigint",
      [running.job.id.toString(10), `${filePrefix}_bulk_worker`]
    );

    const deleted = await driver.jobDeleteMany({
      all: false,
      ids: [],
      kinds: [`${filePrefix}_bulk_kind`],
      limit: 100,
      priorities: [1],
      queues: [queue],
      states: ["available", "running"],
    });

    expect(deleted.map(({ id }) => id)).toEqual([matching.job.id]);
    await expect(driver.jobGet(wrongPriority.job.id)).resolves.not.toBeNull();
    await expect(driver.jobGet(wrongKind.job.id)).resolves.not.toBeNull();
    await expect(driver.jobGet(running.job.id)).resolves.toMatchObject({
      state: "running",
    });
  });

  it("gets, lists, pauses, resumes, and updates queues", async () => {
    const names = [`${filePrefix}_a`, `${filePrefix}_b`];
    await pool.query(
      `
        INSERT INTO river_queue (name, metadata, updated_at)
        VALUES
          ($1::text, '{}'::jsonb, now()),
          ($2::text, '{}'::jsonb, now())
      `,
      names
    );
    const pauseAt = Temporal.Instant.from("2026-08-30T12:00:00.123456Z");
    const resumeAt = Temporal.Instant.from("2026-08-30T13:00:00.654321Z");

    expect((await driver.queueGet(names[0]!))!.name).toBe(names[0]);
    const listed = await driver.queueList({ limit: 10_000, nameAfter: null });
    expect(
      listed.filter(({ name }) => names.includes(name)).map(({ name }) => name)
    ).toEqual(names);

    await expect(
      driver.queuePauseWithOptions({ name: names[0]!, now: pauseAt })
    ).resolves.toBe(1);
    // Like Go, pausing an already paused queue matches it but keeps its
    // original pause and update times.
    await expect(
      driver.queuePauseWithOptions({ name: names[0]!, now: resumeAt })
    ).resolves.toBe(1);
    const paused = await driver.queueGet(names[0]!);
    expect(paused!.pausedAt!.toString()).toBe("2026-08-30T12:00:00.123456Z");
    expect(paused!.updatedAt.toString()).toBe("2026-08-30T12:00:00.123456Z");
    await expect(driver.queuePause(names[0]!)).resolves.toMatchObject({
      name: names[0],
      pausedAt: pauseAt,
    });

    await expect(
      driver.queueResumeWithOptions({ name: names[0]!, now: resumeAt })
    ).resolves.toBe(1);
    await expect(
      driver.queueResumeWithOptions({ name: names[0]!, now: pauseAt })
    ).resolves.toBe(1);
    const resumed = await driver.queueGet(names[0]!);
    expect(resumed!.pausedAt).toBeNull();
    expect(resumed!.updatedAt.toString()).toBe("2026-08-30T13:00:00.654321Z");

    const updated = await driver.queueUpdate(names[0]!, {
      metadata: { owner: "javascript" },
    });
    expect(updated!.metadata).toEqual({ owner: "javascript" });

    await expect(
      driver.queuePauseWithOptions({ name: `${filePrefix}_missing` })
    ).resolves.toBe(0);
    await expect(
      driver.queueResumeWithOptions({ name: `${filePrefix}_missing` })
    ).resolves.toBe(0);
    await expect(
      driver.queueUpdate(`${filePrefix}_missing`, {})
    ).resolves.toBeNull();
  });

  it("pauses every queue with one row each and one notification", async () => {
    const names = [1, 2, 3].map((index) => `${filePrefix}_all_${index}`);
    for (const name of names) {
      await pool.query(
        "INSERT INTO river_queue (name, metadata) VALUES ($1::text, '{}')",
        [name]
      );
    }
    const abort = new AbortController();
    const notifications = driver.listen(["river_control"], abort.signal);
    const first = notifications.next();
    await waitForListenerPID(pool, "river_control");

    const paused = await driver.queuePauseWithOptions({ name: "*" });
    const all = await pool.query<{ count: number }>(
      "SELECT count(*)::int AS count FROM river_queue"
    );
    // Previously every queue row was repeated once per notification.
    expect(paused).toBe(all.rows[0]!.count);
    expect(JSON.parse((await first).value!.payload)).toEqual({
      action: "pause",
      queue: "*",
    });
    // Resuming everything sends exactly one more notification.
    const second = notifications.next();
    await driver.queueResumeWithOptions({ name: "*" });
    expect(JSON.parse((await second).value!.payload)).toEqual({
      action: "resume",
      queue: "*",
    });
    abort.abort();
    await notifications.return(undefined);
  });

  it("notifies producers only for inserted available jobs", async () => {
    const queue = `${filePrefix}_insert_notify`;
    const job = defineJob({ kind: `${filePrefix}_insert_notify` });
    const client = new Client(driver);
    const abort = new AbortController();
    const notifications = driver.listen(["river_insert"], abort.signal);
    const next = notifications.next();
    await waitForListenerPID(pool, "river_insert");

    // Inserting through the driver alone notifies nobody.
    await driver.jobInsert(
      insertParams(`${filePrefix}_insert_notify`, {
        queue: `${queue}_driver`,
      })
    );
    const [scheduled] = await client.insertMany([
      {
        args: {},
        job,
        options: {
          queue: `${queue}_scheduled`,
          scheduledAt: Temporal.Now.instant().add({ hours: 1 }),
        },
      },
      { args: {}, job, options: { pending: true, queue: `${queue}_pending` } },
    ]);
    await client.insert(job, {}, { queue });

    // Notifications arrive in commit order, so the first one proves the
    // earlier inserts sent none.
    expect(JSON.parse((await next).value!.payload)).toEqual({ queue });

    // Like River for Go, a retry that makes the scheduled job available
    // notifies nothing, so the next notification is a later insert's.
    const later = notifications.next();
    await client.jobs.retry(scheduled.job.id);
    await client.insert(job, {}, { queue: `${queue}_later` });
    expect(JSON.parse((await later).value!.payload)).toEqual({
      queue: `${queue}_later`,
    });
    abort.abort();
    await notifications.return(undefined);
  });

  it("announces queue metadata changes", async () => {
    const name = `${filePrefix}_metadata_notify`;
    await pool.query(
      "INSERT INTO river_queue (name, metadata) VALUES ($1::text, '{}')",
      [name]
    );
    const abort = new AbortController();
    const notifications = driver.listen(["river_control"], abort.signal);
    const next = notifications.next();
    await waitForListenerPID(pool, "river_control");

    await driver.queueUpdate(name, {
      metadata: { b: "<x> & y", a: [1, 2.5] },
    });

    expect(JSON.parse((await next).value!.payload)).toEqual({
      action: "metadata_changed",
      metadata: { a: [1, 2.5], b: "<x> & y" },
      queue: name,
    });

    abort.abort();
    await notifications.return(undefined);
  });

  it("claims by queue capacity and rejects stale attempt completions", async () => {
    const first = await driver.jobInsert(
      insertParams(`${filePrefix}_claim_first`)
    );
    const second = await driver.jobInsert(
      insertParams(`${filePrefix}_claim_second`)
    );

    const claimed = (
      await driver.jobClaim({
        attemptedBy: `${filePrefix}_worker`,
        kinds: [`${filePrefix}_claim_first`, `${filePrefix}_claim_second`],
        queues: [{ limit: 2, name: "default" }],
      })
    ).jobs;
    const ours = claimed.filter(({ id }) =>
      [first.job.id, second.job.id].includes(id)
    );
    expect(ours).toHaveLength(2);
    expect(
      ours.every(({ attempt, state }) => attempt === 1 && state === "running")
    ).toBe(true);

    const stale = await driver.jobCompleteMany([
      {
        attempt: 2,
        attemptedBy: `${filePrefix}_worker`,
        error: null,
        id: first.job.id,
        kind: "complete",
        finalizedAt: Temporal.Now.instant(),
        output: null,
        outputSet: false,
        scheduledAt: null,
      },
      {
        attempt: 1,
        attemptedBy: `${filePrefix}_other_worker`,
        error: null,
        id: second.job.id,
        kind: "complete",
        finalizedAt: Temporal.Now.instant(),
        output: null,
        outputSet: false,
        scheduledAt: null,
      },
    ]);
    expect(stale.map(({ status }) => status)).toEqual(["stale", "stale"]);
    expect((await driver.jobGet(first.job.id))!.state).toBe("running");
    expect((await driver.jobGet(second.job.id))!.state).toBe("running");

    const capturedFinalizedAt = Temporal.Instant.from(
      "2026-08-30T18:30:01.123456789Z"
    );
    const applied = await driver.jobCompleteMany([
      {
        attempt: 1,
        attemptedBy: `${filePrefix}_worker`,
        error: null,
        id: first.job.id,
        kind: "complete",
        finalizedAt: capturedFinalizedAt,
        output: { engine: "javascript" },
        outputSet: true,
        scheduledAt: null,
      },
    ]);
    expect(applied[0]).toMatchObject({ status: "applied" });
    expect(applied[0]!.job!.state).toBe("completed");
    // Truncated to microseconds like Go's pgx, not rounded.
    expect(applied[0]!.job!.finalizedAt?.toString()).toBe(
      "2026-08-30T18:30:01.123456Z"
    );
    expect(applied[0]!.job!.metadata.output).toEqual({
      engine: "javascript",
    });

    await pool.query(
      "UPDATE river_job SET state = 'discarded', finalized_at = now() WHERE id = $1::bigint",
      [second.job.id.toString(10)]
    );
    const raced = await driver.jobCompleteMany([
      {
        attempt: 1,
        attemptedBy: `${filePrefix}_worker`,
        error: null,
        id: second.job.id,
        kind: "complete",
        finalizedAt: Temporal.Now.instant(),
        output: null,
        outputSet: false,
        scheduledAt: null,
      },
    ]);
    expect(raced[0]).toMatchObject({ status: "stale" });
    expect(raced[0]!.job!.state).toBe("discarded");
  });

  it("claims by priority, then scheduled time, then ID, like Go", async () => {
    const queue = `${filePrefix}_claim_order`;
    const base = Temporal.Instant.from("2026-01-01T00:00:00Z");
    const insert = async (priority: number, seconds: number) =>
      (
        await driver.jobInsert(
          insertParams(`${filePrefix}_claim_order`, {
            priority,
            queue,
            scheduledAt: base.add({ seconds }),
          })
        )
      ).job.id;
    // Inserted so that ID order disagrees with both other orders.
    const late = await insert(1, 2);
    const lowPriority = await insert(2, 0);
    const early = await insert(1, 1);
    const tiedFirst = await insert(1, 3);
    const tiedSecond = await insert(1, 3);

    const claimed: bigint[] = [];
    for (let index = 0; index < 5; index++) {
      const result = await driver.jobClaim({
        attemptedBy: "claim-order",
        kinds: [],
        queues: [{ limit: 1, name: queue }],
      });
      claimed.push(...result.jobs.map(({ id }) => id));
    }

    expect(claimed).toEqual([early, late, tiedFirst, tiedSecond, lowPriority]);
  });

  it("filters claims by kind before the limit", async () => {
    const queue = `${filePrefix}_claim_kinds`;
    const other = await driver.jobInsert(
      insertParams(`${filePrefix}_claim_other`, { queue })
    );
    const known = await driver.jobInsert(
      insertParams(`${filePrefix}_claim_known`, { queue })
    );

    const claimed = await driver.jobClaim({
      attemptedBy: "kind-filter",
      kinds: [`${filePrefix}_claim_known`],
      queues: [{ limit: 1, name: queue }],
    });

    expect(claimed.jobs.map(({ id }) => id)).toEqual([known.job.id]);
    expect(await driver.jobGet(other.job.id)).toMatchObject({
      attempt: 0,
      state: "available",
    });
  });

  it("keeps the last 100 attempted_by entries on claim like Go", async () => {
    const queue = `${filePrefix}_history`;
    const history = Array.from(
      { length: 101 },
      (_, index) => `worker-${index.toString().padStart(3, "0")}`
    );
    const inserted = await driver.jobInsert(
      insertParams(`${filePrefix}_history`, { queue })
    );
    await pool.query(
      "UPDATE river_job SET attempted_by = $2::text[] WHERE id = $1::bigint",
      [inserted.job.id.toString(10), history]
    );

    const claimed = await driver.jobClaim({
      attemptedBy: "worker-101",
      kinds: [`${filePrefix}_history`],
      queues: [{ limit: 1, name: queue }],
    });

    expect(claimed.jobs.map(({ id }) => id)).toEqual([inserted.job.id]);
    expect(claimed.jobs[0]?.attemptedBy).toEqual([
      ...history.slice(2),
      "worker-101",
    ]);
    expect((await driver.jobGet(inserted.job.id))?.attemptedBy).toHaveLength(
      100
    );
  });

  it("persists captured completion times for every outcome kind", async () => {
    const finish = Temporal.Instant.from("2026-08-30T18:31:00.123456789Z");
    const scheduledAt = Temporal.Instant.from("2026-08-30T19:00:00Z");
    const kinds = [
      "cancel",
      "complete",
      "discard",
      "interrupt",
      "retry",
      "snooze",
    ] as const;
    const jobs = await Promise.all(
      kinds.map(
        async (kind) =>
          (
            await driver.jobInsert(
              insertParams(`${filePrefix}_completion_${kind}`)
            )
          ).job
      )
    );
    const claimed = (
      await driver.jobClaim({
        attemptedBy: `${filePrefix}_completion_timing_worker`,
        kinds: kinds.map((kind) => `${filePrefix}_completion_${kind}`),
        queues: [{ limit: kinds.length, name: "default" }],
      })
    ).jobs;
    expect(claimed).toHaveLength(kinds.length);

    const results = await driver.jobCompleteMany(
      jobs.map((job, index) => {
        const kind = kinds[index]!;
        const terminal =
          kind === "cancel" || kind === "complete" || kind === "discard";
        return {
          attempt: 1,
          attemptedBy: `${filePrefix}_completion_timing_worker`,
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
      "2026-08-30T18:31:00.123456Z",
      "2026-08-30T18:31:00.123456Z",
      "2026-08-30T18:31:00.123456Z",
      null,
      null,
      null,
    ]);
  });

  it("merges completion metadata after exact-attempt external finalization", async () => {
    const queue = `${filePrefix}_external_finalization`;
    const attemptedBy = `${filePrefix}_external_worker`;
    const inserted = await driver.jobInsert(
      insertParams(`${filePrefix}_external_finalization`, {
        metadata: { existing: true },
        queue,
      })
    );
    const [claimed] = (
      await driver.jobClaim({
        attemptedBy,
        kinds: [`${filePrefix}_external_finalization`],
        queues: [{ limit: 1, name: queue }],
      })
    ).jobs;
    const finalizedAt = Temporal.Instant.from("2026-08-30T14:15:16.123456Z");
    await pool.query(
      `
        UPDATE river_job
        SET finalized_at = $2::timestamptz, state = 'discarded'
        WHERE id = $1::bigint
      `,
      [inserted.job.id.toString(10), finalizedAt.toString()]
    );

    const raced = await driver.jobCompleteMany([
      {
        attempt: 1,
        attemptedBy,
        error: null,
        id: inserted.job.id,
        kind: "complete",
        finalizedAt: Temporal.Now.instant(),
        metadata: { checkpoint: "finished" },
        output: { engine: "javascript" },
        outputSet: true,
        scheduledAt: Temporal.Instant.from("2026-09-30T00:00:00Z"),
      },
    ]);

    expect(raced[0]).toMatchObject({
      job: {
        attempt: 1,
        errors: [],
        metadata: {
          checkpoint: "finished",
          existing: true,
          output: { engine: "javascript" },
        },
        state: "discarded",
      },
      status: "stale",
    });
    expect(raced[0]!.job!.finalizedAt!.toString()).toBe(finalizedAt.toString());
    expect(raced[0]!.job!.scheduledAt.toString()).toBe(
      claimed!.scheduledAt.toString()
    );

    const metadataOnly = await driver.jobCompleteMany([
      {
        attempt: 1,
        attemptedBy,
        error: null,
        id: inserted.job.id,
        kind: "complete",
        finalizedAt: Temporal.Now.instant(),
        metadata: { resumed: true },
        output: null,
        outputSet: false,
        scheduledAt: null,
      },
    ]);
    expect(metadataOnly[0]).toMatchObject({
      job: {
        metadata: {
          checkpoint: "finished",
          existing: true,
          output: { engine: "javascript" },
          resumed: true,
        },
        state: "discarded",
      },
      status: "stale",
    });

    const explicitNull = await driver.jobCompleteMany([
      {
        attempt: 1,
        attemptedBy,
        error: null,
        id: inserted.job.id,
        kind: "complete",
        finalizedAt: Temporal.Now.instant(),
        output: null,
        outputSet: true,
        scheduledAt: null,
      },
    ]);
    expect(explicitNull[0]).toMatchObject({
      job: {
        metadata: {
          checkpoint: "finished",
          existing: true,
          output: null,
          resumed: true,
        },
        state: "discarded",
      },
      status: "stale",
    });
  });

  it("never merges a stale completion into a newer attempt", async () => {
    const queue = `${filePrefix}_newer_attempt`;
    const oldWorker = `${filePrefix}_old_worker`;
    const newWorker = `${filePrefix}_new_worker`;
    const inserted = await driver.jobInsert(
      insertParams(`${filePrefix}_newer_attempt`, {
        metadata: { generation: "new" },
        queue,
      })
    );
    await driver.jobClaim({
      attemptedBy: oldWorker,
      kinds: [`${filePrefix}_newer_attempt`],
      queues: [{ limit: 1, name: queue }],
    });
    await pool.query(
      `
        UPDATE river_job
        SET
          attempt = 2,
          attempted_by = array_append(attempted_by, $2::text),
          state = 'running'
        WHERE id = $1::bigint
      `,
      [inserted.job.id.toString(10), newWorker]
    );

    const raced = await driver.jobCompleteMany([
      {
        attempt: 1,
        attemptedBy: oldWorker,
        error: null,
        id: inserted.job.id,
        kind: "complete",
        finalizedAt: Temporal.Now.instant(),
        metadata: { stale_checkpoint: true },
        output: { stale: true },
        outputSet: true,
        scheduledAt: null,
      },
    ]);

    expect(raced[0]).toMatchObject({
      job: {
        attempt: 2,
        attemptedBy: [oldWorker, newWorker],
        metadata: { generation: "new" },
        state: "running",
      },
      status: "stale",
    });
    expect(raced[0]!.job!.metadata).not.toHaveProperty("output");
    expect(raced[0]!.job!.metadata).not.toHaveProperty("stale_checkpoint");
  });

  it("merges a rescued attempt's output without changing its state", async () => {
    const queue = `${filePrefix}_rescued_output`;
    const worker = `${filePrefix}_rescued_worker`;
    const inserted = await driver.jobInsert(
      insertParams(`${filePrefix}_rescued_output`, { queue })
    );
    await driver.jobClaim({
      attemptedBy: worker,
      kinds: [`${filePrefix}_rescued_output`],
      queues: [{ limit: 1, name: queue }],
    });
    // The rescuer retried the attempt before its completion arrived.
    await pool.query(
      "UPDATE river_job SET state = 'retryable' WHERE id = $1::bigint",
      [inserted.job.id.toString(10)]
    );

    const [result] = await driver.jobCompleteMany([
      {
        attempt: 1,
        attemptedBy: worker,
        error: null,
        finalizedAt: Temporal.Now.instant(),
        id: inserted.job.id,
        kind: "complete",
        metadata: { checkpoint: 3 },
        output: { rows: 7 },
        outputSet: true,
        scheduledAt: null,
      },
    ]);

    // Like River for Go, the output and metadata still merge.
    expect(result).toMatchObject({
      job: {
        finalizedAt: null,
        metadata: { checkpoint: 3, output: { rows: 7 } },
        state: "retryable",
      },
      status: "stale",
    });
  });

  it("snoozes by returning the claim attempt to the scheduler", async () => {
    const inserted = await driver.jobInsert(
      insertParams(`${filePrefix}_snooze`, { queue: `${filePrefix}_snooze` })
    );
    await driver.jobClaim({
      attemptedBy: `${filePrefix}_snooze_worker`,
      kinds: [`${filePrefix}_snooze`],
      queues: [{ limit: 1, name: `${filePrefix}_snooze` }],
    });
    const scheduledAt = Temporal.Instant.from("2026-08-31T12:00:00.123456Z");

    const snoozed = await driver.jobCompleteMany([
      {
        attempt: 1,
        attemptedBy: `${filePrefix}_snooze_worker`,
        error: null,
        id: inserted.job.id,
        kind: "snooze",
        finalizedAt: null,
        output: null,
        outputSet: false,
        scheduledAt,
      },
    ]);

    expect(snoozed[0]).toMatchObject({
      job: { attempt: 0, state: "scheduled" },
      status: "applied",
    });
    expect(snoozed[0]!.job!.scheduledAt.toString()).toBe(
      "2026-08-31T12:00:00.123456Z"
    );
  });

  it("persists near-future retries and snoozes as available", async () => {
    const queue = `${filePrefix}_fast_path`;
    const attemptedBy = `${filePrefix}_fast_path_worker`;
    const snoozing = await driver.jobInsert(
      insertParams(`${filePrefix}_fast_path`, { queue })
    );
    const failing = await driver.jobInsert(
      insertParams(`${filePrefix}_fast_path`, { queue })
    );
    await driver.jobClaim({
      attemptedBy,
      kinds: [`${filePrefix}_fast_path`],
      queues: [{ limit: 2, name: queue }],
    });
    const scheduledAt = Temporal.Now.instant()
      .round({ roundingMode: "floor", smallestUnit: "microsecond" })
      .add({ seconds: 1 });
    const common = {
      attempt: 1,
      attemptedBy,
      available: true,
      finalizedAt: null,
      output: null,
      outputSet: false,
      scheduledAt,
    };

    const results = await driver.jobCompleteMany([
      { ...common, error: null, id: snoozing.job.id, kind: "snooze" },
      {
        ...common,
        error: { at: Temporal.Now.instant(), error: "boom", trace: "" },
        id: failing.job.id,
        kind: "retry",
      },
    ]);

    // Like River's `JobSetStateSnoozedAvailable`, a snooze refunds the attempt.
    expect(results[0]).toMatchObject({
      job: { attempt: 0, errors: [], state: "available" },
      status: "applied",
    });
    // Like `JobSetStateErrorAvailable`, an error keeps it.
    expect(results[1]).toMatchObject({
      job: { attempt: 1, errors: [{ error: "boom" }], state: "available" },
      status: "applied",
    });
    expect(results[1]!.job!.scheduledAt.toString()).toBe(
      scheduledAt.toString()
    );
    // The row is not claimable before its scheduled time.
    await expect(
      driver.jobClaim({
        attemptedBy,
        kinds: [`${filePrefix}_fast_path`],
        queues: [{ limit: 2, name: queue }],
      })
    ).resolves.toEqual({ jobs: [] });
  });

  it("cancels an interrupted attempt whose cancellation never reached it", async () => {
    const kind = `${filePrefix}_interrupt_cancel`;
    const inserted = await driver.jobInsert(
      insertParams(kind, { queue: kind })
    );
    await driver.jobClaim({
      attemptedBy: `${kind}_worker`,
      kinds: [kind],
      queues: [{ limit: 1, name: kind }],
    });
    // A cancellation requested without its notification being delivered.
    await pool.query(
      `UPDATE river_job SET metadata = jsonb_set(metadata, '{cancel_attempted_at}', '"2026-01-02T03:04:05Z"') WHERE id = $1`,
      [inserted.job.id.toString()]
    );

    const [result] = await driver.jobCompleteMany([
      {
        attempt: 1,
        attemptedBy: `${kind}_worker`,
        error: null,
        finalizedAt: null,
        id: inserted.job.id,
        kind: "interrupt",
        output: null,
        outputSet: false,
        scheduledAt: Temporal.Now.instant(),
      },
    ]);

    expect(result).toMatchObject({
      job: { state: "cancelled" },
      status: "applied",
    });
    expect(result!.job!.finalizedAt).not.toBeNull();
  });

  it("returns undecodable claimed rows with their errors and completes them", async () => {
    const kind = `${filePrefix}_undecodable`;
    const ordinary = await driver.jobInsert(
      insertParams(kind, { queue: kind })
    );
    const corrupt = await driver.jobInsert(insertParams(kind, { queue: kind }));
    const sparse = await driver.jobInsert(insertParams(kind, { queue: kind }));
    // Array metadata is valid for Go but not a JSON object River can decode.
    await pool.query(
      `UPDATE river_job SET metadata = '[1]'::jsonb WHERE id = $1`,
      [corrupt.job.id.toString()]
    );
    // Go's encoding/json tolerates missing and unknown attempt error fields.
    await pool.query(
      `UPDATE river_job SET errors = ARRAY['{"error": "sparse", "extra": true}'::jsonb] WHERE id = $1`,
      [sparse.job.id.toString()]
    );

    const claimed = await driver.jobClaim({
      attemptedBy: `${kind}_worker`,
      kinds: [kind],
      queues: [{ limit: 10, name: kind }],
    });

    // Every claimed row comes back, in claim order, with the decode error of
    // the one that couldn't be decoded alongside.
    expect(claimed.jobs.map((job) => job.id).sort()).toEqual(
      [ordinary.job.id, corrupt.job.id, sparse.job.id].sort()
    );
    expect([...(claimed.decodeErrors?.keys() ?? [])]).toEqual([corrupt.job.id]);
    expect(
      claimed.jobs.find((job) => job.id === sparse.job.id)?.errors
    ).toEqual([
      {
        at: Temporal.Instant.from("0001-01-01T00:00:00Z"),
        attempt: 0,
        error: "sparse",
        trace: "",
      },
    ]);
    expect(claimed.jobs.find((job) => job.id === corrupt.job.id)).toMatchObject(
      {
        attempt: 1,
        metadata: {},
        state: "running",
      }
    );
    expect(claimed.decodeErrors?.get(corrupt.job.id)?.message).toContain(
      "metadata"
    );

    // Failing the attempt appends the error without touching the bad value,
    // and the completion still returns the row.
    const [failed] = await driver.jobCompleteMany([
      {
        attempt: 1,
        attemptedBy: `${kind}_worker`,
        error: {
          at: Temporal.Now.instant(),
          error: "job row couldn't be decoded",
          trace: "",
        },
        finalizedAt: null,
        id: corrupt.job.id,
        kind: "retry",
        output: null,
        outputSet: false,
        scheduledAt: Temporal.Now.instant().add({ hours: 1 }),
      },
    ]);
    expect(failed).toMatchObject({
      job: { metadata: {}, state: "retryable" },
      status: "applied",
    });
    const row = await pool.query<{ error: string; metadata: unknown }>(
      `SELECT metadata, errors[array_length(errors, 1)] ->> 'error' AS error
       FROM river_job WHERE id = $1`,
      [corrupt.job.id.toString()]
    );
    expect(row.rows[0]).toEqual({
      error: "job row couldn't be decoded",
      metadata: [1],
    });
  });

  it("interrupts without consuming an attempt or recording an error", async () => {
    const queue = `${filePrefix}_interrupt`;
    const inserted = await driver.jobInsert(
      insertParams(`${filePrefix}_interrupt`, { queue })
    );
    await driver.jobClaim({
      attemptedBy: `${filePrefix}_interrupt_worker`,
      kinds: [`${filePrefix}_interrupt`],
      queues: [{ limit: 1, name: queue }],
    });
    const now = Temporal.Instant.from("2026-08-30T14:00:00.123456Z");

    const interrupted = await driver.jobCompleteMany([
      {
        attempt: 1,
        attemptedBy: `${filePrefix}_interrupt_worker`,
        error: null,
        id: inserted.job.id,
        kind: "interrupt",
        finalizedAt: null,
        output: null,
        outputSet: false,
        scheduledAt: now,
      },
    ]);

    expect(interrupted[0]).toMatchObject({
      job: { attempt: 0, errors: [], state: "available" },
      status: "applied",
    });
    expect(interrupted[0]!.job!.scheduledAt.toString()).toBe(now.toString());
  });

  it("lets persisted cancellation win a concurrent snooze", async () => {
    const inserted = await driver.jobInsert(
      insertParams(`${filePrefix}_cancel_snooze`, {
        queue: `${filePrefix}_cancel_snooze`,
      })
    );
    const [claimed] = (
      await driver.jobClaim({
        attemptedBy: `${filePrefix}_cancel_snooze_worker`,
        kinds: [`${filePrefix}_cancel_snooze`],
        queues: [{ limit: 1, name: `${filePrefix}_cancel_snooze` }],
      })
    ).jobs;
    await driver.jobCancel(inserted.job.id);

    const result = await driver.jobCompleteMany([
      {
        attempt: 1,
        attemptedBy: `${filePrefix}_cancel_snooze_worker`,
        error: null,
        id: inserted.job.id,
        kind: "snooze",
        finalizedAt: null,
        output: null,
        outputSet: false,
        scheduledAt: Temporal.Instant.from("2026-09-01T00:00:00Z"),
      },
    ]);

    expect(result[0]).toMatchObject({
      job: { attempt: 1, state: "cancelled" },
      status: "applied",
    });
    expect(result[0]!.job!.scheduledAt.toString()).toBe(
      claimed!.scheduledAt.toString()
    );
  });

  it("does not claim work from a persisted paused queue", async () => {
    const queue = `${filePrefix}_paused_claim`;
    await driver.queueUpsert({ name: queue });
    await driver.queuePause(queue);
    const inserted = await driver.jobInsert(
      insertParams(`${filePrefix}_paused_claim`, { queue })
    );

    await expect(
      driver.jobClaim({
        attemptedBy: `${filePrefix}_paused_worker`,
        kinds: [`${filePrefix}_paused_claim`],
        queues: [{ limit: 1, name: queue }],
      })
    ).resolves.toEqual({ jobs: [] });

    await driver.queueResume(queue);
    const claimed = (
      await driver.jobClaim({
        attemptedBy: `${filePrefix}_paused_worker`,
        kinds: [`${filePrefix}_paused_claim`],
        queues: [{ limit: 1, name: queue }],
      })
    ).jobs;
    expect(claimed.map(({ id }) => id)).toEqual([inserted.job.id]);
  });

  it("cancels a blocked completion server-side when cancellation fires", async () => {
    const kind = `${filePrefix}_cancel_blocked_completion`;
    const attemptedBy = `${filePrefix}_cancel_blocked_worker`;
    const inserted = await driver.jobInsert(insertParams(kind));
    const [claimed] = (
      await driver.jobClaim({
        attemptedBy,
        kinds: [kind],
        queues: [{ limit: 1, name: "default" }],
      })
    ).jobs;
    expect(claimed?.id).toBe(inserted.job.id);

    const locker = await pool.connect();
    try {
      await locker.query("BEGIN");
      await locker.query("SELECT id FROM river_job WHERE id = $1 FOR UPDATE", [
        inserted.job.id.toString(10),
      ]);
      const cancellation = new AbortController();
      const reason = new Error("completion cancelled");
      const completion = driver.jobCompleteMany(
        [
          {
            attempt: 1,
            attemptedBy,
            error: null,
            id: inserted.job.id,
            kind: "complete",
            finalizedAt: Temporal.Now.instant(),
            output: null,
            outputSet: false,
            scheduledAt: null,
          },
        ],
        { signal: cancellation.signal }
      );
      await new Promise((resolve) => setTimeout(resolve, 50));
      const abortedAt = Date.now();
      cancellation.abort(reason);

      await expect(completion).rejects.toBe(reason);
      expect(Date.now() - abortedAt).toBeLessThan(1_000);
      await expect(pool.query("SELECT 1 AS healthy")).resolves.toMatchObject({
        rows: [{ healthy: 1 }],
      });
      // Destroying the socket alone would leave the UPDATE waiting on the
      // row lock, and it would commit as soon as the lock is released.
      await waitFor(async () => {
        const active = await pool.query(
          `
            SELECT count(*)::int AS count
            FROM pg_stat_activity
            WHERE state = 'active'
              AND query LIKE '/* river:jobCompleteMany */%'
          `
        );
        return active.rows[0]?.count === 0;
      });
    } finally {
      await locker.query("ROLLBACK");
      locker.release();
    }
    expect((await driver.jobGet(inserted.job.id))?.state).toBe("running");
  });

  it("lists with stable cursors and applies semantic job patches", async () => {
    const inserted = await driver.jobInsert(
      insertParams(`${filePrefix}_list`, {
        metadata: { keep: true, output: { old: true } },
        priority: 2,
        queue: `${filePrefix}_queue`,
        tags: ["beta", "shared"],
      })
    );
    await driver.jobInsert(
      insertParams(`${filePrefix}_list`, {
        metadata: { keep: false },
        priority: 2,
        queue: `${filePrefix}_queue`,
        tags: ["beta", "shared"],
      })
    );
    const updated = await driver.jobUpdate(inserted.job.id, {
      metadata: { added: true, output: { ignored: true } },
      output: { new: true },
    });
    expect(updated).toMatchObject({
      metadata: { added: true, keep: true, output: { new: true } },
    });
    await expect(
      driver.jobUpdate(123_456_789_012n, { output: 1 })
    ).resolves.toBeNull();

    const listParams = {
      after: null,
      ids: [],
      kinds: [`${filePrefix}_list`],
      limit: 10,
      metadata: { keep: true },
      priorities: [2],
      queues: [`${filePrefix}_queue`],
      sortDirection: "asc",
      sortField: "id",
      states: ["available"],
      tagsAll: ["shared"],
      tagsAny: ["beta"],
    } as const;
    const rows = await driver.jobList(listParams);
    expect(rows.map(({ id }) => id)).toEqual([inserted.job.id]);

    const after = await driver.jobList({
      ...listParams,
      after: {
        id: inserted.job.id,
        kind: inserted.job.kind,
        queue: updated!.queue,
        sortField: "id",
        time: null,
      },
    });
    expect(after).toEqual([]);

    const transaction = await pool.connect();
    try {
      await transaction.query("BEGIN");
      await driver.jobUpdate(
        inserted.job.id,
        { metadata: { keep: false } },
        { tx: transaction }
      );
      await expect(
        driver.jobList(listParams, { tx: transaction })
      ).resolves.toEqual([]);
      expect((await driver.jobList(listParams)).map(({ id }) => id)).toEqual([
        inserted.job.id,
      ]);
    } finally {
      await transaction.query("ROLLBACK");
      transaction.release();
    }
  });

  it("paginates one finalized state across tied timestamps", async () => {
    const kind = `${filePrefix}_finalized_pages`;
    const base = Temporal.Instant.from("2026-08-30T12:00:00.000001Z");
    const offsets = [0, 1, 1, 1, 2];
    const ids: bigint[] = [];
    for (const offset of offsets) {
      const result = await pool.query<{ id: string }>(
        `
          INSERT INTO river_job (args, finalized_at, kind, max_attempts, state)
          VALUES ('{}', $1::timestamptz, $2::text, 25, 'completed')
          RETURNING id::text
        `,
        [base.add({ seconds: offset }).toString(), kind]
      );
      ids.push(BigInt(result.rows[0]!.id));
    }
    const expectedAsc = ids
      .map((id, index) => ({ id, offset: offsets[index] ?? 0 }))
      .sort(
        (left, right) =>
          left.offset - right.offset || (left.id < right.id ? -1 : 1)
      )
      .map(({ id }) => id);

    for (const direction of ["asc", "desc"] as const) {
      const seen: bigint[] = [];
      let after: JobRow | undefined;
      for (;;) {
        const page = await driver.jobList({
          after:
            after === undefined
              ? null
              : {
                  id: after.id,
                  kind: after.kind,
                  queue: after.queue,
                  sortField: "time",
                  time: after.finalizedAt,
                },
          ids: [],
          kinds: [kind],
          limit: 2,
          metadata: null,
          priorities: [],
          queues: [],
          sortDirection: direction,
          sortField: "time",
          states: ["completed"],
          tagsAll: [],
          tagsAny: [],
        });
        seen.push(...page.map(({ id }) => id));
        after = page.at(-1);
        if (page.length < 2) break;
      }
      expect(seen).toEqual(
        direction === "asc" ? expectedAsc : [...expectedAsc].reverse()
      );
    }
  });

  it("pages mixed states by the first state's time field like Go", async () => {
    const kind = `${filePrefix}_mixed_time_list`;
    const at = (minutes: number) =>
      Temporal.Instant.from("2026-08-30T10:00:00.123456Z").add({ minutes });
    // Inserted so that ID order differs from each time order.
    const firstAvailable = await driver.jobInsert(
      insertParams(kind, { scheduledAt: at(30) })
    );
    const laterCompleted = await driver.jobInsert(
      insertParams(kind, { scheduledAt: at(10) })
    );
    const secondAvailable = await driver.jobInsert(
      insertParams(kind, { scheduledAt: at(30) })
    );
    const earlierCompleted = await driver.jobInsert(
      insertParams(kind, { scheduledAt: at(20) })
    );
    for (const [job, finalizedAt] of [
      [laterCompleted, at(50)],
      [earlierCompleted, at(40)],
    ] as const) {
      await pool.query(
        `UPDATE river_job
         SET state = 'completed', finalized_at = $2::timestamptz
         WHERE id = $1::bigint`,
        [job.job.id.toString(10), finalizedAt.toString()]
      );
    }
    const base = {
      ids: [],
      kinds: [kind],
      limit: 1,
      metadata: null,
      priorities: [],
      queues: [],
      sortField: "time",
      tagsAll: [],
      tagsAny: [],
    } as const;

    /** Page one job at a time, alternating encoded and row cursors. */
    const pageIds = async (
      sortDirection: "asc" | "desc",
      states: readonly JobState[]
    ): Promise<bigint[]> => {
      const params = { ...base, sortDirection, states };
      const ids: bigint[] = [];
      let after: JobListCursorValue | null = null;
      for (let page = 0; page < 10; page++) {
        const [job] = await driver.jobList({ ...params, after });
        if (job === undefined) break;
        ids.push(job.id);
        after =
          page % 2 === 0
            ? decodeJobListCursor(encodeJobListCursor(job, params))
            : jobListCursorValue(job, params);
      }
      return ids;
    };

    const available = [firstAvailable.job.id, secondAvailable.job.id];
    const completed = [earlierCompleted.job.id, laterCompleted.job.id];
    // Finalized time, which the available jobs lack: nulls sort last
    // ascending and first descending, by ID.
    expect(await pageIds("asc", ["completed", "available"])).toEqual([
      ...completed,
      ...available,
    ]);
    expect(await pageIds("desc", ["completed", "available"])).toEqual(
      [...completed, ...available].reverse()
    );
    // Scheduled time for every job, including the completed ones.
    const byScheduledAt = [
      laterCompleted.job.id,
      earlierCompleted.job.id,
      ...available,
    ];
    expect(await pageIds("asc", ["available", "completed"])).toEqual(
      byScheduledAt
    );
    expect(await pageIds("desc", ["available", "completed"])).toEqual(
      byScheduledAt.toReversed()
    );
    // No state filter orders by scheduled time too.
    expect(await pageIds("asc", [])).toEqual(byScheduledAt);
    // Like Go, a cursor without a time for a field that can't be null
    // resumes after its ID alone.
    expect(
      (
        await driver.jobList({
          ...base,
          after: {
            id: laterCompleted.job.id,
            kind,
            queue: "default",
            sortField: "time",
            time: null,
          },
          limit: 10,
          sortDirection: "asc",
          states: ["available", "completed"],
        })
      ).map(({ id }) => id)
    ).toEqual([earlierCompleted.job.id, secondAvailable.job.id]);
  });

  it("elects, renews, and resigns exact leadership terms", async () => {
    const now = Temporal.Instant.from("2026-08-30T12:00:00.123456Z");
    await driver.leaderDeleteExpired(
      Temporal.Instant.from("9999-12-31T23:59:59Z")
    );

    const elected = await driver.leaderElect({
      leaderId: `${filePrefix}_leader`,
      now,
      ttlSeconds: 30,
    });
    expect(elected!.electedAt.toString()).toBe(now.toString());
    await expect(
      driver.leaderElect({ leaderId: "competitor", now, ttlSeconds: 30 })
    ).resolves.toBeNull();

    const renewed = await driver.leaderReelect({
      electedAt: elected!.electedAt,
      leaderId: `${filePrefix}_leader`,
      now,
      ttlSeconds: 60,
    });
    expect(renewed!.expiresAt.epochNanoseconds).toBeGreaterThan(
      elected!.expiresAt.epochNanoseconds
    );
    await expect(
      driver.leaderResign({
        electedAt: elected!.electedAt,
        leaderId: "competitor",
        leadershipTopic: "river_leadership",
        ttlSeconds: 30,
      })
    ).resolves.toBe(false);
    await expect(
      driver.leaderResign({
        electedAt: elected!.electedAt,
        leaderId: `${filePrefix}_leader`,
        leadershipTopic: "river_leadership",
        ttlSeconds: 30,
      })
    ).resolves.toBe(true);
    await expect(driver.leaderGet()).resolves.toBeNull();
  });

  it("renews only the held term and never adopts a same-ID term", async () => {
    const leaderId = `${filePrefix}_same_identity`;
    const now = Temporal.Now.instant();
    const first = (await driver.maintenanceLeaderAcquire(
      leaderId,
      now,
      60_000,
      null
    ))!;
    try {
      // A live term is renewed only by its holder, like Go's elector.
      await expect(
        driver.maintenanceLeaderAcquire(leaderId, now, 60_000, null)
      ).resolves.toBeNull();
      await expect(
        driver.maintenanceLeaderAcquire(leaderId, now, 60_000, first)
      ).resolves.toMatchObject({ electedAt: first.electedAt, leaderId });

      // Another process with the same client ID takes over with a newer term.
      await pool.query(
        "UPDATE river_leader SET elected_at = elected_at + interval '1 second'"
      );
      await expect(
        driver.maintenanceLeaderAcquire(leaderId, now, 60_000, first)
      ).resolves.toBeNull();
      await expect(driver.leaderGet()).resolves.toMatchObject({
        electedAt: first.electedAt.add({ seconds: 1 }),
        leaderId,
      });
    } finally {
      await pool.query("DELETE FROM river_leader WHERE leader_id = $1", [
        leaderId,
      ]);
    }
  });

  it("fences maintenance mutations to the exact current term", async () => {
    const now = Temporal.Now.instant();
    const first = (await driver.maintenanceLeaderAcquire(
      `${filePrefix}_maintenance_one`,
      now,
      60_000,
      null
    ))!;
    const inserted = await driver.jobInsert(
      insertParams(`${filePrefix}_fenced_scheduler`, {
        scheduledAt: now,
        state: "scheduled",
      })
    );
    await expect(driver.maintenanceLeaderResign(first)).resolves.toBe(true);
    const second = (await driver.maintenanceLeaderAcquire(
      `${filePrefix}_maintenance_two`,
      now,
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

    await expect(driver.maintenanceSchedule(first, params)).resolves.toBe(0);
    await expect(driver.jobGet(inserted.job.id)).resolves.toMatchObject({
      state: "scheduled",
    });
    await expect(driver.maintenanceSchedule(second, params)).resolves.toBe(1);
    await expect(driver.jobGet(inserted.job.id)).resolves.toMatchObject({
      state: "available",
    });
    await expect(driver.maintenanceLeaderResign(second)).resolves.toBe(true);
  });

  it("cleans finalized jobs except in excluded queues", async () => {
    const now = Temporal.Now.instant();
    const leader = (await driver.maintenanceLeaderAcquire(
      `${filePrefix}_cleaner_excluded`,
      now,
      60_000,
      null
    ))!;
    const ids: Record<string, bigint> = {};
    try {
      for (const queue of [`${filePrefix}_kept`, `${filePrefix}_cleaned`]) {
        const inserted = await driver.jobInsert(
          insertParams(`${filePrefix}_cleaner_excluded_job`, { queue })
        );
        ids[queue] = inserted.job.id;
      }
      await pool.query(
        `UPDATE river_job
         SET finalized_at = $2::timestamptz, state = 'completed'
         WHERE id = ANY($1::bigint[])`,
        [
          Object.values(ids).map((id) => id.toString(10)),
          now.subtract({ hours: 2 }).toString(),
        ]
      );

      await driver.maintenanceCleanJobs(
        leader,
        {
          cancelledBefore: now,
          completedBefore: now,
          discardedBefore: now,
          limit: 1_000,
          queuesExcluded: [`${filePrefix}_kept`],
        },
        null,
        new AbortController().signal
      );
      expect(
        await driver.jobGet(ids[`${filePrefix}_kept`] as bigint)
      ).not.toBeNull();
      expect(
        await driver.jobGet(ids[`${filePrefix}_cleaned`] as bigint)
      ).toBeNull();
    } finally {
      await driver.maintenanceLeaderResign(leader);
    }
  });

  it("bounds each job-cleaner query with a database timeout", async () => {
    const now = Temporal.Now.instant();
    const leader = (await driver.maintenanceLeaderAcquire(
      `${filePrefix}_cleaner_timeout`,
      now,
      60_000,
      null
    ))!;
    const inserted = await driver.jobInsert(
      insertParams(`${filePrefix}_cleaner_timeout_job`)
    );
    await pool.query(
      `UPDATE river_job
       SET finalized_at = $2::timestamptz, state = 'completed'
       WHERE id = $1::bigint`,
      [inserted.job.id.toString(10), now.subtract({ hours: 2 }).toString()]
    );
    const blocker = await pool.connect();
    try {
      await blocker.query("BEGIN");
      await blocker.query("LOCK TABLE river_job IN ACCESS EXCLUSIVE MODE");
      const startedAt = performance.now();

      await expect(
        driver.maintenanceCleanJobs(
          leader,
          {
            cancelledBefore: now,
            completedBefore: now,
            discardedBefore: now,
            limit: 10,
          },
          20,
          new AbortController().signal
        )
      ).rejects.toMatchObject({
        cause: expect.objectContaining({ code: "57014" }),
      });
      expect(performance.now() - startedAt).toBeLessThan(1_000);
    } finally {
      await blocker.query("ROLLBACK");
      blocker.release();
      await driver.maintenanceLeaderResign(leader);
    }
  });

  it("bounds scheduler, rescuer, and queue cleaner batches with a database timeout", async () => {
    const now = Temporal.Now.instant();
    const leader = (await driver.maintenanceLeaderAcquire(
      `${filePrefix}_batch_timeout`,
      now,
      60_000,
      null
    ))!;
    const batch = { signal: new AbortController().signal, timeoutMs: 20 };
    const blocker = await pool.connect();
    try {
      await blocker.query("BEGIN");
      await blocker.query("LOCK TABLE river_job IN ACCESS EXCLUSIVE MODE");
      await blocker.query("LOCK TABLE river_queue IN ACCESS EXCLUSIVE MODE");
      const timedOut = { cause: expect.objectContaining({ code: "57014" }) };
      const startedAt = performance.now();

      await expect(
        driver.maintenanceSchedule(
          leader,
          {
            allowInsertNotifications: allowEveryQueue,
            limit: 10,
            notificationHorizon: now,
            now,
            scheduledAtHorizon: now,
          },
          batch
        )
      ).rejects.toMatchObject(timedOut);
      await expect(
        driver.maintenanceGetStuck(leader, now, 0n, 10, batch)
      ).rejects.toMatchObject(timedOut);
      await expect(
        driver.maintenanceCleanQueues(leader, now, 10, batch)
      ).rejects.toMatchObject(timedOut);
      expect(performance.now() - startedAt).toBeLessThan(3_000);
    } finally {
      await blocker.query("ROLLBACK");
      blocker.release();
      await driver.maintenanceLeaderResign(leader);
    }
  });

  it("renews the held term while leader maintenance is blocked", async () => {
    const leaderId = `${filePrefix}_blocked_renewal`;
    const now = Temporal.Now.instant();
    const leader = (await driver.maintenanceLeaderAcquire(
      leaderId,
      now,
      60_000,
      null
    ))!;
    const blocker = await pool.connect();
    let cleaning: Promise<number> | undefined;
    try {
      await blocker.query("BEGIN");
      await blocker.query("LOCK TABLE river_job IN ACCESS EXCLUSIVE MODE");
      cleaning = driver.maintenanceCleanJobs(
        leader,
        {
          cancelledBefore: now,
          completedBefore: now,
          discardedBefore: now,
          limit: 10,
        },
        null,
        new AbortController().signal
      );
      // Wait until the cleaner's transaction is blocked on the job table.
      for (;;) {
        const waiting = await pool.query(
          `SELECT 1 FROM pg_locks l JOIN pg_class c ON c.oid = l.relation
           WHERE NOT l.granted AND c.relname = 'river_job'`
        );
        if ((waiting.rowCount ?? 0) > 0) break;
        await new Promise((resolve) => setTimeout(resolve, 10));
      }

      // Like Go River, renewing the lease never waits for maintenance.
      const renewed = await Promise.race([
        driver.maintenanceLeaderAcquire(
          leaderId,
          Temporal.Now.instant(),
          60_000,
          leader
        ),
        new Promise<"blocked">((resolve) =>
          setTimeout(resolve, 2_000, "blocked")
        ),
      ]);
      expect(renewed).toMatchObject({ electedAt: leader.electedAt, leaderId });
    } finally {
      await blocker.query("ROLLBACK");
      blocker.release();
      await cleaning;
      await driver.maintenanceLeaderResign(leader);
    }
  });

  it("promotes ahead without prematurely notifying workers", async () => {
    const now = Temporal.Now.instant();
    const leader = (await driver.maintenanceLeaderAcquire(
      `${filePrefix}_lookahead`,
      now,
      60_000,
      null
    ))!;
    const scheduledAt = now.add({ milliseconds: 100 });
    const inserted = await driver.jobInsert(
      insertParams(`${filePrefix}_scheduler_lookahead`, {
        scheduledAt,
        state: "scheduled",
      })
    );
    const abort = new AbortController();
    let listening!: () => void;
    const ready = new Promise<void>((resolve) => {
      listening = resolve;
    });
    const iterator = driver.listen(["river_insert"], abort.signal, listening);
    const next = iterator.next();
    await ready;

    await expect(
      driver.maintenanceSchedule(leader, {
        allowInsertNotifications: allowEveryQueue,
        limit: 10,
        notificationHorizon: now.add({ milliseconds: 5 }),
        now,
        scheduledAtHorizon: now.add({ seconds: 5 }),
      })
    ).resolves.toBe(1);
    await expect(driver.jobGet(inserted.job.id)).resolves.toMatchObject({
      state: "available",
    });
    await expect(
      Promise.race([
        next.then(() => "notification"),
        new Promise<string>((resolve) =>
          setTimeout(() => resolve("quiet"), 50)
        ),
      ])
    ).resolves.toBe("quiet");

    abort.abort();
    await iterator.return(undefined);
    await expect(driver.maintenanceLeaderResign(leader)).resolves.toBe(true);
  });

  it("delivers LISTEN notifications as polling hints", async () => {
    const abort = new AbortController();
    let listening!: () => void;
    const ready = new Promise<void>((resolve) => {
      listening = resolve;
    });
    const iterator = driver.listen(["river_insert"], abort.signal, listening);
    const next = iterator.next();
    await ready;

    await driver.notifyMany("river_insert", ["payload"]);
    await expect(next).resolves.toEqual({
      done: false,
      value: { payload: "payload", topic: "river_insert" },
    });
    abort.abort();
    await iterator.return(undefined);
  });

  it("decodes exact bigint IDs from cancellation notifications", async () => {
    const exactID = 9_007_199_254_741_001n;
    await pool.query(
      `INSERT INTO river_job (id, args, kind, max_attempts)
       VALUES ($1::bigint, '{}'::jsonb, $2::text, 25)`,
      [exactID.toString(10), `${filePrefix}_large_cancel_id`]
    );
    const abort = new AbortController();
    const iterator = driver.jobCancellationSubscribe(
      `${filePrefix}_cancel_owner`,
      abort.signal
    );
    const next = iterator.next();
    await waitForListenerPID(pool, "river_control");

    await driver.jobCancel(exactID);
    await expect(next).resolves.toEqual({
      done: false,
      value: {
        attemptedBy: `${filePrefix}_cancel_owner`,
        id: exactID,
      },
    });
    abort.abort();
    await iterator.return(undefined);
  });

  it("notifies listeners of a committed insert notification", async () => {
    const abort = new AbortController();
    let listening!: () => void;
    const ready = new Promise<void>((resolve) => {
      listening = resolve;
    });
    const iterator = driver.listen(["river_insert"], abort.signal, listening);
    const next = iterator.next();
    await ready;

    await driver.notifyInsert([`${filePrefix}_notification_queue`]);
    const notification = await next;
    expect(notification.done).toBe(false);
    expect(notification.value!.topic).toBe("river_insert");
    expect(JSON.parse(notification.value!.payload)).toEqual({
      queue: `${filePrefix}_notification_queue`,
    });
    abort.abort();
    await iterator.return(undefined);
  });

  it("fails a forcibly terminated listener without leaking leases", async () => {
    const topic = `${filePrefix}_fault_listener`;
    const baselineConnections = pool.totalCount;
    const abort = new AbortController();
    const iterator = driver.listen([topic], abort.signal);
    const failed = iterator.next().catch((error: unknown) => error);
    const pid = await waitForListenerPID(pool, topic);

    await pool.query("SELECT pg_terminate_backend($1::int)", [pid]);
    // The runtime's notification pump logs the failure and subscribes again.
    expect(await failed).toBeInstanceOf(Error);
    await waitFor(() => pool.waitingCount === 0);
    expect(pool.totalCount).toBeLessThanOrEqual(baselineConnections);
  });

  describe("stale rescue snapshots", () => {
    // Mirrors River's riverdrivertest `JobRescueMany_*` coverage for upstream
    // "guard job rescue against stale snapshots": a rescue computed from a
    // fetched snapshot must not touch a job completed, released, or claimed
    // again before the rescue write.
    const now = Temporal.Instant.from("2025-04-30T13:26:39.123400Z");
    const horizon = now.subtract({ hours: 1 });

    async function insertRunning(
      label: string,
      attemptedAt: Temporal.Instant,
      metadata: JsonObject = { "river:rescue_count": 5, something: "else" }
    ): Promise<bigint> {
      const result = await pool.query<{ id: string }>(
        `
          INSERT INTO river_job (
            args, attempt, attempted_at, attempted_by, kind, max_attempts,
            metadata, queue, scheduled_at, state
          ) VALUES (
            '{}', 1, $1::timestamptz, ARRAY['old-worker'], $2::text, 25,
            $3::jsonb, $2::text, $1::timestamptz, 'running'
          )
          RETURNING id::text
        `,
        [attemptedAt.toString(), `${filePrefix}_${label}`, metadata]
      );
      return BigInt(result.rows[0]!.id);
    }

    function snapshot(job: JobRow | null): string {
      return JSON.stringify(job, (_key, value: unknown) =>
        typeof value === "bigint" ? value.toString() : value
      );
    }

    function rescue(
      id: bigint,
      state: "cancelled" | "discarded" | "retryable",
      error = "stuck job rescued"
    ): PgJobRescue {
      const rescueAt = now.add({ minutes: 1 });
      return {
        error: { at: now, attempt: 1, error, trace: "" },
        ...(state === "retryable" ? {} : { finalizedAt: rescueAt }),
        id,
        scheduledAt: rescueAt,
        state,
      };
    }

    for (const state of ["cancelled", "discarded", "retryable"] as const) {
      it(`leaves a job completed after fetch untouched (${state})`, async () => {
        const completed = await insertRunning(
          `rescue_done_${state}`,
          now.subtract({ hours: 2 })
        );
        const stillRunning = await insertRunning(
          `rescue_done_${state}`,
          now.subtract({ hours: 2 })
        );
        const stuck = await driver.jobGetStuck({
          afterId: 0n,
          max: 10_000,
          stuckHorizon: horizon,
        });
        expect(stuck.map(({ id }) => id)).toEqual(
          expect.arrayContaining([completed, stillRunning])
        );

        // The worker completes after the rescuer fetched the job, but before
        // the rescue write.
        const [done] = await driver.jobCompleteMany([
          {
            attempt: 1,
            attemptedBy: "old-worker",
            error: null,
            finalizedAt: now,
            id: completed,
            kind: "complete",
            metadata: { worker: "finished" },
            output: null,
            outputSet: false,
            scheduledAt: null,
          },
        ]);
        expect(done!.job!.state).toBe("completed");
        const before = snapshot(await driver.jobGet(completed));

        await expect(
          driver.jobRescueMany({
            items: [
              rescue(completed, state, "stale rescue"),
              rescue(stillRunning, state),
            ],
            stuckHorizon: horizon,
          })
        ).resolves.toBe(1);

        expect(snapshot(await driver.jobGet(completed))).toBe(before);
        const rescued = await driver.jobGet(stillRunning);
        expect(rescued).toMatchObject({
          errors: [{ error: "stuck job rescued" }],
          metadata: { "river:rescue_count": 6, something: "else" },
          state,
        });
        expect(rescued!.scheduledAt.toString()).toBe(
          now.add({ minutes: 1 }).toString()
        );
        expect(rescued!.finalizedAt?.toString()).toBe(
          state === "retryable" ? undefined : now.add({ minutes: 1 }).toString()
        );
      });
    }

    for (const release of ["failed", "interrupted"] as const) {
      it(`leaves a job claimed again after fetch untouched (${release})`, async () => {
        const queue = `${filePrefix}_rescue_reclaim_${release}`;
        const id = await insertRunning(
          `rescue_reclaim_${release}`,
          now.subtract({ hours: 2 }),
          { "river:rescue_count": 5 }
        );
        const stuck = await driver.jobGetStuck({
          afterId: id - 1n,
          max: 1,
          stuckHorizon: horizon,
        });
        expect(stuck.map((job) => job.id)).toEqual([id]);

        // The old worker releases the job and a new worker claims it before
        // the stale rescue arrives; its state alone still matches.
        const [released] = await driver.jobCompleteMany([
          {
            attempt: 1,
            attemptedBy: "old-worker",
            ...(release === "failed" ? { available: true } : {}),
            error:
              release === "failed"
                ? { at: now, error: "worker failed", trace: "" }
                : null,
            finalizedAt: null,
            id,
            kind: release === "failed" ? "retry" : "interrupt",
            output: null,
            outputSet: false,
            scheduledAt: now,
          },
        ]);
        expect(released!.job!.state).toBe("available");
        const [claimed] = (
          await driver.jobClaim({
            attemptedBy: "new-worker",
            kinds: [],
            queues: [{ limit: 1, name: queue }],
          })
        ).jobs;
        expect(claimed?.id).toBe(id);
        const before = snapshot(await driver.jobGet(id));

        await expect(
          driver.jobRescueMany({
            items: [rescue(id, "retryable", "stale rescue")],
            stuckHorizon: horizon,
          })
        ).resolves.toBe(0);

        expect(snapshot(await driver.jobGet(id))).toBe(before);
      });
    }

    it("rescues only attempts strictly before the horizon", async () => {
      const ids = await Promise.all(
        [-1, 0, 1].map((offset) =>
          insertRunning(
            `rescue_horizon_${offset + 1}`,
            horizon.add({ microseconds: offset })
          )
        )
      );
      const before = await Promise.all(ids.map((id) => driver.jobGet(id)));

      await expect(
        driver.jobRescueMany({
          items: ids.map((id) => rescue(id, "retryable")),
          stuckHorizon: horizon,
        })
      ).resolves.toBe(1);

      const after = await Promise.all(ids.map((id) => driver.jobGet(id)));
      expect(after[0]).toMatchObject({ errors: [{}], state: "retryable" });
      // As in `jobGetStuck`, attempts at or after the horizon are ineligible.
      expect(snapshot(after[1]!)).toBe(snapshot(before[1]!));
      expect(snapshot(after[2]!)).toBe(snapshot(before[2]!));
    });
  });

  it("schedules, rescues, cleans, and introspects maintenance artifacts", async () => {
    const now = Temporal.Instant.from("2026-08-30T12:00:00Z");
    const uniqueKey = Uint8Array.from([9, 8, 7, 6, 5, 4]);
    await driver.jobInsert(
      insertParams(`${filePrefix}_schedule_active`, {
        uniqueKey,
        uniqueStates: ["available"],
      })
    );
    const conflicting = await driver.jobInsert(
      insertParams(`${filePrefix}_schedule_conflict`, {
        scheduledAt: now,
        state: "scheduled",
        uniqueKey,
        uniqueStates: ["available"],
      })
    );
    const due = await driver.jobInsert(
      insertParams(`${filePrefix}_schedule_due`, {
        scheduledAt: now,
        state: "scheduled",
      })
    );

    const scheduled = await driver.jobSchedule({ max: 10, now });
    const scheduledByID = new Map(
      scheduled.map((result) => [result.job.id, result])
    );
    expect(scheduledByID.get(conflicting.job.id)).toMatchObject({
      conflictDiscarded: true,
      job: { state: "discarded" },
    });
    expect(scheduledByID.get(due.job.id)).toMatchObject({
      conflictDiscarded: false,
      job: { state: "available" },
    });

    const rescueCandidate = await driver.jobInsert(
      insertParams(`${filePrefix}_rescue`, { queue: `${filePrefix}_rescue` })
    );
    await driver.jobClaim({
      attemptedBy: `${filePrefix}_rescuer_worker`,
      kinds: [`${filePrefix}_rescue`],
      queues: [{ limit: 1, name: `${filePrefix}_rescue` }],
    });
    const stuckHorizon = Temporal.Instant.from("9999-12-31T23:59:59Z");
    const stuck = await driver.jobGetStuck({
      max: 10,
      stuckHorizon,
    });
    expect(stuck.map(({ id }) => id)).toContain(rescueCandidate.job.id);
    await expect(
      driver.jobRescueMany({
        items: [
          {
            error: {
              at: now,
              attempt: 1,
              error: "stuck",
              trace: "trace",
            },
            id: rescueCandidate.job.id,
            scheduledAt: now,
            state: "retryable",
          },
        ],
        stuckHorizon,
      })
    ).resolves.toBe(1);
    expect((await driver.jobGet(rescueCandidate.job.id))!.state).toBe(
      "retryable"
    );

    const cleanQueue = `${filePrefix}_clean`;
    const clean = await driver.jobInsert(
      insertParams(`${filePrefix}_clean`, { queue: cleanQueue })
    );
    await pool.query(
      "UPDATE river_job SET state = 'completed', finalized_at = $2::timestamptz WHERE id = $1::bigint",
      [clean.job.id.toString(10), now.toString()]
    );
    await expect(
      driver.jobDeleteBefore({
        completedFinalizedAt: Temporal.Instant.from("2026-08-30T13:00:00Z"),
        max: 10,
        queuesIncluded: [cleanQueue],
      })
    ).resolves.toBe(1);

    const expiredQueue = `${filePrefix}_expired_queue`;
    await driver.queueUpsert({
      name: expiredQueue,
      now,
      updatedAt: now,
    });
    const expired = await driver.queueDeleteExpired({
      max: 10,
      updatedAtHorizon: Temporal.Instant.from("2026-08-30T13:00:00Z"),
    });
    expect(expired.map(({ name }) => name)).toContain(expiredQueue);

    const artifacts = await driver.indexReindexArtifacts(
      "river_job_args_index"
    );
    expect(artifacts).toEqual([]);
    await pool.query(
      "CREATE INDEX river_job_args_index_ccnew1 ON river_job (id)"
    );
    await expect(
      driver.indexReindexArtifacts("river_job_args_index")
    ).resolves.toEqual(["river_job_args_index_ccnew1"]);
    await pool.query("DROP INDEX river_job_args_index_ccnew1");

    await expect(
      driver.indexesExist(["river_job_args_index", "river_job_does_not_exist"])
    ).resolves.toEqual(
      new Map([
        ["river_job_args_index", true],
        ["river_job_does_not_exist", false],
      ])
    );
    const reindexLeader = (await driver.maintenanceLeaderAcquire(
      `${filePrefix}_reindexer`,
      Temporal.Now.instant(),
      60_000,
      null
    ))!;
    await expect(
      driver.maintenanceReindex(
        reindexLeader,
        ["river_job_args_index", "river_job_does_not_exist"],
        60_000,
        new AbortController().signal
      )
    ).resolves.toBe(1);
    await expect(driver.maintenanceLeaderResign(reindexLeader)).resolves.toBe(
      true
    );
  });
});

describe("PgDriver custom-schema integration", () => {
  const schema = `${filePrefix}_schema`;
  let driver: PgRuntime;
  let pool: pg.Pool;

  beforeAll(async () => {
    pool = new pg.Pool({ connectionString: TEST_DATABASE_URL });
    await pool.query(`CREATE SCHEMA "${schema}"`);
    await migrateSchema(pool, schema);
    driver = testPgDriver(pool, { schema });
  });

  afterAll(async () => {
    await pool.query(`DROP SCHEMA "${schema}" CASCADE`);
    await pool.end();
  });

  // Mirrors River's driver tests for `NotificationDeleteBefore`: rows are
  // inserted out of age order, and one sits exactly at the horizon.
  async function insertNotifications(): Promise<Temporal.Instant> {
    await pool.query(`DELETE FROM "${schema}".river_notification`);
    const now = Temporal.Now.instant()
      .round({ roundingMode: "floor", smallestUnit: "second" })
      .add({ milliseconds: 120 });
    await pool.query(
      `INSERT INTO "${schema}".river_notification (created_at, payload, topic)
       VALUES ($1, 'old_payload', 'topic'), ($2, 'oldest_payload', 'topic'),
              ($3, 'horizon_payload', 'topic'), ($4, 'new_payload', 'topic')`,
      [
        now.subtract({ minutes: 61 }).toString(),
        now.subtract({ hours: 2 }).toString(),
        now.subtract({ hours: 1 }).toString(),
        now.subtract({ minutes: 30 }).toString(),
      ]
    );
    return now.subtract({ hours: 1 });
  }

  async function notificationPayloads(): Promise<string[]> {
    const result = await pool.query<{ payload: string }>(
      `SELECT payload FROM "${schema}".river_notification ORDER BY created_at`
    );
    return result.rows.map(({ payload }) => payload);
  }

  it("deletes notifications before a horizon", async () => {
    const createdAtHorizon = await insertNotifications();

    await expect(
      driver.notificationDeleteBefore({ createdAtHorizon, max: 10 })
    ).resolves.toBe(2);
    expect(await notificationPayloads()).toEqual([
      "horizon_payload",
      "new_payload",
    ]);
  });

  it("deletes at most max notifications before a horizon, oldest first", async () => {
    const createdAtHorizon = await insertNotifications();
    const params = { createdAtHorizon, max: 1 };

    await expect(driver.notificationDeleteBefore(params)).resolves.toBe(1);
    // Delete by age, even when the oldest notification was inserted later.
    expect((await notificationPayloads())[0]).toBe("old_payload");
    await expect(driver.notificationDeleteBefore(params)).resolves.toBe(1);
    await expect(driver.notificationDeleteBefore(params)).resolves.toBe(0);
    // Keeps the notification exactly at the horizon.
    expect(await notificationPayloads()).toEqual([
      "horizon_payload",
      "new_payload",
    ]);
  });

  it("runs claim, completion, queues, and leadership in the configured schema", async () => {
    const inserted = await driver.jobInsert(
      insertParams(`${filePrefix}_custom`)
    );
    const claimed = (
      await driver.jobClaim({
        attemptedBy: `${filePrefix}_custom_worker`,
        kinds: [`${filePrefix}_custom`],
        queues: [{ limit: 1, name: "default" }],
      })
    ).jobs;
    expect(claimed.map(({ id }) => id)).toEqual([inserted.job.id]);

    const completed = await driver.jobCompleteMany([
      {
        attempt: 1,
        attemptedBy: `${filePrefix}_custom_worker`,
        error: null,
        id: inserted.job.id,
        kind: "complete",
        finalizedAt: Temporal.Now.instant(),
        output: null,
        outputSet: true,
        scheduledAt: null,
      },
    ]);
    expect(completed[0]).toMatchObject({ status: "applied" });
    expect(completed[0]!.job!.metadata).toMatchObject({ output: null });

    const queue = await driver.queueUpsert({
      metadata: { schema },
      name: `${filePrefix}_custom_queue`,
    });
    expect(queue.metadata).toEqual({ schema });

    const leader = await driver.leaderElect({
      leaderId: `${filePrefix}_custom_leader`,
      ttlSeconds: 30,
    });
    expect(leader!.leaderId).toBe(`${filePrefix}_custom_leader`);

    const publicCount = await pool.query<{ count: string }>(
      "SELECT count(*) FROM river_job WHERE kind = $1::text",
      [`${filePrefix}_custom`]
    );
    expect(publicCount.rows[0]!.count).toBe("0");
  });
});

describe("PgDriver client stop", () => {
  const schema = `${filePrefix}_stop`;
  let pool: pg.Pool;

  beforeAll(async () => {
    pool = new pg.Pool({ connectionString: TEST_DATABASE_URL });
    await pool.query(`CREATE SCHEMA "${schema}"`);
    await migrateSchema(pool, schema);
  });

  afterAll(async () => {
    await pool.query(`DROP SCHEMA "${schema}" CASCADE`);
    await pool.end();
  });

  it("waits for a maintenance transaction in flight and leaves none open", async () => {
    const definition = defineJob({ kind: `${filePrefix}_stop` });
    const inserted = await pool.query<{ id: string }>(
      `INSERT INTO "${schema}".river_job
         (args, finalized_at, kind, max_attempts, queue, state)
       VALUES ('{}', now() - interval '1 hour', $1, 25, 'default', 'completed')
       RETURNING id`,
      [definition.kind]
    );
    // A lock on the completed job blocks the job cleaner's delete inside
    // its leader-fenced transaction.
    const blocker = await pool.connect();
    const leaseClient = new pg.Client({ connectionString: TEST_DATABASE_URL });
    await leaseClient.connect();
    try {
      await blocker.query("BEGIN");
      await blocker.query(
        `SELECT id FROM "${schema}".river_job WHERE id = $1 FOR UPDATE`,
        [inserted.rows[0]?.id]
      );
      const client = new Client(new PgDriver(pool, { schema }), {
        clientId: `${filePrefix}_stop`,
        maintenance: {
          completedJobRetention: { milliseconds: 1 },
          electionInterval: { milliseconds: 50 },
          jobCleanerInterval: { milliseconds: 10 },
        },
        queues: { default: { maxWorkers: 1 } },
        workers: new Workers().add(definition, () => undefined),
      });
      const run = await client.start();
      await waitFor(async () => {
        const waiting = await leaseClient.query(
          `SELECT 1 FROM pg_stat_activity
           WHERE wait_event_type = 'Lock' AND query ILIKE '%river_job%'
             AND query ILIKE '%DELETE%'`
        );
        return (waiting.rowCount ?? 0) > 0;
      });

      let stopped = false;
      const stopping = run.stop().then(() => {
        stopped = true;
      });
      await new Promise((resolve) => setTimeout(resolve, 200));
      // Like River for Go, a stop waits for maintenance already running.
      expect(stopped).toBe(false);
      await blocker.query("ROLLBACK");
      await stopping;

      const open = await leaseClient.query(
        `SELECT pid, query FROM pg_stat_activity
         WHERE state LIKE 'idle in transaction%' AND pid <> pg_backend_pid()
           AND datname = current_database()`
      );
      expect(open.rows).toEqual([]);
    } finally {
      await blocker.query("ROLLBACK").catch(() => undefined);
      blocker.release();
      await leaseClient.end();
    }
  });

  it("waits for a maintenance transaction blocked at its leader fence", async () => {
    const definition = defineJob({ kind: `${filePrefix}_stop` });
    const inserted = await pool.query<{ id: string }>(
      `INSERT INTO "${schema}".river_job
         (args, finalized_at, kind, max_attempts, queue, state)
       VALUES ('{}', now() - interval '1 hour', $1, 25, 'default', 'completed')
       RETURNING id`,
      [definition.kind]
    );
    // A lock on the completed job blocks the job cleaner's delete inside
    // its leader-fenced transaction.
    const blocker = await pool.connect();
    const leaseClient = new pg.Client({ connectionString: TEST_DATABASE_URL });
    await leaseClient.connect();
    try {
      void inserted;
      const client = new Client(new PgDriver(pool, { schema }), {
        clientId: `${filePrefix}_stop`,
        maintenance: {
          completedJobRetention: { milliseconds: 1 },
          electionInterval: { milliseconds: 50 },
          jobCleanerInterval: { milliseconds: 10 },
        },
        queues: { default: { maxWorkers: 1 } },
        workers: new Workers().add(definition, () => undefined),
      });
      const run = await client.start();
      await waitFor(async () => {
        const leaders = await pool.query(
          `SELECT 1 FROM "${schema}".river_leader`
        );
        return (leaders.rowCount ?? 0) > 0;
      });
      await blocker.query("BEGIN");
      await blocker.query(`SELECT 1 FROM "${schema}".river_leader FOR UPDATE`);
      await waitFor(async () => {
        const waiting = await leaseClient.query(
          `SELECT 1 FROM pg_stat_activity
           WHERE wait_event_type = 'Lock' AND query ILIKE '%KEY SHARE%'`
        );
        return (waiting.rowCount ?? 0) > 0;
      });

      let stopped = false;
      const stopping = run.stop().then(() => {
        stopped = true;
      });
      await new Promise((resolve) => setTimeout(resolve, 200));
      // Like River for Go, a stop waits for maintenance already running.
      expect(stopped).toBe(false);
      await blocker.query("ROLLBACK");
      await stopping;

      const open = await leaseClient.query(
        `SELECT pid, query FROM pg_stat_activity
         WHERE state LIKE 'idle in transaction%' AND pid <> pg_backend_pid()
           AND datname = current_database()`
      );
      expect(open.rows).toEqual([]);
    } finally {
      await blocker.query("ROLLBACK").catch(() => undefined);
      blocker.release();
      await leaseClient.end();
    }
  });
});

describe("PgDriver clients with leader election disabled", () => {
  const schema = `${filePrefix}_no_leader`;
  let driver: PgRuntime;
  let pool: pg.Pool;

  beforeAll(async () => {
    pool = new pg.Pool({ connectionString: TEST_DATABASE_URL });
    await pool.query(`CREATE SCHEMA "${schema}"`);
    await migrateSchema(pool, schema);
    driver = testPgDriver(pool, { schema });
  });

  afterAll(async () => {
    await pool.query(`DROP SCHEMA "${schema}" CASCADE`);
    await pool.end();
  });

  afterEach(async () => {
    await pool.query(
      `TRUNCATE "${schema}".river_job, "${schema}".river_leader`
    );
  });

  const queueSettings = {
    fetchCooldown: { milliseconds: 1 },
    maxWorkers: 1,
    pollInterval: { milliseconds: 5 },
  };

  it.each([
    ["with notifications", false],
    ["poll only", true],
  ])("works jobs without leader election, %s", async (_name, pollOnly) => {
    const definition = defineJob({ kind: `${filePrefix}_no_leader` });
    const client = new Client(driver, {
      clientId: `${filePrefix}_no_leader`,
      completionFlushInterval: { milliseconds: 1 },
      leaderElectionDisabled: true,
      pollOnly,
      queues: { default: queueSettings },
      workers: new Workers().add(definition, () => undefined),
    });
    const run = await client.start();
    try {
      const inserted = await client.insert(definition, {});
      await expect
        .poll(async () => (await client.jobs.get(inserted.job.id))?.state)
        .toBe("completed");
      await expect(driver.leaderGet()).resolves.toBeNull();
      expect(run.diagnostics.maintenance).toBeNull();
    } finally {
      await run.stop();
    }
    await expect(driver.leaderGet()).resolves.toBeNull();
  });

  it("stays ineligible to lead after the leader stops", async () => {
    const definition = defineJob({ kind: `${filePrefix}_no_leader_periodic` });
    const workers = new Workers().add(definition, () => undefined);
    const workerId = `${filePrefix}_no_leader_worker`;
    const worker = new Client(driver, {
      clientId: workerId,
      completionFlushInterval: { milliseconds: 1 },
      leaderElectionDisabled: true,
      queues: { default: queueSettings },
      workers,
    });
    const workerRun = await worker.start();
    try {
      const leaderId = `${filePrefix}_no_leader_leader`;
      const leader = new Client(driver, {
        clientId: leaderId,
        periodicJobs: [
          periodicJob({
            args: {},
            every: { hours: 1 },
            job: definition,
            runOnStart: true,
          }),
        ],
        queues: { [`${filePrefix}_leader`]: queueSettings },
        workers,
      });
      const leaderRun = await leader.start();
      try {
        // The leader inserts a periodic job on the default queue, which only
        // the client with leader election disabled works.
        await expect
          .poll(
            async () =>
              (
                await worker.jobs.list({
                  kinds: [definition.kind],
                  states: ["completed"],
                })
              ).jobs.map(({ attemptedBy }) => attemptedBy),
            { timeout: 10_000 }
          )
          .toEqual([[workerId]]);
        await expect(driver.leaderGet()).resolves.toMatchObject({ leaderId });
      } finally {
        await leaderRun.stop();
      }

      const inserted = await worker.insert(definition, {});
      await expect
        .poll(async () => (await worker.jobs.get(inserted.job.id))?.state)
        .toBe("completed");
      await expect(driver.leaderGet()).resolves.toBeNull();
    } finally {
      await workerRun.stop();
    }
  });
});

async function waitFor(
  predicate: () => boolean | Promise<boolean>
): Promise<void> {
  const deadline = Date.now() + 2_000;
  while (!(await predicate())) {
    if (Date.now() > deadline) throw new Error("condition was not reached");
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

async function waitForListenerPID(
  pool: pg.Pool,
  topic: string,
  excludedPID?: number
): Promise<number> {
  const deadline = Date.now() + 2_000;
  while (true) {
    const result = await pool.query<{ pid: number }>(
      `SELECT pid
       FROM pg_stat_activity
       WHERE datname = current_database()
         AND query LIKE $1::text
         AND ($2::int IS NULL OR pid != $2::int)
       ORDER BY backend_start DESC
       LIMIT 1`,
      [`LISTEN %${topic}%`, excludedPID ?? null]
    );
    const pid = result.rows[0]?.pid;
    if (pid !== undefined) return pid;
    if (Date.now() > deadline) {
      throw new Error(`listener for ${topic} was not found`);
    }
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

/** Let every queue's insert notification through, with no limiter. */
function allowEveryQueue(queues: readonly string[]): readonly string[] {
  return [...new Set(queues)];
}
