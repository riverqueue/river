import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import pg from "pg";
import { Client, JobArgsObject } from "riverqueue";
import type { JobArgs } from "riverqueue";
import { PgDriver } from "./driver.js";

const TEST_DATABASE_URL =
  process.env.TEST_DATABASE_URL ||
  "postgres://localhost:5432/river_test?sslmode=disable";

// Per-file random prefix so parallel test files don't interfere with each
// other's cleanup.
const filePrefix = `pg_${Math.random().toString(36).slice(2, 8)}`;

describe("PgDriver integration", () => {
  let pool: pg.Pool;
  let client: Client;

  beforeAll(async () => {
    pool = new pg.Pool({ connectionString: TEST_DATABASE_URL });
    client = new Client(new PgDriver(pool));
  });

  afterAll(async () => {
    await pool.end();
  });

  afterEach(async () => {
    await pool.query("DELETE FROM river_job WHERE kind LIKE $1", [
      `${filePrefix}%`,
    ]);
  });

  it("inserts a job and returns it", async () => {
    const result = await client.insert(
      new JobArgsObject(`${filePrefix}_basic`, { key: "value" })
    );

    expect(result.job.id).toBeGreaterThan(0);
    expect(result.job.kind).toBe(`${filePrefix}_basic`);
    expect(result.job.args).toEqual({ key: "value" });
    expect(result.job.state).toBe("available");
    expect(result.job.queue).toBe("default");
    expect(result.job.priority).toBe(1);
    expect(result.job.maxAttempts).toBe(25);
    expect(result.job.attempt).toBe(0);
    expect(result.job.tags).toEqual([]);
    expect(result.job.metadata).toEqual({});
    expect(result.job.createdAt).toBeInstanceOf(Date);
    expect(result.job.scheduledAt).toBeInstanceOf(Date);
    expect(result.job.attemptedAt).toBeNull();
    expect(result.job.attemptedBy).toBeNull();
    expect(result.job.errors).toBeNull();
    expect(result.job.finalizedAt).toBeNull();
    expect(result.uniqueSkippedAsDuplicated).toBe(false);
  });

  it("inserts with all options", async () => {
    const future = new Date(Date.now() + 3_600_000);

    const result = await client.insert(
      new JobArgsObject(`${filePrefix}_opts`, { n: 42 }),
      {
        maxAttempts: 5,
        priority: 3,
        queue: "high_priority",
        scheduledAt: future,
        tags: ["tag_one", "tag_two"],
      }
    );

    expect(result.job.kind).toBe(`${filePrefix}_opts`);
    expect(result.job.maxAttempts).toBe(5);
    expect(result.job.priority).toBe(3);
    expect(result.job.queue).toBe("high_priority");
    expect(result.job.state).toBe("scheduled");
    expect(result.job.tags).toEqual(["tag_one", "tag_two"]);
  });

  it("inserts many jobs", async () => {
    const results = await client.insertMany([
      new JobArgsObject(`${filePrefix}_batch_a`, { i: 1 }),
      new JobArgsObject(`${filePrefix}_batch_b`, { i: 2 }),
      new JobArgsObject(`${filePrefix}_batch_c`, { i: 3 }),
    ]);

    expect(results).toHaveLength(3);

    const ids = results.map((r) => r.job.id);
    expect(new Set(ids).size).toBe(3);

    expect(results[0]!.job.kind).toBe(`${filePrefix}_batch_a`);
    expect(results[1]!.job.kind).toBe(`${filePrefix}_batch_b`);
    expect(results[2]!.job.kind).toBe(`${filePrefix}_batch_c`);
  });

  it("handles unique job insertion", async () => {
    const uniqueOpts = { byArgs: true as const, byQueue: true as const };

    const first = await client.insert(
      new JobArgsObject(`${filePrefix}_unique`, { key: "same" }),
      { uniqueOpts }
    );
    expect(first.uniqueSkippedAsDuplicated).toBe(false);
    expect(first.job.uniqueKey).not.toBeNull();

    const second = await client.insert(
      new JobArgsObject(`${filePrefix}_unique`, { key: "same" }),
      { uniqueOpts }
    );
    expect(second.uniqueSkippedAsDuplicated).toBe(true);
    expect(second.job.id).toBe(first.job.id);
  });

  it("allows unique jobs with different args", async () => {
    const uniqueOpts = { byArgs: true as const };

    const first = await client.insert(
      new JobArgsObject(`${filePrefix}_unique`, { key: "one" }),
      { uniqueOpts }
    );
    const second = await client.insert(
      new JobArgsObject(`${filePrefix}_unique`, { key: "two" }),
      { uniqueOpts }
    );

    expect(first.uniqueSkippedAsDuplicated).toBe(false);
    expect(second.uniqueSkippedAsDuplicated).toBe(false);
    expect(second.job.id).not.toBe(first.job.id);
  });

  it("uses custom class args with toJSON", async () => {
    const kind = `${filePrefix}_email`;

    class EmailArgs implements JobArgs {
      kind = kind;
      constructor(
        public to: string,
        public subject: string
      ) {}
      toJSON() {
        return { to: this.to, subject: this.subject };
      }
    }

    const result = await client.insert(
      new EmailArgs("user@example.com", "Hello")
    );

    expect(result.job.kind).toBe(kind);
    expect(result.job.args).toEqual({
      to: "user@example.com",
      subject: "Hello",
    });
  });

  it("verifies job exists in database after insert", async () => {
    const result = await client.insert(
      new JobArgsObject(`${filePrefix}_verify`, { data: "check" })
    );

    const dbResult = await pool.query("SELECT * FROM river_job WHERE id = $1", [
      result.job.id,
    ]);
    expect(dbResult.rowCount).toBe(1);
    expect(dbResult.rows[0].kind).toBe(`${filePrefix}_verify`);
    expect(dbResult.rows[0].args).toEqual({ data: "check" });
  });

  it("inserts within a transaction via tx option", async () => {
    const poolClient = await pool.connect();
    try {
      await poolClient.query("BEGIN");

      await client.insert(new JobArgsObject(`${filePrefix}_tx_1`, {}), {
        tx: poolClient,
      });
      await client.insert(new JobArgsObject(`${filePrefix}_tx_2`, {}), {
        tx: poolClient,
      });

      await poolClient.query("COMMIT");
    } finally {
      poolClient.release();
    }

    const dbResult = await pool.query(
      `SELECT kind FROM river_job WHERE kind LIKE '${filePrefix}_tx_%' ORDER BY kind`
    );
    expect(dbResult.rows.map((r) => r.kind)).toEqual([
      `${filePrefix}_tx_1`,
      `${filePrefix}_tx_2`,
    ]);
  });

  it("rolls back transaction via tx option", async () => {
    const poolClient = await pool.connect();
    try {
      await poolClient.query("BEGIN");

      await client.insert(new JobArgsObject(`${filePrefix}_rollback`, {}), {
        tx: poolClient,
      });

      await poolClient.query("ROLLBACK");
    } finally {
      poolClient.release();
    }

    const dbResult = await pool.query(
      `SELECT * FROM river_job WHERE kind = '${filePrefix}_rollback'`
    );
    expect(dbResult.rowCount).toBe(0);
  });
});
