import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { Pool, PoolClient } from "pg";
import { Client, JobArgsObject } from "riverqueue";
import type { JobArgs } from "riverqueue";
import { PrismaDriver } from "./driver.js";
import type { PrismaClientLike } from "./driver.js";

const TEST_DATABASE_URL =
  process.env.TEST_DATABASE_URL ||
  "postgres://localhost:5432/river_test?sslmode=disable";

// Per-file random prefix so parallel test files don't interfere with each
// other's cleanup.
const filePrefix = `prisma_${Math.random().toString(36).slice(2, 8)}`;

// Adapts a pg Pool/PoolClient to the PrismaClientLike interface so the Prisma
// driver's actual SQL and row mapping can be tested against a real database
// without requiring a full Prisma setup.
class PgPrismaAdapter implements PrismaClientLike {
  constructor(private pool: Pool | PoolClient) {}

  async $queryRawUnsafe<T = unknown>(
    sql: string,
    ...values: unknown[]
  ): Promise<T> {
    const result = await this.pool.query(sql, values);
    return result.rows as T;
  }
}

describe("PrismaDriver integration", () => {
  let pool: Pool;
  let client: Client;

  beforeAll(async () => {
    pool = new Pool({ connectionString: TEST_DATABASE_URL });
    client = new Client(new PrismaDriver(new PgPrismaAdapter(pool)));
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
    expect(result.job.createdAt).toBeInstanceOf(Date);
    expect(result.job.scheduledAt).toBeInstanceOf(Date);
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

    const second = await client.insert(
      new JobArgsObject(`${filePrefix}_unique`, { key: "same" }),
      { uniqueOpts }
    );
    expect(second.uniqueSkippedAsDuplicated).toBe(true);
    expect(second.job.id).toBe(first.job.id);
  });

  it("uses custom class args with toJSON", async () => {
    const kind = `${filePrefix}_notify`;

    class NotifyArgs implements JobArgs {
      kind = kind;
      constructor(public channel: string) {}
      toJSON() {
        return { channel: this.channel };
      }
    }

    const result = await client.insert(new NotifyArgs("general"));

    expect(result.job.kind).toBe(kind);
    expect(result.job.args).toEqual({ channel: "general" });
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
  });

  it("inserts within a transaction via tx option", async () => {
    const poolClient = await pool.connect();
    try {
      await poolClient.query("BEGIN");
      const txAdapter = new PgPrismaAdapter(poolClient);

      await client.insert(new JobArgsObject(`${filePrefix}_tx_1`, {}), {
        tx: txAdapter,
      });
      await client.insert(new JobArgsObject(`${filePrefix}_tx_2`, {}), {
        tx: txAdapter,
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
      const txAdapter = new PgPrismaAdapter(poolClient);

      await client.insert(new JobArgsObject(`${filePrefix}_rollback`, {}), {
        tx: txAdapter,
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
