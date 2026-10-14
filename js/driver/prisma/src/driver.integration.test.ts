import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { Pool } from "pg";
import type { PoolClient } from "pg";
import {
  Client,
  defineJob,
  exactJsonNumber,
  type InsertClient,
  isExactJsonNumber,
  type JsonValue,
} from "riverqueue";
import { PrismaDriver } from "./driver.js";
import type { PrismaClientLike } from "./driver.js";

const TEST_DATABASE_URL =
  process.env.TEST_DATABASE_URL ??
  "postgres://localhost:5432/river_test?sslmode=disable";
const filePrefix = `prisma_${Math.random().toString(36).slice(2, 8)}`;

class PgPrismaAdapter implements PrismaClientLike {
  constructor(private readonly pool: Pool | PoolClient) {}

  async $queryRawUnsafe<T = unknown>(
    sql: string,
    ...values: unknown[]
  ): Promise<T> {
    const result = await this.pool.query(sql, values);
    return result.rows as T;
  }
}

/** A root client whose `$transaction` stands in for Prisma's. */
class PgPrismaRootAdapter extends PgPrismaAdapter {
  constructor(private readonly rootPool: Pool) {
    super(rootPool);
  }

  async $transaction<R>(
    callback: (tx: PrismaClientLike) => Promise<R>
  ): Promise<R> {
    const client = await this.rootPool.connect();
    try {
      await client.query("BEGIN");
      try {
        const result = await callback(new PgPrismaAdapter(client));
        await client.query("COMMIT");
        return result;
      } catch (error: unknown) {
        await client.query("ROLLBACK");
        throw error;
      }
    } finally {
      client.release();
    }
  }
}

function job(suffix: string) {
  return defineJob<{ key?: string; n?: number }>()({
    kind: `${filePrefix}_${suffix}`,
  });
}

describe("PrismaDriver integration", () => {
  let pool: Pool;
  let client: InsertClient<PrismaClientLike>;

  beforeAll(() => {
    pool = new Pool({ connectionString: TEST_DATABASE_URL });
    client = new Client(new PrismaDriver(new PgPrismaRootAdapter(pool)));
  });

  afterAll(async () => {
    await pool.end();
  });

  afterEach(async () => {
    await pool.query("DELETE FROM river_job WHERE kind LIKE $1", [
      `${filePrefix}%`,
    ]);
  });

  it("inserts and decodes an exact job row", async () => {
    const definition = job("basic");

    const result = await client.insert(definition, { key: "value" });

    expect(result.job.id).toBeGreaterThan(0n);
    expect(result.job.kind).toBe(definition.kind);
    expect(result.job.args).toEqual({ key: "value" });
    expect(result.job.createdAt).toBeInstanceOf(Temporal.Instant);
    expect(result.job.scheduledAt).toBeInstanceOf(Temporal.Instant);
    expect(result.status).toBe("inserted");
  });

  it("inserts with all options", async () => {
    const definition = job("opts");
    const future = Temporal.Now.instant()
      .add({ hours: 1 })
      .round({ roundingMode: "trunc", smallestUnit: "microsecond" });

    const result = await client.insert(
      definition,
      { n: 42 },
      {
        maxAttempts: 5,
        metadata: { source: "integration" },
        priority: 3,
        queue: "high_priority",
        scheduledAt: future,
        tags: ["tag_one", "tag_two"],
      }
    );

    expect(result.job.maxAttempts).toBe(5);
    expect(result.job.metadata).toEqual({ source: "integration" });
    expect(result.job.priority).toBe(3);
    expect(result.job.queue).toBe("high_priority");
    expect(result.job.scheduledAt.toString()).toBe(future.toString());
    expect(result.job.state).toBe("scheduled");
  });

  it("clamps a wide maxAttempts to Postgres's smallint like Go", async () => {
    const result = await client.insert(
      job("clamped"),
      {},
      { maxAttempts: 40_000 }
    );

    expect(result.job.maxAttempts).toBe(32_767);
  });

  it("preserves order for heterogeneous batches", async () => {
    const a = job("batch_a");
    const b = job("batch_b");

    const results = await client.insertMany([
      { args: { n: 1 }, job: a },
      { args: { n: 2 }, job: b },
    ]);

    expect(results.map((result) => result.job.kind)).toEqual([a.kind, b.kind]);
    expect(new Set(results.map((result) => result.job.id)).size).toBe(2);
  });

  it("returns the existing row for a unique conflict", async () => {
    const definition = job("unique");
    const unique = { byArgs: true as const, byQueue: true as const };

    const first = await client.insert(definition, { key: "same" }, { unique });
    const second = await client.insert(definition, { key: "same" }, { unique });

    expect(first.status).toBe("inserted");
    expect(second.status).toBe("duplicate");
    expect(second.job.id).toBe(first.job.id);
  });

  it("keeps the existing job's kind on a unique skip of another kind", async () => {
    const a = job("unique_kind_a");
    const b = job("unique_kind_b");
    const options = { unique: { byArgs: true, excludeKind: true } } as const;

    const first = await client.insert(a, { key: "same" }, options);
    const single = await client.insert(b, { key: "same" }, options);
    const [batched] = await client.insertMany([
      { args: { key: "same" }, job: b, options },
    ]);

    expect(first.status).toBe("inserted");
    for (const result of [single, batched]) {
      expect(result.status).toBe("duplicate");
      expect(result.job.id).toBe(first.job.id);
      expect(result.job.kind).toBe(a.kind);
    }
    const stored = await pool.query<{ kind: string }>(
      "SELECT kind FROM river_job WHERE kind = ANY($1)",
      [[a.kind, b.kind]]
    );
    expect(stored.rows).toEqual([{ kind: a.kind }]);
  });

  it("uses the exact caller-owned transaction", async () => {
    const definition = job("tx");
    const poolClient = await pool.connect();
    try {
      await poolClient.query("BEGIN");
      const tx = new PgPrismaAdapter(poolClient);
      await client.insert(definition, {}, { tx });
      await poolClient.query("ROLLBACK");
    } finally {
      poolClient.release();
    }

    const result = await pool.query(
      "SELECT count(*)::int AS count FROM river_job WHERE kind = $1",
      [definition.kind]
    );
    expect(result.rows[0]?.count).toBe(0);
  });

  it("keeps integers beyond JavaScript's safe range exact", async () => {
    const definition = defineJob<{ id: JsonValue }>()({
      kind: `${filePrefix}_exact`,
    });

    const result = await client.insert(
      definition,
      { id: exactJsonNumber("9007199254740993") },
      { metadata: { tenant: exactJsonNumber("9007199254740995") } }
    );

    expect(isExactJsonNumber(result.job.args.id)).toBe(true);
    expect(JSON.stringify(result.job.args)).toBe('{"id":9007199254740993}');
    expect(JSON.stringify(result.job.metadata)).toBe(
      '{"tenant":9007199254740995}'
    );
  });

  it("notifies producers of available jobs when the transaction commits", async () => {
    const available = job("notify_available");
    const scheduled = job("notify_scheduled");
    const queue = `${filePrefix}_notify`;
    const listener = await pool.connect();
    const payloads: string[] = [];
    listener.on("notification", (message) => {
      if (message.payload !== undefined) payloads.push(message.payload);
    });
    const poolClient = await pool.connect();
    try {
      const schema = await listener.query<{ schema: string }>(
        "SELECT current_schema()::text AS schema"
      );
      await listener.query(`LISTEN "${schema.rows[0]!.schema}.river_insert"`);
      await poolClient.query("BEGIN");
      const tx = new PgPrismaAdapter(poolClient);
      await client.insertMany(
        [
          {
            args: {},
            job: scheduled,
            options: {
              queue: `${queue}_later`,
              scheduledAt: Temporal.Now.instant().add({ hours: 1 }),
            },
          },
          { args: {}, job: available, options: { queue } },
        ],
        { tx }
      );
      // NOTIFY is transactional: nothing is delivered before commit.
      await listener.query("SELECT 1");
      expect(payloads).toEqual([]);
      await poolClient.query("COMMIT");

      const deadline = Date.now() + 2_000;
      while (payloads.length === 0 && Date.now() < deadline) {
        await listener.query("SELECT 1");
      }
      expect(payloads.map((payload) => JSON.parse(payload))).toEqual([
        { queue },
      ]);
    } finally {
      poolClient.release();
      await listener.query("UNLISTEN *");
      listener.release();
    }
  });
});
