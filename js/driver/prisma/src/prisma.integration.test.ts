// Runs the driver against a real generated Prisma client and Prisma's
// PostgreSQL adapter, rather than the `pg`-backed stand-in the other
// integration tests use, so Prisma's own parameter and result handling is
// covered. The client is generated into a temporary directory at startup.
import { execFile } from "node:child_process";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import { PrismaPg } from "@prisma/adapter-pg";
import pg from "pg";
import {
  Client,
  defineJob,
  exactJsonNumber,
  type InsertClient,
  isExactJsonNumber,
  jsonNumberToBigInt,
} from "riverqueue";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { testPrismaDriver } from "./driver.js";
import type { PrismaClientLike, PrismaInserter } from "./driver.js";

const TEST_DATABASE_URL =
  process.env.TEST_DATABASE_URL ??
  "postgres://localhost:5432/river_test?sslmode=disable";
const packageDirectory = fileURLToPath(new URL("..", import.meta.url));
const filePrefix = `real_prisma_${Math.random().toString(36).slice(2, 8)}`;

interface GeneratedPrismaClient extends PrismaClientLike {
  $disconnect(): Promise<void>;
  $transaction<T>(
    callback: (transaction: PrismaClientLike) => Promise<T>
  ): Promise<T>;
}

const accountJob = defineJob()({
  kind: `${filePrefix}_account`,
});

describe("PrismaDriver with a real Prisma client", () => {
  let client: InsertClient<PrismaClientLike>;
  let driver: PrismaInserter;
  let generatedDirectory: string;
  let pool: pg.Pool;
  let prisma: GeneratedPrismaClient;

  beforeAll(async () => {
    // Generate inside the package, in its ignored temp directory, so the
    // client resolves Prisma's runtime from the package's dependencies.
    const tempDirectory = join(packageDirectory, "temp");
    await mkdir(tempDirectory, { recursive: true });
    generatedDirectory = await mkdtemp(join(tempDirectory, "prisma-"));
    const schema = join(generatedDirectory, "schema.prisma");
    await writeFile(
      schema,
      `generator client {
  provider               = "prisma-client"
  output                 = "./client"
  moduleFormat           = "esm"
  generatedFileExtension = "ts"
  importFileExtension    = "ts"
}

datasource db {
  provider = "postgresql"
}
`
    );
    await promisify(execFile)(
      join(packageDirectory, "node_modules", ".bin", "prisma"),
      ["generate", "--schema", schema],
      { cwd: packageDirectory }
    );
    const generated = (await import(
      join(generatedDirectory, "client", "client.ts")
    )) as {
      PrismaClient: new (options: {
        adapter: PrismaPg;
      }) => GeneratedPrismaClient;
    };
    prisma = new generated.PrismaClient({
      adapter: new PrismaPg({ connectionString: TEST_DATABASE_URL }),
    });
    driver = testPrismaDriver(prisma);
    client = new Client(driver);
    pool = new pg.Pool({ connectionString: TEST_DATABASE_URL });
  }, 60_000);

  afterAll(async () => {
    await pool.query("DELETE FROM river_job WHERE kind LIKE $1", [
      `${filePrefix}%`,
    ]);
    await pool.end();
    await prisma.$disconnect();
    await rm(generatedDirectory, { force: true, recursive: true });
  });

  it("keeps integers beyond JavaScript's safe range exact", async () => {
    const { job } = await client.insert(accountJob, {
      accountId: exactJsonNumber("9223372036854775807"),
    });

    const accountId = job.args.accountId;
    expect(isExactJsonNumber(accountId) && jsonNumberToBigInt(accountId)).toBe(
      9_223_372_036_854_775_807n
    );
    const stored = await pool.query<{ args: string }>(
      "SELECT args::text FROM river_job WHERE id = $1",
      [job.id.toString(10)]
    );
    expect(stored.rows[0]?.args).toBe('{"accountId": 9223372036854775807}');
  });

  it("commits and rolls back with Prisma's interactive transactions", async () => {
    const committed = await prisma.$transaction(
      async (tx) =>
        (await client.insert(accountJob, { accountId: "committed" }, { tx }))
          .job
    );
    const rollback = new Error("roll back");
    let rolledBackId: bigint | undefined;
    await expect(
      prisma.$transaction(async (tx) => {
        rolledBackId = (
          await client.insert(accountJob, { accountId: "rolled back" }, { tx })
        ).job.id;
        throw rollback;
      })
    ).rejects.toBe(rollback);

    const ids = await pool.query<{ id: string }>(
      "SELECT id::text FROM river_job WHERE id = ANY($1::bigint[])",
      [[committed.id.toString(10), rolledBackId?.toString(10) ?? "0"]]
    );
    expect(ids.rows.map(({ id }) => id)).toEqual([committed.id.toString(10)]);
  });

  it("rolls a non-transactional insert back when middleware throws after next()", async () => {
    const failure = new Error("fails after the write");
    const failing = new Client(driver, {
      insertMiddleware: [
        async (_context, next) => {
          await next();
          throw failure;
        },
      ],
    });

    await expect(
      failing.insert(accountJob, { accountId: "middleware rollback" })
    ).rejects.toBe(failure);

    const rows = await pool.query(
      "SELECT 1 FROM river_job WHERE kind = $1 AND args->>'accountId' = $2",
      [accountJob.kind, "middleware rollback"]
    );
    expect(rows.rowCount).toBe(0);
  });

  it("notifies workers of an available job once its transaction commits", async () => {
    const queue = `${filePrefix}_queue`;
    const listener = new pg.Client({ connectionString: TEST_DATABASE_URL });
    await listener.connect();
    try {
      const notified = new Promise<string | undefined>((resolve) => {
        listener.on("notification", ({ payload }) => {
          if (payload?.includes(queue) === true) resolve(payload);
        });
      });
      const schema = await listener.query<{ schema: string }>(
        "SELECT current_schema() AS schema"
      );
      await listener.query(
        `LISTEN "${schema.rows[0]?.schema ?? "public"}.river_insert"`
      );

      await prisma.$transaction(async (tx) => {
        await client.insert(accountJob, { accountId: "notify" }, { queue, tx });
      });

      await expect(notified).resolves.toContain(queue);
    } finally {
      await listener.end();
    }
  });

  it("keeps a reinserted job's creation time", async () => {
    const createdAt = Temporal.Instant.from("2026-01-02T03:04:05.123456Z");

    const [result] = await driver.jobInsertMany([
      {
        args: { accountId: "reinserted" },
        createdAt,
        encodedArgs: '{"accountId":"reinserted"}',
        kind: accountJob.kind,
        maxAttempts: 25,
        metadata: {},
        priority: 1,
        queue: "default",
        scheduledAt: Temporal.Now.instant(),
        state: "available",
        tags: [],
        uniqueKey: null,
        uniqueStates: null,
      },
    ]);

    expect(result?.job.createdAt.toString()).toBe(createdAt.toString());
  });
});
