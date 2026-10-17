// Exercise native interactive transactions with a generated Prisma client,
// including model writes on the same connection as River's completion.
import { execFile } from "node:child_process";
import { randomUUID } from "node:crypto";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";
import { PrismaPg } from "@prisma/adapter-pg";
import { Pool, type ClientBase } from "pg";
import {
  Client,
  defineJob,
  exactJsonNumber,
  type JobRow,
  type WorkHandler,
  Workers,
} from "riverqueue";
import { PilotClient, type PilotFactory } from "riverqueue/unstable-driver";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { createMigrator } from "../../../migrate/src/index.js";
import { PgDriver } from "../../pg/src/index.js";
import {
  prismaTransactionAdapter,
  type PrismaClientLike,
  type PrismaTransactionClientLike,
} from "./index.js";

const TEST_DATABASE_URL =
  process.env.TEST_DATABASE_URL ??
  "postgres://localhost:5432/river_test?sslmode=disable";
const packageDirectory = fileURLToPath(new URL("..", import.meta.url));

interface ApplicationRows {
  count(): Promise<number>;
  create(options: { data: { value: string } }): Promise<{ value: string }>;
  findMany(options?: {
    orderBy: { value: "asc" };
  }): Promise<{ value: string }[]>;
}

class CompanionClient extends PilotClient<
  ClientBase | PrismaTransactionClientLike
> {}

interface GeneratedPrismaClient extends PrismaClientLike {
  applicationRow: ApplicationRows;
  $disconnect(): Promise<void>;
  $transaction<T>(
    callback: (transaction: TransactionClient) => Promise<T>
  ): Promise<T>;
}

interface TransactionClient extends PrismaTransactionClientLike {
  applicationRow: ApplicationRows;
  $executeRawUnsafe(query: string, ...values: unknown[]): Promise<number>;
  $transaction<T>(
    callback: (transaction: TransactionClient) => Promise<T>
  ): Promise<T>;
}

describe("Prisma transactional completion", () => {
  const schema = `prisma_work_${randomUUID().replaceAll("-", "")}`;
  const definition = defineJob({ kind: "prisma_work" });
  let generatedDirectory: string;
  let pool: Pool;
  let prisma: GeneratedPrismaClient;

  beforeAll(async () => {
    pool = new Pool({ connectionString: TEST_DATABASE_URL });
    await pool.query(`CREATE SCHEMA "${schema}"`);
    await createMigrator({ pool, schema }).migrateUp();
    await pool.query(
      `CREATE TABLE "${schema}".application_row (value text PRIMARY KEY)`
    );
    const tempDirectory = join(packageDirectory, "temp");
    await mkdir(tempDirectory, { recursive: true });
    generatedDirectory = await mkdtemp(join(tempDirectory, "prisma-work-"));
    const schemaFile = join(generatedDirectory, "schema.prisma");
    await writeFile(
      schemaFile,
      `generator client {
  provider               = "prisma-client"
  output                 = "./client"
  moduleFormat           = "esm"
  generatedFileExtension = "ts"
  importFileExtension    = "ts"
}

datasource db {
  provider = "postgresql"
  schemas  = ["${schema}"]
}

model ApplicationRow {
  value String @id
  @@map("application_row")
  @@schema("${schema}")
}
`
    );
    await promisify(execFile)(
      join(packageDirectory, "node_modules", ".bin", "prisma"),
      ["generate", "--schema", schemaFile],
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
  }, 60_000);

  afterAll(async () => {
    await prisma.$disconnect();
    await pool.query(`DROP SCHEMA "${schema}" CASCADE`);
    await pool.end();
    await rm(generatedDirectory, { force: true, recursive: true });
  });

  afterEach(async () => {
    await pool.query(
      `TRUNCATE "${schema}".application_row, "${schema}".river_job`
    );
  });

  const runWorker = async (
    handler: WorkHandler<typeof definition, PrismaTransactionClientLike>,
    createPilot?: PilotFactory<ClientBase | PrismaTransactionClientLike>
  ): Promise<JobRow> => {
    const driver = new PgDriver(pool, {
      schema,
      transactionAdapters: [prismaTransactionAdapter],
    });
    const options = {
      completionBatchSize: 1,
      leaderElectionDisabled: true,
      queues: {
        default: {
          fetchCooldown: { milliseconds: 1 },
          maxWorkers: 1,
          pollInterval: { milliseconds: 5 },
        },
      },
      workers: new Workers<PrismaTransactionClientLike>().add(
        definition,
        handler
      ),
    };
    const client =
      createPilot === undefined
        ? new Client(driver, options)
        : new CompanionClient(driver, options, createPilot);
    const inserted = await client.insert(
      definition,
      { id: exactJsonNumber("9223372036854775807") },
      { maxAttempts: 1 }
    );
    await pool.query(
      `UPDATE "${schema}".river_job SET created_at = '2026-10-17T12:34:56.123456Z' WHERE id = $1`,
      [inserted.job.id.toString()]
    );
    const events = client.subscribe({
      kinds: ["job_completed", "job_failed"],
      signal: AbortSignal.timeout(10_000),
    });
    const run = await client.start();
    try {
      let completed = false;
      for await (const event of events) {
        if (
          event.kind === "subscription_lag" ||
          event.job.id !== inserted.job.id
        )
          continue;
        completed = true;
        break;
      }
      expect(completed).toBe(true);
      const row = await client.jobs.get(inserted.job.id);
      expect(row).not.toBeNull();
      return row!;
    } finally {
      events.close();
      await run.stop({ timeout: { seconds: 5 } });
    }
  };

  it("commits model writes and exact completion values together", async () => {
    let completed: JobRow | undefined;
    const row = await runWorker(async ({ completeTx, job, setMetadata }) => {
      setMetadata("custom", "value");
      await prisma.$transaction(async (tx) => {
        await tx.applicationRow.create({
          data: { value: "committed ? $1 ' ☃" },
        });
        completed = await completeTx(tx, {
          output: { id: exactJsonNumber("9223372036854775807") },
        });
        expect(completed.id).toBe(job.id);
        expect(completed.createdAt.toString()).toBe(
          "2026-10-17T12:34:56.123456Z"
        );
        expect(completed.finalizedAt).toBeInstanceOf(Temporal.Instant);
        expect(completed.state).toBe("completed");
        expect(JSON.stringify(completed.args)).toBe(
          '{"id":9223372036854775807}'
        );
        expect(
          (
            await pool.query(
              `SELECT state FROM "${schema}".river_job WHERE id = $1`,
              [job.id.toString()]
            )
          ).rows[0].state
        ).toBe("running");
        expect(await prisma.applicationRow.count()).toBe(0);
      });
      // A later handler failure cannot undo a completion already committed.
      throw new Error("after commit");
    });
    expect(row.state).toBe("completed");
    expect(row.metadata.custom).toBe("value");
    expect(JSON.stringify(row.metadata.output)).toBe(
      '{"id":9223372036854775807}'
    );
    expect(await prisma.applicationRow.findMany()).toEqual([
      { value: "committed ? $1 ' ☃" },
    ]);
    expect(completed?.finalizedAt?.toString()).toBe(
      row.finalizedAt?.toString()
    );
  });

  it("inserts and reads through the configured runtime transaction adapter", async () => {
    const client = new Client(
      new PgDriver(pool, {
        schema,
        transactionAdapters: [prismaTransactionAdapter],
      })
    );
    let id: bigint | undefined;
    await expect(
      prisma.$transaction(async (tx) => {
        const { job } = await client.insert(
          definition,
          { id: exactJsonNumber("9223372036854775807") },
          { tx }
        );
        id = job.id;
        const fetched = await client.jobs.get(id, { tx });
        expect(fetched).toEqual(job);
        expect(await client.jobs.get(id)).toBeNull();
        throw new Error("rollback inserted job");
      })
    ).rejects.toThrow("rollback inserted job");
    expect(id).toBeDefined();
    expect(await client.jobs.get(id!)).toBeNull();
  });

  it("keeps completion interceptors inside the Prisma transaction", async () => {
    let expectedTx: TransactionClient | undefined;
    let intercepted = false;
    const row = await runWorker(
      async ({ completeTx }) => {
        await prisma.$transaction(async (tx) => {
          expectedTx = tx;
          await completeTx(tx);
          expect(intercepted).toBe(true);
          expect(await tx.applicationRow.findMany()).toEqual([
            { value: "intercepted" },
          ]);
          throw new Error("rollback interceptor and completion");
        });
      },
      () => ({
        intercept: {
          complete: async (context, next) => {
            if (context.tx !== expectedTx) return next();
            intercepted = true;
            await expectedTx.applicationRow.create({
              data: { value: "intercepted" },
            });
            const result = await next();
            expect(await prisma.applicationRow.count()).toBe(0);
            return result;
          },
        },
      })
    );
    expect(intercepted).toBe(true);
    expect(row.state).toBe("discarded");
    expect(await prisma.applicationRow.count()).toBe(0);
  });

  it("keeps Postgres retryability for a blocked transactional write", async () => {
    const client = new Client(
      new PgDriver(pool, {
        schema,
        transactionAdapters: [prismaTransactionAdapter],
      })
    );
    const { job } = await client.insert(definition, {});
    const holder = await pool.connect();
    try {
      await holder.query("BEGIN");
      await holder.query(
        `SELECT id FROM "${schema}".river_job WHERE id = $1 FOR UPDATE`,
        [job.id.toString()]
      );
      await expect(
        prisma.$transaction(async (tx) => {
          await tx.$executeRawUnsafe("SET LOCAL lock_timeout = '10ms'");
          await client.jobs.update(
            job.id,
            { metadata: { blocked: true } },
            { tx }
          );
        })
      ).rejects.toMatchObject({
        code: "database",
        operation: "jobUpdate",
        retryable: true,
      });
    } finally {
      await holder.query("ROLLBACK");
      holder.release();
    }
    expect((await client.jobs.get(job.id))?.metadata).not.toHaveProperty(
      "blocked"
    );
  });

  it("preserves exact rows across transactional job and queue operations", async () => {
    const client = new Client(
      new PgDriver(pool, {
        schema,
        transactionAdapters: [prismaTransactionAdapter],
      })
    );
    const exact = exactJsonNumber("9223372036854775807");
    await pool.query(
      `INSERT INTO "${schema}".river_queue (name) VALUES ('controls')`
    );
    await prisma.$transaction(async (tx) => {
      const inserted = await client.insertMany(
        [
          {
            job: definition,
            args: { id: exact },
            options: { unique: { byArgs: true } },
          },
          { job: definition, args: { id: "second" } },
        ],
        { tx }
      );
      expect(inserted.map(({ status }) => status)).toEqual([
        "inserted",
        "inserted",
      ]);
      const first = inserted[0].job;
      const second = inserted[1].job;
      const duplicate = await client.insert(
        definition,
        { id: exact },
        { unique: { byArgs: true }, tx }
      );
      expect(duplicate.status).toBe("duplicate");
      expect(duplicate.job).toEqual(first);
      expect(first.uniqueKey).toBeInstanceOf(Uint8Array);
      expect(first.uniqueStates).not.toBeNull();
      expect((await client.jobs.list({ tx })).jobs.map(({ id }) => id)).toEqual(
        [first.id, second.id]
      );
      expect(
        (await client.jobs.update(first.id, { metadata: { exact } }, { tx }))
          ?.metadata.exact
      ).toEqual(exact);
      expect((await client.jobs.cancel(first.id, { tx }))?.state).toBe(
        "cancelled"
      );
      expect((await client.jobs.retry(first.id, { tx }))?.state).toBe(
        "available"
      );
      expect((await client.jobs.delete(first.id, { tx }))?.id).toBe(first.id);
      expect(
        (await client.jobs.deleteMany({ ids: [second.id], tx })).map(
          ({ id }) => id
        )
      ).toEqual([second.id]);
      expect(await client.jobs.get(first.id, { tx })).toBeNull();
      expect(await client.jobs.delete(first.id, { tx })).toBeNull();
      expect(
        (
          await client.queues.update(
            "controls",
            { metadata: { exact } },
            { tx }
          )
        )?.metadata.exact
      ).toEqual(exact);
      expect(
        (await client.queues.get("controls", { tx }))?.createdAt
      ).toBeInstanceOf(Temporal.Instant);
      expect(
        (await client.queues.pause("controls", { tx }))?.pausedAt
      ).toBeInstanceOf(Temporal.Instant);
      expect(
        (await client.queues.resume("controls", { tx }))?.pausedAt
      ).toBeNull();
      expect(
        (await client.queues.list({ tx })).queues.map(({ name }) => name)
      ).toContain("controls");
      expect(await client.queues.pause("missing", { tx })).toBeNull();
    });
  });

  it("rejects closed, root, and unconfigured transaction clients", async () => {
    const client = new Client(
      new PgDriver(pool, {
        schema,
        transactionAdapters: [prismaTransactionAdapter],
      })
    );
    // @ts-expect-error A root PrismaClient is not an interactive transaction.
    await expect(client.jobs.get(1n, { tx: prisma })).rejects.toMatchObject({
      code: "backend_mismatch",
    });
    const unconfigured = new Client(new PgDriver(pool, { schema }));
    const tx = await prisma.$transaction(async (tx) => {
      // @ts-expect-error Prisma transactions require a configured adapter.
      await expect(unconfigured.jobs.get(1n, { tx })).rejects.toMatchObject({
        code: "backend_mismatch",
      });
      return tx;
    });
    let rejected = false;
    const row = await runWorker(async ({ completeTx }) => {
      await expect(completeTx(tx)).rejects.toMatchObject({
        cause: { code: "P2028" },
      });
      rejected = true;
      throw new Error("transaction closed");
    });
    expect(rejected).toBe(true);
    expect(row.state).toBe("discarded");
  });

  it("rejects completion from an attempt whose identity changed", async () => {
    let rejected = false;
    const row = await runWorker(async ({ completeTx, job }) => {
      await prisma.$transaction(async (tx) => {
        await tx.$executeRawUnsafe(
          `UPDATE "${schema}".river_job SET attempt = attempt + 1 WHERE id = $1::bigint`,
          job.id.toString()
        );
        await expect(completeTx(tx)).rejects.toThrow("no longer running");
        rejected = true;
        throw new Error("rollback attempt change");
      });
    });
    expect(rejected).toBe(true);
    expect(row.state).toBe("discarded");
    expect(row.attempt).toBe(1);
  });

  it("rolls back model writes and completion together", async () => {
    let completed = false;
    const row = await runWorker(async ({ completeTx }) => {
      await prisma.$transaction(async (tx) => {
        await tx.applicationRow.create({ data: { value: "rolled back" } });
        await completeTx(tx, { output: { rolledBack: true } });
        completed = true;
        throw new Error("rollback both writes");
      });
    });
    expect(row.state).toBe("discarded");
    expect(completed).toBe(true);
    expect(row.metadata).not.toHaveProperty("output");
    expect(await prisma.applicationRow.count()).toBe(0);
  });

  it("uses native savepoints without ending the outer transaction", async () => {
    const row = await runWorker(async ({ completeTx }) => {
      await prisma.$transaction(async (tx) => {
        await tx.applicationRow.create({ data: { value: "outer" } });
        await expect(
          tx.$transaction(async (nested) => {
            await nested.applicationRow.create({ data: { value: "inner" } });
            await completeTx(nested, { output: { inner: true } });
            throw new Error("rollback savepoint");
          })
        ).rejects.toThrow("rollback savepoint");
        await completeTx(tx, { output: { outer: true } });
        await tx.applicationRow.create({ data: { value: "after completion" } });
      });
    });
    expect(row.state).toBe("completed");
    expect(row.metadata.output).toEqual({ outer: true });
    expect(
      await prisma.applicationRow.findMany({ orderBy: { value: "asc" } })
    ).toEqual([{ value: "after completion" }, { value: "outer" }]);
  });
});
