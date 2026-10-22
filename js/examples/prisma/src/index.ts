import { randomUUID } from "node:crypto";
import assert from "node:assert/strict";

import { PrismaPg } from "@prisma/adapter-pg";
import { PgDriver } from "@riverqueue/driver-pg";
import {
  PrismaDriver,
  prismaTransactionAdapter,
} from "@riverqueue/driver-prisma";
import { Pool } from "pg";
import { Client, defineJob, Workers } from "riverqueue";
import { PrismaClient, type Prisma } from "./generated/prisma/client.js";
import { z } from "zod";

const sort = defineJob<{ strings: string[] }>()({ kind: "sort" });
const syncAccount = defineJob({
  kind: "sync_account",
  schema: z.object({ accountId: z.string().min(1) }),
});

const sendEmail = defineJob<{
  body: string;
  subject: string;
  to: string;
}>()({
  defaults: {
    maxAttempts: 5,
    priority: 2,
    queue: "email",
  },
  kind: "send_email",
});

async function main() {
  const connectionString =
    process.env.DATABASE_URL ?? "postgres://localhost:5432/river_dev";
  const prisma = new PrismaClient({
    adapter: new PrismaPg({ connectionString }),
  });
  const client = new Client(new PrismaDriver(prisma));

  const sortResult = await client.insert(sort, {
    strings: ["whale", "tiger", "bear"],
  });
  console.log(`Inserted sort job with ID: ${sortResult.job.id}`);

  const emailResult = await client.insert(
    sendEmail,
    {
      body: "Welcome aboard!",
      subject: "Hello",
      to: "user@example.com",
    },
    {
      scheduledAt: Temporal.Now.instant().add({ hours: 1 }),
    }
  );
  console.log(
    `Inserted email job with ID: ${emailResult.job.id}, ` +
      `scheduled for: ${emailResult.job.scheduledAt}`
  );

  const batchResults = await client.insertMany([
    { args: { strings: ["alpha", "gamma", "beta"] }, job: sort },
    {
      args: { strings: ["one", "two", "three"] },
      job: sort,
      options: { priority: 3 },
    },
  ]);
  console.log(`Batch inserted ${batchResults.length} jobs`);

  await prisma.$executeRaw`
    CREATE TABLE IF NOT EXISTS riverqueue_example_account (
      id text PRIMARY KEY,
      created_at timestamptz NOT NULL DEFAULT now()
    )
  `;
  const accountId = `prisma_${randomUUID()}`;
  const queue = `prisma_${randomUUID()}`;
  const inserted = await prisma.$transaction(async (tx) => {
    await tx.$executeRaw`
      INSERT INTO riverqueue_example_account (id) VALUES (${accountId})
    `;
    return client.insert(syncAccount, { accountId }, { queue, tx });
  });
  console.log(`Committed account ${accountId} and its job atomically`);

  await prisma.$executeRaw`
    CREATE TABLE IF NOT EXISTS riverqueue_example_synced_account (
      id text PRIMARY KEY
    )
  `;
  const pool = new Pool({ connectionString });
  const workers = new Workers<Prisma.TransactionClient>().add(
    syncAccount,
    async ({ completeTx, job }) => {
      await prisma.$transaction(async (tx) => {
        await tx.$executeRaw`
        INSERT INTO riverqueue_example_synced_account (id)
        VALUES (${job.args.accountId})
      `;
        await completeTx(tx);
      });
    }
  );
  const workerClient = new Client(
    new PgDriver(pool, { transactionAdapters: [prismaTransactionAdapter] }),
    { queues: { [queue]: { maxWorkers: 1 } }, workers }
  );
  const events = workerClient.subscribe({
    kinds: ["job_completed", "job_failed"],
    signal: AbortSignal.timeout(10_000),
  });
  try {
    const run = await workerClient.start();
    try {
      for await (const event of events) {
        if (
          event.kind === "subscription_lag" ||
          event.job.id !== inserted.job.id
        )
          continue;
        assert.equal(event.kind, "job_completed");
        break;
      }
      assert.equal(
        (await workerClient.jobs.get(inserted.job.id))?.state,
        "completed"
      );
      const rows = await prisma.$queryRaw<{ id: string }[]>`
        SELECT id FROM riverqueue_example_synced_account WHERE id = ${accountId}
      `;
      assert.deepEqual(rows, [{ id: accountId }]);
      console.log(`Committed account sync and job completion atomically`);
    } finally {
      await run.stop();
    }
  } finally {
    events.close();
    await prisma.$disconnect();
    await pool.end();
  }
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
