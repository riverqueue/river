import { randomUUID } from "node:crypto";

import { PrismaPg } from "@prisma/adapter-pg";
import { PrismaDriver } from "@riverqueue/driver-prisma";
import { Client, defineJob } from "riverqueue";
import { PrismaClient } from "./generated/prisma/client.js";
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
  await prisma.$transaction(async (tx) => {
    await tx.$executeRaw`
      INSERT INTO riverqueue_example_account (id) VALUES (${accountId})
    `;
    await client.insert(syncAccount, { accountId }, { tx });
  });
  console.log(`Committed account ${accountId} and its job atomically`);

  await prisma.$disconnect();
}

main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
