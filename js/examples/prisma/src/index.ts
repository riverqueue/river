import { PrismaPg } from "@prisma/adapter-pg";
import { Client, InsertManyParams } from "riverqueue";
import type { JobArgs, InsertOpts } from "riverqueue";
import { PrismaDriver } from "@riverqueue/driver-prisma";
import { PrismaClient } from "./generated/prisma/client.js";

// Define a job that sorts strings. `kind` uniquely identifies the job type and
// must match the worker name on the Go side.
class SortArgs implements JobArgs {
  kind = "sort";

  strings: string[];

  constructor(strings: string[]) {
    this.strings = strings;
  }

  toJSON() {
    return { strings: this.strings };
  }
}

// A job with default insert options baked in.
class SendEmailArgs implements JobArgs {
  kind = "send_email";

  insertOpts: InsertOpts = {
    maxAttempts: 5,
    queue: "email",
    priority: 2,
  };

  to: string;
  subject: string;
  body: string;

  constructor(to: string, subject: string, body: string) {
    this.to = to;
    this.subject = subject;
    this.body = body;
  }

  toJSON() {
    return { to: this.to, subject: this.subject, body: this.body };
  }
}

async function main() {
  const connectionString =
    process.env.DATABASE_URL ?? "postgres://localhost:5432/river_dev";
  const adapter = new PrismaPg({ connectionString });
  const prisma = new PrismaClient({ adapter });

  const client = new Client(new PrismaDriver(prisma));

  // Insert a single job.
  const sortResult = await client.insert(
    new SortArgs(["whale", "tiger", "bear"])
  );
  console.log(`Inserted sort job with ID: ${sortResult.job.id}`);

  // Insert with options, scheduling for 1 hour in the future.
  const emailResult = await client.insert(
    new SendEmailArgs("user@example.com", "Hello", "Welcome aboard!"),
    { scheduledAt: new Date(Date.now() + 60 * 60 * 1000) }
  );
  console.log(
    `Inserted email job with ID: ${emailResult.job.id}, scheduled for: ${emailResult.job.scheduledAt}`
  );

  // Insert many jobs at once.
  const batchResults = await client.insertMany([
    new SortArgs(["alpha", "gamma", "beta"]),
    new InsertManyParams(new SortArgs(["one", "two", "three"]), {
      priority: 3,
    }),
  ]);
  console.log(`Batch inserted ${batchResults.length} jobs`);

  await prisma.$disconnect();
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
