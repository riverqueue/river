import { PgDriver } from "@riverqueue/driver-pg";
import { createMigrator } from "@riverqueue/migrate";
import { Pool } from "pg";
import { Client, Workers, snooze } from "riverqueue";

import { chargeInvoice, sendReceipt } from "./jobs.js";

const pool = new Pool({
  connectionString:
    process.env.DATABASE_URL ?? "postgres://localhost:5432/river_dev",
  max: 20,
});
const driver = new PgDriver(pool);

// A real deployment runs migrations as a separate step before workers start.
await createMigrator(driver).migrateUp();

let providerBusy = true;
const workers = new Workers()
  .add(chargeInvoice, async ({ client, job, logger, signal }) => {
    signal.throwIfAborted();
    // Simulate a provider that is briefly unavailable: snoozing retries later
    // without using one of the job's attempts.
    if (providerBusy) {
      providerBusy = false;
      logger.info("payment provider busy; snoozing");
      return snooze({ seconds: 0 });
    }
    logger.info({ amountCents: job.args.amountCents }, "charged invoice");
    // Insert follow-up work with the worker's own client.
    await client.insert(sendReceipt, { invoiceId: job.args.invoiceId });
    return undefined;
  })
  .add(sendReceipt, ({ job, logger }) => {
    logger.info({ invoiceId: job.args.invoiceId }, "sent receipt");
  });

const client = new Client(driver, {
  queues: {
    billing: { maxWorkers: 5 },
    default: { maxWorkers: 10 },
  },
  workers,
});

await using events = client.subscribe({
  kinds: ["job_completed"],
  signal: AbortSignal.timeout(30_000),
});
const run = await client.start();
process.once("SIGTERM", () => {
  void run.stop({ timeout: { seconds: 30 } });
});

const inserted = await client.insert(chargeInvoice, {
  amountCents: 1_999,
  invoiceId: `inv_${Date.now()}`,
});
console.log(`inserted job ${inserted.job.id}`);

// Wait for the receipt, which is inserted only after the charge succeeds.
for await (const event of events) {
  if (event.kind === "job_completed" && event.job.kind === sendReceipt.kind) {
    console.log(`completed job ${event.job.id} (${event.job.kind})`);
    break;
  }
}

await run.stop({ timeout: { seconds: 5 } });
await pool.end();
