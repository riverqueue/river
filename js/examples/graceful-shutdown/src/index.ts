import { setTimeout } from "node:timers/promises";

import { SqliteDriver } from "@riverqueue/driver-sqlite";
import { createMigrator } from "@riverqueue/migrate";
import { Client, Workers, defineJob } from "riverqueue";
import { z } from "zod";

const finishReport = defineJob({
  kind: "example.finish_report",
  schema: z.object({ reportId: z.string().min(1) }),
});

let started!: () => void;
const handlerStarted = new Promise<void>((resolve) => {
  started = resolve;
});
const workers = new Workers();
workers.add(finishReport, async ({ job, signal }) => {
  started();
  await setTimeout(50, undefined, { signal });
  console.log(`finished report ${job.args.reportId}`);
});

const driver = SqliteDriver.memory();
await createMigrator(driver).migrateUp();
const client = new Client(driver, {
  queues: { default: { maxWorkers: 1 } },
  workers,
});
const inserted = await client.insert(finishReport, { reportId: "report_1" });
await using run = await client.start();
await handlerStarted;

await run.stop({ mode: "graceful", timeout: { seconds: 5 } });
const completed = await client.jobs.get(inserted.job.id);
if (completed?.state !== "completed") {
  throw new Error("graceful shutdown did not persist active work");
}
driver.close();
