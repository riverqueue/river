import { SqliteDriver } from "@riverqueue/driver-sqlite";
import { createMigrator } from "@riverqueue/migrate";
import { Client, Workers, defineJob } from "riverqueue";
import { z } from "zod";

const greet = defineJob({
  kind: "example.greet",
  schema: z.object({ name: z.string().min(1) }),
});

const workers = new Workers();
workers.add(greet, ({ job }) => {
  console.log(`hello, ${job.args.name}`);
});

const driver = SqliteDriver.memory();
await createMigrator(driver).migrateUp();

const client = new Client(driver, {
  queues: { default: { maxWorkers: 1 } },
  workers,
});
await using events = client.subscribe({
  kinds: ["job_completed"],
  signal: AbortSignal.timeout(10_000),
});
const inserted = await client.insert(greet, { name: "River" });
await using run = await client.start();
for await (const event of events) {
  if (event.kind === "job_completed" && event.job.id === inserted.job.id) {
    break;
  }
}

await run.stop({ mode: "graceful", timeout: { seconds: 5 } });
driver.close();
