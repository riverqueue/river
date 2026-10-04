import { SqliteDriver } from "@riverqueue/driver-sqlite";
import { createMigrator } from "@riverqueue/migrate";
import { Client, Workers, defineJob } from "riverqueue";
import { z } from "zod";

const collectMetric = defineJob({
  kind: "example.collect_metric",
  schema: z.object({ name: z.string().min(1) }),
});
const workers = new Workers();
workers.add(collectMetric, ({ job }) => {
  console.log(`collected ${job.args.name}`);
});

const counters = new Map<string, number>();
const increment = (name: string) =>
  counters.set(name, (counters.get(name) ?? 0) + 1);
const driver = SqliteDriver.memory();
await createMigrator(driver).migrateUp();
const client = new Client(driver, {
  hooks: {
    afterInsert(_context, results) {
      counters.set("jobs_inserted", results.length);
    },
    afterWork(_context, result) {
      increment(`attempt_${result.status}`);
    },
    beforeWork({ job }) {
      increment(`started_${job.kind}`);
    },
    onEvent(event) {
      increment(`event_${event.kind}`);
    },
  },
  middleware: [
    async (context, next) => {
      const startedAt = Temporal.Now.instant();
      try {
        return await next();
      } finally {
        const elapsed = Temporal.Now.instant().since(startedAt);
        console.log(
          `${context.job.kind} attempt took ${elapsed.total("milliseconds")} ms`
        );
      }
    },
  ],
  queues: { default: { maxWorkers: 1 } },
  workers,
});

await using events = client.subscribe({
  kinds: ["job_completed"],
  signal: AbortSignal.timeout(10_000),
});
const inserted = await client.insert(collectMetric, { name: "queue_depth" });
await using run = await client.start();
for await (const event of events) {
  if (event.kind === "job_completed" && event.job.id === inserted.job.id) break;
}
await run.stop({ mode: "graceful", timeout: { seconds: 5 } });

for (const required of [
  "attempt_succeeded",
  "event_job_completed",
  "jobs_inserted",
  "started_example.collect_metric",
]) {
  if ((counters.get(required) ?? 0) < 1) {
    throw new Error(`missing metric ${required}`);
  }
}
console.log(Object.fromEntries(counters));
driver.close();
