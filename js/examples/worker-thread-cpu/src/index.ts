import { SqliteDriver } from "@riverqueue/driver-sqlite";
import { createMigrator } from "@riverqueue/migrate";
import {
  WorkerThreads,
  type WorkerThreadModule,
} from "@riverqueue/worker-threads";
import { Client, Workers } from "riverqueue";

import type * as primeHandlers from "./handler.ts";
import { findPrime } from "./jobs.ts";

// The `.js` name works both from the build and from source: when only
// `handler.ts` exists, River's threads load it through Node's type stripping.
const handlerModule: WorkerThreadModule<typeof primeHandlers> = new URL(
  "./handler.js",
  import.meta.url
);

// The application owns the executor and closes it when this scope ends.
await using executor = new WorkerThreads({ maxThreads: 2 });
const workers = new Workers().addExecutor(
  findPrime,
  executor.handler(findPrime, {
    exportName: "findPrimeHandler",
    module: handlerModule,
  })
);

const driver = SqliteDriver.memory();
await createMigrator(driver).migrateUp();
const client = new Client(driver, {
  queues: { default: { maxWorkers: 2 } },
  workers,
});
await using events = client.subscribe({
  kinds: ["job_completed"],
  signal: AbortSignal.timeout(10_000),
});
const inserted = await client.insert(findPrime, { ordinal: 2_000 });
await using run = await client.start();
for await (const event of events) {
  if (event.kind === "job_completed" && event.job.id === inserted.job.id) {
    console.log("2,000th prime:", JSON.stringify(event.job.metadata["output"]));
    break;
  }
}
await run.stop({ mode: "graceful", timeout: { seconds: 5 } });
driver.close();
