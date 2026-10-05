# River for JavaScript and TypeScript

River is a fast, reliable background job system backed by PostgreSQL or SQLite.
This implementation runs on Node.js and shares River's database protocol with
River for Go and Rust, so services in all three languages can insert and work
the same jobs in the same database.

It is currently an alpha and is not published from this branch.

## Requirements

- **Node.js 26 with native `Temporal`.** River uses `Temporal.Instant` for
  timestamps and `bigint` for IDs, so no value read from the database is lost.
  Official Node.js 26 builds (nodejs.org, `actions/setup-node`, the official
  Docker images) enable Temporal. Some distribution and Homebrew builds do
  not; check with:

  ```sh
  node -p "typeof Temporal" # must print "object"
  ```

- **TypeScript 6.0 or newer**, if you use TypeScript. Install `@types/node`
  26 or newer (and `@types/pg` with `@riverqueue/driver-pg`) and list `"node"`
  in `compilerOptions.types`. JavaScript users need neither.
- **ESM.** The packages ship one ES module build. CommonJS code can
  `require()` them on Node 26, which returns the same module instance as
  `import`; TypeScript CommonJS projects need `module: "node20"` or
  `"nodenext"`.

Keep every `@riverqueue/*` package on the same version as `riverqueue`; the
packages declare that as an exact peer dependency.

## Quickstart with PostgreSQL

Install the core package, the PostgreSQL driver, migrations, and a validator
(any [Standard Schema](https://standardschema.dev) library works; this uses
Zod):

```sh
npm install riverqueue @riverqueue/driver-pg @riverqueue/migrate pg zod
```

Define a job, migrate the database, insert a job, and work it:

```ts
import { createMigrator } from "@riverqueue/migrate";
import { PgDriver } from "@riverqueue/driver-pg";
import { Pool } from "pg";
import { Client, Workers, defineJob } from "riverqueue";
import { z } from "zod";

// A job definition is a plain value that producers and workers both import.
const sendWelcomeEmail = defineJob({
  kind: "send_welcome_email",
  schema: z.object({ to: z.email() }),
});

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const driver = new PgDriver(pool);

// Migrations are an explicit step, typically run at deploy time.
await createMigrator(driver).migrateUp();

const workers = new Workers().add(
  sendWelcomeEmail,
  async ({ job, logger, signal }) => {
    signal.throwIfAborted();
    logger.info({ to: job.args.to }, "sending welcome email");
  }
);

const client = new Client(driver, {
  queues: { default: { maxWorkers: 50 } },
  workers,
});

const inserted = await client.insert(sendWelcomeEmail, {
  to: "person@example.com",
});
console.log(`inserted job ${inserted.job.id}`); // a bigint

const run = await client.start();
process.once("SIGTERM", () => {
  void run.stop({ timeout: { seconds: 30 } });
});
await run.completed; // settles after a stop, or rejects on a fatal error
await pool.end();
```

River never closes the pool you give it and never migrates on its own. Web
servers that only insert jobs construct the same `Client` without `queues` or
`workers` and never call `start()`.

For SQLite, install `@riverqueue/driver-sqlite` instead of the PostgreSQL
packages; it uses Node's built-in `node:sqlite`. See the
[SQLite driver](./driver/sqlite/README.md).

## Packages

| Package                      | Purpose                                                             |
| ---------------------------- | ------------------------------------------------------------------- |
| `riverqueue`                 | Job definitions, insertion, workers, runtime, queries, and events   |
| `@riverqueue/driver-pg`      | PostgreSQL through `node-postgres`                                  |
| `@riverqueue/driver-prisma`  | Insert jobs inside Prisma transactions                              |
| `@riverqueue/driver-sqlite`  | SQLite through Node's built-in `node:sqlite`                        |
| `@riverqueue/migrate`        | PostgreSQL and SQLite migrations                                    |
| `@riverqueue/worker-threads` | Run CPU-bound handlers on worker threads                            |
| `@riverqueue/test`           | Test helpers for producers and workers                              |
| `@riverqueue/cli`            | The `riverqueue` command: migrations, benchmarks, and a 0.1 codemod |

## Documentation

Start with the [guide](./docs/README.md), then the topic guides:

- [Errors, retries, timeouts, and cancellation](./docs/errors-and-retries.md)
- [Periodic jobs](./docs/periodic-jobs.md)
- [Resumable jobs](./docs/resumable-jobs.md)
- [Testing](./docs/testing.md)
- [Runtime, concurrency, and the event loop](./docs/runtime.md)
- [Databases, pools, and migrations](./docs/databases.md)
- [Logging, events, and metrics](./docs/observability.md)
- [Running alongside Go and Rust](./docs/deployment.md)
- [Migrating from `riverqueue` 0.1](./docs/migrating-from-0.1.md)

API reference documentation is generated with `pnpm run docs:api`.

## License

River for JavaScript and TypeScript is licensed under the GNU Lesser General
Public License v3.0 or later. See [LICENSE](./LICENSE).
