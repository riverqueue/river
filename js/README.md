# River for JavaScript and TypeScript

River is a fast, reliable background job system backed by Postgres or SQLite.
This implementation runs on Node.js and shares River's database protocol with
River's other language implementations, so services can insert and work
the same jobs in the same database.

## Requirements

- **Node.js 24 or newer.** River uses `Temporal.Instant` for
  timestamps and `bigint` for IDs, so no value read from the database is lost.
  Node.js 26 provides native `Temporal`. On runtimes without it, including
  Node.js 24, preload a polyfill as described below. River checks for Temporal
  on import and never installs or replaces it.
- **TypeScript 6.0 or newer**, if you use TypeScript. Install `@types/node`
  24 or newer (and `@types/pg` with `@riverqueue/driver-pg`) and list `"node"`
  in `compilerOptions.types`. JavaScript users need neither.
- **ESM.** The packages ship one ES module build. CommonJS code can
  `require()` them on Node 24 or newer, which returns the same module instance as
  `import`; TypeScript CommonJS projects need `module: "node20"` or
  `"nodenext"`.

Keep every `@riverqueue/*` package on the same version as `riverqueue`; the
packages declare that as an exact peer dependency.

### Temporal on Node.js 24

Install a Temporal polyfill of your choice. For example:

```sh
npm install @js-temporal/polyfill
```

Create `temporal-setup.mjs` in your application:

```js
if (globalThis.Temporal === undefined) {
  const { Temporal } = await import("@js-temporal/polyfill");
  Object.defineProperty(globalThis, "Temporal", {
    configurable: true,
    value: Temporal,
    writable: true,
  });
}
```

Preload it before your application starts:

```sh
node --import ./temporal-setup.mjs app.mjs
```

Node also preloads this module in worker threads, including those started by
`@riverqueue/worker-threads`. Setting a global only in your main application
module does not initialize worker threads. A plain import of
`@js-temporal/polyfill` does not install a global either.

For package scripts, use an absolute path in `NODE_OPTIONS`:

```sh
NODE_OPTIONS="--import=/absolute/path/temporal-setup.mjs" npm start
```

The standalone `riverqueue` CLI supplies its own polyfill and needs no preload.
Importing `@riverqueue/cli` programmatically uses the application's Temporal,
just like the other library packages.

Use the same global Temporal constructors throughout your application when
passing values to River. Instances from another polyfill or a separate copy
may fail River's `instanceof` checks. Choose the implementation before importing
River and keep it in place for the lifetime of the process. Node.js 26 needs
no polyfill or preload.

## Quickstart with Postgres

Install the core package, the Postgres driver, migrations, and a validator
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

For SQLite, install `@riverqueue/driver-sqlite` instead of the Postgres
packages; it uses Node's built-in `node:sqlite`. See the
[SQLite driver](./driver/sqlite/README.md).

## Transactions and other features

[Insert jobs in an application transaction](./docs/README.md#transactions) so the job and its related database changes commit or roll back together. River supports transactions with node-postgres, Prisma, and SQLite.

- [Bulk insertion](./docs/README.md#batches) and [unique jobs](./docs/README.md#unique-jobs).
- [Retries, timeouts, cancellation, and snoozing](./docs/errors-and-retries.md).
- [Periodic jobs](./docs/periodic-jobs.md) and [resumable jobs](./docs/resumable-jobs.md).
- [Job and queue administration](./docs/README.md#query-and-control-jobs).
- [Logging, events, and metrics](./docs/observability.md).
- [Worker threads](./worker-threads/README.md) for CPU-bound handlers.

## Packages

| Package                      | Purpose                                                             |
| ---------------------------- | ------------------------------------------------------------------- |
| `riverqueue`                 | Job definitions, insertion, workers, runtime, queries, and events   |
| `@riverqueue/driver-knex`    | Insert jobs inside Knex transactions on Postgres                    |
| `@riverqueue/driver-pg`      | Postgres through `node-postgres`                                    |
| `@riverqueue/driver-prisma`  | Insert jobs inside Prisma transactions                              |
| `@riverqueue/driver-sqlite`  | SQLite through Node's built-in `node:sqlite`                        |
| `@riverqueue/migrate`        | Postgres and SQLite migrations                                      |
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

## Development

See [developing River for JavaScript and TypeScript](./docs/development.md).

## License

River for JavaScript and TypeScript is licensed under the Mozilla Public
License 2.0. See [LICENSE](./LICENSE).
