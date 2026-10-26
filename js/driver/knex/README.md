# `@riverqueue/driver-knex`

Insert River jobs through Knex on Postgres, including atomically with your
application's writes. This package provides an insertion client; use
`@riverqueue/driver-pg` to run workers against the same database and schema.

```sh
npm install riverqueue @riverqueue/driver-knex knex pg
```

```ts
import knex from "knex";
import { Client, defineJob } from "riverqueue";
import { KnexDriver } from "@riverqueue/driver-knex";

const db = knex({
  client: "pg",
  connection: process.env.DATABASE_URL ?? "postgres://localhost/river",
});
const client = new Client(new KnexDriver(db));
const syncAccount = defineJob<{ accountId: string }>()({
  kind: "sync_account",
});

await db.transaction(async (tx) => {
  const accountId = "acct_1";
  await tx("account").insert({ id: accountId });
  await client.insert(syncAccount, { accountId }, { tx });
});

await db.destroy(); // Close your application's pool at shutdown.
```

Pass a root Knex instance to `KnexDriver` and an active `Knex.Transaction` as
`{ tx }` to `insert` or `insertMany`. River uses that exact transaction,
including nested transactions backed by savepoints, and leaves commit and
rollback to the caller. An outer rollback removes both application writes and
jobs. Insert notifications reach workers only after commit.

Without `{ tx }`, River opens a Knex transaction around insertion, middleware,
hooks, and notifications. If any of them throws, the transaction rolls back.
The Knex instance remains caller-owned; River never destroys its pool.

Configure a River schema with `new KnexDriver(db, { schema: "river" })`.
Otherwise River uses the connection's `search_path`. Apply River migrations
separately with `@riverqueue/migrate` or `@riverqueue/cli`, using the same
database and schema. River's migrations are independent of Knex's application
migrations.

Job IDs remain `bigint`, timestamps retain microsecond precision as
`Temporal.Instant`, and JSON numbers beyond JavaScript's safe integer range
remain exact. Batch ordering and unique jobs follow River's normal semantics.

## Complete jobs inside Knex transactions

Workers run through `@riverqueue/driver-pg`. Add that package and configure
`knexTransactionAdapter` on its driver to pass native Knex transactions to
`completeTx`:

```ts
import knex, { type Knex } from "knex";
import { Pool } from "pg";
import { Client, defineJob, Workers } from "riverqueue";
import { knexTransactionAdapter } from "@riverqueue/driver-knex";
import { PgDriver } from "@riverqueue/driver-pg";

const connectionString =
  process.env.DATABASE_URL ?? "postgres://localhost/river";
const db = knex({ client: "pg", connection: connectionString });
const pool = new Pool({ connectionString });
const syncAccount = defineJob<{ accountId: string }>()({
  kind: "sync_account",
});
const workers = new Workers<Knex.Transaction>().add(
  syncAccount,
  async ({ completeTx, job }) => {
    await db.transaction(async (tx) => {
      await tx("account")
        .where({ id: job.args.accountId })
        .update({ synced: true });
      await completeTx(tx);
    });
  }
);
const client = new Client(
  new PgDriver(pool, { transactionAdapters: [knexTransactionAdapter] }),
  { queues: { default: { maxWorkers: 10 } }, workers }
);
const run = await client.start();
```

The application write and job completion use the same Knex transaction and
commit together. Knex owns commit, rollback, and nested savepoints. If the
transaction throws, both writes roll back, and River handles the worker's
error normally. Await the transaction before returning from the handler.

Configure the same database and River schema on the worker and insertion
clients. The adapter also enables Knex `{ tx }` options on the worker client's
other operations, including inserts from `ctx.client`. Importing the adapter
doesn't enable it automatically; configure each `PgDriver` that needs it.
Neither `riverqueue` nor `@riverqueue/driver-pg` depends on Knex.

At shutdown, stop `run`, then close the application's Knex and `pg` pools.

## Knex response processing

River uses parameterized `raw` queries with `queryContext({ river: true })`.
If your Knex configuration uses `postProcessResponse`, return the original
response when `queryContext?.river` is true. River needs the Postgres response's
`rows` array and its original column names and values for exact decoding.

## Requirements

Knex 3.1 or newer within the 3.x line, `pg` 8.x, and Node.js 24 or newer with
a global `Temporal` implementation ([Node.js 24 setup](../../README.md#temporal-on-nodejs-24)). This
adapter supports Knex's Postgres client (`client: "pg"` or `"postgresql"`).
For SQLite, use `@riverqueue/driver-sqlite`.

Install `riverqueue` at exactly this package's version. TypeScript users need
TypeScript 6.0 or newer and `@types/node`, with `"node"` in
`compilerOptions.types`.
