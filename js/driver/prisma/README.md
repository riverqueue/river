# `@riverqueue/driver-prisma`

This package inserts River jobs through Prisma and adapts native Prisma
transactions for `completeTx` in workers. Use `PrismaDriver` for insertion
and `@riverqueue/driver-pg` with `prismaTransactionAdapter` for workers.

```sh
npm install riverqueue @riverqueue/driver-prisma @prisma/client
```

```ts
import { Client, defineJob } from "riverqueue";
import { z } from "zod";
import {
  PrismaDriver,
  type PrismaClientLike,
  type PrismaTransactionClientLike,
} from "@riverqueue/driver-prisma";

interface ApplicationTransaction extends PrismaTransactionClientLike {
  account: {
    create(options: { data: { id: string } }): Promise<unknown>;
    update(options: {
      where: { id: string };
      data: { synced: boolean };
    }): Promise<unknown>;
  };
}
interface ApplicationPrismaClient extends PrismaClientLike {
  $transaction<T>(
    callback: (transaction: ApplicationTransaction) => Promise<T>
  ): Promise<T>;
}

declare const prisma: ApplicationPrismaClient; // Your generated client.
const client = new Client(new PrismaDriver(prisma));
const accountId = "acct_1";
const syncAccount = defineJob({
  kind: "sync_account",
  schema: z.object({ accountId: z.string() }),
});

await prisma.$transaction(async (tx) => {
  await tx.account.create({ data: { id: accountId } });
  await client.insert(syncAccount, { accountId }, { tx });
});
```

See the [runnable Prisma example](../../examples/prisma) for Prisma's generated
client and Postgres adapter setup.

The Prisma client and transaction remain caller-owned. River neither connects
nor disconnects Prisma. Apply River migrations separately with
`@riverqueue/migrate` or `@riverqueue/cli`; River's tables are not Prisma model
state.

Prisma's transaction object is structurally detected and bound to the operation
that receives it. The worker runtime uses a separate `pg` pool for job
claiming, notifications, leadership, and maintenance.

Without `{ tx }`, River runs the insertion in an interactive transaction it
begins with the root client's `$transaction`: argument validation, insert
middleware, hooks, and the write commit together, and an error thrown by any of
them rolls the jobs back. Construct the driver with your root `PrismaClient` for
this. A client without `$transaction` requires `{ tx }` on every insertion.

Prisma bounds an interactive transaction with its defaults: a 2 second wait
for a connection and a 5 second timeout, which also covers any I/O insert
middleware awaits before calling `next()`. Change them with
`transactionOptions`:

```ts continued
const patientClient = new Client(
  new PrismaDriver(prisma, {
    transactionOptions: { maxWait: { seconds: 5 }, timeout: { seconds: 15 } },
  })
);
```

Like River's other clients, inserting an immediately available job sends an
insert notification, at most one per queue per client `fetchCooldown`, so
running workers claim it without waiting for their next poll. Inside a transaction the notification is delivered only when the
transaction commits. Inserted rows are decoded exactly, so integers beyond
JavaScript's safe range in arguments or metadata stay exact.

## Complete jobs inside Prisma transactions

Workers run through `@riverqueue/driver-pg`. Add that package and `pg`, and
configure `prismaTransactionAdapter` to pass native Prisma interactive
transactions to `completeTx`. Building on the setup above:

```ts continued
import { Pool } from "pg";
import { Workers } from "riverqueue";
import { prismaTransactionAdapter } from "@riverqueue/driver-prisma";
import { PgDriver } from "@riverqueue/driver-pg";

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const workers = new Workers<PrismaTransactionClientLike>().add(
  syncAccount,
  async ({ completeTx, job }) => {
    await prisma.$transaction(async (tx) => {
      await tx.account.update({
        where: { id: job.args.accountId },
        data: { synced: true },
      });
      await completeTx(tx);
    });
  }
);
const workerClient = new Client(
  new PgDriver(pool, { transactionAdapters: [prismaTransactionAdapter] }),
  { queues: { default: { maxWorkers: 10 } }, workers }
);
const run = await workerClient.start();
```

The application write and job completion use the same Prisma transaction and
commit together. If the transaction throws, both writes roll back and River
handles the worker's error normally. Await the transaction before returning
from the handler. Prisma owns transaction timeouts and any nested savepoints
supported by your Prisma version.

Use the same database and River schema for Prisma and the worker's `PgDriver`.
The adapter also enables Prisma `{ tx }` options on the worker client's other
operations, including inserts from `ctx.client`. Pass the transaction client
received by `$transaction`, rather than the root Prisma client. Generated
`Prisma.TransactionClient` types can also be used as the `Workers` type argument.

River preserves exact JSON numbers and microsecond timestamps by transporting
its query results as text, without changing Prisma's handling of application
queries. Importing the adapter doesn't enable it automatically; configure each
`PgDriver` that needs it. Neither `riverqueue` nor `@riverqueue/driver-pg` depends
on Prisma.

At shutdown, stop `run`, then disconnect Prisma and close the `pg` pool.

## Requirements

Node.js 24 or newer with a global `Temporal` implementation. Node.js 26
provides it natively; on Node.js 24, [preload a polyfill](../../README.md#temporal-on-nodejs-24)
before importing River, including in worker threads. River never installs one.

Install `riverqueue` at exactly this package's version. It is a peer dependency,
so npm rejects a mismatched pair instead of loading two copies.

River's tests run this driver against Prisma 7.9 with `@prisma/adapter-pg`.

TypeScript users need TypeScript 6.0 or newer and `@types/node`, with `"node"`
listed in `compilerOptions.types`. See [River's
requirements](https://github.com/riverqueue/river/tree/master/js#requirements) for
details.
