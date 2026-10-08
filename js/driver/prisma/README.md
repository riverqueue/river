# `@riverqueue/driver-prisma`

This package inserts River jobs through Prisma, including inside an existing
Prisma transaction. It is intentionally an insertion adapter, not a worker
runtime. Run workers with `@riverqueue/driver-pg` or another complete backend.

```sh
npm install riverqueue @riverqueue/driver-prisma @prisma/client
```

```ts
import { Client, defineJob } from "riverqueue";
import { z } from "zod";
import { PrismaDriver, type PrismaClientLike } from "@riverqueue/driver-prisma";

interface ApplicationTransaction extends PrismaClientLike {
  account: {
    create(options: { data: { id: string } }): Promise<unknown>;
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
that receives it. A transaction cannot provide runtime features such as job
claiming, listening for notifications, leadership, or maintenance.

Without `{ tx }`, River runs the insertion in an interactive transaction it
begins with the root client's `$transaction`, like River for Go: argument
validation, insert middleware, hooks, and the write commit together, and an
error thrown by any of them rolls the jobs back. Construct the driver with
your root `PrismaClient` for this; a transaction client has no `$transaction`,
so a driver built from one requires `{ tx }` on every insertion.

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

## Requirements

Node.js 26 or newer with native `Temporal`: `node -p "typeof Temporal"` must
print `object`. Official Node.js binaries include it; some builds compiled from
source, including some distribution and Homebrew packages, do not.

Install `riverqueue` at exactly this package's version. It is a peer dependency,
so npm rejects a mismatched pair instead of loading two copies.

River's tests run this driver against Prisma 7.9 with `@prisma/adapter-pg`.

TypeScript users need TypeScript 6.0 or newer and `@types/node`, with `"node"`
listed in `compilerOptions.types`. See [River's
requirements](https://github.com/riverqueue/river/tree/master/js#requirements) for
details.
