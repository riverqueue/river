# `@riverqueue/driver-pg`

This package is River's complete Postgres backend for Node.js 26 and newer.
It accepts a caller-owned `pg.Pool`, performs insertion and runtime operations,
and implements notifications, leadership, maintenance, and queue control.

```sh
npm install riverqueue @riverqueue/driver-pg pg
```

```ts
import { Client, defineJob } from "riverqueue";
import { z } from "zod";
import { PgDriver } from "@riverqueue/driver-pg";
import { Pool } from "pg";

const pool = new Pool({ connectionString: process.env.DATABASE_URL });
const client = new Client(new PgDriver(pool));
const accountId = "acct_1";
const syncAccount = defineJob({
  kind: "sync_account",
  schema: z.object({ accountId: z.string() }),
});
```

The pool remains application-owned. Stopping River does not close it. Size the
pool for River's worker concurrency plus application queries and migrations;
observe pool wait and saturation through the application's `pg` instrumentation
before increasing worker concurrency.

## Transactions

Pass the exact checked-out `PoolClient` as `tx`. River uses that connection but
does not begin, commit, roll back, or release the transaction:

```ts continued
const tx = await pool.connect();
try {
  await tx.query("BEGIN");
  await tx.query("INSERT INTO accounts (id) VALUES ($1)", [accountId]);
  await client.insert(syncAccount, { accountId }, { tx });
  await tx.query("COMMIT");
} catch (error) {
  await tx.query("ROLLBACK");
  throw error;
} finally {
  tx.release();
}
```

Without `{ tx }`, River begins a transaction of its own on a connection it
leases from the pool, like River for Go: argument validation, insert
middleware, hooks, and the write commit together, and an error thrown by any
of them rolls the jobs back. A driver constructed from a single client, rather
than a pool, has no connection to lease, so it rejects insertions without
`{ tx }` with a `ConfigurationError`. The transaction costs two more round
trips per call, for `BEGIN` and `COMMIT`, as it does in Go. To insert many
jobs, pass them to one `insertMany` call rather than calling `insert` for
each.

Apply migrations explicitly with `@riverqueue/migrate` or
`@riverqueue/cli` before starting workers.

## Connection health and cancellation

River pings its LISTEN connection after five idle seconds, by repeating an
idempotent `LISTEN`, and replaces it when the ping fails or goes unanswered for
five seconds, so a half-open socket cannot silently stop notifications. Every reconnection makes the runtime poll for work
and queue changes it may have missed.

When River abandons an in-flight statement, such as a completion query that
exceeded its 10 second bound or a reindex interrupted by shutdown, it destroys
that connection and asks Postgres to cancel the statement with
`pg_cancel_backend` from another pooled connection. The cancellation only
targets a backend still running that River statement. A statement can still
commit before the cancellation arrives; River's attempt-identity guard keeps
that harmless, because a retried completion then observes the committed row
and reports it. Leader maintenance passes are abandoned the same way when
their leadership term ends or the client stops, which rolls them back.

Like River for Go, a claim that has started runs to completion, and so do
River's queue reports and leader elections, so on a connection whose socket
went half-open they wait for the operating system to notice. Construct the
`Pool` with `keepAlive: true`, and consider a `query_timeout`, so such sockets
fail, and pass a `timeout` to `run.stop()` to bound how long a stop waits.

## PgBouncer and schemas

Transaction-mode PgBouncer is supported for ordinary operations. River's
dedicated notification and leadership sessions must connect somewhere that
preserves session state; use a direct connection or session-mode endpoint for
those capabilities. Configure a custom River schema consistently in the
driver, migrator, and CLI. Never interpolate an untrusted schema name.

## Requirements

Node.js 26 or newer with native `Temporal`: `node -p "typeof Temporal"` must
print `object`. Official Node.js binaries include it; some builds compiled from
source, including some distribution and Homebrew packages, do not.

Install `riverqueue` at exactly this package's version. It is a peer dependency,
so npm rejects a mismatched pair instead of loading two copies.

River parses the values its queries return with its own parsers, so changes an
application makes to node-postgres's global parsers (`pg.types.setTypeParser`)
don't affect it. It reads timestamps in Postgres's default `DateStyle` of
`ISO`; a session with another `DateStyle` fails with a clear error.

TypeScript users need TypeScript 6.0 or newer and `@types/node` and `@types/pg`,
with `"node"` listed in `compilerOptions.types`. See [River's
requirements](https://github.com/riverqueue/river/tree/master/js#requirements) for
details.
