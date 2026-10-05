# `@riverqueue/driver-sqlite`

This package is River's preview SQLite backend for Node 26 and newer. It uses
the built-in synchronous `node:sqlite` API.

Apply River's generated canonical migrations explicitly before constructing a
client. The driver does not migrate during construction or startup:

```ts
import { DatabaseSync } from "node:sqlite";
import { SqliteDriver } from "@riverqueue/driver-sqlite";
import { createMigrator } from "@riverqueue/migrate";
import { Client, defineJob } from "riverqueue";
import { z } from "zod";

const database = new DatabaseSync("river.db");
const driver = new SqliteDriver(database);
await createMigrator(driver).migrateUp();

const client = new Client(driver);
const syncAccount = defineJob({
  kind: "sync_account",
  schema: z.object({ accountId: z.string() }),
});
```

## Connections

Like River for Go, River runs on a connection of its own. `new
SqliteDriver(database)` opens a private `DatabaseSync` on the same file and
never uses, hands out, or closes `database`. Application statements therefore
never run inside River's transactions, and River's never run inside the
application's. Close River's connection with `driver.close()` (or `using`)
after stopping every client that uses the driver.

The driver switches the database to WAL mode, which SQLite records in the
file. In WAL mode readers never wait for the writer, so application reads keep
working while River writes, and River's reads keep working during an
application transaction. When another connection keeps the database busy as
the driver is created, River makes the switch at its first operation instead,
waiting for the database like any other River statement.

SQLite allows one writer at a time across every connection and process,
including Go or Rust River clients sharing the file. River's connection has a
zero `busy_timeout`, so SQLite never blocks the event loop waiting for a lock:
when another connection holds the write lock, River retries its operation with
an asynchronous exponential backoff for up to `busyTimeout` (five seconds by
default), so timers, I/O, and running jobs keep making progress. If the lock is
still held after `busyTimeout`, the operation fails with a
`DatabaseOperationError` whose `retryable` is `true`.

```ts continued
const patientDriver = new SqliteDriver(database, {
  busyTimeout: { seconds: 10 },
});
```

`node:sqlite` statements are synchronous and briefly block the event loop. Run
a heavily loaded SQLite worker in its own Node process so its short statements
can't delay an HTTP server's requests.

### Application handles and busy timeouts

How long an application statement should wait for another connection's write
lock depends on who else writes the database:

- **Only this process writes.** River's own transactions hold the write lock
  only within one turn of the event loop (see
  [River's own transactions](#rivers-own-transactions)), so an application
  statement run from a request handler, timer, or other callback never meets
  it. Keep `node:sqlite`'s default zero `timeout`, and begin write
  transactions with the `transaction` helper below, which waits
  asynchronously for other application transactions.
- **Other processes write too**, such as a separate worker process or Go and
  Rust River clients. Their transactions hold the lock across many
  milliseconds, and a zero-`timeout` statement then fails at once with
  `SQLITE_BUSY`. Either run every write through `transaction`, which retries
  asynchronously, or give the handle a nonzero `timeout`. A nonzero timeout
  blocks the event loop while it waits for another process. In this process
  it can meet River's lock only when application code writes in the same turn
  of the event loop as a River insertion that has insert middleware or hooks,
  such as in a `Promise.all` beside `client.insert`. River can't finish until
  the event loop runs again, so that statement blocks for its whole timeout
  and then fails.

```ts continued
// Wait up to a second for other processes' write locks.
const applicationDatabase = driver.connect({ timeout: 1_000 });
```

## In-memory databases

A `:memory:` database belongs to the one connection that opened it, so River
can't open its own connection to it, and `new SqliteDriver(database)` rejects
one. `SqliteDriver.memory()` creates a new in-memory database that River and
the application share instead. Open handles on it with `driver.connect()`:

```ts continued
using memoryDriver = SqliteDriver.memory();
await createMigrator(memoryDriver).migrateUp();
const application = memoryDriver.connect();
application.exec("CREATE TABLE accounts (id text PRIMARY KEY)");
```

The database lives until the driver and every handle from `connect()` are
closed. `connect()` works for file databases too. An in-memory database has no
WAL: while one connection writes, a read on another connection fails with
`SQLITE_BUSY` at once when its `timeout` is zero, or waits up to its timeout.

## Transactions

Pass a handle as `{ tx }` while it has a transaction open to make River's
statements part of that transaction, so jobs commit or roll back with the
application's rows. The handle may be any `DatabaseSync` open on the driver's
database. River runs its statements directly in that transaction, opening no
savepoint, and never commits or rolls it back. When a River call fails,
statements it already ran stay in the transaction: roll the transaction back,
or wrap the call in a savepoint of your own to recover and continue.

The `transaction` helper begins a transaction with `BEGIN IMMEDIATE`, commits
when its callback resolves, and rolls back when it throws. `BEGIN IMMEDIATE`
takes the write lock up front: a deferred `BEGIN` that reads first can fail
later with `SQLITE_BUSY_SNAPSHOT`, which no retry can fix. While another
connection holds the lock, the helper retries asynchronously for up to
`busyTimeout`:

```ts continued
import { transaction } from "@riverqueue/driver-sqlite";

const accountId = "acct_1";
await transaction(database, async (tx) => {
  tx.prepare("INSERT INTO accounts (id) VALUES (?)").run(accountId);
  await client.insert(syncAccount, { accountId }, { tx });
});
```

The callback may await freely: River's own work runs on its connection and
waits, asynchronously, for the application's transaction to end. Keep it
short anyway, because it holds the database's only write lock, and River's
background work fails after `busyTimeout`.

Plain `node:sqlite` transactions work the same way. With a zero `timeout`,
`BEGIN IMMEDIATE` fails at once with `SQLITE_BUSY` when another connection
holds the lock, which the helper would have retried:

```ts continued
database.exec("BEGIN IMMEDIATE");
try {
  database.prepare("INSERT INTO accounts (id) VALUES (?)").run("acct_2");
  await client.insert(syncAccount, { accountId: "acct_2" }, { tx: database });
  database.exec("COMMIT");
} catch (error) {
  if (database.isTransaction) database.exec("ROLLBACK");
  throw error;
}
```

An ORM built on the same handle, such as Drizzle's `node-sqlite` driver, uses
the helper's transaction too. Don't use the ORM's own transaction method for
this: Drizzle's and Bun's SQLite transactions can't await, and commit at the
callback's first `await`, so a later error rolls nothing back.

<!-- ts-setup
declare function drizzle(options: { client: DatabaseSync }): {
  insert(table: unknown): { values(row: { id: string }): Promise<unknown> };
};
declare const accounts: unknown;
-->

```ts continued
// import { drizzle } from "drizzle-orm/node-sqlite";
const orm = drizzle({ client: database });
await transaction(database, async (tx) => {
  await orm.insert(accounts).values({ id: "acct_3" });
  await client.insert(syncAccount, { accountId: "acct_3" }, { tx });
});
```

### River's own transactions

Without `{ tx }`, an insertion runs in a transaction River owns on its
connection, like River for Go. Argument validation, insert middleware,
`beforeInsert` and `afterInsert` hooks, and the write commit together, and an
error thrown by any of them, even after middleware's `next()` returns, rolls
the jobs back. River takes the write lock with `BEGIN IMMEDIATE` only at the
insertion's first statement, so work a middleware does before calling `next()`
(such as encrypting arguments with a remote key service) holds no lock.

From there until the transaction commits, River holds SQLite's write lock. On
SQLite, insert middleware and hooks must therefore not await I/O after
`next()` returns or in `afterInsert`: every other writer, in this process and
others, would wait for it. React to committed jobs with `client.subscribe`
instead, or do the I/O before calling `next()`. Awaiting promises that resolve
without I/O is fine, and an insertion without middleware or hooks never lets
the event loop turn while it holds the lock.

River checks this. When its transaction is still open at the event loop's next
turn, River rolls it back at once, which releases the lock for every other
writer, and fails the insertion with a `TransactionScopeError` whose `reason`
is `"event_loop_turn"`. The check finds most mistakes, including all slow I/O,
such as network calls, and all I/O awaited by an insertion started from an I/O
callback, such as an HTTP request handler. It isn't deterministic for fast
local I/O, such as a cached `fs.stat` or a WebCrypto digest, awaited by an
insertion started from a timer or `setImmediate` callback: that I/O can finish
before the check runs, and the insertion then succeeds while briefly holding
the lock across a turn. Don't rely on the check to find every case.

Once River holds the write lock, a River call without `{ tx }` from inside its
transaction, such as from middleware after `next()` or an `afterInsert` hook,
would wait for River's transaction forever. It fails at once with a
`TransactionScopeError` instead. So does `transaction` on the same database,
which could only fail after its `busyTimeout`, and so do nested `transaction`
calls on one database and River writes without `{ tx }` inside a
`transaction` on the same database. Before River's first statement, in
middleware before `next()` and in `beforeInsert` hooks, River holds no lock,
and such calls run normally.

River can't see a transaction begun with a raw `BEGIN`. A River write without
`{ tx }` issued inside one waits for that transaction's lock and fails after
`busyTimeout` with a `DatabaseOperationError` whose message suggests passing
the handle as `{ tx }`.

All result-producing statements use `StatementSync.setReadBigInts(true)`.
Persisted 64-bit values are therefore decoded as `bigint`, never a lossy
`number`, and timestamps cross the public boundary as `Temporal.Instant`.
SQLite stores River timestamps at exact millisecond precision.

## Compatibility notes

The driver reads every row Go's `riversqlite` accepts, including unbounded
`attempt` and `max_attempts` values and JSON `null` tags or errors. If a
claimed row still cannot be decoded, River doesn't work it: its attempt fails
with the error `job row couldn't be decoded: …` through the normal failure
path (the error handler runs, and the retry policy retries it or discards it
after its last attempt), while the rest of the batch is worked. The bad value
is left in place.

Like River for Go's client, a client writes at most one insert notification per
queue per `fetchCooldown` (100 ms by default), so a large batch does not wake
listeners once per job.

## Requirements

Node.js 26 or newer with native `Temporal`: `node -p "typeof Temporal"` must
print `object`. Official Node.js binaries include it; some builds compiled from
source, including some distribution and Homebrew packages, do not.

Install `riverqueue` at exactly this package's version. It is a peer dependency,
so npm rejects a mismatched pair instead of loading two copies.

TypeScript users need TypeScript 6.0 or newer and `@types/node`, with `"node"`
listed in `compilerOptions.types`. See [River's
requirements](https://github.com/riverqueue/river/tree/master/js#requirements) for
details.
