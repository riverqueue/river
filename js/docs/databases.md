# Databases, pools, and migrations

Each database lives in its own driver package, so an application installs only
the database it uses. The core API never exposes one database's types as
universal: a client's transaction type comes from its driver.

## PostgreSQL

`@riverqueue/driver-pg` is the complete `node-postgres` runtime. The
application owns the pool; River never ends it. River reads timestamps in
PostgreSQL's default ISO `DateStyle` and rejects any other with a
`ConfigurationError`; a server configured otherwise needs `DateStyle=ISO` for
River's connections, such as through the pool's
`options: "-c DateStyle=ISO"`.

### Pool sizing

River doesn't hold a connection per running job: handlers use your pool for
their own queries, and River borrows connections briefly to claim and complete
jobs. It holds a few connections for longer:

- one for `LISTEN` notifications (unless `pollOnly: true`);
- one for maintenance while this client is the leader;
- one more while the leader rebuilds indexes;
- at most two completion queries at a time, each borrowing briefly.

`client.start()` refuses a pool whose `max` is below what the enabled services
need. A practical size for a worker process is

```text
max = (connections your handlers use concurrently) + 5
```

For example, 50 workers whose handlers each run one query at a time need
about 55 connections, not counting anything else the process does with the
same pool. Watch pool wait time (`pool.waitingCount`) before raising
`maxWorkers`; a starved pool slows claims and completions for every queue.
Producer-only processes need no extra connections for River.

For PgBouncer transaction mode, ordinary queries may use the pooled endpoint,
but session-scoped notifications and leadership need a direct or session-mode
connection. A custom schema must be configured consistently in the driver,
migrator, and CLI.

### YugabyteDB

River's PostgreSQL drivers detect YugabyteDB from the server's `version()`
the first time they need to, and cache what they find for the driver's
lifetime, like River for Go. YugabyteDB has no `xmax`, so a unique insertion
marks each row with a `river:unique_nonce` metadata value instead, as on
SQLite. Without `LISTEN`/`NOTIFY`, which needs YugabyteDB 2025.2.3 or later
with `ysql_yb_enable_listen_notify=true` on both masters and tservers, River
sends no notifications and a client polls even without `pollOnly: true`: it
claims new jobs every queue's `pollInterval`, rereads queue pauses, resumes,
and metadata, and checks its running jobs for cancellations every
`queueControlPollInterval` (two seconds by default), including while a stop
drains them. After enabling notifications, construct a new driver, for
example by restarting, so River detects them. On PostgreSQL 18 and later,
unique insertions read the conflicting row through `RETURNING OLD`.

On PostgreSQL, `@riverqueue/migrate` serializes concurrent migrators with a
transaction-scoped advisory lock. YugabyteDB has advisory locks only behind a
preview flag, so there, like River for Go's migrator everywhere, it takes no
lock; run one migrator at a time.

`@riverqueue/driver-prisma` is deliberately producer-only. It lets application
writes and job insertion share a Prisma transaction, but it does not claim or
work jobs. Use the PostgreSQL runtime in worker services.

## SQLite

`@riverqueue/driver-sqlite` uses Node's synchronous `DatabaseSync`. Each
statement briefly blocks the event loop, so run heavily loaded SQLite workers
in a process separate from latency-sensitive HTTP handling.

Like River for Go, River runs on a private connection to the application's
database file, in WAL mode, so application statements never join River's
transactions. Pass an application handle with an open transaction as `{ tx }`
to insert jobs with application rows; the driver's `transaction` helper begins
one with `BEGIN IMMEDIATE`. When another connection or process (for example a
Go River client, or an application transaction) holds SQLite's write lock,
River waits with an asynchronous backoff instead of blocking the event loop,
and fails with a retryable error after `busyTimeout`. For in-memory databases,
use `SqliteDriver.memory()`.

An insertion without `{ tx }` holds SQLite's write lock from its first
statement until it commits, and its insert middleware and hooks run inside
that transaction. On SQLite they must not await I/O after `next()`. See the
[driver's README](../driver/sqlite/README.md) for the transaction model.

River's SQLite tables live in the database's main schema. On SQLite, River
for Go's `Config.Schema` can place them in an attached database instead, but
`SqliteDriver` has no schema option, so Go clients sharing queues with
JavaScript on SQLite must leave `Schema` unset, as with River for Rust.

SQLite timestamps have millisecond precision. IDs remain `bigint`, and public
instants remain `Temporal.Instant`. Backend-specific capability differences are
reported explicitly instead of being silently emulated.

## Migrations

Run `@riverqueue/migrate` or `@riverqueue/cli` as an explicit deployment step.
Ordinary client construction and startup never migrate. The JavaScript bundle
is a generated mirror of River's canonical migrations and is checked for drift;
do not maintain a second hand-edited migration history.

Pin runtime and migration packages to the same River version line. In a
mixed-language deployment, apply only migrations supported by every still-live
binary and preserve the normal expand/deploy/contract rollout order.
