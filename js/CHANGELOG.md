# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

This release turns the insert-only 0.1 client into a complete River
implementation that interoperates with River for Go and Rust in the same
database. It requires Node.js 26 with native `Temporal`. See
[migrating from 0.1](./docs/migrating-from-0.1.md); `riverqueue codemod-0.1`
rewrites the mechanical changes.

### Added

- Workers and a supervised runtime: `Workers`, `client.start()` returning a `RunHandle` (`stop`, `completed`, `await using`), per-queue concurrency, dynamic queues, cooperative cancellation through `AbortSignal`, job timeouts, stuck-job detection with `jobStuckThreshold` (10 seconds by default, like River for Go's `JobStuckThreshold`), graceful and cancelling shutdown, and resilience to transient database failures.
- Like River for Go, a stopping leader resigns and ends maintenance as the stop begins, while its queues drain, and a failed runtime shuts down the same way before `run.completed` rejects. A removed queue's name stays reserved until its jobs finish.
- A client-wide `fetchCooldown` (100 ms by default, like River for Go's `FetchCooldown`) is the claim cooldown of queues that don't set their own, and limits insert notifications like Go's client: on every driver, a client notifies a queue of new jobs at most once per cooldown, whether it inserts them directly, as periodic jobs, or by scheduling them. Retrying a job sends no insert notification.
- `fetchOnlyKnownKinds: true` limits a client's claims to the kinds it has workers for when it starts, like River for Go's `Config.FetchOnlyKnownKinds`, so clients with different workers can share a queue while jobs of other kinds stay available without using an attempt.
- Like River for Go, a client without notifications, whether `pollOnly: true` or on a server without them, checks its running jobs for cancellation requests every `queueControlPollInterval` (two seconds by default), including while a stop drains them, so another client's cancellation reaches it.
- YugabyteDB is supported as a PostgreSQL server, like River for Go: `PgDriver` and `PrismaDriver` detect it, mark unique insertions with a `river:unique_nonce` metadata value in place of `xmax`, and, unless `yb_enable_listen_notify` is on, send no notifications while clients poll as if `pollOnly` were set. On PostgreSQL 18 and later, unique insertions read the conflicting row through `RETURNING OLD`.
- Like River for Go, when a new leader's periodic job enqueuer fails to start three times, the leader resigns that term itself instead of continuing to retry.
- Client options, including `maintenance` and `eventLoopDelay`, queue configuration, in `queues` and in `addQueue` and `updateQueue`, and stop options reject keys River doesn't know, such as a misspelled `pollInterval`, with a `ValidationError`.
- Job definitions with `defineJob`, validated by any Standard Schema library or an explicit decoder on insert and again before work. A definition's `kindAliases` lets its worker also work jobs stored under other kinds, like River for Go's `JobArgsWithKindAliases`, so a kind can be renamed safely.
- Work outcomes `complete`, `snooze`, `discard`, and `cancel`; retry policies per client or, like River for Go's `Worker.NextRetry`, per worker; an error handler, resumable steps, output recording, and transactional completion with `ctx.completeTx`.
- Like River for Go, a handler's `signal` aborts with a `JobAttemptFinishedError` once its attempt finished, so work the handler left running stops.
- Leader election and maintenance matching Go: scheduler, rescuer, job and queue cleaners, PostgreSQL reindexer, and periodic jobs (`periodicJob`) with fixed intervals or custom schedules such as cron. Like Go, maintenance works in batches of 10,000 rows with a timeout on each batch and a random 50 ms to 1 s pause between batches, and a service switches to batches of 1,000 after three timed-out batches in a row.
- `leaderElectionDisabled: true` keeps a client out of leader election, like River for Go's `Config.LeaderElectionDisabled`: it works jobs from its queues while other eligible clients in the same database and schema handle scheduling, retries, periodic jobs, rescue, and cleanup. Such a client rejects `periodicJobs` and changes to `client.periodicJobs` with a `ConfigurationError`.
- `cron()`, a periodic schedule that parses cron expressions exactly like River for Go (robfig/cron's `ParseStandard`, including descriptors, `@every`, and `CRON_TZ=` prefixes) and fires at the same times. Expressions without a `CRON_TZ=` prefix or `timeZone` option are evaluated in the process's local time zone, as in Go; pin a zone in mixed-language fleets.
- Job and queue queries and controls under `client.jobs` and `client.queues`, with opaque keyset pagination. Job list cursors use River for Go's `JobListCursor` text format, so page tokens pass freely between River for Go, Rust, and JavaScript. Like Go, ordering by `time` sorts every listed job by the first listed state's time field (`scheduledAt` without a state filter), with null times last ascending and first descending, and cursors carry that field's value.
- Hooks, work and insert middleware, plugins, bounded event subscriptions, `diagnostics_channel` publishing, and pino-compatible logging.
- `Workers.add` also takes a `WorkHandlerFactory`, whose `createWorkHandler` builds the handler from the definition it's registered with, so an integration's worker is registered as `workers.add(definition, integrationWorker(options))`.
- `@riverqueue/driver-sqlite`, a complete runtime on Node's built-in `node:sqlite`. Like River for Go, River runs on a private connection to the application's database file, in WAL mode, so application statements never join River's transactions.
  - Pass an application handle with a transaction open as `{ tx }` to insert jobs with application rows. `transaction(database, callback)` begins one with `BEGIN IMMEDIATE`, retrying asynchronously while another connection holds the write lock.
  - `SqliteDriver.memory()` and `driver.connect()` share an in-memory database, and `driver.close()` closes River's connection.
  - Once River's own transaction holds the write lock, a River call without `{ tx }` from inside it, such as from insert middleware after `next()`, fails at once with a `TransactionScopeError` instead of waiting for it forever.
  - On SQLite, insert middleware and hooks must not await I/O after `next()`, while River holds the write lock. When River's transaction is still open at the event loop's next turn, River rolls it back and fails the insertion with a `TransactionScopeError` whose `reason` is `"event_loop_turn"`. The check finds most mistakes, including all slow I/O and every insertion started from an I/O callback such as an HTTP handler, but it isn't deterministic for fast local I/O awaited by an insertion started from a timer or `setImmediate` callback.
- `@riverqueue/migrate` with River's canonical PostgreSQL and SQLite migrations, and `@riverqueue/cli` with migration commands, `bench`, and the 0.1 codemod.
- River's migration 8: on SQLite it rebuilds `river_job` with an `AUTOINCREMENT` key so a deleted job's ID is never reused, and, like River for Go, refuses to run while an extension's `river_job_sequence`, `river_job_workflow_scheduling`, or `river_workflow` schema exists; on PostgreSQL it changes nothing.
- `@riverqueue/worker-threads` for CPU-bound handlers, and `@riverqueue/test` with test clients, insertion assertions, and `workOnce`. A worker-thread handler that still ignores its abort after the client's `jobStuckThreshold` has its thread terminated; when the abort came from a stopping client, it fails with a `JobAbortedError`, so its attempt counts and a job that hangs on every stop doesn't retry forever.

### Changed

- Job IDs are `bigint` and timestamps are `Temporal.Instant`, so no database value is rounded. Job args and metadata use an exact JSON domain that preserves numbers JavaScript cannot represent.
- Argument classes (`JobArgs`, `JobArgsObject`) and `InsertManyParams` are replaced by job definitions and plain `{ job, args, options }` batch items; insert results report `status: "inserted" | "duplicate"`.
- Insert options use `unique` (was `uniqueOpts`) with duration values such as `byPeriod: { seconds: 60 }`, add `delay`, and accept `Date` or `Temporal.Instant` for `scheduledAt`. Every duration option takes a `Temporal.Duration` or duration-like object.
- Like River for Go, `insertMany` rejects a batch in which a unique key appears more than once among jobs whose state the key covers with a `ValidationError`, before writing any of it. 0.1 inserted the first such job and reported the rest as duplicates of it.
- Like River for Go, `unique.excludeKind` requires `byArgs`, `byQueue`, or `byPeriod` and is otherwise rejected with a `ValidationError`, since the unique key would be the same for every job.
- `ClientOpts`, `InsertOpts`, and `UniqueOpts` are renamed `ClientOptions`, `InsertOptions`, and `UniqueOptions`; `riverqueue codemod-0.1` imports them under their 0.1 names.
- The PostgreSQL schema is configured on `PgDriver`, which accepts any `node-postgres` client as a transaction. `@riverqueue/driver-prisma` clients are typed as insert-only.
- Errors River throws all extend `RiverError` with a stable `code`.
- River for Go's `InsertManyFast` has no counterpart yet; use `insertMany`, which returns one result per row, reports a unique conflict as `status: "duplicate"` rather than skipping the row (SQLite) or failing the batch (PostgreSQL), and runs insert middleware and hooks.
- Like River for Go, a job inserted without a schedule takes its creation and scheduled times from the database's clock, so a producer never waits out a difference between the application's clock and the database's.
- The packages ship as ESM only; `require()` works through Node 26's `require(esm)`.
- Like River for Go, an operation given a caller's transaction as `{ tx }` runs directly in it and opens no savepoint or nested transaction, on every driver, including transactional completion with `completeTx`. When the operation fails after writing, such as from insert middleware or a hook that throws after the write, its writes stay in the caller's transaction, which the caller rolls back; on PostgreSQL a database error aborts that transaction. Validation still fails before anything is written. An application that needs to recover and continue the transaction can wrap the call in a savepoint of its own.
- Like River for Go, `insert` and `insertMany` without `{ tx }` run argument validation, insert middleware, `beforeInsert` and `afterInsert` hooks, and the write in one transaction River owns, so an error thrown by middleware after `next()` returns or by an `afterInsert` hook rolls the jobs back.
- A `PgDriver` constructed from a single `node-postgres` client, not a pool, rejects insertions without `{ tx }` with a `ConfigurationError`, like River for Go's drivers without a pool: River needs a connection of its own to begin a transaction. Pass `{ tx }` or construct the driver with a `Pool`. `PrismaDriver` runs such insertions in a Prisma interactive transaction, so its client must be a root `PrismaClient` with `$transaction`; its `transactionOptions` sets that transaction's `maxWait` and `timeout`.
- Drivers are opaque: `PgDriver`, `SqliteDriver`, and `PrismaDriver` expose only their construction, plus `SqliteDriver.memory()`, `connect()`, and `close()`. River's operations, such as 0.1's `jobInsert` and `jobInsertMany`, aren't callable on them, and `PgDriver` no longer exposes its `pool` or `schema`, nor `SqliteDriver` its `database`: keep your own reference to the connection. `createMigrator(driver)` still migrates the driver's connection and schema. `new Client()` accepts only a River driver, not any object with insertion methods; test clients come from `@riverqueue/test`.
- Updated the Prisma example and `@riverqueue/driver-prisma` usage documentation for Prisma 7. The example now uses `@prisma/adapter-pg`, `prisma.config.ts`, and an explicitly generated ESM TypeScript client; its build automatically runs `prisma generate` before compilation. Prisma 7 requires Node.js `^20.19`, `^22.12`, or `>=24`. [PR #31](https://github.com/riverqueue/riverqueue-js/pull/31).

### Fixed

- A unique insertion with `excludeKind` that's skipped as a duplicate of a job of another kind no longer changes that job's kind to its own, which made workers of the other kind run it with the first job's arguments. The existing job keeps its kind, like River for Go.

## [0.1.0] - 2026-06-01

### Added

- Initial release of the River TypeScript client with insert-only support, matching the semantics of the Go River client. Includes a core `riverqueue` package with `Client`, `JobArgsObject`, `InsertManyParams`, unique job support, and configurable schema. Driver packages `@riverqueue/driver-pg` (node-postgres) and `@riverqueue/driver-prisma` (Prisma) are provided as separate workspace packages to keep transitive dependencies minimal. [PR #1](https://github.com/riverqueue/riverqueue-js/pull/1).
