# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Add `@riverqueue/driver-knex` for inserting jobs on Postgres inside Knex transactions, including batches, unique jobs, and nested savepoints. Configure its `knexTransactionAdapter` on `PgDriver` to use native Knex transactions with `completeTx` in workers. [PR #1516](https://github.com/riverqueue/river/pull/1516).
- Support native Prisma interactive transactions with `completeTx` in workers and `{ tx }` on Postgres client operations. Configure `prismaTransactionAdapter` from `@riverqueue/driver-prisma` on `PgDriver` to commit application writes and job completion together. [PR #1517](https://github.com/riverqueue/river/pull/1517).
- Support Node.js 24 across all packages. River automatically installs a `Temporal` polyfill when the runtime has no native implementation, preserving native `Temporal` on Node.js 26.

### Changed

- **Breaking:** Resumable steps can no longer be nested. Calls fail before running child callbacks, preventing unreachable resume points and rolled-back parent checkpoints from being saved again. [PR #1534](https://github.com/riverqueue/river/pull/1534).
- Like River for Go, `insertMany` rejects an empty batch with a `ValidationError` instead of returning an empty result. [PR #1504](https://github.com/riverqueue/river/pull/1504).
- Like River for Go, the client no longer caps `maxAttempts` at 32767: SQLite stores wider attempt counts, and the Postgres and Prisma drivers clamp `maxAttempts` to their 16-bit column on insert instead of failing. [PR #1504](https://github.com/riverqueue/river/pull/1504).
- Like River for Go's `ResumableStep`, a failed resumable step records the step's own error as the attempt's error instead of wrapping it in a `LifecycleError`. [PR #1504](https://github.com/riverqueue/river/pull/1504).

### Fixed

- Refresh a durable periodic job's saved row only after its next run time is loaded at startup or committed. When a startup write rolled back, keep-alive could revive an expired row, and the next leader enqueued every run since the row's old next run time. [PR #1540](https://github.com/riverqueue/river/pull/1540).
- Fail retries that omit a saved cursor-only step, while preserving their checkpoints for recovery. Jobs with an existing step marker retain their behavior, including legacy stale cursor entries. [PR #1534](https://github.com/riverqueue/river/pull/1534).

- Replace whole top-level metadata values on SQLite, including JSON null, matching Postgres. Completed resumable cursor steps no longer run again because of stale cursor entries. [PR #1533](https://github.com/riverqueue/river/pull/1533).

- Preserve first-step resumable cursors on failure and step progress on snooze. Transactional checkpoints no longer leak into deferred updates after rollback or a failed write; rolling back a successful checkpoint write may replay earlier unpersisted steps.

- Worker threads respect cancellation received while loading a handler module, before invoking its handler.
- Like River for Go, a leader whose renewal finds its term gone, because it expired or another process replaced it, gives up leadership at once instead of running maintenance until its local deadline passes. [PR #1504](https://github.com/riverqueue/river/pull/1504).
- `updateQueue` raising a full queue's `maxWorkers` starts jobs at once instead of waiting for a running job to finish. [PR #1504](https://github.com/riverqueue/river/pull/1504).

## [0.2.0] - 2026-10-07

### Added

- JavaScript now implements full River client support for Postgres and SQLite, interoperating with Go and Rust in the same database. Includes workers, graceful stopping and cancellation, leader election and maintenance, periodic and resumable jobs, job and queue management, hooks, middleware, events, migrations, CLI tooling, worker threads, and test helpers. Requires Node.js 26 with native `Temporal`; job IDs are `bigint` and timestamps are `Temporal.Instant`. This replaces the insert-only 0.1 API; see [migrating from 0.1](./docs/migrating-from-0.1.md) and `riverqueue codemod-0.1` for upgrades. [PR #1443](https://github.com/riverqueue/river/pull/1443).

## [0.1.0] - 2026-06-01

### Added

- Initial release of the River TypeScript client with insert-only support, matching the semantics of the Go River client. Includes a core `riverqueue` package with `Client`, `JobArgsObject`, `InsertManyParams`, unique job support, and configurable schema. Driver packages `@riverqueue/driver-pg` (node-postgres) and `@riverqueue/driver-prisma` (Prisma) are provided as separate workspace packages to keep transitive dependencies minimal. [PR #1](https://github.com/riverqueue/riverqueue-js/pull/1).
