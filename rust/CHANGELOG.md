# Changelog

All notable changes to River's Rust crates are documented in this file. The
workspace crates (`riverqueue`, `riverqueue-macros`, `riverqueue-migrate`,
`riverqueue-cli`, and `riverqueue-test`) are versioned and released together.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Changes to River for Go are recorded in the [repository changelog](../CHANGELOG.md).

## [Unreleased]

### Changed

- **Breaking:** Replace Chrono with Jiff 0.2 throughout the Rust API. Job timestamps, insertion options, retry hooks, periodic schedules, and test helpers now use `jiff::Timestamp`; River re-exports Jiff as `riverqueue::jiff`. `CronSchedule::next_after` takes and returns `jiff::Zoned`, `CronTimeZone::Fixed` uses `jiff::tz::Offset`, and `CronTimeZone::Named` uses `jiff::tz::TimeZone`. `CronTimeZone` is now `Clone` rather than `Copy`. `PostgresReindexSchedule::DailyUtc` takes `jiff::civil::Time`. Rename the optional `chrono-tz` feature to `tzdb`. Database formats, canonical timestamp JSON, and Go-compatible cron behavior remain unchanged. Timestamp ranges and checked arithmetic follow Jiff's supported range. [PR #1567](https://github.com/riverqueue/river/pull/1567).

### Fixed

- Fixed a running job not being cancelled when its cancellation notification was lost, as happens if it's sent while the client's listener is reconnecting. Clients with notifications now also check their running jobs for cancellation requests once a minute, like River for Go. Retrying a job now clears its earlier attempt's cancellation request so that the retried job isn't cancelled again. [PR #1564](https://github.com/riverqueue/river/pull/1564).

## [0.4.0] - 2026-10-26

### Added

- Extend the unstable extension API with peer work hooks, resolved insertion options, and bounded producer claims. Post-insert callbacks now include existing unique duplicates, requested metadata, and duplicate flags. [PR #1501](https://github.com/riverqueue/river/pull/1501).
- Add optional pilot hooks for single-job, bulk, and cleaner-driven deletion. Hooks run within the deletion transaction and can extend or replace the standard deletion. [PR #1502](https://github.com/riverqueue/river/pull/1502).
- A worker cancels its job with a reason by returning `JobCancelError`, like River for Go's `JobCancel(err)`. Anywhere in the worker error's source chain, it cancels the job whatever attempts it has left, skips the error handler, and records `JobCancelError: <reason>` on the attempt as Go does. `WorkOutcome::Cancel` still cancels without a reason. [PR #1504](https://github.com/riverqueue/river/pull/1504).

### Changed

- **Breaking:** Resumable steps can no longer be nested. Calls fail before running child callbacks, preventing unreachable resume points and rolled-back parent checkpoints from being saved again. [PR #1534](https://github.com/riverqueue/river/pull/1534).
- **Breaking:** Attempt counts are now `i32` instead of `i16`, like River for Go's `int`: `JobRow::attempt` and `max_attempts`, `AttemptError::attempt` and `AttemptError::new`, `InsertOpts::max_attempts` and `with_max_attempts`, `InsertParams::max_attempts`, `ClientBuilder::default_max_attempts`, `MAX_ATTEMPTS_DEFAULT`, and `riverqueue-test`'s builders. On SQLite, which stores native integers, a job with more than 32,767 maximum attempts inserts and reads back unchanged, as in Go. On Postgres, whose columns are 16-bit, an insert clamps `max_attempts` to 32,767 like Go's drivers instead of failing. [PR #1504](https://github.com/riverqueue/river/pull/1504).

### Fixed

- Preserve original error types through work-begin hooks and peer outcomes so error handlers can downcast them. A `JobCancelError` reported for a peer cancels it, matching worker cancellation. [PR #1501](https://github.com/riverqueue/river/pull/1501).
- Fail retries that omit a saved cursor-only step, while preserving their checkpoints for recovery. Jobs with an existing step marker retain their behavior, including legacy stale cursor entries. [PR #1534](https://github.com/riverqueue/river/pull/1534).
- Replace whole top-level metadata values on SQLite, including JSON null, matching Postgres. Completed resumable cursor steps no longer run again because of stale cursor entries. [PR #1533](https://github.com/riverqueue/river/pull/1533).
- Preserve first-step resumable cursors on failure and step progress on snooze. Transactional checkpoints no longer leak into deferred updates after rollback or a failed write; rolling back a successful checkpoint write may replay earlier unpersisted steps. [PR #1521](https://github.com/riverqueue/river/pull/1521).

### Security

- Update locked TLS and constant-time comparison dependencies (`rustls`, `rustls-webpki`, `webpki-roots`, and `ctutils`) to address security advisories. [PR #1505](https://github.com/riverqueue/river/pull/1505), [PR #1527](https://github.com/riverqueue/river/pull/1527), [PR #1521](https://github.com/riverqueue/river/pull/1521).

## [0.3.0] - 2026-10-07

### Changed

- **Breaking:** Renamed `WorkerRegistry` to `Workers`, `register` to `add`, and `register_fn` to `add_fn` to align worker registration with Go's naming. Both methods retain their `Result` return type and registration behavior. [PR #1469](https://github.com/riverqueue/river/pull/1469).

## [0.2.0] - 2026-10-06

### Changed

- **Breaking:** Renamed `Client::start_with_graceful_shutdown` to `Client::start_with_graceful_stop` to match River's standard start/stop terminology. This is a rare breaking name change as the new Rust API stabilizes. [PR #1467](https://github.com/riverqueue/river/pull/1467).

## [0.1.0] - 2026-10-06

### Added

- First release of River for Rust. `riverqueue` provides a typed, Tokio-based client for Postgres (through SQLx) and SQLite that shares River's database schema and job protocol with River for Go, so Rust and Go clients can insert and work jobs in the same database. It includes typed workers, transactional inserts and completion, unique, scheduled, periodic, and resumable jobs, queue management, job cancellation, events, hooks, middleware, leader election, and maintenance services. Companion crates provide `#[derive(JobArgs)]` with unique options (`riverqueue-macros`), migration application and validation using Go's migration history (`riverqueue-migrate`), the `riverqueue` command for migrations and benchmarks (`riverqueue-cli`), and fixtures, insertion assertions, and worker test helpers (`riverqueue-test`). Requests run with `.tx(...)` use the caller's transaction directly, without a savepoint or nested transaction, like River for Go's `*Tx` methods. Errors may leave partial writes, so roll back the transaction or create an explicit savepoint around the request. [PR #1442](https://github.com/riverqueue/river/pull/1442).
