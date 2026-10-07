# Changelog

All notable changes to River's Rust crates are documented in this file. The
workspace crates (`riverqueue`, `riverqueue-macros`, `riverqueue-migrate`,
`riverqueue-cli`, and `riverqueue-test`) are versioned and released together.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Changes to River for Go are recorded in the [repository changelog](../CHANGELOG.md).

## [Unreleased]

## [0.2.0] - 2026-10-06

### Changed

- Renamed `Client::start_with_graceful_shutdown` to `Client::start_with_graceful_stop` to match River's standard start/stop terminology. This is a rare breaking name change as the new Rust API stabilizes. [PR #1467](https://github.com/riverqueue/river/pull/1467).

## [0.1.0] - 2026-10-06

### Added

- First release of River for Rust. `riverqueue` provides a typed, Tokio-based client for PostgreSQL (through SQLx) and SQLite that shares River's database schema and job protocol with River for Go, so Rust and Go clients can insert and work jobs in the same database. It includes typed workers, transactional inserts and completion, unique, scheduled, periodic, and resumable jobs, queue management, job cancellation, events, hooks, middleware, leader election, and maintenance services. Companion crates provide `#[derive(JobArgs)]` with unique options (`riverqueue-macros`), migration application and validation using Go's migration history (`riverqueue-migrate`), the `riverqueue` command for migrations and benchmarks (`riverqueue-cli`), and fixtures, insertion assertions, and worker test helpers (`riverqueue-test`). Requests run with `.tx(...)` use the caller's transaction directly, without a savepoint or nested transaction, like River for Go's `*Tx` methods. Errors may leave partial writes, so roll back the transaction or create an explicit savepoint around the request. [PR #1442](https://github.com/riverqueue/river/pull/1442).
