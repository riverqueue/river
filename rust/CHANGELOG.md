# Changelog

All notable changes to River's Rust crates are documented in this file. The
workspace crates (`riverqueue`, `riverqueue-macros`, `riverqueue-migrate`,
`riverqueue-cli`, and `riverqueue-test`) are versioned and released together.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
Changes to River for Go are recorded in the [repository changelog](../CHANGELOG.md).

## [Unreleased]

### Added

- First preview release of River for Rust. `riverqueue` provides a typed,
  Tokio-based client for PostgreSQL (through SQLx) and SQLite that shares
  River's database schema and job protocol with River for Go, so Rust and Go
  clients can insert and work jobs in the same database. It includes typed
  workers, transactional inserts and completion, unique, scheduled, periodic,
  and resumable jobs, queue management, job cancellation, events, hooks,
  middleware, leader election, and maintenance services.
- `riverqueue-macros` provides `#[derive(JobArgs)]`, including unique options.
- `riverqueue-migrate` applies and validates River's migration lines on
  PostgreSQL and SQLite, sharing migration history with River for Go.
- `riverqueue-cli` installs the `riverqueue` command for migrations and
  benchmarks.
- `riverqueue-test` provides fixtures, insertion assertions, and helpers for
  running workers in tests.
