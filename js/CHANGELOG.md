# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.2.0] - 2026-10-07

### Added

- JavaScript now implements full River client support for Postgres and SQLite, interoperating with Go and Rust in the same database. Includes workers, graceful stopping and cancellation, leader election and maintenance, periodic and resumable jobs, job and queue management, hooks, middleware, events, migrations, CLI tooling, worker threads, and test helpers. Requires Node.js 26 with native `Temporal`; job IDs are `bigint` and timestamps are `Temporal.Instant`. This replaces the insert-only 0.1 API; see [migrating from 0.1](./docs/migrating-from-0.1.md) and `riverqueue codemod-0.1` for upgrades. [PR #1443](https://github.com/riverqueue/river/pull/1443).

## [0.1.0] - 2026-06-01

### Added

- Initial release of the River TypeScript client with insert-only support, matching the semantics of the Go River client. Includes a core `riverqueue` package with `Client`, `JobArgsObject`, `InsertManyParams`, unique job support, and configurable schema. Driver packages `@riverqueue/driver-pg` (node-postgres) and `@riverqueue/driver-prisma` (Prisma) are provided as separate workspace packages to keep transitive dependencies minimal. [PR #1](https://github.com/riverqueue/riverqueue-js/pull/1).
