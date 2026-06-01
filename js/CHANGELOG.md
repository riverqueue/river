# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.1.0] - 2026-06-01

### Added

- Initial release of the River TypeScript client with insert-only support, matching the semantics of the Go River client. Includes a core `riverqueue` package with `Client`, `JobArgsObject`, `InsertManyParams`, unique job support, and configurable schema. Driver packages `@riverqueue/driver-pg` (node-postgres) and `@riverqueue/driver-prisma` (Prisma) are provided as separate workspace packages to keep transitive dependencies minimal. [PR #1](https://github.com/riverqueue/riverqueue-js/pull/1).
