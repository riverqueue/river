# Changelog

All notable changes to River for Java will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **Breaking:** Move runtime configuration to `Client.builder(database)` and startup to `Client.start()`, which returns an `AutoCloseable` `RunHandle` for `stop()`, `stopAndCancel()`, and runtime queue/periodic controls. `Workers` is now a handler registry supplied through `.workers(registry)`; `.worker(type, handler)` supports inline registration. Move subscriptions and event types to `Client`, allowing subscriptions before startup. A client allows one active run and can restart after shutdown completes. [PR #1555](https://github.com/riverqueue/river/pull/1555).

## [0.1.0] - 2026-10-26

### Changed

- First non-prerelease release of River for Java.
- Publish the parent POM, library, and CLI as complete GitHub release assets served anonymously through `https://riverqueue.com/maven`. Maven Central supplies third-party dependencies. [PR #1543](https://github.com/riverqueue/river/pull/1543).
- **Breaking:** Replace the shaded CLI `-all.jar` with a thin JAR. Install `river-java` with JBang or use Maven to download the CLI and its runtime dependencies; both paths support Postgres and SQLite. Existing published alpha.1 standalone JARs remain available. See the [CLI installation instructions](https://github.com/riverqueue/river/blob/java/v0.1.0/java/README.md#migration-cli). [PR #1543](https://github.com/riverqueue/river/pull/1543).

### Fixed

- The thin CLI JAR now reports its release version and supports `java -jar` when the runtime JARs are in a sibling `dependency/` directory, as produced by a source build. [PR #1543](https://github.com/riverqueue/river/pull/1543).

## [0.1.0-alpha.1] - 2026-10-24

### Added

- First alpha release of River for Java. `com.riverqueue:river` provides a Java 21 client and worker runtime for Postgres and SQLite, sharing River's database schema and job protocol with other River clients. Includes typed jobs, virtual-thread workers, transactional inserts, unique, scheduled, periodic, and resumable jobs, queue management, hooks, middleware, events, and migrations.
- Standalone migration CLI, published as `com.riverqueue:river-cli` with classifier `all`. The executable JAR bundles both JDBC drivers and supports applying, reverting, listing, and exporting migrations.

### Changed

- **Breaking:** Replaced `Extension` and `Client.withExtension` with operation-specific hook and middleware interfaces, matching River for Go: `HookInsertBegin`, `HookPeriodicJobsStart`, `HookWorkBegin`, `HookWorkEnd`, `JobInsertMiddleware`, and `WorkerMiddleware`. Register them with `Client.withPlugins` to activate all interfaces an instance implements, or with `withHooks` and `withMiddleware` to activate only that side. Plugins run before separately registered hooks and middleware. Each operation-specific interface supports lambdas and checked exceptions. The `afterClaim` callback was removed; use worker middleware for work before the handler. [PR #1519](https://github.com/riverqueue/river/pull/1519).
- `HookWorkEnd.workEnd` returns the attempt's failure: return the incoming failure to preserve it, a replacement to change it, or `null` to report success. Each hook receives the previous hook's result, so hooks migrated from `Extension.afterWork` must explicitly return the failure to preserve the outcome. A work-begin failure now skips both the handler and work-end hooks. [PR #1519](https://github.com/riverqueue/river/pull/1519).
- Insertion middleware now receives the batch's submissions and returns its insertion results. It runs once per batch, including a one-element batch for single inserts, while insertion hooks run once per job. Hooks and middleware run inside the insertion's transaction. [PR #1519](https://github.com/riverqueue/river/pull/1519).
- Renamed the internal River Pro storage interface and its registration method from `Plugin`/`Client.withPlugin` to `Pilot`/`Client.withPilot`. `Plugin` is now the common marker interface for application hooks and middleware. [PR #1519](https://github.com/riverqueue/river/pull/1519).

### Fixed

- Fail retries that omit a saved cursor-only step, while preserving their checkpoints for recovery. Jobs with an existing step marker retain their behavior, including legacy stale cursor entries. [PR #1534](https://github.com/riverqueue/river/pull/1534).

- Replace whole top-level metadata values on SQLite, including JSON null, matching Postgres. Completed resumable cursor steps no longer run again because of stale cursor entries. [PR #1533](https://github.com/riverqueue/river/pull/1533).
