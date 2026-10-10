# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- Receive Postgres and SQLite notifications for insertions, queue controls, cancellation, and leadership handoffs, with reconnect recovery and a `poll_only` option. [PR #1496](https://github.com/riverqueue/river/pull/1496).
- Add `Client#request_resign` to request that the maintenance leader relinquish leadership after the caller's transaction commits. [PR #1496](https://github.com/riverqueue/river/pull/1496).
- Allow `River::Workers#add` to register a kind and a work block, using the client's default retry and timeout policies. [PR #1486](https://github.com/riverqueue/river/pull/1486).

### Fixed

- Wake workers when scheduled jobs become available or jobs are manually retried, including workers in other clients. [PR #1496](https://github.com/riverqueue/river/pull/1496).
- Honor cancellations that arrive between claiming a job and starting its worker. [PR #1496](https://github.com/riverqueue/river/pull/1496).
- Preserve worker wakeups that arrive between a fetch and the producer going to sleep. [PR #1496](https://github.com/riverqueue/river/pull/1496).

## [0.13.0] - 2026-10-08

### Added

- Expanded checks against Go-generated conformance fixtures to cover cron schedules, snooze counters, all shared metadata keys, and queue-control and leadership notification emission through both SQL drivers. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Added `CRON_TZ=` / `TZ=` prefixes, `?` wildcards, and Go-style `@every` durations to `PeriodicCron`. [PR #1463](https://github.com/riverqueue/river/pull/1463).

### Changed

- Ruby is now developed in the main [River repository](https://github.com/riverqueue/river/tree/master/ruby). Gem names and require paths are unchanged. The four public gems (`riverqueue`, `riverqueue-activerecord`, `riverqueue-sequel`, and `riverqueue-rails`) are released together. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Job insertion accepts only `available`, `pending`, and `scheduled` states. `running`, `retryable`, and terminal states are rejected, including when set by insertion hooks. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Insertion rejects queue names containing characters other than ASCII letters, digits, underscores, hyphens, colons, or periods. Nonzero `UniqueOpts#by_period` values must be at least one second. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Attempt counts must be between 0 and 32,767, and `max_attempts` must be between 1 and 32,767. These bounds also apply on SQLite. `job_retry` raises `ArgumentError` when another attempt would exceed the limit. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Job and queue metadata updates require a Hash; JSON-encoded strings are no longer accepted. Pass an empty Hash to clear metadata. Updates to `attempted_by` require string entries. [PR #1463](https://github.com/riverqueue/river/pull/1463).

### Fixed

- Externally claimed jobs honor worker retry hooks, including Active Job's retry policy, and use their claim timestamps for queue-wait statistics and attempt errors. Worker initialization failures are logged without losing the reported work error. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Externally reported cancellation, snooze, and interruption signals follow normal worker finalization instead of being recorded as ordinary failures. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Graceful shutdown and queue removal continue observing remote cancellation until active jobs finish, including when worker timeouts are disabled or cancellation polling temporarily fails. Draining waits respect polling intervals and wake when the last attempt finishes. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Blocking lifecycle calls from the client's own runtime threads fail immediately instead of waiting for themselves. Workers can still request nonblocking stop. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Inserts reject states other than `available`, `pending`, and `scheduled`, preventing stranded `running` jobs without attempt timestamps. Insertion hooks are checked after all callbacks run, and invalid batches roll back hook writes. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- The Sequel driver preserves application `ArgumentError` exceptions raised inside SQLite transactions after rolling back, including validation errors from insertion hooks. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Job inserts and updates reject attempt limits above 32,767 consistently across databases. Attempt updates and manual retries enforce the portable counter range, and claims tolerate counters already at the limit. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Wildcard queue pause and resume publish events for every affected queue, including beyond 100 queues. Events use the update's snapshots so concurrent changes cannot replace their contents or introduce unrelated queues. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Manually retrying a cancelled job clears the old cancellation request so its next attempt can complete. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Cancellation wins atomically over discarded attempts and other state transitions, including the last attempt or a worker that disables retries. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Stuck-job rescue falls back to the default retry policy per job when an application policy fails or returns an invalid time, allowing recovery and cleanup to continue. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Clients without consumer queues run configured periodic jobs and maintenance services. Adding periodic jobs starts maintenance when needed; registration is rejected when leader election is disabled. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Bulk deletion excludes running jobs before applying its limit and locks eligible Postgres rows so concurrent retries cannot cause deletion of jobs that no longer match the filters. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Stuck-job rescue respects longer or disabled client and worker timeouts and scans past protected attempts without consuming the rescue limit. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Snoozing and rescue tolerate JSON counters that overflow SQLite's floating-point representation. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Snoozing tolerates nonnumeric metadata counters instead of failing on booleans or collections. Ruby-specific recovery cases are tested independently of the shared Go fixtures. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Periodic jobs now carry `periodic: true` and, when named, `river:periodic_job_id`, while preserving constructor options and application metadata. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Queue pause, resume, metadata changes, and leader resignation now broadcast notifications for clients in other languages. Notifications commit and roll back with the corresponding database change. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Cron schedules include both occurrences of repeated daylight-saving times when given a local reference time. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Job insertion rejects invalid queue names, nonpositive attempt limits, and nonzero uniqueness periods shorter than one second before writing any jobs. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Custom maintenance service failures are logged without blocking other services, stuck-job rescue, or cleanup. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Job updates reject non-object metadata before writing to the database, preserving the existing job when invalid metadata is supplied. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Job completion atomically checks for cancellation so a concurrent cancellation cannot be overwritten by completion, including when finalization hooks are configured. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Job updates reject nonpositive attempt limits consistently on Postgres and SQLite. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Job lifecycle events report worker execution and completion durations separately using a monotonic clock. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Finalization hooks that delete jobs honor concurrent cancellation and record the cancelled attempt instead of deleting it. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Queue metadata writes and job metadata merges reject non-object values before changing persisted data. Job updates also reject non-string worker IDs in attempt histories. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Unique inserts with `exclude_kind` preserve the existing job's kind when a different kind conflicts, on both databases and through both drivers. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Retrying, snoozing, and interrupting jobs preserve the original queue-wait duration in lifecycle events. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Invalid rescue counters no longer prevent other stuck jobs in the batch from being rescued. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- SQLite retains only the newest 100 worker IDs when claiming a job, matching Postgres. [PR #1463](https://github.com/riverqueue/river/pull/1463).
- Attempt-error serialization supports frozen timestamps and preserves the caller's timezone. [PR #1463](https://github.com/riverqueue/river/pull/1463).

## [0.12.0] - 2026-10-01

### Added

- Add a full Ruby client for River with Go-compatible job insertion and execution on Postgres, SQLite, and YugabyteDB through ActiveRecord or Sequel. Includes workers, retries, cancellation, periodic and resumable jobs, job-persisted logging, job administration, migration and worker CLIs, and testing helpers. Rails and Active Job integration is available through `riverqueue-rails`, with workflows, batches, sequences, concurrency controls, and other advanced features in the separately distributed `riverqueue-pro` gem. [PR #70](https://github.com/riverqueue/riverqueue-ruby/pull/70).

## [0.11.0] - 2026-09-02

### Added

- Add SQLite support to the ActiveRecord and Sequel drivers, including current River JSONB storage, atomic bulk inserts, unique jobs, and notification-outbox writes. [PR #67](https://github.com/riverqueue/riverqueue-ruby/pull/67).

### Changed

- Stop pulling in `pg` as a hard dependency of either driver. Applications now select their database adapter by including `pg` for Postgres or `sqlite3` for SQLite. [PR #67](https://github.com/riverqueue/riverqueue-ruby/pull/67).

## [0.10.1] - 2026-04-09

### Fixed

- Fix ActiveRecord JSONB args. [PR #60](https://github.com/riverqueue/riverqueue-ruby/pull/60).

## [0.10.0] - 2026-03-29

### Changed

- Upgrade to Ruby 4.0. [PR #53](https://github.com/riverqueue/riverqueue-ruby/pull/53).

## [0.9.1] - 2025-10-21

### Changed

- Minor README fixes. [PR #50](https://github.com/riverqueue/riverqueue-ruby/pull/50).
- Periodic gem update (including some security upgrades in dependencies). [PR #51](https://github.com/riverqueue/riverqueue-ruby/pull/51).

## [0.9.0] - 2025-04-11

### Changed

- `by_period` uniqueness is now based off a job's `scheduled_at` instead of the current time if it has a value. [PR #39](https://github.com/riverqueue/riverqueue-ruby/pull/39).

## Fixed

- Correct some mistakes in the readme that referenced `SimpleArgs` instead of `SortArgs`. [PR #44](https://github.com/riverqueue/riverqueue-ruby/pull/44).

## [0.8.0] - 2024-12-19

⚠️ Version 0.8.0 contains breaking changes to transition to River's new unique jobs implementation and to enable broader, more flexible application of unique jobs. Detailed notes on the implementation are contained in [the original River PR](https://github.com/riverqueue/river/pull/590), and the notes below include short summaries of the ways this impacts this client specifically.

Users should upgrade backends to River v0.12.0 before upgrading this library in order to ensure a seamless transition of all in-flight jobs. Afterward, the latest River version may be used.

### Breaking

- **Breaking change:** The return type of `Client#insert_many` has been changed. Rather than returning just the number of rows inserted, it returns an array of all the `InsertResult` values for each inserted row. Unique conflicts which are skipped as duplicates are indicated in the same fashion as single inserts (the `unique_skipped_as_duplicated` attribute), and in such cases the conflicting row will be returned instead. [PR #32](https://github.com/riverqueue/riverqueue-ruby/pull/32).
- **Breaking change:** Unique jobs no longer allow total customization of their states when using the `by_state` option. The pending, scheduled, available, and running states are required whenever customizing this list. [PR #32](https://github.com/riverqueue/riverqueue-ruby/pull/32).

### Added

- The `UniqueOpts` class gains an `exclude_kind` option for cases where uniqueness needs to be guaranteed across multiple job types. [PR #32](https://github.com/riverqueue/riverqueue-ruby/pull/32).
- Unique jobs utilizing `by_args` can now also opt to have a subset of the job's arguments considered for uniqueness. For example, you could choose to consider only the `customer_id` field while ignoring the other fields:

  ```ruby
  UniqueOpts.new(by_args: ["customer_id"])
  ```

  Any fields considered in uniqueness are also sorted alphabetically in order to guarantee a consistent result across implementations, even if the encoded JSON isn't sorted consistently. [PR #32](https://github.com/riverqueue/riverqueue-ruby/pull/32).

### Changed

- Unique jobs have been improved to allow bulk insertion of unique jobs via `Client#insert_many`.

  This updated implementation is significantly faster due to the removal of advisory locks in favor of an index-backed uniqueness system, while allowing some flexibility in which job states are considered. However, not all states may be removed from consideration when using the `by_state` option; pending, scheduled, available, and running states are required whenever customizing this list. [PR #32](https://github.com/riverqueue/riverqueue-ruby/pull/32).

- Update REXML dependency. [PR #28](https://github.com/riverqueue/riverqueue-ruby/pull/36).

## [0.7.0] - 2024-08-30

### Changed

- Now compatible with "fast path" unique job insertion that uses a unique index instead of advisory lock and fetch [as introduced in River #451](https://github.com/riverqueue/river/pull/451). [PR #28](https://github.com/riverqueue/riverqueue-ruby/pull/28).

## [0.6.1] - 2024-08-21

### Fixed

- Fix source files not being correctly included in built Ruby gems. [PR #26](https://github.com/riverqueue/riverqueue-ruby/pull/26).

## [0.6.0] - 2024-07-06

### Changed

- Advisory lock prefixes are now checked to make sure they fit inside of four bytes. [PR #24](https://github.com/riverqueue/riverqueue-ruby/pull/24).

## [0.5.0] - 2024-07-05

### Changed

- Tag format is now checked on insert. Tags should be no more than 255 characters and match the regex `/\A[\w][\w\-]+[\w]\z/`. [PR #22](https://github.com/riverqueue/riverqueue-ruby/pull/22).
- Returned jobs now have a `metadata` property. [PR #21](https://github.com/riverqueue/riverqueue-ruby/pull/22).

## [0.4.0] - 2024-04-28

### Changed

- Implement the FNV (Fowler–Noll–Vo) hashing algorithm in the project and drop dependency on the `fnv-hash` gem. [PR #14](https://github.com/riverqueue/riverqueue-ruby/pull/14).

## [0.3.0] - 2024-04-27

### Added

- Implement unique job insertion. [PR #10](https://github.com/riverqueue/riverqueue-ruby/pull/10).

## [0.2.0] - 2024-04-27

### Added

- Implement `#insert_many` for batch job insertion. [PR #5](https://github.com/riverqueue/riverqueue-ruby/pull/5).

## [0.1.0] - 2024-04-25

### Added

- Initial implementation that supports inserting jobs using either ActiveRecord or Sequel. [PR #1](https://github.com/riverqueue/riverqueue-ruby/pull/1).
