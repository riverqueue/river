# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.9.0] - 2024-07-04

### Added

- `Config.TestOnly` has been added. It disables various features in the River client like staggered maintenance service start that are useful in production, but may be somewhat harmful in tests because they make start/stop slower. [PR #414](https://github.com/riverqueue/river/pull/414).

### Changed

⚠️ Version 0.9.0 has a small breaking change in `ErrorHandler`. As before, we try never to make breaking changes, but this one was deemed quite important because `ErrorHandler` was fundamentally lacking important functionality.

- **Breaking change:** Add stack trace to `ErrorHandler.HandlePanicFunc`. Fixing code only requires adding a new `trace string` argument to `HandlePanicFunc`. [PR #423](https://github.com/riverqueue/river/pull/423).

    ``` go
    # before
    HandlePanic(ctx context.Context, job *rivertype.JobRow, panicVal any) *ErrorHandlerResult

    # after
    HandlePanic(ctx context.Context, job *rivertype.JobRow, panicVal any, trace string) *ErrorHandlerResult
    ```

### Fixed

- Pausing or resuming a queue that was already paused or not paused respectively no longer returns `rivertype.ErrNotFound`. The same goes for pausing or resuming using the all queues string (`*`) when no queues are in the database (previously that also returned `rivertype.ErrNotFound`). [PR #408](https://github.com/riverqueue/river/pull/408).
- Fix a bug where periodic job constructors were only called once when adding the periodic job rather than being invoked every time the periodic job is scheduled. [PR #420](https://github.com/riverqueue/river/pull/420).

## [0.8.0] - 2024-06-25

### Added

- Add transaction variants for queue-related client functions: `QueueGetTx`, `QueueListTx`, `QueuePauseTx`, and `QueueResumeTx`. [PR #402](https://github.com/riverqueue/river/pull/402).

### Fixed

- Fix possible Client shutdown panics if the user-provided context is cancelled while jobs are still running. [PR #401](https://github.com/riverqueue/river/pull/401).

## [0.7.0] - 2024-06-13

### Added

- The default max attempts of 25 can now be customized on a per-client basis using `Config.MaxAttempts`. This is in addition to the ability to customize at the job type level with `JobArgs`, or on a per-job basis using `InsertOpts`. [PR #383](https://github.com/riverqueue/river/pull/383).
- Add `JobDelete` / `JobDeleteTx` APIs on `Client` to allow permanently deleting any job that's not currently running. [PR #390](https://github.com/riverqueue/river/pull/390).

### Fixed

- Fix `StopAndCancel` to not hang if called in parallel to an ongoing `Stop` call. [PR #376](https://github.com/riverqueue/river/pull/376).

## [0.6.1] - 2024-05-21

### Fixed

- River now considers per-worker timeout overrides when rescuing jobs so that jobs with a long custom timeout won't be rescued prematurely. [PR #350](https://github.com/riverqueue/river/pull/350).
- River CLI now exits with status 1 in the case of a problem with commands or flags, like an unknown command or missing required flag. [PR #363](https://github.com/riverqueue/river/pull/363).
- Fix migration version 4 (from 0.5.0) so that the up migration can be re-run after it was originally rolled back. [PR #364](https://github.com/riverqueue/river/pull/364).

## [0.6.0] - 2024-05-08

### Added

- `RequireNotInserted` test helper (in addition to the existing `RequireInserted`) that verifies that a job with matching conditions was _not_ inserted. [PR #237](https://github.com/riverqueue/river/pull/237).

### Changed

- The periodic job enqueuer now sets `scheduled_at` of inserted jobs to the more precise time of when they were scheduled to run, as opposed to when they were inserted. [PR #341](https://github.com/riverqueue/river/pull/341).

### Fixed

- Remove use of `github.com/lib/pq`, making it once again a test-only dependency. [PR #337](https://github.com/riverqueue/river/pull/337).

## [0.5.0] - 2024-05-03

⚠️ Version 0.5.0 contains a new database migration, version 4. This migration is backward compatible with any River installation running the v3 migration. Be sure to run the v4 migration prior to deploying the code from this release.

### Added

- Add `pending` job state. This is currently unused, but will be used to build higher level functionality for staging jobs that are not yet ready to run (for some reason other than their scheduled time being in the future). Pending jobs will never be run or deleted and must first be moved to another state by external code. [PR #301](https://github.com/riverqueue/river/pull/301).
- Queue status tracking, pause and resume. [PR #301](https://github.com/riverqueue/river/pull/301).

  A useful operational lever is the ability to pause and resume a queue without shutting down clients. In addition to pause/resume being a feature request from [#54](https://github.com/riverqueue/river/pull/54), as part of the work on River's UI it's been useful to list out the active queues so that they can be displayed and manipulated.

  A new `river_queue` table is introduced in the v4 migration for this purpose. Upon startup, every producer in each River `Client` will make an `UPSERT` query to the database to either register the queue as being active, or if it already exists it will instead bump the timestamp to keep it active. This query will be run periodically in each producer as long as the `Client` is alive, even if the queue is paused. A separate query will delete/purge any queues which have not been active in awhile (currently fixed to 24 hours).

  `QueuePause` and `QueueResume` APIs have been introduced to `Client` pause and resume a single queue by name, or _all_ queues using the special `*` value. Each producer will watch for notifications on the relevant `LISTEN/NOTIFY` topic unless operating in poll-only mode, in which case they will periodically poll for changes to their queue record in the database.

### Changed

- Job insert notifications are now handled within application code rather than within the database using triggers. [PR #301](https://github.com/riverqueue/river/pull/301).

  The initial design for River utilized a trigger on job insert that issued notifications (`NOTIFY`) so that listening clients could quickly pick up the work if they were idle. While this is good for lowering latency, it does have the side effect of emitting a large amount of notifications any time there are lots of jobs being inserted. This adds overhead, particularly to high-throughput installations.

  To improve this situation and reduce overhead in high-throughput installations, the notifications have been refactored to be emitted at the application level. A client-level debouncer ensures that these notifications are not emitted more often than they could be useful. If a queue is due for an insert notification (on a particular Postgres schema), the notification is piggy-backed onto the insert query within the transaction. While this has the impact of increasing insert latency for a certain percentage of cases, the effect should be small.

  Additionally, initial releases of River did not properly scope notification topics within the global `LISTEN/NOTIFY` namespace. If two River installations were operating on the same Postgres database but within different schemas (search paths), their notifications would be emitted on a shared topic name. This is no longer the case and all notifications are prefixed with a `{schema_name}.` string.

- Add `NOT NULL` constraints to the database for `river_job.args` and `river_job.metadata`. Normal code paths should never have allowed for null values any way, but this constraint further strengthens the guarantee. [PR #301](https://github.com/riverqueue/river/pull/301).
- Stricter constraint on `river_job.finalized_at` to ensure it is only set when paired with a finalized state (completed, discarded, cancelled). Normal code paths should never have allowed for invalid values any way, but this constraint further strengthens the guarantee. [PR #301](https://github.com/riverqueue/river/pull/301).

## [0.4.1] - 2024-04-22

### Fixed

- Update job state references in `./cmd/river` and some documentation to `rivertype`. Thanks Danny Hermes (@dhermes)! 🙏🏻 [PR #315](https://github.com/riverqueue/river/pull/315).

## [0.4.0] - 2024-04-20

### Changed

⚠️ Version 0.4.0 has a number of small breaking changes which we've decided to release all as part of a single version. More breaking changes in one release is inconvenient, but we've tried to coordinate them in hopes that any future breaking changes will be non-existent or very rare. All changes will get picked up by the Go compiler, and each one should be quite easy to fix. The changes don't apply to any of the most common core APIs, and likely many projects won't have to change any code.

- **Breaking change:** There are a number of small breaking changes in the job list API using `JobList`/`JobListTx`:
  - Now support querying jobs by a list of Job Kinds and States. Also allows for filtering by specific timestamp values. Thank you Jos Kraaijeveld (@thatjos)! 🙏🏻 [PR #236](https://github.com/riverqueue/river/pull/236).
  - Job listing now defaults to ordering by job ID (`JobListOrderByID`) instead of a job timestamp dependent on on requested job state. The previous ordering behavior is still available with `NewJobListParams().OrderBy(JobListOrderByTime, SortOrderAsc)`. [PR #307](https://github.com/riverqueue/river/pull/307).
  - The function `JobListCursorFromJob` no longer needs a sort order parameter. Instead, sort order is determined based on the job list parameters that the cursor is subsequently used with. [PR #307](https://github.com/riverqueue/river/pull/307).
- **Breaking change:** Client `Insert` and `InsertTx` functions now return a `JobInsertResult` struct instead of a `JobRow`. This allows the result to include metadata like the new `UniqueSkippedAsDuplicate` property, so callers can tell whether an inserted job was skipped due to unique constraint. [PR #292](https://github.com/riverqueue/river/pull/292).
- **Breaking change:** Client `InsertMany` and `InsertManyTx` now return number of jobs inserted as `int` instead of `int64`. This change was made to make the type in use a little more idiomatic. [PR #293](https://github.com/riverqueue/river/pull/293).
- **Breaking change:** `river.JobState*` type aliases have been removed. All job state constants should be accessed through `rivertype.JobState*` instead. [PR #300](https://github.com/riverqueue/river/pull/300).

See also the [0.4.0 release blog post](https://riverqueue.com/blog/a-few-breaking-changes) with code samples and rationale behind various changes.

## [0.3.0] - 2024-04-15

### Added

- The River client now supports "poll only" mode with `Config.PollOnly` which makes it avoid issuing `LISTEN` statements to wait for new events like a leadership resignation or new job available. The program instead polls periodically to look for changes. A leader resigning or a new job being available will be noticed less quickly, but `PollOnly` potentially makes River operable on systems without listen/notify support, like PgBouncer operating in transaction pooling mode. [PR #281](https://github.com/riverqueue/river/pull/281).
- Added `rivertype.JobStates()` that returns the full list of possible job states. [PR #297](https://github.com/riverqueue/river/pull/297).

## [0.2.0] - 2024-03-28

### Added

- New periodic jobs can now be added after a client's already started using `Client.PeriodicJobs().Add()` and removed with `Remove()`. [PR #288](https://github.com/riverqueue/river/pull/288).

### Changed

- The level of some of River's common log statements has changed, most often demoting `info` statements to `debug` so that `info`-level logging is overall less verbose. [PR #275](https://github.com/riverqueue/river/pull/275).

### Fixed

- Fixed a bug in the (log-only for now) reindexer service in which it might repeat its work loop multiple times unexpectedly while stopping. [PR #280](https://github.com/riverqueue/river/pull/280).
- Periodic job enqueuer now bases next run times on each periodic job's last target run time, instead of the time at which the enqueuer is currently running. This is a small difference that will be unnoticeable for most purposes, but makes scheduling of jobs with short cron frequencies a little more accurate. [PR #284](https://github.com/riverqueue/river/pull/284).
- Fixed a bug in the elector in which it was possible for a resigning, but not completely stopped, elector to reelect despite having just resigned. [PR #286](https://github.com/riverqueue/river/pull/286).

## [0.1.0] - 2024-03-17

Although it comes with a number of improvements, there's nothing particularly notable about version 0.1.0. Until now we've only been incrementing the patch version given the project's nascent nature, but from here on we'll try to adhere more closely to semantic versioning, using the patch version for bug fixes, and incrementing the minor version when new functionality is added.

### Added

- The River CLI now supports `river bench` to benchmark River's job throughput against a database. [PR #254](https://github.com/riverqueue/river/pull/254).
- The River CLI now has a `river migrate-get` command to dump SQL for River migrations for use in alternative migration frameworks. Use it like `river migrate-get --up --version 3 > version3.up.sql`. [PR #273](https://github.com/riverqueue/river/pull/273).
- The River CLI's `migrate-down` and `migrate-up` options get two new options for `--dry-run` and `--show-sql`. They can be combined to easily run a preflight check on a River upgrade to see which migration commands would be run on a database, but without actually running them. [PR #273](https://github.com/riverqueue/river/pull/273).
- The River client gets a new `Client.SubscribeConfig` function that lets a subscriber specify the maximum size of their subscription channel. [PR #258](https://github.com/riverqueue/river/pull/258).

### Changed

- River uses a new job completer that batches up completion work so that large numbers of them can be performed more efficiently. In a purely synthetic (i.e. mostly unrealistic) benchmark, River's job throughput increases ~4.5x. [PR #258](https://github.com/riverqueue/river/pull/258).
- Changed default client IDs to be a combination of hostname and the time which the client started. This can still be changed by specifying `Config.ID`. [PR #255](https://github.com/riverqueue/river/pull/255).
- Notifier refactored for better robustness and testability. [PR #253](https://github.com/riverqueue/river/pull/253).

## [0.0.25] - 2024-03-01

### Fixed

- Fixed a problem in `riverpgxv5`'s `Listener` where it wouldn't unset an internal connection if `Close` returned an error, making the listener not reusable. Thanks @mfrister for pointing this one out! [PR #246](https://github.com/riverqueue/river/pull/246).

## [0.0.24] - 2024-02-29

### Fixed

- Fixed a memory leak caused by not always cancelling the context used to enable jobs to be cancelled remotely. [PR #243](https://github.com/riverqueue/river/pull/243).

## [0.0.23] - 2024-02-29

### Added

- `JobListParams.Kinds()` has been added so that jobs can now be listed by kind. [PR #212](https://github.com/riverqueue/river/pull/212).

### Changed

- The underlying driver system's been entirely revamped so that River's non-test code is now decoupled from `pgx/v5`. This will allow additional drivers to be implemented, although there are no additional ones for now. [PR #212](https://github.com/riverqueue/river/pull/212).

### Fixed

- Fixed a memory leak caused by allocating a new random source on every job execution. Thank you @shawnstephens for reporting ❤️ [PR #240](https://github.com/riverqueue/river/pull/240).
- Fix a problem where `JobListParams.Queues()` didn't filter correctly based on its arguments. [PR #212](https://github.com/riverqueue/river/pull/212).
- Fix a problem in `DebouncedChan` where it would fire on its "out" channel too often when it was being signaled continuousy on its "in" channel. This would have caused work to be fetched more often than intended in busy systems. [PR #222](https://github.com/riverqueue/river/pull/222).

## [0.0.22] - 2024-02-19

### Fixed

- Brings in another leadership election fix similar to #217 in which a TTL equal to the elector's run interval plus a configured TTL padding is also used for the initial attempt to gain leadership (#217 brought it in for reelection only). [PR #219](https://github.com/riverqueue/river/pull/219).

## [0.0.21] - 2024-02-19

### Changed

- Tweaked behavior of `JobRetry` so that it does actually update the `ScheduledAt` time of the job in all cases where the job is actually being rescheduled. As before, jobs which are already available with a past `ScheduledAt` will not be touched by this query so that they retain their place in line. [PR #211](https://github.com/riverqueue/river/pull/211).

### Fixed

- Fixed a leadership re-election issue that was exposed by the fix in #199. Because we were internally using the same TTL for both an internal timer/ticker and the database update to set the new leader expiration time, a leader wasn't guaranteed to successfully re-elect itself even under normal operation. [PR #217](https://github.com/riverqueue/river/pull/217).

## [0.0.20] - 2024-02-14

### Added

- Added an `ID` setting to the `Client` `Config` type to allow users to override client IDs with their own naming convention. Expose the client ID programatically (in case it's generated) in a new `Client.ID()` method. [PR #206](https://github.com/riverqueue/river/pull/206).

### Fixed

- Fix a leadership re-election query bug that would cause past leaders to think they were continuing to win elections. [PR #199](https://github.com/riverqueue/river/pull/199).

## [0.0.19] - 2024-02-10

### Added

- Added `JobGet` and `JobGetTx` to the `Client` to enable easily fetching a single job row from code for introspection. [PR #186].
- Added `JobRetry` and `JobRetryTx` to the `Client` to enable a job to be retried immediately, even if it has already completed, been cancelled, or been discarded. [PR #190].

### Changed

- Validate queue name on job insertion. Allow queue names with hyphen separators in addition to underscore. [PR #184](https://github.com/riverqueue/river/pull/184).

## [0.0.18] - 2024-01-25

### Fixed

- Remove a debug statement from periodic job enqueuer that was accidentally left in. [PR #176](https://github.com/riverqueue/river/pull/176).

## [0.0.17] - 2024-01-22

### Added

- Added `JobCancel` and `JobCancelTx` to the `Client` to enable cancellation of jobs. [PR #141](https://github.com/riverqueue/river/pull/141) and [PR #152](https://github.com/riverqueue/river/pull/152).
- Added `ClientFromContext` and `ClientWithContextSafely` helpers to extract the `Client` from the worker's context where it is now available to workers. This simplifies making the River client available within your workers for i.e. enqueueing additional jobs. [PR #145](https://github.com/riverqueue/river/pull/145).
- Add `JobList` API for listing jobs. [PR #117](https://github.com/riverqueue/river/pull/117).
- Added `river validate` command which fails with a non-zero exit code unless all migrations are applied. [PR #170](https://github.com/riverqueue/river/pull/170).

### Changed

- For short `JobSnooze` times (smaller than the scheduler's run interval) put the job straight into an `available` state with the specified `scheduled_at` time. This avoids an artificially long delay waiting for the next scheduler run. [PR #162](https://github.com/riverqueue/river/pull/162).

### Fixed

- Fixed incorrect default value handling for `ScheduledAt` option with `InsertMany` / `InsertManyTx`. [PR #149](https://github.com/riverqueue/river/pull/149).
- Add missing `t.Helper()` calls in `rivertest` internal functions that caused it to report itself as the site of a test failure. [PR #151](https://github.com/riverqueue/river/pull/151).
- Fixed problem where job uniqueness wasn't being respected when used in conjuction with periodic jobs. [PR #168](https://github.com/riverqueue/river/pull/168).

## [0.0.16] - 2024-01-06

### Changed

- Calls to `Stop` error if the client hasn't been started yet. [PR #138](https://github.com/riverqueue/river/pull/138).

### Fixed

- Fix typo in leadership resignation query to ensure faster new leader takeover. [PR #134](https://github.com/riverqueue/river/pull/134).
- Elector now uses the same `log/slog` instance configured by its parent client. [PR #137](https://github.com/riverqueue/river/pull/137).
- Notifier now uses the same `log/slog` instance configured by its parent client. [PR #140](https://github.com/riverqueue/river/pull/140).

## [0.0.15] - 2023-12-21

### Fixed

- Ensure `ScheduledAt` is respected on `InsertManyTx`. [PR #121](https://github.com/riverqueue/river/pull/121).

## [0.0.14] - 2023-12-13

### Fixed

- River CLI `go.sum` entries fixed for 0.0.13 release.

## [0.0.13] - 2023-12-12

### Added

- Added `riverdriver/riverdatabasesql` driver to enable River Go migrations through Go's built in `database/sql` package. [PR #98](https://github.com/riverqueue/river/pull/98).

### Changed

- Errored jobs that have a very short duration before their next retry (<5 seconds) are set to `available` immediately instead of being made `scheduled` and having to wait for the scheduler to make a pass to make them workable. [PR #105](https://github.com/riverqueue/river/pull/105).
- `riverdriver` becomes its own submodule. It contains types that `riverdriver/riverdatabasesql` and `riverdriver/riverpgxv5` need to reference. [PR #98](https://github.com/riverqueue/river/pull/98).
- The `river/cmd/river` CLI has been made its own Go module. This is possible now that it uses the exported `river/rivermigrate` API, and will help with project maintainability. [PR #107](https://github.com/riverqueue/river/pull/107).

## [0.0.12] - 2023-12-02

### Added

- Added `river/rivermigrate` package to enable migrations from Go code as an alternative to using the CLI. PR #67.

## [0.0.11] - 2023-12-02

### Added

- `Stop` and `StopAndCancel` have been changed to respect the provided context argument. When that context is cancelled or times out, those methods will now immediately return with the context's error, even if the Client's shutdown has not yet completed. Apps may need to adjust their graceful shutdown logic to account for this. PR #79.

### Changed

- `NewClient` no longer errors if it was provided a workers bundle with zero workers. Instead, that check's been moved to `Client.Start` instead. This allows adding workers to a bundle that'd like to reference a River client by letting `AddWorker` be invoked after a client reference is available from `NewClient`. PR #87.

## [0.0.10] - 2023-11-26

### Added

- Added `Example_scheduledJob`, demonstrating how to schedule a job to be run in the future.
- Added `Stopped` method to `Client` to make it easier to wait for graceful shutdown to complete.

### Fixed

- Fixed a panic in the periodic job enqueuer caused by sometimes trying to reset a `time.Ticker` with a negative or zero duration. Fixed in PR #73.

### Changed

- `DefaultClientRetryPolicy`: calculate the next attempt based on the current time instead of the time the prior attempt began.

## [0.0.9] - 2023-11-23

### Fixed

- **DATABASE MIGRATION**: Database schema v3 was introduced in v0.0.8 and contained an obvious flaw preventing it from running against existing tables. This migration was altered to execute the migration in multiple steps.

## [0.0.8] - 2023-11-21

### Changed

- License changed from LGPLv3 to MPL-2.0.
- **DATABASE MIGRATION**: Database schema v3, alter river_job tags column to set a default of `[]` and add not null constraint.

## [0.0.7] - 2023-11-20

### Changed

- Constants renamed so that adjectives like `Default` and `Min` become suffixes instead of prefixes. So for example, `DefaultFetchCooldown` becomes `FetchCooldownDefault`.
- Rename `AttemptError.Num` to `AttemptError.Attempt` to better fit with the name of `JobRow.Attempt`.
- Document `JobState`, `AttemptError`, and all fields its fields.
- A `NULL` tags value read from a database job is left as `[]string(nil)` on `JobRow.Tags` rather than a zero-element slice of `[]string{}`. `append` and `len` both work on a `nil` slice, so this should be functionally identical.

## [0.0.6] - 2023-11-19

### Changed

- `JobRow`, `JobState`, and a other related types move into `river/rivertype` so they can more easily be shared amongst packages. Most of the River API doesn't change because `JobRow` is embedded on `river.Job`, which doesn't move.

## [0.0.5] - 2023-11-19

### Changed

- Remove `replace` directive from the project's `go.mod` so that it's possible to install River CLI with `@latest`.

## [0.0.4] - 2023-11-17

### Changed

- Allow River clients to be created with a driver with `nil` database pool for use in testing.
- Update River test helpers API to use River drivers like `riverdriver/riverpgxv5` to make them agnostic to the third party database package in use.
- Document `Config.JobTimeout`'s default value.
- Functionally disable the `Reindexer` queue maintenance service. It'd previously only operated on currently unused indexes anyway, indexes probably do _not_ need to be rebuilt except under fairly rare circumstances, and it needs more work to make sure it's shored up against edge cases like indexes that fail to rebuild before a clien restart.

## [0.0.3] - 2023-11-13

### Changed

- Fix license detection issues with `riverdriver/riverpgxv5` submodule.
- Ensure that river requires the `riverpgxv5` module with the same version.

## [0.0.2] - 2023-11-13

### Changed

- Pin own `riverpgxv5` dependency to v0.0.1 and make it a direct locally-replaced dependency. This should allow projects to import versioned deps of both river and `riverpgxv5`.

## [0.0.1] - 2023-11-12

### Added

- This is the initial prerelease of River.
