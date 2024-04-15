# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- **Breaking change:** JobList/JobListTx now support querying Jobs by a list of Job Kinds and States. Also allows for filtering by specific timestamp values. Thank you Jos Kraaijeveld (@thatjos)! 🙏🏻 [PR #236](https://github.com/riverqueue/river/pull/236).
- **Breaking change:** `river.JobState*` type aliases have been removed. All job state constants should be accessed through `rivertype.JobState*` instead. [PR #300](https://github.com/riverqueue/river/pull/300).

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
