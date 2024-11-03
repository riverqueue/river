# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.14.0] - 2024-11-03

### Added

- Expose `JobCancelError` and `JobSnoozeError` types to more easily facilitate testing. [PR #665](https://github.com/riverqueue/river/pull/665).

### Changed

- Tune the client to be more aggressive about fetching when it just fetched a full batch of jobs, or when it skipped its previous triggered fetch because it was already full. This should bring more consistent throughput to poll-only mode and in cases where there is a backlog of existing jobs but new ones aren't being actively inserted. This will result in increased fetch load on many installations, with the benefit of increased throughput. As before, `FetchCooldown` still limits how frequently these fetches can occur on each client and can be increased to reduce the amount of fetch querying. Thanks Chris Gaffney ([@gaffneyc](https://github.com/gaffneyc)) for the idea, initial implementation, and benchmarks. [PR #663](https://github.com/riverqueue/river/pull/663).

### Fixed

- `riverpgxv5` driver: `Hijack()` the underlying listener connection as soon as it is acquired from the `pgxpool.Pool` in order to prevent the pool from automatically closing it after it reaches its max age. A max lifetime makes sense in the context of a pool with many conns, but a long-lived listener does not need a max lifetime as long as it can ensure the conn remains healthy. [PR #661](https://github.com/riverqueue/river/pull/661).

## [0.13.0] - 2024-10-07

⚠️ Version 0.13.0 removes the original advisory lock based unique jobs implementation that was deprecated in v0.12.0. See details in the note below or the v0.12.0 release notes.

### Added

- A middleware system was added for job insertion and execution, providing the ability to extract shared functionality across workers. Both `JobInsertMiddleware` and `WorkerMiddleware` can be configured globally on the `Client`, and `WorkerMiddleware` can also be added on a per-worker basis using the new `Middleware` method on `Worker[T]`. Middleware can be useful for logging, telemetry, or for building higher level abstractions on top of base River functionality.

  Despite the interface expansion, users should not encounter any breakage if they're embedding the `WorkerDefaults` type in their workers as recommended. [PR #632](https://github.com/riverqueue/river/pull/632).

### Changed

- **Breaking change:** The advisory lock unique jobs implementation which was deprecated in v0.12.0 has been removed. Users of that feature should first upgrade to v0.12.1 to ensure they don't see any warning logs about using the deprecated advisory lock uniqueness. The new, faster unique implementation will be used automatically as long as the `UniqueOpts.ByState` list hasn't been customized to remove [required states](https://riverqueue.com/docs/unique-jobs#unique-by-state) (`pending`, `scheduled`, `available`, and `running`). As of this release, customizing `ByState` without these required states returns an error. [PR #614](https://github.com/riverqueue/river/pull/614).
- Single job inserts are now unified under the hood to use the `InsertMany` bulk insert query. This should not be noticeable to users, and the unified code path will make it easier to build new features going forward. [PR #614](https://github.com/riverqueue/river/pull/614).

### Fixed

- Allow `river.JobCancel` to accept a `nil` error as input without panicking. [PR #634](https://github.com/riverqueue/river/pull/634).

## [0.12.1] - 2024-09-26

### Changed

- The `BatchCompleter` that marks jobs as completed can now batch database updates for _all_ states of jobs that have finished execution. Prior to this change, only `completed` jobs were batched into a single `UPDATE` call, while jobs moving to any other state used a single `UPDATE` per job. This change should significantly reduce database and pool contention on high volume system when jobs get retried, snoozed, cancelled, or discarded following execution. [PR #617](https://github.com/riverqueue/river/pull/617).

### Fixed

- Unique job changes from v0.12.0 / [PR #590](https://github.com/riverqueue/river/pull/590) introduced a bug with scheduled or retryable unique jobs where they could be considered in conflict with themselves and moved to `discarded` by mistake. There was also a possibility of a broken job scheduler if duplicate `retryable` unique jobs were attempted to be scheduled at the same time. The job scheduling query was corrected to address these issues along with missing test coverage. [PR #619](https://github.com/riverqueue/river/pull/619).

## [0.12.0] - 2024-09-23

⚠️ Version 0.12.0 contains a new database migration, version 6. See [documentation on running River migrations](https://riverqueue.com/docs/migrations). If migrating with the CLI, make sure to update it to its latest version:

```shell
go install github.com/riverqueue/river/cmd/river@latest
river migrate-up --database-url "$DATABASE_URL"
```

If not using River's internal migration system, the raw SQL can alternatively be dumped with:

```shell
go install github.com/riverqueue/river/cmd/river@latest
river migrate-get --version 6 --up > river6.up.sql
river migrate-get --version 6 --down > river6.down.sql
```

The migration **includes a new index**. Users with a very large job table may want to consider raising the index separately using `CONCURRENTLY` (which must be run outside of a transaction), then run `river migrate-up` to finalize the process (it will tolerate an index that already exists):

```sql
ALTER TABLE river_job ADD COLUMN unique_states BIT(8);

CREATE UNIQUE INDEX CONCURRENTLY river_job_unique_idx ON river_job (unique_key)
    WHERE unique_key IS NOT NULL
      AND unique_states IS NOT NULL
      AND river_job_state_in_bitmask(unique_states, state);
```

```shell
go install github.com/riverqueue/river/cmd/river@latest
river migrate-up --database-url "$DATABASE_URL"
```

## Added

- `rivertest.WorkContext`, a test function that can be used to initialize a context to test a `JobArgs.Work` implementation that will have a client set to context for use with `river.ClientFromContext`. [PR #526](https://github.com/riverqueue/river/pull/526).
- A new `river migrate-list` command is available which lists available migrations and which version a target database is migrated to. [PR #534](https://github.com/riverqueue/river/pull/534).
- `river version` or `river --version` now prints River version information. [PR #537](https://github.com/riverqueue/river/pull/537).
- `Config.JobCleanerTimeout` was added to allow configuration of the job cleaner query timeout. In some deployments with millions of stale jobs, the cleaner may not be able to complete its query within the default 30 seconds. [PR #576](https://github.com/riverqueue/river/pull/576).

### Changed

⚠️ Version 0.12.0 has two small breaking changes, one for `InsertMany` and one in `rivermigrate`. As before, we try never to make breaking changes, but these ones were deemed worth it because of minimal impact and to help avoid panics.

- **Breaking change:** `Client.InsertMany` / `InsertManyTx` now return the inserted rows rather than merely returning a count of the inserted rows. The new implementations no longer use Postgres' `COPY FROM` protocol in order to facilitate return values.

  Users who relied on the return count can merely wrap the returned rows in a `len()` to return to that behavior, or you can continue using the old APIs using their new names `InsertManyFast` and `InsertManyFastTx`. [PR #589](https://github.com/riverqueue/river/pull/589).

- **Breaking change:** `rivermigrate.New` now returns a possible error along with a migrator. An error may be returned, for example, when a migration line is configured that doesn't exist. [PR #558](https://github.com/riverqueue/river/pull/558).

  ```go
  # before
  migrator := rivermigrate.New(riverpgxv5.New(dbPool), nil)

  # after
  migrator, err := rivermigrate.New(riverpgxv5.New(dbPool), nil)
  if err != nil {
      // handle error
  }
  ```

- Unique jobs have been improved to allow bulk insertion of unique jobs via `InsertMany` / `InsertManyTx`, and to allow customizing the `ByState` list to add or remove certain states. This enables users to expand the set of unique states to also include `cancelled` and `discarded` jobs, or to remove `retryable` from uniqueness consideration. This updated implementation maintains the speed advantage of the newer index-backed uniqueness system, while allowing some flexibility in which job states.

  Unique jobs utilizing `ByArgs` can now also opt to have a subset of the job's arguments considered for uniqueness. For example, you could choose to consider only the `customer_id` field while ignoring the `trace_id` field:

  ```go
  type MyJobArgs {
    CustomerID string `json:"customer_id" river:"unique`
    TraceID string `json:"trace_id"`
  }
  ```

  Any fields considered in uniqueness are also sorted alphabetically in order to guarantee a consistent result, even if the encoded JSON isn't sorted consistently. For example `encoding/json` encodes struct fields in their defined order, so merely reordering struct fields would previously have been enough to cause a new job to not be considered identical to a pre-existing one with different JSON order.

  The `UniqueOpts` type also gains an `ExcludeKind` option for cases where uniqueness needs to be guaranteed across multiple job types.

  In-flight unique jobs using the previous designs will continue to be executed successfully with these changes, so there should be no need for downtime as part of the migration. However the v6 migration adds a new unique job index while also removing the old one, so users with in-flight unique jobs may also wish to avoid removing the old index until the new River release has been deployed in order to guarantee that jobs aren't duplicated by old River code once that index is removed.

  **Deprecated**: The original unique jobs implementation which relied on advisory locks has been deprecated, but not yet removed. The only way to trigger this old code path is with a single insert (`Insert`/`InsertTx`) and using `UniqueOpts.ByState` with a custom list of states that omits some of the now-required states for unique jobs. Specifically, `pending`, `scheduled`, `available`, and `running` can not be removed from the `ByState` list with the new implementation. These are included in the default list so only the places which customize this attribute need to be updated to opt into the new (much faster) unique jobs. The advisory lock unique implementation will be removed in an upcoming release, and until then emits warning level logs when it's used.

  [PR #590](https://github.com/riverqueue/river/pull/590).

- **Deprecated**: The `MigrateTx` method of `rivermigrate` has been deprecated. It turns out there are certain combinations of schema changes which cannot be run within a single transaction, and the migrator now prefers to run each migration in its own transaction, one-at-a-time. `MigrateTx` will be removed in future version.

- The migrator now produces a better error in case of a non-existent migration line including suggestions for known migration lines that are similar in name to the invalid one. [PR #558](https://github.com/riverqueue/river/pull/558).

## Fixed

- Fixed a panic that'd occur if `StopAndCancel` was invoked before a client was started. [PR #557](https://github.com/riverqueue/river/pull/557).
- A `PeriodicJobConstructor` should be able to return `nil` `JobArgs` if it wishes to not have any job inserted. However, this was either never working or was broken at some point. It's now fixed. Thanks [@semanser](https://github.com/semanser)! [PR #572](https://github.com/riverqueue/river/pull/572).
- Fixed a nil pointer exception if `Client.Subscribe` was called when the client had no configured workers (it still, panics with a more instructive error message now). [PR #599](https://github.com/riverqueue/river/pull/599).

## [0.11.4] - 2024-08-20

### Fixed

- Fixed release script that caused CLI to become uninstallable because its reference to `rivershared` wasn't updated. [PR #541](https://github.com/riverqueue/river/pull/541).

## [0.11.3] - 2024-08-19

### Changed

- Producer's logs are quieter unless jobs are actively being worked. [PR #529](https://github.com/riverqueue/river/pull/529).

### Fixed

- River CLI now accepts `postgresql://` URL schemes in addition to `postgres://`. [PR #532](https://github.com/riverqueue/river/pull/532).

## [0.11.2] - 2024-08-08

### Fixed

- Derive all internal contexts from user-provided `Client` context. This includes the job fetch context, notifier unlisten, and completer. [PR #514](https://github.com/riverqueue/river/pull/514).
- Lowered the `go` directives in `go.mod` to Go 1.21, which River aims to support. A more modern version of Go is specified with the `toolchain` directive. This should provide more flexibility on the minimum required Go version for programs importing River. [PR #522](https://github.com/riverqueue/river/pull/522).

## [0.11.1] - 2024-08-05

### Fixed

- `database/sql` driver: fix default value of `scheduled_at` for `InsertManyTx` when it is not specified in `InsertOpts`. [PR #504](https://github.com/riverqueue/river/pull/504).
- Change `ColumnExists` query to respect `search_path`, thereby allowing migrations to be runnable outside of default schema. [PR #505](https://github.com/riverqueue/river/pull/505).

## [0.11.0] - 2024-08-02

### Added

- Expose `Driver` on `Client` for additional River Pro integrations. This is not a stable API and should generally not be used by others. [PR #497](https://github.com/riverqueue/river/pull/497).

## [0.10.2] - 2024-07-31

### Fixed

- Include `pending` state in `JobListParams` by default so pending jobs are included in `JobList` / `JobListTx` results. [PR #477](https://github.com/riverqueue/river/pull/477).
- Quote strings when using `Client.JobList` functions with the `database/sql` driver. [PR #481](https://github.com/riverqueue/river/pull/481).
- Remove use of `filepath` for interacting with embedded migration files, fixing the migration CLI for Windows. [PR #485](https://github.com/riverqueue/river/pull/485).
- Respect `ScheduledAt` if set to a non-zero value by `JobArgsWithInsertOpts`. This allows for job arg definitions to utilize custom logic at the args level for determining when the job should be scheduled. [PR #487](https://github.com/riverqueue/river/pull/487).

## [0.10.1] - 2024-07-23

### Fixed

- Migration version 005 has been altered so that it can run even if the `river_migration` table isn't present, making it more friendly for projects that aren't using River's internal migration system. [PR #465](https://github.com/riverqueue/river/pull/465).

## [0.10.0] - 2024-07-19

⚠️ Version 0.10.0 contains a new database migration, version 5. See [documentation on running River migrations](https://riverqueue.com/docs/migrations). If migrating with the CLI, make sure to update it to its latest version:

```shell
go install github.com/riverqueue/river/cmd/river@latest
river migrate-up --database-url "$DATABASE_URL"
```

If not using River's internal migration system, the raw SQL can alternatively be dumped with:

```shell
go install github.com/riverqueue/river/cmd/river@latest
river migrate-get --version 5 --up > river5.up.sql
river migrate-get --version 5 --down > river5.down.sql
```

The migration **includes a new index**. Users with a very large job table may want to consider raising the index separately using `CONCURRENTLY` (which must be run outside of a transaction), then run `river migrate-up` to finalize the process (it will tolerate an index that already exists):

```sql
ALTER TABLE river_job
    ADD COLUMN unique_key bytea;

CREATE UNIQUE INDEX CONCURRENTLY river_job_kind_unique_key_idx ON river_job (kind, unique_key) WHERE unique_key IS NOT NULL;
```

```shell
go install github.com/riverqueue/river/cmd/river@latest
river migrate-up --database-url "$DATABASE_URL"
```

### Added

- Fully functional driver for `database/sql` for use with packages like Bun and GORM. [PR #351](https://github.com/riverqueue/river/pull/351).
- Queues can be added after a client is initialized using `client.Queues().Add(queueName string, queueConfig QueueConfig)`. [PR #410](https://github.com/riverqueue/river/pull/410).
- Migration that adds a `line` column to the `river_migration` table so that it can support multiple migration lines. [PR #435](https://github.com/riverqueue/river/pull/435).
- `--line` flag added to the River CLI. [PR #454](https://github.com/riverqueue/river/pull/454).

### Changed

- Tags are now limited to 255 characters in length, and should match the regex `\A[\w][\w\-]+[\w]\z` (importantly, they can't contain commas). [PR #351](https://github.com/riverqueue/river/pull/351).
- Many info logging statements have been demoted to debug level. [PR #452](https://github.com/riverqueue/river/pull/452).
- `pending` is now part of the default set of unique job states. [PR #461](https://github.com/riverqueue/river/pull/461).

## [0.9.0] - 2024-07-04

### Added

- `Config.TestOnly` has been added. It disables various features in the River client like staggered maintenance service start that are useful in production, but may be somewhat harmful in tests because they make start/stop slower. [PR #414](https://github.com/riverqueue/river/pull/414).

### Changed

⚠️ Version 0.9.0 has a small breaking change in `ErrorHandler`. As before, we try never to make breaking changes, but this one was deemed quite important because `ErrorHandler` was fundamentally lacking important functionality.

- **Breaking change:** Add stack trace to `ErrorHandler.HandlePanicFunc`. Fixing code only requires adding a new `trace string` argument to `HandlePanicFunc`. [PR #423](https://github.com/riverqueue/river/pull/423).

  ```go
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
  - Job listing now defaults to ordering by job ID (`JobListOrderByID`) instead of a job timestamp dependent on requested job state. The previous ordering behavior is still available with `NewJobListParams().OrderBy(JobListOrderByTime, SortOrderAsc)`. [PR #307](https://github.com/riverqueue/river/pull/307).
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
- Fix a problem in `DebouncedChan` where it would fire on its "out" channel too often when it was being signaled continuously on its "in" channel. This would have caused work to be fetched more often than intended in busy systems. [PR #222](https://github.com/riverqueue/river/pull/222).

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

- Added an `ID` setting to the `Client` `Config` type to allow users to override client IDs with their own naming convention. Expose the client ID programmatically (in case it's generated) in a new `Client.ID()` method. [PR #206](https://github.com/riverqueue/river/pull/206).

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
- Added `ClientFromContext` and `ClientFromContextSafely` helpers to extract the `Client` from the worker's context where it is now available to workers. This simplifies making the River client available within your workers for i.e. enqueueing additional jobs. [PR #145](https://github.com/riverqueue/river/pull/145).
- Add `JobList` API for listing jobs. [PR #117](https://github.com/riverqueue/river/pull/117).
- Added `river validate` command which fails with a non-zero exit code unless all migrations are applied. [PR #170](https://github.com/riverqueue/river/pull/170).

### Changed

- For short `JobSnooze` times (smaller than the scheduler's run interval) put the job straight into an `available` state with the specified `scheduled_at` time. This avoids an artificially long delay waiting for the next scheduler run. [PR #162](https://github.com/riverqueue/river/pull/162).

### Fixed

- Fixed incorrect default value handling for `ScheduledAt` option with `InsertMany` / `InsertManyTx`. [PR #149](https://github.com/riverqueue/river/pull/149).
- Add missing `t.Helper()` calls in `rivertest` internal functions that caused it to report itself as the site of a test failure. [PR #151](https://github.com/riverqueue/river/pull/151).
- Fixed problem where job uniqueness wasn't being respected when used in conjunction with periodic jobs. [PR #168](https://github.com/riverqueue/river/pull/168).

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

- `JobRow`, `JobState`, and other related types move into `river/rivertype` so they can more easily be shared amongst packages. Most of the River API doesn't change because `JobRow` is embedded on `river.Job`, which doesn't move.

## [0.0.5] - 2023-11-19

### Changed

- Remove `replace` directive from the project's `go.mod` so that it's possible to install River CLI with `@latest`.

## [0.0.4] - 2023-11-17

### Changed

- Allow River clients to be created with a driver with `nil` database pool for use in testing.
- Update River test helpers API to use River drivers like `riverdriver/riverpgxv5` to make them agnostic to the third party database package in use.
- Document `Config.JobTimeout`'s default value.
- Functionally disable the `Reindexer` queue maintenance service. It'd previously only operated on currently unused indexes anyway, indexes probably do _not_ need to be rebuilt except under fairly rare circumstances, and it needs more work to make sure it's shored up against edge cases like indexes that fail to rebuild before a client restart.

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
