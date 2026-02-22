# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Changed

- `riverlog.Middleware` now supports `MiddlewareConfig.MaxTotalBytes` (default 8 MB) to cap total persisted `river:log` history per job. When the cap is exceeded, oldest log entries are dropped first while retaining the newest entry. Values over 64 MB are clamped to 64 MB. [PR #1157](https://github.com/riverqueue/river/pull/1157).
- Improved `riverlog` performance and reduced memory amplification when appending to large persisted `river:log` histories. [PR #1157](https://github.com/riverqueue/river/pull/1157).
- Reduced snooze-path memory amplification by setting `snoozes` in metadata updates before marshaling, avoiding an extra full-payload JSON rewrite. [PR #1159](https://github.com/riverqueue/river/pull/1159).

### Fixed

- `riverpgxv5` now adapts JSON parameters for `simple protocol` / `exec` query modes so `[]byte` JSON payloads are not encoded as `bytea` in pgx text-mode execution paths. This fixes invalid JSON syntax errors when running through protocol-constrained setups like PgBouncer transaction pooling while preserving normal behavior for explicit `bytea` parameters. Fixes [#1153](https://github.com/riverqueue/river/issues/1153). [PR #1155](https://github.com/riverqueue/river/pull/1155).

## [0.31.0] - 2026-02-21

### Added

- Added root River CLI flag `--statement-timeout` so Postgres session statement timeout can be set explicitly for commands like migrations. Explicit flag values take priority over database URL query params, and query params still take priority over built-in defaults. [PR #1142](https://github.com/riverqueue/river/pull/1142).

### Fixed

- Fix connection leak in `Listener.Connect` in case where `afterConnectExec` failed. Thanks Johan Kjölhede ([@GiGurra](https://github.com/GiGurra))! [PR #1147](https://github.com/riverqueue/river/pull/1147).
- Fix missing `ticker.Stop` in producer's `pollForSettingChanges` ([@GiGurra](https://github.com/GiGurra)). [PR #1148](https://github.com/riverqueue/river/pull/1148).
- Fix accidental use of cancelled context for `Notifier.Ping` ([@GiGurra](https://github.com/GiGurra)). [PR #1149](https://github.com/riverqueue/river/pull/1149).
- Add jitter to fetch poll loop to prevent producer stampeding ([@GiGurra](https://github.com/GiGurra)). [PR #1150](https://github.com/riverqueue/river/pull/1150).

### Changed

- Upgrade supported Go versions to 1.25 and 1.26, and update CI accordingly. [PR #1144](https://github.com/riverqueue/river/pull/1144).

### Fixed

- `JobCountByQueueAndState` now returns consistent results across drivers, including requested queues with zero jobs, and deduplicates repeated queue names in input. This resolves an issue with the sqlite driver in River UI reported in [riverqueue/riverui#496](https://github.com/riverqueue/riverui#496). [PR #1140](https://github.com/riverqueue/river/pull/1140).

## [0.30.2] - 2026-01-26

### Fixed

- Fix bug in worker-level stuck job detection. [PR #1133](https://github.com/riverqueue/river/pull/1133).

## [0.30.1] - 2026-01-19

### Fixed

- Stuck job detection now accounts for worker-level timeouts as well as client-level timeouts. [PR #1125](https://github.com/riverqueue/river/pull/1125).

## [0.30.0] - 2026-01-11

### Fixed

- Fix possible nil pointer panic when using nil `opts` in `Migrator.MigrateTx`. [PR #1117](https://github.com/riverqueue/river/pull/1117).

## [0.29.0] - 2025-12-22

### Added

- Added `HookPeriodicJobsStart` that can be used to run custom logic when a periodic job enqueuer starts up on a new leader. [PR #1084](https://github.com/riverqueue/river/pull/1084).
- Added `Client.Notify().RequestResign` and `Client.Notify().RequestResignTx` functions allowing any client to request that the current leader resign. [PR #1085](https://github.com/riverqueue/river/pull/1085).
- Basic stuck detection after a job's exceeded its timeout and still not returned after the executor's initiated context cancellation and waited a short margin for the cancellation to take effect. [PR #1097](https://github.com/riverqueue/river/pull/1097).
- Added `Client.JobUpdate` which can be used to persist job output partway through a running work function instead of having to wait until the job is completed. [PR #1098](https://github.com/riverqueue/river/pull/1098).

### Changed

- Add a little more error flavor for when encountering a deadline exceeded error on leadership election suggesting that the user may want to try increasing their database pool size. [PR #1101](https://github.com/riverqueue/river/pull/1101).
- When migrating without an outer transaction, insert/delete version rows immediately after executing migration SQL so that in case a later migration fails, the migrator knows where to restart from. [PR #1106](https://github.com/riverqueue/river/pull/1106).

## [0.28.0] - 2025-11-23

### Added

- Added `riverlog.LoggerSafely` which provides a non-panic variant of `riverlog.Logger` for use when code may or may not have a context logger available. [PR #1093](https://github.com/riverqueue/river/pull/1093).

## [0.27.0] - 2025-11-14

### Added

- Periodic jobs with IDs may now be removed by ID using the new `PeriodicJobBundle.RemoveByID` and `PeriodicJobBundle.RemoveManyByID`. [PR #1071](https://github.com/riverqueue/river/pull/1071).

### Changed

- Decrease `serviceutil.MaxAttemptsBeforeResetDefault` from 10 to 7, lowering the effective limit on most internal exponential backoffs from ~512 seconds to 64 seconds. Further lowered the leader elector's keep leadership backoff interval to cap out at 4 seconds since leadership without a successful heartbeat will be lost soon after that anyway. [PR #1079](https://github.com/riverqueue/river/pull/1079).

### Fixed

- Fix snoozed events emitted from `rivertest.Worker` when snooze duration is zero seconds. [PR #1057](https://github.com/riverqueue/river/pull/1057).
- Rollbacks now use an uncancelled context so as to not leave transactions in an ambiguous state if a transaction in them fails due to context cancellation. [PR #1062](https://github.com/riverqueue/river/pull/1062).
- Removing periodic jobs with IDs assigned also remove them from ID map. [PR #1070](https://github.com/riverqueue/river/pull/1070).
- Clear periodic jobs also fully clears all those assigned with an ID. [PR #1083](https://github.com/riverqueue/river/pull/1083).
- `river:"unique"` annotations on substructs within `JobArgs` structs are now factored into uniqueness `ByArgs` calculations. [PR #1076](https://github.com/riverqueue/river/pull/1076).
- Stop subservices and embedded `baseservice.Service` on error in the event of a periodic job enqueuer start error. [PR #1081](https://github.com/riverqueue/river/pull/1081).

## [0.26.0] - 2025-10-07

⚠️ Internal APIs used for communication between River and River Pro have changed. If using River Pro, make sure to update River and River Pro to latest at the same time to get compatible versions. River v0.26.0 is compatible with River Pro v0.19.0.

### Added

- The job rescuer now sets `river:rescue_count` with an integer count of how many times the job has been rescued by the `JobRescuer` maintenance process when it's considered stuck. [PR #1047](https://github.com/riverqueue/river/pull/1047).

### Changed

- Errors returned from job workers are now logged in full using a `slog.Any` attribute. Previously, only their error text was logged. [PR #1051](https://github.com/riverqueue/river/pull/1051).

### Fixed

- Set `updated_at` when invoking pilot `PeriodicJobUpsert`. [PR #1045](https://github.com/riverqueue/river/pull/1045).

## [0.25.0] - 2025-09-14

⚠️ Internal APIs used for communication between River and River Pro have changed. If using River Pro, make sure to update River and River Pro to latest at the same time to get compatible versions. River v0.25.0 is compatible with River Pro v0.18.0.

### Changed

- Set minimum Go version to Go 1.24. [PR #1032](https://github.com/riverqueue/river/pull/1032).
- **Breaking change:** `Client.JobDeleteMany` now requires the use of `JobDeleteManyParams.UnsafeAll` to delete all jobs without a filter applied. This is a safety feature to make it more difficult to accidentally delete all non-running jobs. This is a minor breaking change, but on a fairly new feature that's not likely to be used on purpose by very many people yet. [PR #1033](https://github.com/riverqueue/river/pull/1033).

### Fixed

- Don't double log fetch errors. [PR #1025](https://github.com/riverqueue/river/pull/1025).
- When snoozing a job with zero duration so that it's retried immediately, subscription events no longer appear incorrectly with a kind of `rivertype.EventKindJobFailed`. Instead they're assigned `rivertype.EventKindJobSnoozed` just like they would have with a non-zero snooze duration. [PR #1037](https://github.com/riverqueue/river/pull/1037).

## [0.24.0] - 2025-08-16

⚠️ Version 0.24.0 has a breaking change in `HookWorkEnd.WorkEnd` in that a new `JobRow` parameter has been added to the function's signature. Any intergration defining a custom `HookWorkEnd` hook should update its implementation so the hook continues to be called correctly.

⚠️ Internal APIs used for communication between River and River Pro have changed. If using River Pro, make sure to update River and River Pro to latest at the same time to get compatible versions. River v0.24.0 is compatible with River Pro v0.16.0.

### Added

- The project now tests against [libSQL](https://github.com/tursodatabase/libsql), a popular SQLite fork. It's used through the same `riversqlite` driver that SQLite uses. [PR #957](https://github.com/riverqueue/river/pull/957)
- Added `JobDeleteMany` operations that remove many jobs in a single operation according to input criteria. [PR #962](https://github.com/riverqueue/river/pull/962)
- Added `Client.Schema()` method to return a client's configured schema. [PR #983](https://github.com/riverqueue/river/pull/983).
- Integrated riverui queries into the driver system to pave the way for multi-driver UI support. [PR #983](https://github.com/riverqueue/river/pull/983).
- Added `QueueConfig` level `FetchCooldown` and `FetchPollInterval` settings to enable queue-specific job fetch intervals. For example, a queue of high-priority jobs could be checked more often to improve responsiveness, while one with slow or time-insensitive tasks could be checked infrequently to reduce database load. [PR #994](https://github.com/riverqueue/river/pull/994).

### Changed

- Remove unecessary transactions where a single database operation will do. This reduces the number of subtransactions created which can be an operational benefit it many cases. [PR #950](https://github.com/riverqueue/river/pull/950)
- Bring all driver tests into separate package so they don't leak dependencies. This removes dependencies from the top level `river` package that most River installations won't need, thereby reducing the transitive dependency load of most River installations. [PR #955](https://github.com/riverqueue/river/pull/955).
- The reindexer maintenance service now reindexes all `river_job` indexes, including its primary key. This is expected to help in situations where the jobs table has in the past expanded to a very large size (which makes most indexes larger), is now a much more modest size, but has left the indexes in their expanded state. [PR #963](https://github.com/riverqueue/river/pull/963).
- The River CLI now accepts a `--target-version` of 0 with `river migrate-down` to run all down migrations and remove all River tables (previously, -1 was used for this; -1 still works, but now 0 also works). [PR #966](https://github.com/riverqueue/river/pull/966).
- **Breaking change:** The `HookWorkEnd` interface's `WorkEnd` function now receives a `JobRow` parameter in addition to the `error` it received before. Having a `JobRow` to work with is fairly crucial to most functionality that a hook would implement, and its previous omission was entirely an error. [PR #970](https://github.com/riverqueue/river/pull/970).
- Add maximum bound to each job's `attempted_by` array so that in degenerate cases where a job is run many, many times (say it's snoozed hundreds of times), it doesn't grow to unlimited bounds. [PR #974](https://github.com/riverqueue/river/pull/974).
- A logger passed in via `river.Config` now overrides the default test-based logger when using `rivertest.NewWorker`. [PR #980](https://github.com/riverqueue/river/pull/980).
- Cleaner retention periods (`CancelledJobRetentionPeriod`, `CompletedJobRetentionPeriod`, `DiscardedJobRetentionPeriod`) can be configured to -1 to disable them so that the corresponding type of job is retained indefinitely. [PR #990](https://github.com/riverqueue/river/pull/990).
- Jobs inserted from periodic jobs with IDs now have metadata `river:periodic_job_id` set so they can be traced back to the periodic job that inserted them. [PR #992](https://github.com/riverqueue/river/pull/992).
- The unused function `WorkerDefaults.Hooks` has been removed. This is technically a breaking change, but this function was a vestigal refactoring artifact that was never used by anything, so in practice it shouldn't be breaking. [PR #997](https://github.com/riverqueue/river/pull/997).
- Periodic job records are upserted immediately through a pilot when a client is started rather than the first time their associated job would run. This doesn't mean they're run immediately (they'll only run if `RunOnStart` is enabled), but rather just tracked immediately. [PR #998](https://github.com/riverqueue/river/pull/998).
- The job scheduler still schedules jobs in batches of up to 10,000, but when it encounters a series of consecutive timeouts it assumes that the database is in a degraded state and switches to doing work in a smaller batch size of 1,000 jobs. [PR #1013](https://github.com/riverqueue/river/pull/1013).
- Other maintenance services including the job cleaner, job rescuer, and queue cleaner also prefer a batch size of 10,000, but will fall back to smaller batches of 1,000 on consecutive database timeouts. [PR #1016](https://github.com/riverqueue/river/pull/1016).

### Fixed

- Cleanly error on invalid schema names in `Config.Schema`. [PR #952](https://github.com/riverqueue/river/pull/952).
- Jobs rescued by `JobRescuer` no longer have their trace set to "TODO". This becomes an empty string instead. [PR #1010](https://github.com/riverqueue/river/pull/1010).

## [0.23.1] - 2025-06-04

This includes a minor CLI bugfix for riverpro and no other changes, see the v0.23.0 notes for major changes.

### Fixed

- Fixed a riverpro CLI integration point broken in v0.23.0. [PR #945](https://github.com/riverqueue/river/pull/945)

## [0.23.0] - 2025-06-04

⚠️ Internal APIs used for communication between River and River Pro have changed. If using River Pro, make sure to update River and River Pro to latest at the same time to get compatible versions. River v0.23.0 is compatible with River Pro v0.15.0.

**Terminal UI:** @almottier wrote a very cool [terminal UI for River](https://github.com/almottier/rivertui) featuring real-time job monitoring with automatic refresh, job filtering, a job details view providing detailed information (plus look up by ID in the UI or by command line argument), and job actions like retry and cancellation. And as good as all that might sound, go take a look because it's even better in person.

### Added

- Preliminary River driver for SQLite (`riverdriver/riversqlite`). This driver seems to produce good results as judged by the test suite, but so far has minimal real world vetting. Try it and let us know how it works out. [PR #870](https://github.com/riverqueue/river/pull/870).
- CLI `river migrate-get` now takes a `--schema` option to inject a custom schema into dumped migrations and schema comments are hidden if `--schema` option isn't provided. [PR #903](https://github.com/riverqueue/river/pull/903).
- Added `riverlog.NewMiddlewareCustomContext` that makes the use of `riverlog` job-persisted logging possible with non-slog loggers. [PR #919](https://github.com/riverqueue/river/pull/919).
- Added `RequireInsertedOpts.Schema`, allowing an explicit schema to be set when asserting on job inserts with `rivertest`. [PR #926](https://github.com/riverqueue/river/pull/926).
- When using a driver that doesn't support listen/notify, producers within same process are notified immediately of new job inserts and queue changes (e.g. pause/resume) without having to poll when non-transactional variants are used (i.e. `Insert` instead of `InsertTx`). [PR #928](https://github.com/riverqueue/river/pull/928).
- Added `JobListParams.Where`, which provides an escape hatch for job listing that runs arbitrary SQL with named parameters. [PR #933](https://github.com/riverqueue/river/pull/933).

### Changed

- Optimized the job completer's query `JobSetStateIfRunningMany`, resulting in an approximately 15% reduction in its duration when completing 2000 jobs, and around a 15-20% increase in `riverbench` throughput. [PR #904](https://github.com/riverqueue/river/pull/904).
- `TimeStub` has been removed from the `rivertest` package. Its original inclusion was entirely accidentally and it should be considered entirely an internal API. [PR #912](https://github.com/riverqueue/river/pull/912).
- When storing job-persisted logging with `riverlog`, if a work run's logging was completely empty, no metadata value is stored at all (previously, an empty value was stored). [PR #919](https://github.com/riverqueue/river/pull/919).
- Changed the internal integration APIs for River Pro. River Pro users must upgrade both libraries as part of this update. [PR #929](https://github.com/riverqueue/river/pull/929).

### Fixed

- Resuming an already unpaused queue is now fully an no-op, and won't touch the row's `updated_at` like it (unintentionally) did before. [PR #870](https://github.com/riverqueue/river/pull/870).
- Suppress an error log line from the producer that may occur on normal shutdown when operating in poll-only mode. [PR #896](https://github.com/riverqueue/river/pull/896).
- Added missing help documentation for CLI command `river migrate-list`. [PR #903](https://github.com/riverqueue/river/pull/903).
- Correct handling an explicit schema in the reindexer maintenance service. [PR #916](https://github.com/riverqueue/river/pull/916).
- Return specific explanatory error when attempting to use `JobListParams.Metadata` with `JobListTx` on SQLite. [PR #924](https://github.com/riverqueue/river/pull/924).
- The reindexer now skips work if artifacts from a failed reindex are present under the assumption that if they are, a new reindex build is likely to fail again. Context cancel timeout is increased from 15 seconds to 1 minute, allowing more time for reindexes to finish. Timeout becomes configurable with `Config.ReindexerTimeout`. [PR #935](https://github.com/riverqueue/river/pull/935).
- Accessing `Client.PeriodicJobs()` on an insert-only client now panics with a more helpful explanatory error message rather than an unhelpful nil pointer panic. [PR #938](https://github.com/riverqueue/river/pull/938).
- Return an error when adding a new queue at runtime via the `QueueBundle` if that queue was already added. [PR #929](https://github.com/riverqueue/river/pull/929).

## [0.22.0] - 2025-05-10

### Added

- A new `JobArgsWithKindAliases` interface lets job args implement `KindAliases` to register a second kind that their worker will respond to. This provides a way to safely rename job kinds even with jobs using the original kind already in the database. [PR #880](https://github.com/riverqueue/river/pull/880).

### Changed

- Job kinds must comply to a format of `\A[\w][\w\-\[\]<>\/.·:+]+\z`, mainly in an attempt to eliminate commas and spaces to make format more predictable for an upcoming search UI. This check can be disabled for now using `Config.SkipJobKindValidation`, but this option will likely be removed in a future version of River. The new `JobArgsWithKindAliases` interface (see above) can be used to rename non-compliant kinds. [PR #879](https://github.com/riverqueue/river/pull/879).

### Fixed

- The `riverdatabasesql` now fully supports raw connections through [`lib/pq`](https://github.com/lib/pq) rather than just `database/sql` through Pgx. We don't recommend the use of `lib/pq` as it's an unmaintained project, but this change should help with compatibility for older projects. [PR #883](https://github.com/riverqueue/river/pull/883).

## [0.21.0] - 2025-05-02

⚠️ Internal APIs used for communication between River and River Pro have changed. If using River Pro, make sure to update River and River Pro to latest at the same time to get compatible versions. River v0.21.0 is compatible with River Pro v0.13.0.

### Added

- Added `river/riverlog` containing middleware that injects a context logger to workers that collates log output and persists it with job metadata. [PR #844](https://github.com/riverqueue/river/pull/844).
- Added `JobInsertMiddlewareFunc` and `WorkerMiddlewareFunc` to easily implement middleware with a function instead of a struct. [PR #844](https://github.com/riverqueue/river/pull/844).
- Added `Config.Schema` which lets a non-default schema be injected explicitly into a River client that'll be used for all database operations. This may be particularly useful for proxies like PgBouncer that may not respect a schema configured in `search_path`. [PR #848](https://github.com/riverqueue/river/pull/848).
- Added `rivertype.HookWorkEnd` hook interface that runs after a job has been worked. [PR #863](https://github.com/riverqueue/river/pull/863).
- Added support for filtering jobs by a list of job IDs and by priorities in `JobList` and `JobListParams`. For more flexible job listing. [PR #871](https://github.com/riverqueue/river/pull/871).

### Changed

- Client no longer returns an error if stopped before startup could complete (previously, it returned the unexported `ErrShutdown`). [PR #841](https://github.com/riverqueue/river/pull/841).

### Fixed

- A queue unpausing triggers an immediate fetch so that available jobs in the paused queue may be started faster than before. [PR #854](https://github.com/riverqueue/river/pull/854).

## [0.20.2] - 2025-04-08

### Added

- Added `QueueUpdateTx` API so there's a transactional variant of the `QueueUpdate` API from [PR #834](https://github.com/riverqueue/river/pull/834). [PR #838](https://github.com/riverqueue/river/pull/838).

## [0.20.1] - 2025-04-05

### Fixed

- Corrected the serialization of queue control event payloads emitted by `QueueUpdate`. [PR #834](https://github.com/riverqueue/river/pull/834).

## [0.20.0] - 2025-04-04

### Added

- Added a `QueueUpdate` API to the `Client` which will be used for upcoming functionality. [PR #822](https://github.com/riverqueue/river/pull/822).

### Changed

- Set minimum Go version to Go 1.23. [PR #811](https://github.com/riverqueue/river/pull/811).
- Deprecate `river.JobInsertMiddlewareDefaults` and `river.WorkerMiddlewareDefaults` in favor of the more general `river.MiddlewareDefaults` embeddable struct. The two former structs will be removed in a future version. [PR #815](https://github.com/riverqueue/river/pull/815).

### Fixed

- Cleanly error when attempting to add a queue at runtime to a `Client` which was not configured to run jobs (no `Workers`). [PR #826](https://github.com/riverqueue/river/pull/826).

## [0.19.0] - 2025-03-16

⚠️ Version 0.19.0 has minor breaking changes for the `Worker.Middleware`, introduced fairly recently in 0.17.0 that has a worker's `Middleware` function now taking a non-generic `JobRow` parameter instead of a generic `Job[T]`. We tried not to make this change, but found the existing middleware interface insufficient to provide the necessary range of functionality we wanted, and this is a secondary middleware facility that won't be in use for many users, so it seemed worthwhile.

### Added

- Added a new "hooks" API for tying into River functionality at various points like job inserts or working. Differs from middleware in that it doesn't go on the stack and can't modify context, but in some cases is able to run at a more granular level (e.g. for each job insert rather than each _batch_ of inserts). [PR #789](https://github.com/riverqueue/river/pull/789).
- `river.Config` has a generic `Middleware` setting that can be used as a convenient way to configure middlewares that implement multiple middleware interfaces (e.g. `JobInsertMiddleware` _and_ `WorkerMiddleware`). Use of this setting is preferred over `Config.JobInsertMiddleware` and `Config.WorkerMiddleware`, which have been deprecated. [PR #804](https://github.com/riverqueue/river/pull/804).

### Changed

- The `river.RecordOutput` function now returns an error if the output is too large. The output is limited to 32MB in size. [PR #782](https://github.com/riverqueue/river/pull/782).
- **Breaking change:** The `Worker` interface's `Middleware` function now takes a `JobRow` parameter instead of a generic `Job[T]`. This was necessary to expand the potential of what middleware can do: by letting the executor extract a middleware stack from a worker before a job is fully unmarshaled, the middleware can also participate in the unmarshaling process. [PR #783](https://github.com/riverqueue/river/pull/783).
- `JobList` has been reimplemented to use sqlc. [PR #795](https://github.com/riverqueue/river/pull/795).

## [0.18.0] - 2025-02-20

⚠️ Version 0.18.0 has breaking changes for the `rivertest.Worker` type that was just introduced. While attempting to round out some edge cases with its design, we realized some of them simply couldn't be solved adequately without changing the overall design such that all tested jobs are inserted into the database. Given the short duration since it was released (over a weekend) it's unlikely many users have adopted it and it seemed best to rip off the bandaid to fix it before it gets widely used.

### Added

- Jobs can now store a recorded "output" value, a JSON-encoded payload set by the job during execution and stored in the job's metadata. The `river.RecordOutput` function makes it easy to use the job row to store transient/temporary values that are needed for introspection or for other downstream jobs. The output can be accessed using the `JobRow.Output()` helper method.

  This output is stored at the same time as the job is completed following execution, so it does not require additional database calls or overhead. Output can be anything that can be stored in a Postgres JSONB field, though for performance reasons it should be limited in size. [PR #758](https://github.com/riverqueue/river/pull/758).

### Changed

- **Breaking change:** The `rivertest.Worker` type now requires all jobs to be inserted into the database. The original design allowed workers to be tested without hitting the database at all. Ultimately this design made it hard to correctly simulate features like `JobCompleteTx` and the other potential solutions seemed undesirable.

  As part of this change, the `Work` and `WorkJob` methods now take a transaction argument. The expectation is that a transaction will be opened by the caller and rolled back after test completion. Additionally, the return signature was changed to return a `WorkResult` struct alongside the error. The struct includes the post-execution job row as well as the event kind that occurred, making it easy to inspect the job's state after execution.

  Finally, the implementation was refactored so that it uses the _real_ `river.Client` insert path, and also uses the same job execution path as real execution. This minimizes the potential for differences in behavior between testing and real execution.
  [PR #766](https://github.com/riverqueue/river/pull/766).

- Adjusted panic stack traces to filter out irrelevant frames like the ones generated by the runtime package that constructed the trace, or River's internal rescuing code. This makes the first panic frame reflect the actual panic origin for easier debugging. [PR #774](https://github.com/riverqueue/river/pull/774).

### Fixed

- Fix error message on unsuccessful client subscribe that erroneously referred to "Workers" not configured. [PR #771](https://github.com/riverqueue/river/pull/771).
- Fix an issue with encoding unique keys in riverdatabasesql driver. [PR #777](https://github.com/riverqueue/river/pull/777).

## [0.17.0] - 2025-02-16

### Added

- Exposed `TestConfig` struct on `Config` under the `Test` field for configuration that is specific to test environments. For now, the only field on this type is `Time`, which can be used to set a synthetic `TimeGenerator` for tests. A stubbable time generator was added as `rivertest.TimeStub` to allow time to be easily stubbed in tests. [PR #754](https://github.com/riverqueue/river/pull/754).
- New `rivertest.Worker` type to make it significantly easier to test River workers. Either real or synthetic jobs can be worked using this interface, generally without requiring any database interactions. The `Worker` type provides a realistic execution environment with access to the full range of River features, including `river.ClientFromContext`, middleware (both global and per-worker), and timeouts. [PR #753](https://github.com/riverqueue/river/pull/753).

### Changed

- Errors returned from retryable jobs are now logged with warning logs instead of error logs. Error logs are still used for jobs that error after reaching `max_attempts`. [PR #743](https://github.com/riverqueue/river/pull/743).
- Remove range variable capture in `for` loops and use simplified `range` syntax. Each of these requires Go 1.22 or later, which was already our minimum required version since Go 1.23 was released. [PR #755](https://github.com/riverqueue/river/pull/755).

### Fixed

- `riverdatabasesql` driver: properly handle `nil` values in `bytea[]` inputs. This fixes the driver's handling of empty unique keys on insert for non-unique jobs with the newer unique jobs implementation. [PR #739](https://github.com/riverqueue/river/pull/739).
- `JobCompleteTx` now returns `rivertype.ErrNotFound` if the job doesn't exist instead of panicking. [PR #753](https://github.com/riverqueue/river/pull/753).
- - `NeverSchedule.Next` now returns the correct maximum time value, ensuring that the periodic job truly never runs. This fixes an issue where an incorrect maximum timestamp was previously used. Thanks Hubert Krauze ([@krhubert](https://github.com/krhubert))! [PR #760](https://github.com/riverqueue/river/pull/760)

## [0.16.0] - 2024-01-27

### Added

- `NeverSchedule` returns a `PeriodicSchedule` that never runs. This can be used to effectively disable the reindexer or any other maintenance service. [PR #718](https://github.com/riverqueue/river/pull/718).
- Add `SkipUnknownJobCheck` client config option to skip job arg worker validation. [PR #731](https://github.com/riverqueue/river/pull/731).

### Changed

- The reindexer maintenance process has been enabled. As of now, it will reindex only the `river_job_args_index` and `river_jobs_metadata_index` `GIN` indexes, which are more prone to bloat than b-tree indexes. By default it runs daily at midnight UTC, but can be customized on the `river.Config` type via `ReindexerSchedule`. Most installations will benefit from this process, but it can be disabled altogether using `NeverSchedule`. [PR #718](https://github.com/riverqueue/river/pull/718).
- Periodic jobs now have a `"periodic": true` attribute set in their metadata to make them more easily distinguishable from other types of jobs. [PR #728](https://github.com/riverqueue/river/pull/728).
- Snoozing a job now causes its `attempt` to be _decremented_, whereas previously the `max_attempts` would be incremented. In either case, this avoids allowing a snooze to exhaust a job's retries; however the new behavior also avoids potential issues with wrapping the `max_attempts` value, and makes it simpler to implement a `RetryPolicy` based on either `attempt` or `max_attempts`. The number of snoozes is also tracked in the job's metadata as `snoozes` for debugging purposes.

  The implementation of the builtin `RetryPolicy` implementations is not changed, so this change should not cause any user-facing breakage unless you're relying on `attempt - len(errors)` for some reason. [PR #730](https://github.com/riverqueue/river/pull/730).

- `ByPeriod` uniqueness is now based off a job's `ScheduledAt` instead of the current time if it has a value. [PR #734](https://github.com/riverqueue/river/pull/734).

## [0.15.0] - 2024-12-26

### Added

- The River CLI will now respect the standard set of `PG*` environment variables like `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD`, and `PGSSLMODE` to configure a target database when the `--database-url` parameter is omitted. [PR #702](https://github.com/riverqueue/river/pull/702).
- Add missing doc for `JobRow.UniqueStates` + reveal `rivertype.UniqueOptsByStateDefault()` to provide access to the default set of unique job states. [PR #707](https://github.com/riverqueue/river/pull/707).

### Changed

- Sleep durations are now logged as Go-like duration strings (e.g. "10s") in either text or JSON instead of duration strings in text and nanoseconds in JSON. [PR #699](https://github.com/riverqueue/river/pull/699).
- Altered the migration comments from `river migrate-get` to include the "line" of the migration being run (`main`, or for River Pro `workflow` and `sequence`) to make them more distinguishable. [PR #703](https://github.com/riverqueue/river/pull/703).
- Fewer slice allocations during unique insertions. [PR #705](https://github.com/riverqueue/river/pull/705).

### Fixed

- Exponential backoffs at degenerately high job attempts (>= 310) no longer risk overflowing `time.Duration`. [PR #698](https://github.com/riverqueue/river/pull/698).

## [0.14.3] - 2024-12-14

### Changed

- Dropped internal random generators in favor of `math/rand/v2`, which will have the effect of making code fully incompatible with Go 1.21 (`go.mod` has specified a minimum of 1.22 for some time already though). [PR #691](https://github.com/riverqueue/river/pull/691).

### Fixed

- 006 migration now tolerates previous existence of a `unique_states` column in case it was added separately so that the new index could be raised with `CONCURRENTLY`. [PR #690](https://github.com/riverqueue/river/pull/690).

## [0.14.2] - 2024-11-16

### Fixed

- Cancellation of running jobs relied on a channel that was only being received when in the job fetch routine, meaning that jobs which were cancelled would not be cancelled until the next scheduled fetch. This was fixed by also receiving from the job cancellation channel when in the main producer loop, even if no fetches are happening. [PR #678](https://github.com/riverqueue/river/pull/678).
- Job insert middleware were not being utilized for periodic jobs. This insertion path has been refactored to rely on the unified insertion path from the client. Fixes #675. [PR #679](https://github.com/riverqueue/river/pull/679).

## [0.14.1] - 2024-11-04

### Fixed

- In [PR #663](https://github.com/riverqueue/river/pull/663) the client was changed to be more aggressive about re-fetching when it had previously fetched a full batch. Unfortunately a clause was missed, which resulted in the client being more aggressive any time even a single job was fetched on the previous attempt. This was corrected with a conditional to ensure it only happens when the last fetch was full. [PR #668](https://github.com/riverqueue/river/pull/668).

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
