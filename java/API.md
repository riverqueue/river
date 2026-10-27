# Java API review

Reviewed against the exported APIs in this River checkout and the pinned Go
conformance reference. This is an application API comparison, not a claim that
Java exposes every Go helper. Stored values and protocol behavior still follow Go.

| Go surface | Java surface | Reason for the shape |
| --- | --- | --- |
| `NewClient`, `Config`, `Start`, `Stop`, `StopAndCancel` | `Client.builder`, `Client.start`, `RunHandle.stop/stopAndCancel` | A client can insert without starting anything. A `RunHandle` owns one execution and implements `AutoCloseable`. |
| `JobArgs`, `Job[T]`, worker interfaces and registration | `JobType<A>`, `Job<A>`, `Workers.add(type, lambda)`, `Client.Builder.worker` | Records need no River interface; explicit kinds stay stable when Java classes are renamed. |
| `Insert`, `InsertTx`, `InsertMany`, `InsertManyTx` | `insert`, `insertMany`, connection-first overloads | Arguments are typed; results contain persisted `Job<JsonNode>` rows, including cross-kind duplicates. Mixed batches use `JobType.submission`. |
| `InsertOpts`, `UniqueOpts`, struct tags | `InsertOptions`, `Unique`, `JobType.uniqueBy` | Named builders and fluent options avoid positional flags. Explicit JSON paths replace struct tags. |
| `JobGet`, `JobList`, cursor/order/filter params | `get`, `JobQuery`, `JobQuery.Page` | Raw reads use `JsonNode`; `get(id, type)` checks the kind and decodes a known argument type. Pagination remains explicit. |
| `JobCancel`, `JobDelete`, `JobDeleteMany`, `JobRetry` and transaction variants | `cancel`, `delete`, `deleteMany`, `retry`, connection-first overloads | Short operation names are unambiguous on `Client`; transaction ownership stays visible in the argument list. |
| `JobUpdate` output parameter, `RecordOutput` | `Client.output`, `WorkContext.output` | Names describe the supported operation; attempt output is buffered until completion. |
| `JobCompleteTx` | `Client.complete(connection, job)`, `WorkContext.complete(connection)` | Completion keeps `Job<A>` and commits with application changes. Only a running attempt can be completed. A committed completion remains authoritative if the handler subsequently throws. |
| Context client, cancellation, cancel/snooze errors | `WorkContext.client`, cancellation methods, `cancel`, `discard`, `snooze` | Java has no Go context parameter convention. Control methods end the attempt; virtual-thread interruption supplements cooperative cancellation. |
| Plugins, hooks, and middleware | `Plugin`, `HookInsertBegin`, `HookPeriodicJobsStart`, `HookWorkBegin`, `HookWorkEnd`, `JobInsertMiddleware`, `WorkerMiddleware` | `withPlugins` activates every hook and middleware interface on a value. `withHooks` and `withMiddleware` restrict registration to that side. Plugins run first, as in Go. Functional interfaces support lambdas and checked exceptions. |
| `ResumableSetStepTx`, `ResumableSetStepCursorTx` | `WorkContext.checkpoint(connection[, cursor])` | Checkpoints commit with application writes; failed transactions leave the last committed progress intact. |
| Worker timeout | `Workers.Handler.timeout(job)` | A default method preserves lambda handlers; per-job overrides also govern rescue decisions. |
| Retry policy | `RetryPolicy` | A lambda receives the full job and returns `Duration`; the default uses the same error-count backoff and jitter. |
| `Queues`, queue CRUD/control APIs | `RunHandle.addQueue/removeQueue`, `Client.queues()` | Runtime concurrency and persisted queue state have separate owners. Reads and mutations support caller transactions. |
| `Subscribe`, `EventKind`, `Event` | `Client.subscribe`, `EventKind`, `Event`, `Subscription` | Enum filters and an `AutoCloseable` subscription replace channels. Callbacks are synchronous; queue events include their queue. |
| Periodic jobs and schedules | `Client.Builder.periodic`, `RunHandle.periodicJobs()`, `PeriodicJobBundle`, `PeriodicJob`, `Schedule` | Java time types and a functional scheduling interface; cron implementation details are package-private. |
| Leadership notifications and maintenance config | `requestResign`, client builder settings | Coordination stays in the database; operational durations use `Duration`. |
| `rivermigrate` | `Migrator`, `Direction`, `Options`, executable CLI | Named options replace booleans and sentinel target versions. Down defaults to one migration; explicit target zero removes the line. |
| Driver, pool, transaction integration | `Database`, JDBC `DataSource`/`Connection` | Use existing Java database pools. Database drivers remain optional library dependencies. |
| Errors and row types | `RiverException.Code`, `Job.State`, nested result records | Exceptions carry failures; enums model closed sets; records keep related values together. Missing rows throw `NOT_FOUND`. |

## Changes made before release

- Replaced `Extension` and `withExtension` with separate hook and middleware
  interfaces. Names follow Go, while functional interfaces allow Java lambdas.
  Work-end hooks can preserve, replace, or clear the failure; a work-begin failure
  skips the handler and work-end hooks. The conformance claim barrier now uses
  worker middleware instead of an application-facing `afterClaim` callback.
- Added the common `Plugin` marker and `withPlugins` registration to install all
  of an implementation's hooks and middleware together. Renamed the internal Pro
  storage interface and registration from `Plugin`/`withPlugin` to `Pilot`/`withPilot`.
- Removed no-op `Client.close()`. Close the `RunHandle` returned by `Client.start()` and the application-owned pool.
- Added typed retrieval and typed transactional completion, including running-state validation.
- Unified bulk insertion around typed homogeneous lists and mixed-kind submissions,
  with both owned and caller-owned transactions.
- Made insertion results contain persisted JSON rows so a duplicate can safely
  refer to a different kind or argument schema, matching Go's insertion result.
- Added savepoint protection to queue controls and connection-based reads, and a
  public transaction callback overload for grouping operations in a savepoint.
- Allowed checked failures from insert hooks and middleware; SQL failures retain
  their cause under `RiverException.Code.DATABASE`.
- Replaced string event kinds with enums, added subscription filters and queue
  payloads, and made subscription closure free of checked exceptions.
- Made repeated stop calls continue to wait after a timeout. A graceful-stop
  timeout leaves attempts running, as in Go; `stopAndCancel` explicitly escalates it.
- Changed retry callbacks to receive the full job, enabling kind- and metadata-specific policies.
- Added named migration options and fluent uniqueness state/kind overrides.
- Kept destructive reset and retention helpers behind the internal driver seam.
  `Pilot`, `Client.Driver`, protocol JSON helpers, and explicitly marked companion
  methods are not stable application extension APIs.

Options copy collections and metadata on construction; `InsertOptions.metadata()`
and `JobQuery.metadata()` also return copies. Job/result JSON values are snapshots,
not database-backed objects. Builders are mutable and should be confined to
configuration code.

## Runtime and option review

- Lists now default to ascending job ID order, matching the Go implementation and
  keeping pagination independent of rescheduling. Explicit time ordering remains
  available with `JobQuery.Order.TIME`.
- Attempt timeouts are supervised independently of fetching and continue during
  graceful stop. Finished handlers are no longer cancelled while their completion
  is awaiting a database commit; an interrupted handler cannot abandon completion
  retries or strand its worker slot.
- Worker durations and snoozes must fit in signed 64-bit nanoseconds, matching Go's
  duration range. Invalid worker settings fail during configuration, and an invalid
  snooze fails the attempt normally instead of trapping it in completion retries.
- An explicit `rescueAfter` cannot be shorter than `jobTimeout`. The default is one
  hour, plus a positive explicitly configured job timeout, following Go and Rust.
- `JobType.uniqueBy("account_id", "address")` selects top-level JSON fields without
  nested lists. The existing component-list overload handles nested paths and
  retains literal dots in field names.
- Reusing a client builder generates a separate client ID on each `build()`.
  Restarting one client retains its ID; an explicit `id` remains the caller's
  responsibility to keep unique.
- Observer and subscriber failures are reported without losing a worker slot,
  changing the attempt outcome, or preventing delivery to the remaining callbacks.
  Failures in the error handler fall back to the system logger. These callbacks
  remain synchronous and must finish quickly.
- Retry policies run only when the attempt will be retried, matching Go's terminal
  state handling. Throwing policies and invalid delays are reported and use the
  default backoff; they cannot prevent completion from being persisted.
- Query metadata is copied on access so reusable filters cannot change through
  an exposed JSON node.

## Remaining Go API gaps

These are implementation limits, not claims that Java conventions require a
smaller feature set:

- OSS periodic definitions carry fixed arguments rather than a constructor
  invoked each time.
- Events do not yet include Go's per-job timing statistics. Java supplies callback
  delivery rather than a channel buffer and its associated subscription options.
- The Go logging middleware and worker-test harness have no packaged Java
  equivalents.
- Some maintenance configuration is coarser: reindexing accepts an interval,
  polling policy belongs to a runtime, and insert defaults belong to a job type.

See [differences](DIFFERENCES.md), the [feature inventory](conformance/feature-inventory.json),
and [validation](VALIDATION.md) for protocol coverage and existing limitations.
