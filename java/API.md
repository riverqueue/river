# Java API review

Reviewed against the exported APIs in this River checkout and the pinned Go
conformance reference. This is an application API comparison, not a claim that
Java exposes every Go helper. Stored values and protocol behavior still follow Go.

| Go surface | Java surface | Reason for the shape |
| --- | --- | --- |
| `NewClient`, `Config`, `Start`, `Stop`, `StopAndCancel` | `Client`, `Workers.Builder`, `start`, `stop`, `stopAndCancel` | A client can insert without starting anything. A running `Workers` owns threads and implements `AutoCloseable`. |
| `JobArgs`, `Job[T]`, worker interfaces and registration | `JobType<A>`, `Job<A>`, `handle(type, lambda)` | Records need no River interface; explicit kinds stay stable when Java classes are renamed. |
| `Insert`, `InsertTx`, `InsertMany`, `InsertManyTx` | `insert`, `insertMany`, connection-first overloads | Overloading expresses the same operation with a caller-owned transaction. Homogeneous batches retain their argument type; mixed batches use `JobType.submission`. |
| `InsertOpts`, `UniqueOpts`, struct tags | `InsertOptions`, `Unique`, `JobType.uniqueBy` | Named builders and fluent options avoid positional flags. Explicit JSON paths replace struct tags. |
| `JobGet`, `JobList`, cursor/order/filter params | `get`, `JobQuery`, `JobQuery.Page` | Raw reads use `JsonNode`; `get(id, type)` checks the kind and decodes a known argument type. Pagination remains explicit. |
| `JobCancel`, `JobDelete`, `JobDeleteMany`, `JobRetry` and transaction variants | `cancel`, `delete`, `deleteMany`, `retry`, connection-first overloads | Short operation names are unambiguous on `Client`; transaction ownership stays visible in the argument list. |
| `JobUpdate` output parameter, `RecordOutput` | `Client.output`, `WorkContext.output` | Names describe the supported operation; attempt output is buffered until completion. |
| `JobCompleteTx` | `Client.complete(connection, job)`, `WorkContext.complete(connection)` | Completion keeps `Job<A>` and commits with application changes. Only a running attempt can be completed. A committed completion remains authoritative if the handler subsequently throws. |
| Context client, cancellation, cancel/snooze errors | `WorkContext.client`, cancellation methods, `cancel`, `discard`, `snooze` | Java has no Go context parameter convention. Control methods end the attempt; virtual-thread interruption supplements cooperative cancellation. |
| Hooks and middleware | `Extension` | Default methods group related callbacks. JDBC hooks and middleware may throw checked exceptions; insert middleware receives a standard `Callable<T>`. |
| Retry policy | `RetryPolicy` | A lambda receives the full job and returns `Duration`; the default uses the same error-count backoff and jitter. |
| `Queues`, queue CRUD/control APIs | `Workers.addQueue/removeQueue`, `Client.queues()` | Runtime concurrency and persisted queue state have separate owners. Reads and mutations support caller transactions. |
| `Subscribe`, `EventKind`, `Event` | `Workers.subscribe`, `EventKind`, `Event`, `Subscription` | Enum filters and an `AutoCloseable` subscription replace channels. Callbacks are synchronous; queue events include their queue. |
| Periodic jobs and schedules | `Workers.Builder.periodic`, `Schedule` | Java time types and a functional scheduling interface; cron implementation details are package-private. |
| Leadership notifications and maintenance config | `requestResign`, worker builder settings | Coordination stays in the database; operational durations use `Duration`. |
| `rivermigrate` | `Migrator`, `Direction`, `Options`, executable CLI | Named options replace booleans and sentinel target versions. Down defaults to one migration; explicit target zero removes the line. |
| Driver, pool, transaction integration | `Database`, JDBC `DataSource`/`Connection` | Use existing Java database pools. Database drivers remain optional library dependencies. |
| Errors and row types | `RiverException.Code`, `Job.State`, nested result records | Exceptions carry failures; enums model closed sets; records keep related values together. Missing rows throw `NOT_FOUND`. |

## Changes made before release

- Removed no-op `Client.close()`. Close `Workers` and the application-owned pool.
- Added typed retrieval and typed transactional completion, including running-state validation.
- Unified bulk insertion around typed homogeneous lists and mixed-kind submissions,
  with both owned and caller-owned transactions.
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
  `Plugin`, `Client.Driver`, protocol JSON helpers, and explicitly marked companion
  methods are not stable application extension APIs.

Options copy collections and metadata on construction; `InsertOptions.metadata()`
also returns a copy. Job/result JSON values are snapshots, not database-backed
objects. Builders are mutable and should be confined to configuration code.

## Remaining Go API gaps

These are implementation limits, not claims that Java conventions require a
smaller feature set:

- OSS periodic definitions cannot be added or removed on a running runtime and
  currently carry fixed arguments rather than a constructor invoked each time.
- Events do not yet include Go's per-job timing statistics. Java supplies callback
  delivery rather than a channel buffer and its associated subscription options.
- Resumable progress is buffered until the attempt ends; transactional checkpoint
  helpers are not exposed. The Go logging middleware and worker-test harness have
  no packaged Java equivalents.
- Some maintenance configuration is coarser: reindexing accepts an interval,
  polling policy belongs to a runtime, and insert defaults belong to a job type.

See [differences](DIFFERENCES.md), the [feature inventory](conformance/feature-inventory.json),
and [validation](VALIDATION.md) for protocol coverage and existing limitations.
