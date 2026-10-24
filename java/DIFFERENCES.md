# Intentional differences from Go

- Java 21 is the minimum version. Records, lambdas, `Instant`, `Duration`, JDBC
  transactions, and virtual threads are the native API; no Go-shaped worker
  inheritance hierarchy is required.
- Job kinds are explicit `JobType` values, independent of Java class names.
  Unique argument fields are explicit paths instead of Go struct tags.
- JSON uses Jackson 3 and snake_case record properties. Applications must use
  compatible JSON field names in every implementation sharing a job kind.
- A `DataSource` and its pool belong to the application. `Database.connect`
  uses DriverManager without pooling; callers wanting pooled connections supply
  a data source. River-owned JDBC connections are closed after each transaction.
- Caller-owned transactions use JDBC savepoints. Hooks run within those
  savepoints, so a failed River operation leaves the outer transaction usable.
- Cancellation is cooperative and followed by interruption after the configured
  stuck threshold. The JVM has no safe way to kill an arbitrary uncooperative
  thread. Such a handler retains its slot until it returns; handlers should use
  interruptible I/O or inspect their `WorkContext` cancellation signal.
- Transaction variants use connection-first overloads instead of `Tx` suffixes.
  Storage failures are unchecked `RiverException` values with a stable code and
  original cause; JDBC callbacks may throw checked exceptions.
- Runtime events use enum kinds and are process-local callbacks. They are not a durable delivery
  mechanism. A callback should finish quickly, and callback failures are sent to
  the runtime error handler.
- Retry policies return relative `Duration` values. Exceptions and null, negative,
  or overflowing retry delays are reported and fall back to the default backoff.
- Explicit retries that update a job send a standard insert notification in the
  same transaction, so workers wake after commit. Go currently leaves discovery
  of explicitly retried jobs to polling.
- Short rescued retries become `available` with their retry timestamp and notify
  the queue once due. This avoids waiting for Java's combined maintenance cadence
  when `serviceInterval` is long; Go rescues into `retryable` for its scheduler.
- Java's `serviceInterval` controls elections, maintenance, and queue heartbeats.
  It must be shorter than the one-day queue retention period; Go reports queue
  heartbeats separately, every ten minutes by default.
- Cron schedules follow Go's DST gap/overlap choices. Java uses the JVM's
  time-zone database, which must agree with Go's for identical results. When a
  zone skips an entire calendar date, Java advances past it instead of
  reproducing Go's nonterminating date loop.
- SQL is kept in dialect-specific resource catalogs. The Go sqlc driver seam is
  not part of the Java API.
- Resumable cursor updates are saved even when the first step fails before any
  step completes. Go currently drops cursor updates in that case. Updating a
  cursor never marks a step completed; a failed plain step still runs on retry.
- Resumable steps cannot be nested in Java. Each attempt has one active step and
  checkpoint scope, so rejecting nesting avoids ambiguous resume order and
  checkpoint ownership. Go currently permits nested steps.
- Transactional checkpoints require the job to still be running with the same
  `attempt` and `attempted_at`. Go updates by job ID alone. Java rejects a stale
  attempt's checkpoint to prevent it from changing a replacement attempt.
- A transactional checkpoint with no cursors stores `river:resumable_cursor` as
  JSON null, clearing any older cursor map. Go may omit that key; readers in both
  languages accept null as an empty cursor map.
- SQLite follows Go's JSONB storage and millisecond timestamp representation;
  Postgres uses microsecond timestamps. These are shared storage contracts,
  not configurable Java serialization choices.

Stored uniqueness hashes, job state values, migration versions, and notification
payloads must agree with Go. Reserved metadata uses the same shapes and encodings,
with the resumable persistence differences documented above. Other conformance
failures in these areas are defects, not API design choices.

The following are current API limitations, not differences required by Java
conventions. The [API review](API.md) maps the complete application surface.

OSS periodic registrations are local to a worker runtime and can be changed
while it runs. Polling policies belong to a worker configuration; use separate
runtimes for queues with different polling intervals. Fetch wakeups are coalesced
without a separate cooldown setting. Reindexing supports the daily UTC default or a
duration override, rather than an arbitrary cron expression. Job-type defaults
replace process-wide insert defaults.

Insertion is independent of worker registration so a client can enqueue jobs
owned by another language. New job kinds always use Go's validated kind syntax;
the legacy bypass is omitted. `Workers.stop()` (also called by `close()`) and
`Workers.stopAndCancel()` wait for attempts to finish instead of exposing a
Go-style stopped channel. `Workers.Builder.stopTimeout` configures their waits.

The [feature inventory](conformance/feature-inventory.json) accounts for all 304
entries in the pinned upstream inventory, including internal driver details
that are not application APIs.

Java has no bundled `riverlog` middleware or `rivertest.Worker` harness. Applications
can write the shared log metadata format explicitly and test with JUnit against
an isolated database. See the [README examples](README.md#features) for the
available APIs.
