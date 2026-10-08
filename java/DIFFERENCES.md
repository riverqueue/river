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
- SQLite follows Go's JSONB storage and millisecond timestamp representation;
  PostgreSQL uses microsecond timestamps. These are shared storage contracts,
  not configurable Java serialization choices.

No intentional differences in stored uniqueness hashes, job state values, reserved
metadata, migration versions, or notification payloads are allowed. Any remaining
conformance failure in these areas is a defect, not an API design choice.

The following are current API limitations, not differences required by Java
conventions. The [API review](API.md) maps the complete application surface.

OSS periodic registrations and polling policies belong to a worker configuration;
start a new runtime to change its periodic definitions, or use separate runtimes
for queues with different polling intervals. Fetch wakeups are coalesced without
a separate cooldown setting. Reindexing supports the daily UTC default or a
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
an isolated database. Resumable steps buffer progress until the attempt ends;
Go's transactional resumable checkpoint helpers are not yet exposed. See the
[README examples](README.md#features) for these API limitations.
