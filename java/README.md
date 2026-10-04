# River for Java

Prerelease Java 25 client and worker runtime for River's PostgreSQL and SQLite
schemas. Jobs are ordinary River jobs: another language can insert, cancel,
retry, or work them using the same database.

## Quick start

Define job arguments as a record, give the job a stable kind, and register a
worker lambda. This complete `Example.java` starts a client, inserts a job,
waits for its completion, and stops the workers. Set `DATABASE_URL` to a
PostgreSQL URL or a file-backed SQLite JDBC URL.

```java
import com.riverqueue.*;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

public class Example {
  record SendEmail(String address, String subject) {}

  public static void main(String[] args) throws Exception {
    var database = Database.connect(System.getenv("DATABASE_URL"));
    var client = new Client(database);
    new Migrator(database).migrate();

    var completed = new CompletableFuture<Workers.Event>();
    try (var workers = client.workers()
        .queue("default", 10)
        .handle(JobType.of("send_email", SendEmail.class), context -> {
          System.out.println("Sending " + context.args().subject() + " to " + context.args().address());
          context.output("sent");
        })
        .start();
        var subscription = workers.subscribe(completed::complete, Workers.EventKind.JOB_COMPLETED)) {
      client.insert(JobType.of("send_email", SendEmail.class),
          new SendEmail("hello@example.com", "Welcome"));
      System.out.println("Completed job " + completed.get(30, TimeUnit.SECONDS).job().id());
    }
  }
}
```

Run this demonstration against a development database with no other workers.
The worker prints a message; replace its body with your mail service. In a
service, keep `Workers` open until the application stops. Each active job runs
on a virtual thread; queue limits bound concurrent jobs. In production, run
migrations as a deployment step using the CLI below.

## Installation

The Maven artifact is `com.riverqueue:river:0.48.0-alpha.1`. Build locally with
`mvn install` from this directory with `JAVA_HOME` pointing to JDK 25. The pinned
formatter does not run on JDK 27. Add the JDBC driver for your database to your
application. Jackson 3 is included transitively.

```xml
<dependency>
  <groupId>com.riverqueue</groupId>
  <artifactId>river</artifactId>
  <version>0.48.0-alpha.1</version>
</dependency>
```

Job kinds are explicit, stable wire names independent of Java class names.
Record properties use snake_case in JSON by default, so `accountId` becomes
`account_id`. Match kinds and JSON fields across languages sharing a job.

Use a pooled `DataSource` in applications. `Database.connect(url)` is a small
DriverManager convenience for scripts and tests and opens a physical connection
for each operation. River never closes an application-owned data source. An
[insert-only client](https://riverqueue.com/docs/insert-only-clients) is simply a
`Client` instance without a worker runtime; inserting needs no start or stop call.

## Migration CLI

The `river-cli` executable JAR includes PostgreSQL and SQLite JDBC drivers and
runs with Java 25 or newer. It needs no Go installation or application classpath.
Build it with `mvn install` from `java/`, then run:

```sh
RIVER_CLI=cli/target/river-cli-0.48.0-alpha.1-all.jar
export DATABASE_URL=postgres://localhost/myapp
# For SQLite: export DATABASE_URL=jdbc:sqlite:/absolute/path/myapp.db

java -jar "$RIVER_CLI" --help
java -jar "$RIVER_CLI" migrate-list
java -jar "$RIVER_CLI" migrate-up --dry-run
java -jar "$RIVER_CLI" migrate-up
```

The Maven executable artifact is `com.riverqueue:river-cli` with version
`0.48.0-alpha.1` and classifier `all`. Copy that single JAR into your deployment image to run
migrations before starting workers. `--database-url URL` overrides `DATABASE_URL`.
`--schema background_jobs` selects a PostgreSQL schema; an actual migration up
creates it if needed. Listing and dry runs do not create schemas or migration tables.

| Command | Behavior |
| --- | --- |
| `migrate-up` | Applies all pending migrations, committing each separately. |
| `migrate-down` | Reverses one migration by default; may remove tables and data. |
| `migrate-list` | Lists bundled versions and whether each is applied. |
| `migrate-get` | Exports canonical SQL without connecting to a database. |
| `version` | Prints the CLI version; `--version` works too. |

Use `--target-version N` to stop at a version, `--max-steps N` to limit the count,
and `--dry-run --show-sql` to inspect a planned change. On `migrate-down`, target
version `0` explicitly removes the entire migration line. A zero `--max-steps`
uses the command's default: all pending up migrations or one down migration.

```sh
java -jar "$RIVER_CLI" migrate-down --max-steps 2 --dry-run --show-sql
java -jar "$RIVER_CLI" migrate-up --target-version 8
java -jar "$RIVER_CLI" migrate-get --version 3 --up > river_3.up.sql
java -jar "$RIVER_CLI" migrate-get --driver sqlite --all --up > river_sqlite.up.sql
```

SQL export defaults to PostgreSQL without a database URL. Use `--driver sqlite`
for SQLite, or select the dialect through the URL. Export accepts comma-separated
`--version` values or `--all`; `--exclude-version 1` excludes migration history
setup when exporting for another migration framework. Exit codes are `0` for
success, `1` for an operation failure, and `2` for invalid arguments.

The [migration history](https://riverqueue.com/docs/migrations) is shared with Go
and the other ports. Use the CLI version matching your Java library. The Java
API remains available for embedded use:

```java
var result = new Migrator(database).migrate();
System.out.println("Applied migrations: " + result.applied());
var preview = new Migrator(database).migrate(Migrator.Direction.DOWN,
    Migrator.Options.defaults().maxSteps(2).dryRun(true));
```

## Transactional enqueueing

[Enqueue in the application's transaction](https://riverqueue.com/docs/transactional-enqueueing)
so the job and application writes commit or roll back together. Pass a JDBC
connection with auto-commit disabled. River uses savepoints and never commits,
rolls back the outer transaction, or closes that connection. Insert hooks and
middleware run inside the same transaction.

```java
try (var connection = applicationDataSource.getConnection()) {
  connection.setAutoCommit(false);
  try {
    accounts.create(connection, account);
    client.insert(connection, email, new SendEmail(account.email(), "Welcome"));
    connection.commit();
  } catch (Exception failure) {
    connection.rollback();
    throw failure;
  }
}
```

`client.transaction(connection -> ...)` manages a new transaction for River-owned
operations. `client.transaction(existingConnection, connection -> ...)` groups
multiple operations under one savepoint in an existing transaction. Neither
`Client` nor `Database` needs closing; the application owns its pool. Workers should be [idempotent](https://riverqueue.com/docs/reliable-workers):
process failure can cause an attempt to run again after its external effects
have succeeded.

## Inserting many jobs

[Bulk insertion](https://riverqueue.com/docs/inserting-many-jobs) inserts a list atomically.
For an existing JDBC transaction, use the overload taking a connection and a
either form below. Homogeneous batches retain their argument type. Mixed batches
use submissions with their own kind and options, and return `Job<JsonNode>` values.

```java
var inserted = client.insertMany(email, List.of(
    new SendEmail("one@example.com", "Welcome"),
    new SendEmail("two@example.com", "Welcome")));

record GenerateReport(long accountId) {}
var report = JobType.of("generate_report", GenerateReport.class);
var mixed = client.insertMany(List.of(
    email.submission(new SendEmail("one@example.com", "Welcome")),
    report.submission(new GenerateReport(42), InsertOptions.builder().queue("reports").build())));
```

## Reading typed jobs

`client.get(id)` returns `Job<JsonNode>` because an ID does not identify a Java
argument class. Supply a job type when it is known:

```java
Job<SendEmail> job = client.get(jobId, email);
System.out.println(job.args().address());
```

Typed retrieval checks the kind and decodes the arguments. Insertion and
`context.complete(connection)` also retain their `Job<A>` argument type.
See the [API comparison](API.md) for the Go mapping and prerelease API changes.

## Job retries

A thrown exception triggers [retries](https://riverqueue.com/docs/job-retries)
until `maxAttempts` is exhausted, when the job becomes discarded. The default
policy uses River's quartic backoff with jitter. Configure a different policy
on the worker builder, or explicitly retry an existing job with `client.retry(id)`.
A retry policy receives the full job snapshot before the current failure is
appended to `errors`; use `job.errors().size() + 1` to include that failure.

```java
var retryingEmail = email.withDefaults(InsertOptions.builder().maxAttempts(10).build());
var workerConfig = client.workers()
    .queue("default", 10)
    .retryPolicy(job -> Duration.ofSeconds(Math.min(300, 5L * (job.errors().size() + 1))))
    .handle(retryingEmail, context ->
        mailer.send(context.args().address(), context.args().subject()));
client.insert(retryingEmail, new SendEmail("hello@example.com", "Welcome"));
```

Call `workerConfig.start()` and keep the runtime open as in the first example.
Job-type defaults apply when inserting through that `JobType`; another producer
must set its own compatible options.

## Features

These sections follow the non-Pro entries in the River documentation's
[Features sidebar](https://riverqueue.com/docs). Snippets are independent and
reuse `client` and `database` from the quick start, with imports from
`com.riverqueue`, `java.time`, and `java.util`. Handler examples configure a builder;
start it after registering your handlers. `mailer`, `accounts`, and `deliveries`
are application services, `applicationDataSource` is your JDBC connection pool,
`application.awaitStop()` waits for your service to stop, and `jobId` is an
existing job's ID.

```java
var email = JobType.of("send_email", SendEmail.class);
var workerConfig = client.workers().queue("default", 10);
```

### Cancelling jobs

[Cancel](https://riverqueue.com/docs/cancelling-jobs) an enqueued or running job
with `client.cancel(id)`. Running attempts receive a cooperative cancellation
signal. A handler can cancel itself permanently with `context.cancel(reason)`;
this ends the attempt without further retries.

```java
client.cancel(jobId);

workerConfig.handle(email, context -> {
  context.checkCancelled();
  if (context.args().address().isBlank()) context.cancel("Missing address");
  mailer.send(context.args().address(), context.args().subject());
});
```

Long-running handlers should check cancellation between operations and use
interruptible I/O. `context.awaitCancellation()` waits for the signal.

### Getting the client within workers

[`context.client()`](https://riverqueue.com/docs/context-client) gives a handler
its client, including configured extensions. Use its transaction overloads when
enqueueing follow-up work that must commit with other writes or job completion.

```java
record Welcome(String address) {}
var welcome = JobType.of("welcome", Welcome.class);
workerConfig.handle(welcome, context ->
    context.client().insert(email, new SendEmail(context.args().address(), "Welcome")));
```

### Error and panic handling

Java exceptions and uncaught `Error`s are handled as failed attempts; River
persists failures and retries according to policy. For
[application error reporting](https://riverqueue.com/docs/error-handling), attach
an `Extension.afterWork` hook. `Workers.Builder.errorHandler` separately receives
runtime failures, such as database or subscriber errors.

```java
var monitoredClient = client.withExtension(new Extension() {
  @Override
  public void afterWork(WorkContext<?> context, Throwable failure) {
    if (failure != null) {
      System.getLogger("jobs").log(System.Logger.Level.ERROR,
          "Job " + context.job().id() + " failed", failure);
    }
  }
});
var workerConfig = monitoredClient.workers()
    .queue("default", 10)
    .errorHandler(failure ->
        System.getLogger("river").log(System.Logger.Level.ERROR, "Runtime failure", failure))
    .handle(email, context -> mailer.send(context.args().address(), context.args().subject()));
```

Hooks also see the control exceptions used for snoozing and cancellation. Keep
reporting hooks fast and avoid throwing from them.

### Job-persisted logging

[Job logs](https://riverqueue.com/docs/job-logging) live in `river:log` metadata
as an array of `{attempt, log}` objects. Java has no bundled equivalent of Go's
`riverlog` middleware yet. You can explicitly write that shared format; this
example appends a fixed message and retains the last ten attempt entries.

```java
workerConfig.handle(email, context -> {
  var logs = new ArrayList<Object>();
  context.job().metadata().path("river:log").forEach(logs::add);
  try {
    mailer.send(context.args().address(), context.args().subject());
  } finally {
    logs.add(Map.of("attempt", context.job().attempt(), "log", "Email attempt finished\n"));
    context.metadata("river:log", logs.subList(Math.max(0, logs.size() - 10), logs.size()));
  }
});
```

Metadata is saved when the attempt finishes, including on failure. General-purpose
logging integrations should also bound each entry's byte size; this snippet does
not capture SLF4J or `System.Logger` output automatically.

### Multiple queues

[Queues](https://riverqueue.com/docs/multiple-queues) isolate concurrency for
different workloads. Set a queue at insertion and configure its worker count.
The count is local to each runtime; it is not a cluster-wide concurrency limit.

```java
var workerConfig = client.workers()
    .queue("default", 10)
    .queue("mail", 4)
    .handle(email, context -> mailer.send(context.args().address(), context.args().subject()));
client.insert(email, new SendEmail("hello@example.com", "Welcome"),
    InsertOptions.builder().queue("mail").build());
```

A running runtime also supports `workers.addQueue(name, count)` and
`workers.removeQueue(name)`, which drains that queue's active attempts.
Use `fetchOnlyKnownKinds(true)` when sharing queues with other language workers
that handle different kinds.

### Pausing queues

[Pausing](https://riverqueue.com/docs/pausing-queues) stops new fetches across
clients sharing the database; active attempts may finish. Inserts continue while
a queue is paused. Queues must already exist, for example after starting workers.

```java
client.queues().pause("mail");
// Resume after the maintenance window.
client.queues().resume("mail");
```

Both operations also accept a JDBC transaction as their first argument.

### Periodic and cron jobs

Register [periodic jobs](https://riverqueue.com/docs/periodic-jobs) on every
potential leader with matching schedules and arguments. Only the elected leader
enqueues them. The final boolean enables insertion whenever that client becomes
leader. Five-field cron expressions support explicit time zones.

```java
var workerConfig = client.workers()
    .queue("default", 10)
    .handle(email, context -> mailer.send(context.args().address(), context.args().subject()))
    .periodic("hourly-status", Schedule.every(Duration.ofHours(1)), email,
        new SendEmail("ops@example.com", "Hourly status"), InsertOptions.defaults(), true)
    .periodic("daily-report", Schedule.cron("CRON_TZ=America/Chicago 0 9 * * *"), email,
        new SendEmail("ops@example.com", "Daily report"), InsertOptions.defaults(), false);
```

OSS schedules are kept in memory and can miss occurrences during downtime or
leader changes. Periodic definitions are fixed when the Java runtime starts;
restart it to change them.

### Recorded output

[Output](https://riverqueue.com/docs/recorded-output) is a JSON value stored under
`metadata.output`. Set it in a worker with `context.output`; other languages and
River UI can read the result once the attempt finishes.

```java
workerConfig.handle(email, context -> {
  mailer.send(context.args().address(), context.args().subject());
  context.output(Map.of("delivered", true, "address", context.args().address()));
});

var output = client.get(jobId).metadata().path("output");
```

### Resumable jobs

[Named steps](https://riverqueue.com/docs/resumable-jobs) let retries skip work
completed before an earlier failure. Keep step names and ordering stable across
deployments and languages; anything outside a step runs on every attempt.

```java
workerConfig.handle(email, context -> {
  context.step("send", () ->
      mailer.send(context.args().address(), context.args().subject()));
  context.step("record-delivery", () -> deliveries.record(context.job().id()));
});
```

For loops, `context.stepWithCursor(name, cursor -> ...)` receives a saved JSON
cursor, and `context.cursor(value)` updates it. Java buffers step progress until
the attempt ends; it does not yet expose Go's transactional checkpoint helpers.
A process crash can lose buffered progress, so steps must remain idempotent.

### Scheduled jobs

[Schedule a job](https://riverqueue.com/docs/scheduled-jobs) by supplying an
`Instant`. The leader's scheduler makes it available once due; execution also
depends on scheduler cadence and queue capacity.

```java
client.insert(email, new SendEmail("hello@example.com", "Reminder"),
    InsertOptions.builder()
        .scheduledAt(Instant.now().plus(Duration.ofHours(3)))
        .build());
```

### Snoozing jobs

[Snooze](https://riverqueue.com/docs/snoozing-jobs) when work should wait without
consuming a retry. `context.snooze` ends the current attempt and schedules the
same job to run again; code after the call does not execute on that attempt.

```java
workerConfig.handle(email, context -> {
  if (!mailer.isReady()) context.snooze(Duration.ofMinutes(5));
  mailer.send(context.args().address(), context.args().subject());
});
```

### Subscriptions

[Subscribe](https://riverqueue.com/docs/subscriptions) to events from a running
`Workers` instance. Callbacks are process-local, run synchronously, and should
finish quickly. They are not a durable stream of all events in the cluster.
Close the subscription to unregister it.

```java
try (var subscription = workers.subscribe(
    event -> System.out.println("Completed job " + event.job().id()),
    Workers.EventKind.JOB_COMPLETED)) {
  application.awaitStop();
}
```

Event kinds are `Workers.EventKind` values. Omit the filter to receive every kind.
Job events carry `event.job()`; queue events carry `event.queue()` and a null job.
The enum's `value()` retains River's lower-case protocol spelling.

### Testing

Use [isolated database tests](https://riverqueue.com/docs/testing) with JUnit and
signals instead of sleeps. This complete test method uses a fresh SQLite file,
subscribes before inserting, and waits for committed completion. Add the SQLite
JDBC driver and JUnit Jupiter to the test classpath.

```java
@org.junit.jupiter.api.Test
void completes(@org.junit.jupiter.api.io.TempDir java.nio.file.Path directory) throws Exception {
  record Echo(String message) {}
  var echo = JobType.of("echo", Echo.class);
  var database = Database.connect("jdbc:sqlite:" + directory.resolve("river.db"));
  new Migrator(database).migrate();
  var client = new Client(database);
  var completed = new java.util.concurrent.CompletableFuture<Workers.Event>();

  try (var workers = client.workers()
      .queue("default", 1)
      .handle(echo, context -> context.output(context.args().message()))
      .start();
      var subscription = workers.subscribe(event -> {
        if (event.kind() == Workers.EventKind.JOB_COMPLETED) completed.complete(event);
      })) {
    var inserted = client.insert(echo, new Echo("hello"));
    var event = completed.get(5, java.util.concurrent.TimeUnit.SECONDS);
    org.junit.jupiter.api.Assertions.assertEquals(inserted.job().id(), event.job().id());
    org.junit.jupiter.api.Assertions.assertEquals(Job.State.COMPLETED, event.job().state());
    org.junit.jupiter.api.Assertions.assertEquals("hello", event.job().metadata().path("output").asString());
  }
}
```

`new Client(database, clock)` accepts an injected `Clock` for deterministic insert
timestamps. The worker runtime uses wall time. Java does not currently provide
a separate equivalent of Go's `rivertest.Worker` harness.

### Transactional job completion

[Complete a job atomically](https://riverqueue.com/docs/transactional-job-completion)
with application writes using `context.complete(connection)`. If the transaction
rolls back, neither change survives. Return from the handler after committing;
external effects such as sending email cannot participate in a JDBC transaction.

```java
workerConfig.handle(email, context -> {
  context.transaction(connection -> {
    deliveries.record(connection, context.job().id());
    context.output(Map.of("recorded", true));
    context.complete(connection);
    return null;
  });
});
```

### Unique jobs

[Uniqueness](https://riverqueue.com/docs/unique-jobs) can combine arguments,
queue, time period, and job states. A duplicate insert returns the existing job
with `uniqueSkippedAsDuplicate()` set. The key format is shared with Go and the
other ports.

```java
var options = InsertOptions.builder()
    .unique(Unique.args().perQueue().per(Duration.ofHours(1)))
    .build();
var inserted = client.insert(email, new SendEmail("hello@example.com", "Welcome"), options);
boolean duplicate = inserted.uniqueSkippedAsDuplicate();
```

A period is an aligned time bucket, not a sliding delay since the last insertion.
Use `email.uniqueBy(List.of(List.of("address")))` to hash only selected JSON
argument paths, and insert with that returned `JobType`. Default unique states
include completed jobs, so a retained completed row can still prevent insertion.

### Work functions

[Workers can be functions](https://riverqueue.com/docs/work-functions). Java's
`Workers.Handler<A>` accepts lambdas and method references, keeping arguments,
registration, and behavior together without a worker subclass for every kind.

```java
Workers.Handler<SendEmail> sendEmail = context ->
    mailer.send(context.args().address(), context.args().subject());
workerConfig.handle(email, sendEmail);
```

## SQLite

Use a [file database](https://riverqueue.com/docs/sqlite) shared by every
process. Connections opened by River enable WAL, foreign keys, and a five-second
busy timeout. Configure application-created connections equivalently.

```java
var database = Database.connect("jdbc:sqlite:/var/lib/myapp/river.db");
new Migrator(database).migrate();
var client = new Client(database);
```

SQLite permits one writer at a time: keep application transactions short, and
insert using that transaction's connection. Do not open another writing
connection while holding an application write transaction. In-memory databases
with independent connections are not suitable for this runtime.

## Alternate schema

For PostgreSQL, select an [alternate schema](https://riverqueue.com/docs/alternate-schema)
on the `Database` used by both migrations and clients. All collaborating clients
must target the same schema. The migrator creates it if needed.

```java
var jobsDatabase = database.withSchema("background_jobs");
new Migrator(jobsDatabase).migrate();
var jobsClient = new Client(jobsDatabase);
```

## Stopping workers

For [graceful stopping](https://riverqueue.com/docs/graceful-shutdown),
`workers.stop()` stops fetching and waits for active attempts. `close()` does the
same, making try-with-resources convenient. `workers.stopAndCancel()` also
requests cancellation of active attempts. Both wait for handlers to return.
A graceful-stop timeout leaves attempts running; call `stop()` again to keep
waiting or `stopAndCancel()` to request cancellation.

```java
var workers = client.workers()
    .queue("default", 10)
    .stopTimeout(Duration.ofSeconds(30))
    .handle(email, context -> mailer.send(context.args().address(), context.args().subject()))
    .start();
try {
  application.awaitStop();
} finally {
  workers.stop();
}
```

The timeout bounds each stop wait and a queue-removal drain; a timeout throws a
`RiverException`. Cancellation is cooperative, followed by interruption after
the configured stuck threshold. The JVM cannot forcibly terminate code that
ignores interrupts; handlers must cooperate with cancellation.

## Leader election and maintenance

[Leader election](https://riverqueue.com/docs/leader-election) coordinates Java
and other River clients through the database. The leader runs periodic insertion
and [maintenance](https://riverqueue.com/docs/maintenance-services): scheduling
due jobs, rescuing abandoned attempts, cleaning expired jobs and queues, and
reindexing configured PostgreSQL indexes. Leadership is enabled by default.

```java
var workerConfig = client.workers()
    .queue("default", 10)
    .retention(Duration.ofDays(1), Duration.ofDays(1), Duration.ofDays(7))
    .handle(email, context -> mailer.send(context.args().address(), context.args().subject()));
```

Retention arguments are cancelled, completed, and discarded durations, in that
order. Negative durations retain that state indefinitely. If a runtime uses
`leadership(false)`, another eligible client must run maintenance; periodic
registrations require leadership. `workers.isLeader()` reports the local status.

## Development

```sh
mvn test
mvn spotless:apply
mvn verify

RIVER_CONFORMANCE_DATABASE_URL=postgres://localhost/river_java_conformance \
  python3 conformance/bin/run.py postgres
python3 conformance/bin/run.py sqlite
```

Conformance requires Go and uses the pinned upstream revision recorded in
`conformance/reference-revision`. The runner extracts that revision into ignored
build storage and registers the Java candidate there. `--refresh` fetches the
current reference branch; `--reference /path/to/checkout` uses an existing
harness. Use only disposable databases: the upstream harness resets job tables.

See [intentional differences](DIFFERENCES.md) for the Java API and lifecycle
choices. Passing a conformance profile demonstrates the scenarios in that
profile; it is not a claim about untested behavior or performance.

The full peer matrix uses `multi` with `RIVER_CONFORMANCE_PEER_FILE` set to a
colon-separated list of Rust and JS candidate descriptors, and
`RIVERQUEUE_JS_ROOT` pointing to the JS checkout. `multi-soak` also requires
`RIVER_CONFORMANCE_MULTI_ENGINE_SOAK_DURATION=5m` (or longer). The upstream
harness currently has a PostgreSQL soak; SQLite endurance validation repeats
`TestMultiEngineSQLiteConformance` with `-count=5`.

`python3 bin/import-reference.py --check /path/to/reference` verifies vendored
migrations and fixtures. Omit `--check` to import them; the reference revision
and adapter profile declarations must be reviewed when updating upstream.

See [validation results](VALIDATION.md) for the tested revisions and remaining
limitations.
