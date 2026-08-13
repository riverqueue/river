# riverqueue

`riverqueue` is the native Rust and Tokio client for
[River](https://riverqueue.com), a fast and reliable background job system.
It uses the same database protocol as River Go so Rust and Go producers,
workers, migrators, and maintenance services can operate on one queue.

This crate is a pre-release preview matched to the exact Go revision in the
repository's compatibility manifest. The crates are prepared for packaging but
are not published by this project yet.

## Quick start

Define serializable arguments, register an async function or a [`Worker`], and
start a client:

```rust,no_run
use std::convert::Infallible;

use riverqueue::{
    Client, Job, JobArgs, QueueConfig, WorkContext, WorkOutcome, WorkerRegistry,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "send_email")]
struct SendEmail {
    address: String,
}

async fn send_email(
    context: WorkContext,
    job: Job<SendEmail>,
) -> Result<WorkOutcome, Infallible> {
    println!("sending email to {}", job.args.address);
    context
        .record_output(&serde_json::json!({"delivered": true}))
        .await
        .expect("static JSON is serializable");
    Ok(WorkOutcome::Complete)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    let mut workers = WorkerRegistry::new();
    workers.register_fn(send_email)?;

    let client = Client::builder(pool)
        .workers(workers)
        .queue("default", QueueConfig::new(10))
        .build()?;
    let run = client.start()?;

    client
        .insert(SendEmail {
            address: "person@example.com".to_owned(),
        })
        .await?;

    run.shutdown().await?;
    Ok(())
}
```

Run the matching `riverqueue-migrate` migrations before starting a producer or
worker. `Client::start` must run inside a Tokio runtime. Keep its `RunHandle`
and await `shutdown`, `shutdown_now`, or `wait`; dropping it requests immediate
cancellation, while `detach` explicitly leaves the runtime unsupervised.

## Inserting jobs

`Client::insert(args)` is the common path. Job-type defaults come from
`JobArgs::default_insert_opts`; client defaults and River defaults fill values
the job type leaves unspecified. A call can override only the fields it needs:

```rust,no_run
# use riverqueue::{Client, InsertOpts, JobArgs};
# use serde::{Deserialize, Serialize};
# use sqlx::PgPool;
# #[derive(Clone, Deserialize, JobArgs, Serialize)]
# #[river(kind = "send_email")]
# struct SendEmail { address: String }
# async fn example(client: Client) -> Result<(), riverqueue::Error> {
client
    .insert_with(
        SendEmail { address: "urgent@example.com".to_owned() },
        InsertOpts::default()
            .with_queue("critical")
            .with_priority(1)
            .with_max_attempts(8),
    )
    .await?;
# Ok(())
# }
```

The precedence is call override, job-type default, client default, then River
default. It is based on whether an option was supplied, not whether its value
happens to equal a default. `insert_many` preserves input/result order and is
atomic. PostgreSQL's `insert_many_fast` uses `COPY`, returns only a count, and
rejects the entire batch on a unique conflict.

Use `insert_tx` or `insert_tx_with` with `&mut transaction` to enqueue in the
same SQL transaction as application writes. Notifications become visible only
on commit, and jobs do not survive rollback. With a multi-connection SQLite
pool, start transactions that may write with
`pool.begin_with("BEGIN IMMEDIATE")`. SQLite's ordinary deferred `begin()` can
establish a read snapshot that cannot be upgraded after another connection
commits, producing `SQLITE_BUSY_SNAPSHOT` even when a busy timeout is set.
River starts its own SQLite writer transactions in immediate mode; callers
choose the mode of transactions passed to `_tx` methods.

## Worker outcomes and cancellation

An `Ok(WorkOutcome::Complete)` completes a job. `Snooze` reschedules without
consuming an attempt, `Discard` finalizes without another retry, and `Cancel`
finalizes as cancelled. A worker error is passed through its retry policy until
the maximum attempt count is reached.

The `WorkContext` cancellation token is triggered by a job timeout, remote job
cancellation, or client stop. Workers should select or check cancellation at
natural await points. After the configured stuck threshold, River can abort a
Tokio task that yields, but Rust cannot forcibly stop arbitrary CPU work or an
already-running blocking call.

Implement [`Worker`] when a kind needs a custom timeout or next-retry decision.
Use `WorkerRegistry::register_fn` for the common async function or capturing
closure case. Worker error types remain inspectable at the typed boundary;
extension and database errors preserve their original source chains.

## Events and backpressure

Subscriptions are local observations, not a durable event stream. Subscribe
before starting a client if startup races matter. `Event` has separate job and
queue variants, so payloads cannot represent an invalid kind/data combination.
Receivers are bounded and report `EventRecvError::Lagged` with the dropped
count. Job events are sent only after persistence commits, but independent jobs
have no global completion order.

## Reliability features

- Unique jobs can hash kind, encoded argument paths, queue, period, and active
  states. The derive macro follows Serde's serialization names. Missing optional
  fields are omitted to match River Go.
- Periodic jobs are leader-owned and can be configured statically or at
  runtime. Stable IDs prevent duplicate registration.
- Resumable steps persist the last completed step and optional cursor. Use the
  transactional checkpoint helpers when progress and business data must commit
  together.
- Completion, cancellation, retry, snooze, rescue, queue state, and reserved
  metadata transitions match River Go and are exercised by the shared
  conformance harness.
- Hooks and middleware run in documented order around insertion and work.
  Exact-version companion crates use a hidden, source-preserving extension
  seam; it is not a third-party driver API.

## Database support

River accepts only built-in, sealed database backends rather than exposing a
user-implemented SQL dialect trait. PostgreSQL and SQLite implement the same
public job and queue protocol. The public database shape is non-generic so
backend types do not leak through workers, contexts, plugins, or ordinary
client code. Each exact-version companion operation uses the selected backend
or returns a structured unsupported-backend error. MySQL can be added as
another built-in backend without redesigning the public client API.

Database-specific behavior stays behind each backend: PostgreSQL uses schemas,
`LISTEN`/`NOTIFY`, advisory locking, and `COPY`; SQLite uses its canonical River
schema, serialized writer transactions, and durable notification-outbox
polling. Capability differences must be explicit rather than silently changing
job semantics.

SQLite requires version 3.45 or newer because River stores JSON using SQLite's
JSONB functions. The client uses the caller-owned SQLx pool and does not change
connection pragmas. For a file database, enable WAL and a busy timeout so a
short writer collision waits instead of immediately failing. A private
`:memory:` database belongs to one connection, so either limit the pool to one
connection or deliberately use a shared-cache URI:

```rust,no_run
use std::{str::FromStr, time::Duration};

use riverqueue::Client;
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};

# async fn example() -> Result<(), Box<dyn std::error::Error>> {
let options = SqliteConnectOptions::from_str("sqlite://river.db")?
    .create_if_missing(true)
    .journal_mode(SqliteJournalMode::Wal)
    .busy_timeout(Duration::from_secs(5));
let pool = SqlitePoolOptions::new()
    .max_connections(5)
    .connect_with(options)
    .await?;
let client = Client::builder(pool).build()?;
# let _ = client;
# Ok(())
# }
```

## Modules

- [`job`] — arguments, insertion options, persisted rows, outcomes, and unique
  job configuration.
- [`worker`] — typed workers, function registration, cancellation, outputs,
  and resumable work.
- [`event`] — valid event payloads and bounded subscriptions.
- [`queue`] and [`query`] — queue records and storage filters/cursors.
- [`periodic`] — schedules and dynamic periodic-job registration.
- [`extension`] — hooks, middleware, policies, and metrics.
- [`database`] — sealed built-in backend source and executor types.
- [`error`] — structured, source-preserving public errors.

The workspace has complete examples for ordinary workers, cancellation,
transactions, custom PostgreSQL schemas, and migrations. The higher-level
[River documentation](https://riverqueue.com/docs) explains queueing concepts;
until the website becomes language-aware, Rust API details live in this
crate's rustdoc and examples.
The repository's
[Rust API design record](https://github.com/riverqueue/river/blob/master/docs/rust-api-design.md)
captures the longer-term compatibility and API decisions behind this crate.

## Benchmarking

Installing this crate also provides `riverqueue bench`, a destructive
development-database benchmark analogous to Go's `river bench`. It truncates
the selected River job table and reports periodic throughput plus final
throughput and p95 latency. Run `riverqueue bench --help` and use a disposable
database.

[`Client::start`]: crate::Client::start
[`Worker`]: crate::Worker
[`database`]: crate::database
[`error`]: crate::error
[`event`]: crate::event
[`extension`]: crate::extension
[`job`]: crate::job
[`periodic`]: crate::periodic
[`query`]: crate::query
[`queue`]: crate::queue
[`worker`]: crate::worker
