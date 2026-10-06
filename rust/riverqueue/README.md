# riverqueue

`riverqueue` is the Rust and Tokio client for [River](https://riverqueue.com),
a fast and reliable background job system backed by PostgreSQL or SQLite. It
shares River's database schema and job protocol with River for Go, so Rust
and Go services can insert and work jobs in the same database.

This crate is a pre-release preview. Each release matches the River for Go
release with the same minor version; the
[mixed deployment guide](https://docs.rs/riverqueue/latest/riverqueue/guide/mixed_deployments/index.html)
covers running both against one database.

## Installation

```toml
[dependencies]
riverqueue = "0.50.0-alpha.1"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal"] }
```

The quick start below needs exactly these. River runs on Tokio, and job
arguments derive Serde's `Serialize` and `Deserialize`. The minimum supported
Rust version is 1.95.

| Feature | Default | Enables |
|---|---|---|
| `postgres` | yes | PostgreSQL through SQLx |
| `sqlite` | no | SQLite 3.45 or newer through SQLx |
| `chrono-tz` | no | IANA zone names such as `America/New_York` in cron `CRON_TZ=` and `TZ=` prefixes |

For SQLite alone, use
`riverqueue = { version = "0.50.0-alpha.1", default-features = false, features = ["sqlite"] }`.

River's API uses types from SQLx (pools and transactions), Chrono
(timestamps), `serde_json` (metadata, outputs, and other JSON values), and
`tokio-util` (the worker's `CancellationToken`). The crate re-exports each one
as `riverqueue::sqlx`, `riverqueue::chrono`, `riverqueue::serde_json`, and
`riverqueue::tokio_util`. Use the re-exports, or depend on versions
compatible with River's (SQLx 0.9, Chrono 0.4, `serde_json` 1, and
`tokio-util` 0.7), so the types match. River doesn't choose a TLS
implementation for SQLx; enable one of SQLx's TLS features in your own SQLx
dependency if your database connections use TLS.

## Quick start

Define serializable arguments, register an async function or a [`Worker`],
apply River's migrations, and start a client:

```rust,no_run
use riverqueue::migrate::PostgresMigrator;
use riverqueue::sqlx::PgPool;
use riverqueue::{
    BoxError, Client, Job, JobArgs, QueueConfig, WorkContext, WorkOutcome, WorkerRegistry,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "send_email")]
struct SendEmail {
    address: String,
}

async fn send_email(
    context: WorkContext,
    job: Job<SendEmail>,
) -> Result<WorkOutcome, BoxError> {
    println!("sending email to {}", job.args.address);
    context.record_output(serde_json::json!({"delivered": true}))?;
    Ok(WorkOutcome::Complete)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    // Create or upgrade River's tables. Applications often run
    // `riverqueue migrate-up` from `riverqueue-cli` at deploy time instead.
    PostgresMigrator::new(pool.clone()).migrate_up().await?;

    let mut workers = WorkerRegistry::new();
    workers.register_fn(send_email)?;

    let client = Client::builder(pool)
        .workers(workers)
        .queue("default", QueueConfig::new(10))
        .build()?;
    // Work jobs until Ctrl-C, then stop fetching and let running jobs finish.
    let mut run = client.start_with_graceful_shutdown(async {
        let _ = tokio::signal::ctrl_c().await;
    })?;

    client
        .insert(SendEmail {
            address: "person@example.com".to_owned(),
        })
        .await?;

    run.wait().await?;
    Ok(())
}
```

Apply migrations before any client starts, and start clients inside a Tokio
runtime. `Client::start` returns a `RunHandle`: await `wait`, `stop` (a
soft stop that lets running jobs finish), or `stop_and_cancel` (which cancels
them). `RunHandle::stopper` returns a cloneable `Stopper` for stopping the
client from another task, such as a signal handler. The handle controls the
running client: dropping every `Client` clone doesn't stop it, dropping the
handle requests a hard stop, and `RunHandle::detach` leaves the client running
unsupervised.

## Inserting jobs

`Client::insert(args)` inserts a job with its type's default options, which
come from `JobArgs::default_insert_opts`. A call can override only the options
it needs:

```rust,no_run
use riverqueue::{Client, InsertOpts, JobArgs};
use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, JobArgs, Serialize)]
#[river(kind = "send_email")]
struct SendEmail {
    address: String,
}

async fn enqueue_urgent(client: &Client) -> Result<(), riverqueue::Error> {
    client
        .insert(SendEmail { address: "urgent@example.com".to_owned() })
        .opts(
            InsertOpts::default()
                .with_queue("critical")
                .with_priority(1)
                .with_max_attempts(8),
        )
        .await?;
    Ok(())
}
```

An option set on the call wins over the job type's default, which wins over
the client's default, which wins over River's. `insert_many` inserts many jobs
of one kind atomically and returns results in input order; `insert_batch` does
the same for jobs of different kinds.

Chain `.tx(&mut transaction)` onto an insertion, or onto any request from
`client.jobs()` or `client.queues()`, to run it in the same SQL transaction as
application writes. Jobs become visible, and clients are notified, only when
the transaction commits. Begin transactions with
`riverqueue::database::begin_postgres(&pool)`, or on SQLite with
`riverqueue::database::begin_sqlite_write(&pool)`, which uses
`BEGIN IMMEDIATE` so a transaction that reads before it writes can't fail with
`SQLITE_BUSY_SNAPSHOT`. Both begin on a separate task, so they're safe to
abandon partway, for example in a `select!` or a timeout; SQLx's own
`pool.begin()` isn't, and can return a connection to the pool still inside a
transaction.

River runs these requests directly in your transaction, without a savepoint.
When one returns an error it may have already written part of its work there,
so roll the transaction back. To recover from the error and continue the
transaction instead, open your own savepoint before the request.

## Managing jobs and queues

`client.jobs()` gets, lists, cancels, retries, updates, and deletes persisted
jobs. `client.queues()` gets, lists, pauses, resumes, and updates the queue
records every client shares, and `client.local_queues()` changes which queues
this client works while it runs. Job and queue requests run when awaited and
take `.tx(&mut transaction)` like insertions:

```rust,no_run
use riverqueue::sqlx::PgPool;
use riverqueue::{Client, JobListParams, JobState, QueueConfig, QueueSelector};

async fn maintain(client: &Client, pool: &PgPool) -> Result<(), riverqueue::Error> {
    let page = client
        .jobs()
        .list(JobListParams::default().states([JobState::Retryable]).limit(50))
        .await?;
    for job in &page.jobs {
        client.jobs().retry(job.id).await?;
    }

    let mut transaction = riverqueue::database::begin_postgres(pool).await?;
    client.queues().pause(QueueSelector::All).tx(&mut transaction).await?;
    transaction.commit().await?;

    client.local_queues().add("reports", QueueConfig::new(2))?;
    Ok(())
}
```

## Worker outcomes and cancellation

An `Ok(WorkOutcome::Complete)` completes a job. `Snooze` reschedules without
consuming an attempt, `Discard` finalizes without another retry, and `Cancel`
finalizes as cancelled. A worker error is passed through its retry policy until
the maximum attempt count is reached.

The `WorkContext` cancellation token is triggered by a job timeout, remote job
cancellation, or client stop. Workers should select or check cancellation at
natural await points, and return `WorkCancelled` when they stop because of it:

```rust
use riverqueue::{Job, JobArgs, WorkCancelled, WorkContext, WorkOutcome};
use serde::{Deserialize, Serialize};

#[derive(Clone, Deserialize, JobArgs, Serialize)]
#[river(kind = "build_report")]
struct BuildReport {}

async fn build_report(
    context: WorkContext,
    _job: Job<BuildReport>,
) -> Result<WorkOutcome, WorkCancelled> {
    tokio::select! {
        () = context.cancellation_token().cancelled() => Err(WorkCancelled),
        () = tokio::time::sleep(std::time::Duration::from_secs(1)) => {
            Ok(WorkOutcome::Complete)
        }
    }
}
```

During a client's hard stop (`RunHandle::stop_and_cancel` or
`Stopper::stop_and_cancel`), a job whose worker returns `WorkCancelled`, anywhere in
its error's source chain, becomes available again without using up its
attempt. Any other error is recorded and consumes the attempt, and `Ok`
completes the job. After the configured stuck threshold, River can abort a
Tokio task that yields, which fails its attempt, but it can't stop CPU-bound
work or a blocking call already in progress.

Implement [`Worker`] when a kind needs a custom timeout or next-retry decision.
Use `WorkerRegistry::register_fn` for an async function or capturing closure.

## Events

Subscriptions are local observations, not a durable event stream. Subscribe
before starting a client to see events from its first jobs. Receivers are
bounded and report `EventRecvError::Lagged` with the number of dropped events,
and `EventReceiver` is also a `Stream`. Job events are sent after their
results are persisted, and independent jobs have no global completion order.

## Reliability features

- Unique jobs deduplicate by kind, encoded arguments or selected argument
  paths, queue, period, and job state. The derive macro follows Serde's
  serialization names and omits missing optional fields.
- Periodic jobs run on the elected leader and can be configured when the client
  is built or at runtime; stable IDs prevent duplicate registration.
  `CronSchedule` accepts standard five-field cron syntax and descriptors such
  as `@hourly` and `@every 90s`, evaluated in the process's local time zone
  unless another `CronTimeZone` is chosen. `CRON_TZ=` and `TZ=` prefixes
  naming IANA zones such as `America/New_York` need the `chrono-tz` feature,
  which bundles the time zone database; without it only `UTC`, `Local`, and
  `Etc/GMT±N` names parse.
- Resumable steps persist the last completed step and an optional cursor. Use
  the transactional checkpoint helpers when progress and business data must
  commit together.
- Insertion middleware wraps the insert-begin hooks, and work middleware wraps
  the work hooks, argument decoding, and the worker. See [`WorkMiddleware`]
  and [`Hook`].

## Leadership and maintenance

One client at a time holds a database lease and runs the leader-owned
services: the job scheduler, the stuck-job rescuer, the job and queue cleaners,
the periodic job enqueuer, the PostgreSQL reindexer, and the SQLite
notification cleaner. Losing the lease or stopping the client stops them
immediately.

The default client ID combines the host name, the creation time, and a random
suffix; set a stable `id` only when it's unique per process.

The rescuer considers a job stuck after `rescue_after`, which defaults to one
hour, or to the job timeout plus one hour when a job timeout is configured, and
must not be shorter than the job timeout. The leader discards stuck jobs of
kinds its own worker registry doesn't know, so clients that share a database
should register the same kinds. A client that doesn't know every kind can be
built with `ClientBuilder::without_leader_election`: it works its queues but
never becomes leader, so at least one other client must stay eligible.

Periodic jobs are scheduled from the time each term begins, and jobs with
`PeriodicJobOpts::with_run_on_start(true)` are inserted once per term gained.
An occurrence whose insert fails is logged and skipped.

## Database support

Pass an SQLx pool, or a `PostgresDatabase` or `SqliteDatabase` with options,
to `Client::builder`. Both backends implement the same job and queue behavior,
and `Client` isn't generic over the database, so backend types don't reach
workers, contexts, or extensions.

SQLite needs the `sqlite` feature, and version 3.45 or newer, since River
stores JSON with SQLite's JSONB functions. River uses the caller's pool as configured and doesn't change
connection pragmas. For a file database, enable WAL and a busy timeout so a
short writer collision waits instead of failing. A private `:memory:` database
belongs to one connection, so limit the pool to one connection or use a
shared-cache URI:

```rust,no_run
use std::{str::FromStr, time::Duration};

use riverqueue::Client;
use riverqueue::sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};

async fn sqlite_client() -> Result<Client, Box<dyn std::error::Error>> {
    let options = SqliteConnectOptions::from_str("sqlite://river.db")?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(Duration::from_secs(5));
    let pool = SqlitePoolOptions::new()
        .max_connections(5)
        .connect_with(options)
        .await?;
    Ok(Client::builder(pool).build()?)
}
```

## Modules

- [`job`]: arguments, insertion options, persisted rows, outcomes, and unique
  job configuration.
- [`encoding`]: the JSON encoding River uses for job arguments, which keeps
  unique keys identical across Rust and Go.
- [`worker`]: typed workers, function registration, cancellation, outputs,
  and resumable work.
- [`event`]: event payloads and bounded subscriptions.
- [`queue`] and [`query`]: queue records and job list filters and cursors.
- [`periodic`]: schedules and runtime periodic job registration.
- [`extension`]: hooks, middleware, policies, and metrics.
- [`database`]: PostgreSQL and SQLite database options, and the transactions
  River's `.tx` methods accept.
- [`error`]: structured errors that keep their sources.
- [`protocol`]: wire values such as notification topics and unique keys, for
  tools that work with River's tables directly.

Setters follow two conventions. Builders and request parameters, which
exist only to be passed on (`ClientBuilder`, `JobListParams`,
`JobUpdateParams`), take plain setter names such as `queue(..)` and
`limit(..)`. Configuration values that also expose each setting through a
same-named getter (`InsertOpts`, `UniqueOpts`, `PeriodicJobOpts`,
`QueueConfig`, `MaintenanceConfig`, `SubscribeConfig`, `PostgresDatabase`,
`PostgresMigrator`) use `with_*` methods that return the value with one
setting changed, like `PathBuf::with_extension`, so `UniqueOpts::by_args`
reads what `UniqueOpts::with_by_args` sets. Durations that can be disabled
are explicit, as in `ClientBuilder::without_job_timeout` and
`Retention::Keep`.

The crate's `examples` directory has runnable programs for a basic worker,
graceful shutdown, cancellation, transactional enqueueing and completion,
unique and periodic jobs, events, custom PostgreSQL schemas, SQLite, and a
Rust and Go service sharing one database. The
[River documentation](https://riverqueue.com/docs) explains queueing concepts.

## Benchmarking

The [`riverqueue-cli`](https://crates.io/crates/riverqueue-cli) crate provides
`riverqueue bench`, a benchmark for development databases. It truncates the
selected River job table, so use a disposable database, and reports periodic
throughput plus final throughput and p95 latency. Run
`riverqueue bench --help` for its options.

[`Hook`]: https://docs.rs/riverqueue/latest/riverqueue/trait.Hook.html
[`WorkMiddleware`]: https://docs.rs/riverqueue/latest/riverqueue/trait.WorkMiddleware.html
[`Worker`]: https://docs.rs/riverqueue/latest/riverqueue/trait.Worker.html
[`database`]: https://docs.rs/riverqueue/latest/riverqueue/database/index.html
[`encoding`]: https://docs.rs/riverqueue/latest/riverqueue/encoding/index.html
[`error`]: https://docs.rs/riverqueue/latest/riverqueue/error/index.html
[`event`]: https://docs.rs/riverqueue/latest/riverqueue/event/index.html
[`extension`]: https://docs.rs/riverqueue/latest/riverqueue/extension/index.html
[`job`]: https://docs.rs/riverqueue/latest/riverqueue/job/index.html
[`periodic`]: https://docs.rs/riverqueue/latest/riverqueue/periodic/index.html
[`protocol`]: https://docs.rs/riverqueue/latest/riverqueue/protocol/index.html
[`query`]: https://docs.rs/riverqueue/latest/riverqueue/query/index.html
[`queue`]: https://docs.rs/riverqueue/latest/riverqueue/queue/index.html
[`worker`]: https://docs.rs/riverqueue/latest/riverqueue/worker/index.html
