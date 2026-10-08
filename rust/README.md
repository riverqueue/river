# River for Rust

River is a fast, reliable background job system backed by Postgres or SQLite. Its Rust implementation uses Tokio and shares River's database schema and job protocol with the other languages, so services can insert and work jobs in the same database.

## Installation

```toml
[dependencies]
riverqueue = "0.3.0"
serde = { version = "1", features = ["derive"] }
serde_json = "1"
tokio = { version = "1", features = ["macros", "rt-multi-thread", "signal"] }
```

The quick start below uses these dependencies. River runs on Tokio, and job
arguments derive Serde's `Serialize` and `Deserialize`. The minimum supported
Rust version is 1.95.

| Feature | Default | Enables |
|---|---|---|
| `postgres` | yes | Postgres through SQLx |
| `sqlite` | no | SQLite 3.45 or newer through SQLx |
| `chrono-tz` | no | IANA zone names such as `America/New_York` in cron `CRON_TZ=` and `TZ=` prefixes |

For SQLite alone, use
`riverqueue = { version = "0.3.0", default-features = false, features = ["sqlite"] }`.

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

Set `DATABASE_URL` to your application's Postgres database. Define serializable job arguments, register a worker, apply migrations, and start a client:

```rust,no_run
use riverqueue::migrate::PostgresMigrator;
use riverqueue::sqlx::PgPool;
use riverqueue::{
    BoxError, Client, Job, JobArgs, QueueConfig, WorkContext, WorkOutcome, Workers,
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

    let mut workers = Workers::new();
    workers.add_fn(send_email)?;

    let client = Client::builder(pool)
        .workers(workers)
        .queue("default", QueueConfig::new(10))
        .build()?;
    // Work jobs until Ctrl-C, then stop fetching and let running jobs finish.
    let mut run = client.start_with_graceful_stop(async {
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

Run the application and press Ctrl-C to stop fetching jobs and let active workers finish. Apply migrations before starting any clients; production deployments can use the [`riverqueue` CLI](riverqueue-cli/README.md) as a separate deployment step.

A client without queues or workers can insert jobs without being started. Job kinds and serialized JSON fields must agree between languages sharing a job.

## Transactions and other features

Insert jobs in the same transaction as application data by chaining `.tx(&mut transaction)` onto `client.insert(args)`. The job becomes available only when that transaction commits. The [insertion guide](riverqueue/README.md#inserting-jobs) covers transaction helpers and error handling.

- [Job and queue administration](riverqueue/README.md#managing-jobs-and-queues), including cancellation, retries, and pausing queues.
- [Worker outcomes and cancellation](riverqueue/README.md#worker-outcomes-and-cancellation), including snoozing and graceful stopping.
- [Unique, periodic, and resumable jobs](riverqueue/README.md#reliability-features).
- [Event subscriptions](riverqueue/README.md#events) for logging and metrics.
- [Leader election and maintenance](riverqueue/README.md#leadership-and-maintenance) for scheduling, rescue, and cleanup.
- [Postgres and SQLite support](riverqueue/README.md#database-support) with caller-owned SQLx pools.

## Crates

| Crate | Purpose |
| --- | --- |
| [`riverqueue`](riverqueue/README.md) | Typed client, workers, job and queue administration, and maintenance |
| [`riverqueue-macros`](riverqueue-macros/README.md) | `#[derive(JobArgs)]` |
| [`riverqueue-migrate`](riverqueue-migrate/README.md) | River's database migrations |
| [`riverqueue-cli`](riverqueue-cli/README.md) | The `riverqueue` command for migrations and benchmarks |
| [`riverqueue-test`](riverqueue-test/README.md) | Fixtures and worker test helpers |

The Rust crates are versioned together, independently of River for Go.

## Documentation

See the [client guide](riverqueue/README.md), [API reference](https://docs.rs/riverqueue), and [mixed deployment guide](riverqueue/docs/mixed-deployments.md). The [examples](riverqueue/examples) cover workers, graceful stopping, transactions, unique and periodic jobs, events, custom schemas, and SQLite.

## Development

See [developing River for Rust](docs/development.md).
