//! Observes job outcomes through client events.
//!
//! ```sh
//! DATABASE_URL=postgres://localhost/river_example cargo run -p riverqueue --example events
//! ```
//!
//! Events describe what this client's workers did. A subscriber opts in to
//! each kind; a slow subscriber drops events rather than blocking workers.

use std::error::Error;

use riverqueue::{
    Client, Event, EventKind, InsertOpts, Job, JobArgs, JobEventKind, QueueConfig, WorkContext,
    WorkOutcome, WorkerRegistry, migrate::PostgresMigrator,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "charge_card")]
struct ChargeCard {
    amount_cents: i64,
}

#[derive(Debug, thiserror::Error)]
#[error("card declined")]
struct Declined;

async fn charge_card(_context: WorkContext, job: Job<ChargeCard>) -> Result<WorkOutcome, Declined> {
    if job.args.amount_cents > 10_000 {
        return Err(Declined);
    }
    Ok(WorkOutcome::Complete)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    PostgresMigrator::new(pool.clone()).migrate_up().await?;

    let mut workers = WorkerRegistry::new();
    workers.register_fn(charge_card)?;
    let client = Client::builder(pool)
        .workers(workers)
        .queue("default", QueueConfig::new(4))
        .build()?;
    // Subscribe before starting so no event is missed.
    let mut events = client.subscribe(&[EventKind::JobCompleted, EventKind::JobFailed])?;
    let run = client.start()?;

    let small = client.insert(ChargeCard { amount_cents: 500 }).await?;
    let large = client
        .insert(ChargeCard {
            amount_cents: 50_000,
        })
        // One attempt, so the failure is final and the example ends quickly.
        .opts(InsertOpts::default().with_max_attempts(1))
        .await?;

    let mut seen = 0;
    while seen < 2 {
        let Event::Job(event) = events.recv().await? else {
            continue;
        };
        if ![small.id(), large.id()].contains(&event.job.id) {
            continue;
        }
        let run_time = event
            .statistics
            .map(|statistics| statistics.run_duration)
            .unwrap_or_default();
        match event.kind {
            JobEventKind::Completed => {
                println!("job {} completed in {run_time:?}", event.job.id);
            }
            JobEventKind::Failed => {
                let error = event
                    .job
                    .errors
                    .last()
                    .map_or("", |error| error.error.as_str());
                println!(
                    "job {} failed ({:?}): {error}",
                    event.job.id, event.job.state
                );
            }
            _ => continue,
        }
        seen += 1;
    }

    run.shutdown().await?;
    Ok(())
}
