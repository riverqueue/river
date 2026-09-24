//! Enqueues jobs on a schedule.
//!
//! ```sh
//! DATABASE_URL=postgres://localhost/river_example cargo run -p riverqueue --example periodic
//! ```
//!
//! Only the elected leader enqueues periodic jobs. Configure the same periodic
//! jobs, with the same IDs, in every client that may become leader.

use std::{error::Error, time::Duration};

use riverqueue::{
    Client, CronSchedule, EventKind, IntervalSchedule, Job, JobArgs, PeriodicJob, PeriodicJobOpts,
    QueueConfig, WorkContext, WorkOutcome, WorkerRegistry, migrate::PostgresMigrator,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "refresh_cache")]
struct RefreshCache {}

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "nightly_report")]
struct NightlyReport {}

async fn refresh_cache(
    _context: WorkContext,
    _job: Job<RefreshCache>,
) -> Result<WorkOutcome, std::io::Error> {
    println!("refreshing cache");
    Ok(WorkOutcome::Complete)
}

async fn nightly_report(
    _context: WorkContext,
    _job: Job<NightlyReport>,
) -> Result<WorkOutcome, std::io::Error> {
    println!("building nightly report");
    Ok(WorkOutcome::Complete)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    PostgresMigrator::new(pool.clone()).migrate_up().await?;

    let mut workers = WorkerRegistry::new();
    workers.register_fn(refresh_cache)?;
    workers.register_fn(nightly_report)?;
    let client = Client::builder(pool)
        .workers(workers)
        .queue("default", QueueConfig::new(4))
        // Every 15 minutes, and once as soon as this client becomes leader.
        .periodic_job(PeriodicJob::with_options(
            IntervalSchedule::new(Duration::from_mins(15))?,
            || RefreshCache {},
            PeriodicJobOpts::new()
                .with_id("refresh_cache")
                .run_on_start(),
        ))
        // Standard five-field cron syntax, as in River Go: 02:30 UTC daily.
        .periodic_job(PeriodicJob::with_options(
            CronSchedule::parse("30 2 * * *")?,
            || NightlyReport {},
            PeriodicJobOpts::new().with_id("nightly_report"),
        ))
        .build()?;
    let mut completed = client.subscribe(&[EventKind::JobCompleted])?;
    let run = client.start()?;

    // Wait for the run-on-start job, then stop.
    loop {
        let event = completed.recv().await?;
        if event
            .as_job()
            .is_some_and(|event| event.job.kind == RefreshCache::KIND)
        {
            break;
        }
    }
    run.shutdown().await?;
    Ok(())
}
