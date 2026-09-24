//! Works one job and stops.
//!
//! ```sh
//! DATABASE_URL=postgres://localhost/river_example cargo run -p riverqueue --example basic_worker
//! ```

use std::error::Error;

use riverqueue::{
    BoxError, Client, EventKind, Job, JobArgs, QueueConfig, WorkContext, WorkOutcome,
    WorkerRegistry, migrate::PostgresMigrator,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "send_email")]
struct SendEmail {
    address: String,
}

async fn send_email(context: WorkContext, job: Job<SendEmail>) -> Result<WorkOutcome, BoxError> {
    println!("sending email to {}", job.args.address);
    context.record_output(serde_json::json!({"delivered": true}))?;
    Ok(WorkOutcome::Complete)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    // Apply River's schema before starting a client.
    PostgresMigrator::new(pool.clone()).migrate_up().await?;
    let mut workers = WorkerRegistry::new();
    workers.register_fn(send_email)?;
    let client = Client::builder(pool)
        .workers(workers)
        .queue("default", QueueConfig::new(10))
        .build()?;
    let mut completed = client.subscribe(&[EventKind::JobCompleted])?;
    let mut run = client.start()?;

    let inserted = client
        .insert(SendEmail {
            address: "person@example.com".to_owned(),
        })
        .await?;
    while completed.recv().await?.as_job().map(|event| event.job.id) != Some(inserted.id()) {}

    run.shutdown().await?;
    Ok(())
}
