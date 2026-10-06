//! Cancels a running job from outside its worker.
//!
//! ```sh
//! DATABASE_URL=postgres://localhost/river_example cargo run -p riverqueue --example cancellation
//! ```
//!
//! Cancelling a running job triggers its worker's cancellation token, on
//! whichever client is working it.

use std::{error::Error, time::Duration};

use riverqueue::sqlx::PgPool;
use riverqueue::{
    Client, Job, JobArgs, QueueConfig, WorkCancelled, WorkContext, WorkOutcome, Worker,
    WorkerRegistry, migrate::PostgresMigrator,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "cancellable_report")]
struct CancellableReport {
    report_id: i64,
}

struct CancellableReportWorker;

impl Worker<CancellableReport> for CancellableReportWorker {
    type Error = WorkCancelled;

    async fn work(
        &self,
        context: WorkContext,
        job: Job<CancellableReport>,
    ) -> Result<WorkOutcome, Self::Error> {
        tokio::select! {
            // Returning `WorkCancelled` records a remote cancellation as
            // cancelled, and during a client's hard stop makes the job
            // available again without using up its attempt.
            () = context.cancellation_token().cancelled() => Err(WorkCancelled),
            () = tokio::time::sleep(Duration::from_secs(30)) => {
                println!("generated report {}", job.args.report_id);
                Ok(WorkOutcome::Complete)
            }
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    PostgresMigrator::new(pool.clone()).migrate_up().await?;
    let mut workers = WorkerRegistry::new();
    workers.register(CancellableReportWorker)?;
    let client = Client::builder(pool)
        .workers(workers)
        .queue("default", QueueConfig::new(1))
        .build()?;
    let mut run = client.start()?;
    let job = client.insert(CancellableReport { report_id: 42 }).await?;

    client.jobs().cancel(job.id()).await?;
    run.stop().await?;
    Ok(())
}
