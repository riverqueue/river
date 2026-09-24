//! Stops a client gracefully on the first Ctrl-C and cancels running jobs on
//! the second.

use std::{error::Error, time::Duration};

use riverqueue::{
    BoxError, Client, Job, JobArgs, QueueConfig, WorkCancelled, WorkContext, WorkOutcome,
    WorkerRegistry,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "generate_report")]
struct GenerateReport {
    report_id: i64,
}

async fn generate_report(
    context: WorkContext,
    job: Job<GenerateReport>,
) -> Result<WorkOutcome, BoxError> {
    // Long-running work should watch its cancellation token so a hard stop
    // can interrupt it. Returning `WorkCancelled` makes the job available
    // again without using up its attempt.
    tokio::select! {
        () = context.cancellation_token().cancelled() => Err(WorkCancelled.into()),
        () = tokio::time::sleep(Duration::from_secs(60)) => {
            println!("generated report {}", job.args.report_id);
            Ok(WorkOutcome::Complete)
        }
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    let mut workers = WorkerRegistry::new();
    workers.register_fn(generate_report)?;
    let client = Client::builder(pool)
        .workers(workers)
        .queue("default", QueueConfig::new(10))
        // Cancel jobs still running 30 seconds after a graceful stop begins.
        .soft_stop_timeout(Some(Duration::from_secs(30)))
        .build()?;
    client.insert(GenerateReport { report_id: 42 }).await?;

    let mut run = client.start()?;
    let stopper = run.stopper();
    tokio::spawn(async move {
        if tokio::signal::ctrl_c().await.is_ok() {
            println!("stopping; press Ctrl-C again to cancel running jobs");
            stopper.stop();
        }
        if tokio::signal::ctrl_c().await.is_ok() {
            stopper.stop_now();
        }
    });

    // Returns once the client has stopped and recorded every job's result.
    run.wait().await?;
    Ok(())
}
