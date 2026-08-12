use std::{convert::Infallible, error::Error, time::Duration};

use riverqueue::{
    Client, Job, JobArgs, QueueConfig, WorkContext, WorkOutcome, Worker, WorkerRegistry,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "cancellable_report")]
struct CancellableReport {
    report_id: i64,
}

struct CancellableReportWorker;

impl Worker<CancellableReport> for CancellableReportWorker {
    type Error = Infallible;

    async fn work(
        &self,
        context: WorkContext,
        job: Job<CancellableReport>,
    ) -> Result<WorkOutcome, Self::Error> {
        tokio::select! {
            () = context.cancellation_token().cancelled() => Ok(WorkOutcome::Cancel),
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
    let mut workers = WorkerRegistry::new();
    workers.register::<CancellableReport, _>(CancellableReportWorker)?;
    let client = Client::builder(pool)
        .workers(workers)
        .queue("default", QueueConfig::new(1))
        .build()?;
    let run = client.start()?;
    let job = client.insert(CancellableReport { report_id: 42 }).await?;

    client.job_cancel(job.job.row.id).await?;
    run.shutdown().await?;
    Ok(())
}
