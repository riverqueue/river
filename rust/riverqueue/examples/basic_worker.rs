use std::{convert::Infallible, error::Error};

use async_trait::async_trait;
use riverqueue::{
    Client, EventKind, Job, JobArgs, QueueConfig, WorkContext, WorkOutcome, Worker, WorkerRegistry,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "send_email")]
struct SendEmail {
    address: String,
}

struct SendEmailWorker;

#[async_trait]
impl Worker<SendEmail> for SendEmailWorker {
    type Error = Infallible;

    async fn work(
        &self,
        context: WorkContext,
        job: Job<SendEmail>,
    ) -> Result<WorkOutcome, Self::Error> {
        println!("sending email to {}", job.args.address);
        context
            .record_output(&serde_json::json!({"delivered": true}))
            .await
            .expect("JSON output is serializable");
        Ok(WorkOutcome::Complete)
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    let mut workers = WorkerRegistry::new();
    workers.register::<SendEmail, _>(SendEmailWorker)?;
    let client = Client::builder(pool)
        .workers(workers)
        .queue("default", QueueConfig::new(10))
        .build()?;
    let mut completed = client.subscribe(&[EventKind::JobCompleted])?;
    let run = client.start()?;

    let inserted = client
        .insert_default(SendEmail {
            address: "person@example.com".to_owned(),
        })
        .await?;
    while completed.recv().await?.job.map(|job| job.id) != Some(inserted.job.row.id) {}

    run.shutdown().await?;
    Ok(())
}
