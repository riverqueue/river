use std::{convert::Infallible, error::Error};

use riverqueue::{
    Client, EventKind, Job, JobArgs, QueueConfig, WorkContext, WorkOutcome, WorkerRegistry,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "send_email")]
struct SendEmail {
    address: String,
}

async fn send_email(context: WorkContext, job: Job<SendEmail>) -> Result<WorkOutcome, Infallible> {
    println!("sending email to {}", job.args.address);
    context
        .record_output(&serde_json::json!({"delivered": true}))
        .await
        .expect("JSON output is serializable");
    Ok(WorkOutcome::Complete)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    let mut workers = WorkerRegistry::new();
    workers.register_fn(send_email)?;
    let client = Client::builder(pool)
        .workers(workers)
        .queue("default", QueueConfig::new(10))
        .build()?;
    let mut completed = client.subscribe(&[EventKind::JobCompleted])?;
    let run = client.start()?;

    let inserted = client
        .insert(SendEmail {
            address: "person@example.com".to_owned(),
        })
        .await?;
    while completed.recv().await?.as_job().map(|event| event.job.id) != Some(inserted.job.row.id) {}

    run.shutdown().await?;
    Ok(())
}
