//! Enqueues and completes jobs in the same transactions as business writes.
//!
//! ```sh
//! DATABASE_URL=postgres://localhost/river_example cargo run -p riverqueue --example transactions
//! ```
//!
//! An order and the job that confirms it are inserted in one transaction, so
//! neither exists without the other. The worker records the confirmation and
//! completes its job in one transaction, so a crash between the two can't
//! send a second confirmation.

use std::error::Error;

use riverqueue::{
    Client, EventKind, Job, JobArgs, QueueConfig, WorkContext, WorkOutcome, WorkerRegistry,
    migrate::PostgresMigrator,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "confirm_order")]
struct ConfirmOrder {
    order_id: i64,
}

async fn confirm_order(
    context: WorkContext,
    job: Job<ConfirmOrder>,
) -> Result<WorkOutcome, riverqueue::Error> {
    let client = context
        .client()
        .expect("jobs worked by a client have a client");
    let pool = client
        .postgres_pool()
        .expect("this example uses PostgreSQL")
        .clone();

    let mut transaction = pool.begin().await?;
    sqlx::query("UPDATE example_orders SET confirmed = true WHERE id = $1")
        .bind(job.args.order_id)
        .execute(&mut *transaction)
        .await?;
    // The job completes only if this transaction commits.
    context.job_complete_tx(&mut transaction).await?;
    transaction.commit().await?;
    println!("confirmed order {}", job.args.order_id);
    Ok(WorkOutcome::Complete)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    PostgresMigrator::new(pool.clone()).migrate_up().await?;
    sqlx::query(
        "CREATE TABLE IF NOT EXISTS example_orders \
         (id bigserial PRIMARY KEY, confirmed boolean NOT NULL DEFAULT false)",
    )
    .execute(&pool)
    .await?;

    let mut workers = WorkerRegistry::new();
    workers.register_fn(confirm_order)?;
    let client = Client::builder(pool.clone())
        .workers(workers)
        .queue("default", QueueConfig::new(4))
        .build()?;
    let mut completed = client.subscribe(&[EventKind::JobCompleted])?;
    let mut run = client.start()?;

    // Insert the order and its job together.
    let mut transaction = pool.begin().await?;
    let order_id: i64 =
        sqlx::query_scalar("INSERT INTO example_orders DEFAULT VALUES RETURNING id")
            .fetch_one(&mut *transaction)
            .await?;
    let inserted = client
        .insert(ConfirmOrder { order_id })
        .tx(&mut transaction)
        .await?;
    transaction.commit().await?;

    while completed.recv().await?.as_job().map(|event| event.job.id) != Some(inserted.id()) {}
    let confirmed: bool = sqlx::query_scalar("SELECT confirmed FROM example_orders WHERE id = $1")
        .bind(order_id)
        .fetch_one(&pool)
        .await?;
    assert!(confirmed);

    run.shutdown().await?;
    Ok(())
}
