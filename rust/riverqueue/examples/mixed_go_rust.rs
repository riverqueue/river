//! Shares one database between a Go service and a Rust service.
//!
//! This Rust service works `resize_image` jobs, which a Go service inserts,
//! and inserts `send_receipt` jobs, which only the Go service works. Each
//! language fetches from its own queue, so neither claims a kind it can't run.
//!
//! The Go side declares the same kinds, JSON field names, and queues:
//!
//! ```go
//! type ResizeImageArgs struct {
//!     Path  string `json:"path"`
//!     Width int    `json:"width"`
//! }
//!
//! func (ResizeImageArgs) Kind() string { return "resize_image" }
//!
//! func (ResizeImageArgs) InsertOpts() river.InsertOpts {
//!     return river.InsertOpts{Queue: "rust_images"}
//! }
//!
//! type SendReceiptArgs struct {
//!     OrderID int64 `json:"order_id"`
//! }
//!
//! func (SendReceiptArgs) Kind() string { return "send_receipt" }
//!
//! // Go works "default" (including send_receipt) and inserts resize_image:
//! client, _ := river.NewClient(riverpgxv5.New(pool), &river.Config{
//!     Queues:  map[string]river.QueueConfig{river.QueueDefault: {MaxWorkers: 10}},
//!     Workers: workers, // registers a SendReceiptArgs worker
//! })
//! client.Insert(ctx, ResizeImageArgs{Path: "cat.jpg", Width: 640}, nil)
//! ```
//!
//! Run migrations once, with either implementation, then start both services:
//!
//! ```sh
//! DATABASE_URL=postgres://localhost/river_example cargo run -p riverqueue --example mixed_go_rust
//! ```
//!
//! The mixed deployment guide (`riverqueue::guide::mixed_deployments`) covers
//! version matching, unique jobs, and rolling deployment.

use std::error::Error;

use riverqueue::{
    Client, EventKind, InsertOpts, Job, JobArgs, QueueConfig, WorkContext, WorkOutcome,
    WorkerRegistry, migrate::PostgresMigrator,
};
use serde::{Deserialize, Serialize};
use sqlx::PgPool;

/// Inserted by Go, worked here. Field names match the Go struct's JSON tags.
#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "resize_image", queue = "rust_images")]
struct ResizeImage {
    path: String,
    width: u32,
}

/// Inserted here, worked by Go in its `default` queue.
#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "send_receipt")]
struct SendReceipt {
    order_id: i64,
}

async fn resize_image(
    context: WorkContext,
    job: Job<ResizeImage>,
) -> Result<WorkOutcome, riverqueue::Error> {
    println!("resizing {} to {}px", job.args.path, job.args.width);
    // Enqueue follow-up work for the Go service from inside a Rust worker.
    if let Some(client) = context.client() {
        client
            .insert(SendReceipt {
                order_id: job.row.id,
            })
            .await?;
    }
    Ok(WorkOutcome::Complete)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let pool = PgPool::connect(&std::env::var("DATABASE_URL")?).await?;
    PostgresMigrator::new(pool.clone()).migrate_up().await?;

    let mut workers = WorkerRegistry::new();
    workers.register_fn(resize_image)?;
    let client = Client::builder(pool)
        .workers(workers)
        // Only Rust's queue: Rust never fetches the Go service's jobs.
        .queue("rust_images", QueueConfig::new(4))
        // Allow inserting kinds only Go works, like Go's SkipUnknownJobCheck.
        .allow_unregistered_job_kinds()
        .build()?;
    let mut completed = client.subscribe(&[EventKind::JobCompleted])?;
    let mut run = client.start()?;

    // Stand in for the Go producer so the example runs on its own.
    let inserted = client
        .insert(ResizeImage {
            path: "cat.jpg".to_owned(),
            width: 640,
        })
        .opts(InsertOpts::default())
        .await?;
    while completed.recv().await?.as_job().map(|event| event.job.id) != Some(inserted.id()) {}
    println!("send_receipt is waiting in the default queue for the Go service");

    run.shutdown().await?;
    Ok(())
}
