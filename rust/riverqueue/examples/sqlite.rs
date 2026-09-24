//! Runs River on SQLite: migrate a database file, work a job, and stop.
//!
//! ```sh
//! cargo run -p riverqueue --example sqlite --features sqlite
//! ```
//!
//! Set `SQLITE_PATH` to use a specific database file; otherwise the example
//! uses a temporary one.

use std::{error::Error, str::FromStr, time::Duration};

use riverqueue::{
    Client, EventKind, Job, JobArgs, QueueConfig, WorkContext, WorkOutcome, WorkerRegistry,
    migrate::SqliteMigrator,
};
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "resize_image")]
struct ResizeImage {
    path: String,
    width: u32,
}

async fn resize_image(
    _context: WorkContext,
    job: Job<ResizeImage>,
) -> Result<WorkOutcome, std::io::Error> {
    println!("resizing {} to {}px", job.args.path, job.args.width);
    Ok(WorkOutcome::Complete)
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let directory = std::env::temp_dir().join(format!("river-sqlite-{}", std::process::id()));
    std::fs::create_dir_all(&directory)?;
    let path = std::env::var("SQLITE_PATH")
        .unwrap_or_else(|_| directory.join("river.sqlite3").display().to_string());

    // Every process sharing a SQLite database needs WAL mode and a busy
    // timeout so readers and the single writer don't fail each other.
    let options = SqliteConnectOptions::from_str(&format!("sqlite://{path}"))?
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(Duration::from_secs(5));
    let pool = SqlitePoolOptions::new().connect_with(options).await?;

    // Apply River's schema before starting a client.
    SqliteMigrator::new(pool.clone()).migrate_up().await?;

    let mut workers = WorkerRegistry::new();
    workers.register_fn(resize_image)?;
    let client = Client::builder(pool.clone())
        .workers(workers)
        .queue("default", QueueConfig::new(4))
        .build()?;
    let mut completed = client.subscribe(&[EventKind::JobCompleted])?;
    let run = client.start()?;

    let inserted = client
        .insert(ResizeImage {
            path: "photos/cat.jpg".to_owned(),
            width: 640,
        })
        .await?;
    while completed.recv().await?.as_job().map(|event| event.job.id) != Some(inserted.id()) {}
    println!("job {} completed", inserted.id());

    run.shutdown().await?;
    pool.close().await;
    std::fs::remove_dir_all(&directory).ok();
    Ok(())
}
