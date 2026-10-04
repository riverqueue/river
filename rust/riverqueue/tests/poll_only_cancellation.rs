//! Cancellation of running jobs in clients without notifications, on every
//! backend.
//!
//! PostgreSQL tests run in a unique schema and fail rather than skip when
//! `RIVER_RUST_DATABASE_URL` is unset; SQLite tests use a temporary file.

#![cfg(any(all(feature = "postgres", river_postgres_tests), feature = "sqlite"))]

mod support;

use std::{convert::Infallible, sync::Arc, time::Duration};

use riverqueue::{
    __private::Database, Client, EventKind, Job, JobArgs, JobState, QueueConfig, WorkContext,
    WorkOutcome, WorkerRegistry,
};
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;

/// Every wait in these tests is bounded by this timeout. It covers a few of
/// the two-second polls for cancellation requests.
const TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_poll_only_cancellation")]
struct WaitArgs {}

/// A migrated database on one backend.
enum Backend {
    #[cfg(all(feature = "postgres", river_postgres_tests))]
    Postgres(support::PostgresSchema),
    #[cfg(feature = "sqlite")]
    Sqlite(sqlx::SqlitePool, std::path::PathBuf),
}

impl Backend {
    fn database(&self) -> Database {
        match self {
            #[cfg(all(feature = "postgres", river_postgres_tests))]
            Self::Postgres(schema) => Database::from_source(
                riverqueue::database::PostgresDatabase::new(schema.pool.clone())
                    .with_schema(schema.schema.clone()),
            ),
            #[cfg(feature = "sqlite")]
            Self::Sqlite(pool, _) => Database::from_source(pool.clone()),
        }
    }

    async fn cleanup(self) {
        match self {
            #[cfg(all(feature = "postgres", river_postgres_tests))]
            Self::Postgres(schema) => schema.cleanup().await,
            #[cfg(feature = "sqlite")]
            Self::Sqlite(pool, path) => support::sqlite_cleanup(pool, path).await,
        }
    }
}

/// A client without notifications cancels its running job once it polls the
/// cancellation another client requested, including while it's stopping and
/// waiting for that job.
async fn polls_for_remote_cancellation(backend: Backend) {
    for while_stopping in [false, true] {
        let started = Arc::new(Semaphore::new(0));
        let mut workers = WorkerRegistry::new();
        let worker_started = Arc::clone(&started);
        workers
            .register_fn(move |context: WorkContext, _job: Job<WaitArgs>| {
                let started = Arc::clone(&worker_started);
                async move {
                    started.add_permits(1);
                    context.cancellation_token().cancelled().await;
                    // Any outcome but completion becomes the cancellation.
                    Ok::<_, Infallible>(WorkOutcome::Snooze(Duration::from_hours(1)))
                }
            })
            .unwrap();
        let client = Client::builder(backend.database())
            .queue(riverqueue::QUEUE_DEFAULT, QueueConfig::new(1))
            .without_leader_election()
            .without_notifications()
            .workers(workers)
            .build()
            .unwrap();
        let other = Client::builder(backend.database()).build().unwrap();
        let mut events = client.subscribe(&[EventKind::JobCancelled]).unwrap();
        let mut run = client.start().unwrap();
        run.wait_ready().await.unwrap();

        let id = other.insert(WaitArgs {}).await.unwrap().job.row.id;
        tokio::time::timeout(TIMEOUT, started.acquire())
            .await
            .expect("the job should start")
            .unwrap()
            .forget();
        if while_stopping {
            run.stopper().stop();
        }
        other.jobs().cancel(id).await.unwrap();

        let event = tokio::time::timeout(TIMEOUT, events.recv())
            .await
            .expect("the job should be cancelled")
            .unwrap();
        let job = &event.as_job().expect("a job event").job;
        assert_eq!(job.id, id);
        assert_eq!(job.state, JobState::Cancelled, "{job:?}");
        tokio::time::timeout(TIMEOUT, run.shutdown())
            .await
            .expect("the client should stop")
            .unwrap();
    }
    backend.cleanup().await;
}

#[cfg(all(feature = "postgres", river_postgres_tests))]
mod postgres {
    use super::*;

    #[tokio::test]
    async fn polls_for_remote_cancellation() {
        super::polls_for_remote_cancellation(Backend::Postgres(
            support::PostgresSchema::new("river_poll_cancel").await,
        ))
        .await;
    }
}

#[cfg(feature = "sqlite")]
mod sqlite {
    use super::*;

    #[tokio::test]
    async fn polls_for_remote_cancellation() {
        let (pool, path) = support::sqlite_file_pool(4).await;
        super::polls_for_remote_cancellation(Backend::Sqlite(pool, path)).await;
    }
}
