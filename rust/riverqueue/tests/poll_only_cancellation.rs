//! Cancellation of running jobs found by polling, in clients without
//! notifications and in clients whose notification was lost, on every
//! backend.
//!
//! Postgres tests run in a unique schema and fail rather than skip when
//! `RIVER_RUST_DATABASE_URL` is unset; SQLite tests use a temporary file.

#![cfg(any(all(feature = "postgres", river_postgres_tests), feature = "sqlite"))]

mod support;

use std::{convert::Infallible, sync::Arc, time::Duration};

use riverqueue::{
    __private::{ClientBuilderExt, Database},
    Client, EventKind, Job, JobArgs, JobState, QueueConfig, WorkContext, WorkOutcome, Workers,
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

    /// Requests a job's cancellation the way `JobCancel` does, but without a
    /// notification, as if it was lost.
    async fn request_cancellation_without_notification(&self, id: i64) {
        let cancel_attempted_at = jiff::Timestamp::now().to_string();
        match self {
            #[cfg(all(feature = "postgres", river_postgres_tests))]
            Self::Postgres(schema) => {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "UPDATE {} SET metadata = jsonb_set(metadata, '{{cancel_attempted_at}}', to_jsonb($2::text)) \
                     WHERE id = $1",
                    schema.table("river_job")
                )))
                .bind(id)
                .bind(cancel_attempted_at)
                .execute(&schema.pool)
                .await
                .unwrap();
            }
            #[cfg(feature = "sqlite")]
            Self::Sqlite(pool, _) => {
                sqlx::query(
                    "UPDATE river_job SET metadata = jsonb_set(metadata, '$.cancel_attempted_at', ?) \
                     WHERE id = ?",
                )
                .bind(cancel_attempted_at)
                .bind(id)
                .execute(pool)
                .await
                .unwrap();
            }
        }
    }
}

/// Workers that block until their attempt is cancelled, adding a permit to
/// `started` when one starts.
fn waiting_workers(started: &Arc<Semaphore>) -> Workers {
    let mut workers = Workers::new();
    let started = Arc::clone(started);
    workers
        .add_fn(move |context: WorkContext, _job: Job<WaitArgs>| {
            let started = Arc::clone(&started);
            async move {
                started.add_permits(1);
                context.cancellation_token().cancelled().await;
                // Any outcome but completion becomes the cancellation.
                Ok::<_, Infallible>(WorkOutcome::Snooze(Duration::from_hours(1)))
            }
        })
        .unwrap();
    workers
}

/// A client with notifications checks for cancellation requests only once
/// its interval (a minute by default) has passed, not on every two-second
/// tick like a client without them. A notified cancellation still arrives
/// right away.
async fn checks_for_lost_cancellations_only_after_interval(backend: Backend) {
    let started = Arc::new(Semaphore::new(0));
    let client = Client::builder(backend.database())
        .queue(riverqueue::QUEUE_DEFAULT, QueueConfig::new(1))
        .without_leader_election()
        .workers(waiting_workers(&started))
        .build()
        .unwrap();
    let mut events = client.subscribe(&[EventKind::JobCancelled]).unwrap();
    let mut run = client.start().unwrap();
    run.wait_ready().await.unwrap();

    let id = client.insert(WaitArgs {}).await.unwrap().job.row.id;
    tokio::time::timeout(TIMEOUT, started.acquire())
        .await
        .expect("the job should start")
        .unwrap()
        .forget();
    backend.request_cancellation_without_notification(id).await;
    // This spans at least one tick, on which a client without notifications
    // would have found the request.
    assert!(
        tokio::time::timeout(Duration::from_secs(3), events.recv())
            .await
            .is_err(),
        "the job shouldn't be cancelled before the interval passes"
    );

    client.jobs().cancel(id).await.unwrap();
    let event = tokio::time::timeout(TIMEOUT, events.recv())
        .await
        .expect("the job should be cancelled")
        .unwrap();
    let job = &event.as_job().expect("a job event").job;
    assert_eq!(job.id, id);
    assert_eq!(job.state, JobState::Cancelled, "{job:?}");
    tokio::time::timeout(TIMEOUT, run.stop())
        .await
        .expect("the client should stop")
        .unwrap();
    backend.cleanup().await;
}

/// A client with notifications still cancels its running job when the
/// cancellation's notification was lost, like if it was sent while the
/// client's notifier was reconnecting, because it also checks for
/// cancellation requests occasionally.
async fn polls_for_lost_cancellation_notification(backend: Backend) {
    let started = Arc::new(Semaphore::new(0));
    let client = Client::builder(backend.database())
        .job_cancel_poll_interval(Duration::ZERO)
        .queue(riverqueue::QUEUE_DEFAULT, QueueConfig::new(1))
        .without_leader_election()
        .workers(waiting_workers(&started))
        .build()
        .unwrap();
    let mut events = client.subscribe(&[EventKind::JobCancelled]).unwrap();
    let mut run = client.start().unwrap();
    run.wait_ready().await.unwrap();

    let id = client.insert(WaitArgs {}).await.unwrap().job.row.id;
    tokio::time::timeout(TIMEOUT, started.acquire())
        .await
        .expect("the job should start")
        .unwrap()
        .forget();
    backend.request_cancellation_without_notification(id).await;

    let event = tokio::time::timeout(TIMEOUT, events.recv())
        .await
        .expect("the job should be cancelled")
        .unwrap();
    let job = &event.as_job().expect("a job event").job;
    assert_eq!(job.id, id);
    assert_eq!(job.state, JobState::Cancelled, "{job:?}");
    tokio::time::timeout(TIMEOUT, run.stop())
        .await
        .expect("the client should stop")
        .unwrap();
    backend.cleanup().await;
}

/// A client without notifications cancels its running job once it polls the
/// cancellation another client requested, including while it's stopping and
/// waiting for that job.
async fn polls_for_remote_cancellation(backend: Backend) {
    for while_stopping in [false, true] {
        let started = Arc::new(Semaphore::new(0));
        let client = Client::builder(backend.database())
            .queue(riverqueue::QUEUE_DEFAULT, QueueConfig::new(1))
            .without_leader_election()
            .without_notifications()
            .workers(waiting_workers(&started))
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
        tokio::time::timeout(TIMEOUT, run.stop())
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
    async fn checks_for_lost_cancellations_only_after_interval() {
        super::checks_for_lost_cancellations_only_after_interval(Backend::Postgres(
            support::PostgresSchema::new("river_poll_cancel_interval").await,
        ))
        .await;
    }

    #[tokio::test]
    async fn polls_for_lost_cancellation_notification() {
        super::polls_for_lost_cancellation_notification(Backend::Postgres(
            support::PostgresSchema::new("river_poll_cancel_lost").await,
        ))
        .await;
    }

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
    async fn checks_for_lost_cancellations_only_after_interval() {
        let (pool, path) = support::sqlite_file_pool(4).await;
        super::checks_for_lost_cancellations_only_after_interval(Backend::Sqlite(pool, path)).await;
    }

    #[tokio::test]
    async fn polls_for_lost_cancellation_notification() {
        let (pool, path) = support::sqlite_file_pool(4).await;
        super::polls_for_lost_cancellation_notification(Backend::Sqlite(pool, path)).await;
    }

    #[tokio::test]
    async fn polls_for_remote_cancellation() {
        let (pool, path) = support::sqlite_file_pool(4).await;
        super::polls_for_remote_cancellation(Backend::Sqlite(pool, path)).await;
    }
}
