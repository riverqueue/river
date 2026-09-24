//! SQLite parity tests for maintenance, leadership, and storage semantics that
//! mirror the Go implementation.

#![cfg(feature = "sqlite")]

mod support;

use riverqueue::__private::ClientBuilderExt;
use riverqueue::{Client, Error};

#[tokio::test(flavor = "multi_thread")]
async fn queue_pause_and_resume() {
    let (pool, path) = support::sqlite_file_pool(2).await;
    let client = Client::builder(pool.clone()).build().unwrap();

    assert!(matches!(
        client.queues().pause("missing").await,
        Err(Error::NotFound)
    ));
    assert!(matches!(
        client.queues().resume("missing").await,
        Err(Error::NotFound)
    ));
    client
        .queues()
        .pause(riverqueue::QueueSelector::All)
        .await
        .unwrap();
    client
        .queues()
        .resume(riverqueue::QueueSelector::All)
        .await
        .unwrap();

    sqlx::query(
        "INSERT INTO river_queue (name, created_at, metadata, updated_at) \
         VALUES ('tenant|emails', datetime('now', 'subsec'), jsonb('{}'), datetime('now', 'subsec'))",
    )
    .execute(&pool)
    .await
    .unwrap();
    client.queues().pause("tenant|emails").await.unwrap();
    assert!(
        client
            .queues()
            .get("tenant|emails")
            .await
            .unwrap()
            .paused_at
            .is_some()
    );
    client.queues().pause("tenant|emails").await.unwrap();
    client.queues().resume("tenant|emails").await.unwrap();
    client.queues().resume("tenant|emails").await.unwrap();
    assert!(
        client
            .queues()
            .get("tenant|emails")
            .await
            .unwrap()
            .paused_at
            .is_none()
    );

    support::sqlite_cleanup(pool, path).await;
}

async fn insert_raw_job(pool: &sqlx::SqlitePool, state: &str) -> i64 {
    sqlx::query_scalar(
        "INSERT INTO river_job (args, kind, max_attempts, state, attempt, attempted_at, finalized_at) \
         VALUES (jsonb('{}'), 'parity_raw', 25, ?1, \
                 CASE WHEN ?1 = 'running' THEN 1 ELSE 0 END, \
                 CASE WHEN ?1 = 'running' THEN datetime('now', 'subsec') END, \
                 CASE WHEN ?1 IN ('cancelled', 'completed', 'discarded') THEN datetime('now', 'subsec') END) \
         RETURNING id",
    )
    .bind(state)
    .fetch_one(pool)
    .await
    .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn job_delete_many() {
    let (pool, path) = support::sqlite_file_pool(2).await;
    let client = Client::builder(pool.clone()).build().unwrap();

    let running = insert_raw_job(&pool, "running").await;
    let first = insert_raw_job(&pool, "available").await;
    let second = insert_raw_job(&pool, "completed").await;
    let third = insert_raw_job(&pool, "cancelled").await;

    // Running jobs are excluded before the limit applies.
    let deleted = client
        .jobs()
        .delete_many(riverqueue::JobDeleteManyParams::matching(
            riverqueue::JobListParams::default()
                .with_ids([running, first, second, third])
                .with_limit(2),
        ))
        .await
        .unwrap();
    assert_eq!(
        deleted.iter().map(|job| job.id).collect::<Vec<_>>(),
        vec![first, second]
    );
    let remaining = client
        .jobs()
        .delete_many(riverqueue::JobDeleteManyParams::all())
        .await
        .unwrap();
    assert_eq!(
        remaining.iter().map(|job| job.id).collect::<Vec<_>>(),
        vec![third]
    );
    assert_eq!(
        client.jobs().get(running).await.unwrap().state,
        riverqueue::JobState::Running
    );

    support::sqlite_cleanup(pool, path).await;
}

/// Hook invocations as `(operation, job ID, state)`.
type HookCalls = std::sync::Arc<std::sync::Mutex<Vec<(&'static str, i64, String)>>>;

#[derive(Clone, Default)]
struct HookPilot {
    calls: HookCalls,
    fail: bool,
}

#[async_trait::async_trait]
impl riverqueue::__private::Pilot for HookPilot {
    fn intercepts_job_cancel_retry(&self) -> bool {
        true
    }

    async fn after_job_cancel(
        &self,
        connection: riverqueue::__private::DatabaseConnection<'_>,
        job: &riverqueue::__private::JobUpdatedParams,
    ) -> Result<(), riverqueue::__private::PilotError> {
        // The hook sees the update inside the same transaction.
        let connection = connection.into_sqlite().expect("SQLite connection");
        let state: String = sqlx::query_scalar("SELECT state FROM river_job WHERE id = ?")
            .bind(job.job.id)
            .fetch_one(connection)
            .await?;
        self.calls
            .lock()
            .unwrap()
            .push(("cancel", job.job.id, state));
        if self.fail {
            return Err(std::io::Error::other("cancel hook failed").into());
        }
        Ok(())
    }

    async fn after_job_retry(
        &self,
        _connection: riverqueue::__private::DatabaseConnection<'_>,
        job: &riverqueue::__private::JobUpdatedParams,
    ) -> Result<(), riverqueue::__private::PilotError> {
        self.calls
            .lock()
            .unwrap()
            .push(("retry", job.job.id, job.job.state.as_str().to_owned()));
        if self.fail {
            return Err(std::io::Error::other("retry hook failed").into());
        }
        Ok(())
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn cancel_and_retry_post_hooks_share_the_transaction() {
    let (pool, path) = support::sqlite_file_pool(2).await;
    let pilot = HookPilot::default();
    let client = Client::builder(pool.clone())
        .pilot(pilot.clone())
        .build()
        .unwrap();
    let id = insert_raw_job(&pool, "available").await;

    client.jobs().cancel(id).await.unwrap();
    client.jobs().retry(id).await.unwrap();
    assert_eq!(
        *pilot.calls.lock().unwrap(),
        [
            ("cancel", id, "cancelled".to_owned()),
            ("retry", id, "available".to_owned())
        ]
    );

    let failing = Client::builder(pool.clone())
        .pilot(HookPilot {
            fail: true,
            ..HookPilot::default()
        })
        .build()
        .unwrap();
    assert!(failing.jobs().cancel(id).await.is_err());
    assert_eq!(
        client.jobs().get(id).await.unwrap().state,
        riverqueue::JobState::Available
    );

    support::sqlite_cleanup(pool, path).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn extension_notify_many_writes_the_outbox() {
    use riverqueue::__private::{
        DatabaseConfig, DatabaseConnection, NotificationTopic, notify_many,
    };

    let (pool, path) = support::sqlite_file_pool(2).await;
    let mut rolled_back = pool.begin().await.unwrap();
    notify_many(
        DatabaseConnection::Sqlite(&mut rolled_back),
        &DatabaseConfig::Sqlite,
        NotificationTopic::Control,
        &["rolled back".to_owned()],
    )
    .await
    .unwrap();
    rolled_back.rollback().await.unwrap();

    let mut committed = pool.begin().await.unwrap();
    notify_many(
        DatabaseConnection::Sqlite(&mut committed),
        &DatabaseConfig::Sqlite,
        NotificationTopic::Control,
        &["first".to_owned(), "second".to_owned()],
    )
    .await
    .unwrap();
    committed.commit().await.unwrap();

    let rows: Vec<(String, String)> =
        sqlx::query_as("SELECT topic, payload FROM river_notification ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap();
    assert_eq!(
        rows,
        [
            ("river_control".to_owned(), "first".to_owned()),
            ("river_control".to_owned(), "second".to_owned())
        ]
    );
    support::sqlite_cleanup(pool, path).await;
}
