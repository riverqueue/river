//! SQLite parity tests for maintenance, leadership, and storage semantics that
//! mirror the Go implementation.

#![cfg(feature = "sqlite")]

mod support;

use riverqueue::{Client, Error};

#[tokio::test(flavor = "multi_thread")]
async fn queue_pause_and_resume() {
    let (pool, path) = support::sqlite_file_pool(2).await;
    let client = Client::builder(pool.clone()).build().unwrap();

    assert!(matches!(
        client.queue_pause("missing").await,
        Err(Error::NotFound)
    ));
    assert!(matches!(
        client.queue_resume("missing").await,
        Err(Error::NotFound)
    ));
    client.queue_pause("*").await.unwrap();
    client.queue_resume("*").await.unwrap();

    sqlx::query(
        "INSERT INTO river_queue (name, created_at, metadata, updated_at) \
         VALUES ('tenant|emails', datetime('now', 'subsec'), jsonb('{}'), datetime('now', 'subsec'))",
    )
    .execute(&pool)
    .await
    .unwrap();
    client.queue_pause("tenant|emails").await.unwrap();
    assert!(
        client
            .queue_get("tenant|emails")
            .await
            .unwrap()
            .paused_at
            .is_some()
    );
    client.queue_pause("tenant|emails").await.unwrap();
    client.queue_resume("tenant|emails").await.unwrap();
    client.queue_resume("tenant|emails").await.unwrap();
    assert!(
        client
            .queue_get("tenant|emails")
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
        .job_delete_many(&riverqueue::JobDeleteManyParams::matching(
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
        .job_delete_many(&riverqueue::JobDeleteManyParams::all())
        .await
        .unwrap();
    assert_eq!(
        remaining.iter().map(|job| job.id).collect::<Vec<_>>(),
        vec![third]
    );
    assert_eq!(
        client.job_get(running).await.unwrap().state,
        riverqueue::JobState::Running
    );

    support::sqlite_cleanup(pool, path).await;
}
