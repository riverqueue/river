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
