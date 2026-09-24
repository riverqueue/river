//! PostgreSQL parity tests for maintenance, leadership, and storage semantics
//! that mirror the Go implementation.

#![cfg(feature = "postgres-tests")]

mod support;

use riverqueue::{Client, Error, database::PostgresDatabase};

use support::PostgresSchema;

fn insert_only_client(database: &PostgresSchema) -> Client {
    Client::builder(PostgresDatabase::new(database.pool.clone()).schema(database.schema.clone()))
        .build()
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn queue_pause_and_resume() {
    let database = PostgresSchema::new("rpp_queue_pause").await;
    let client = insert_only_client(&database);

    // An unknown named queue is reported like Go's `ErrNotFound`, while `*`
    // succeeds even with no persisted queues.
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

    // Go accepts `|` as a queue-name separator.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {} (name, created_at, metadata, updated_at) VALUES ('tenant|emails', now(), '{{}}', now())",
        database.table("river_queue")
    )))
    .execute(&database.pool)
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
    // Pausing an already paused queue still addresses an existing row.
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

    database.cleanup().await;
}
