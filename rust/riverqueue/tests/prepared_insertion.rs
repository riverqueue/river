//! Prepared insertion inserts stored jobs again, such as jobs set aside and
//! retried later, like an ordinary insertion of them: begin hooks and
//! middleware run once and see the stored arguments, nothing decodes the
//! stored row first, and the job keeps its identity with a new ID.
//!
//! PostgreSQL scenarios run in a unique schema and fail rather than skip when
//! `RIVER_RUST_DATABASE_URL` is unset; SQLite scenarios use temporary files.

#![cfg(any(feature = "postgres-tests", feature = "sqlite"))]

mod support;

use std::sync::{
    Arc, Mutex,
    atomic::{AtomicUsize, Ordering},
};

use riverqueue::__private::{ExtensionClient, PreparedInsertParams};
use riverqueue::{
    BoxError, Client, Error, Hook, InsertContext, InsertMiddleware, InsertNext, InsertOpts,
    InsertedJobs, JobArgs, JobRow, UniqueOpts,
};
use serde::{Deserialize, Serialize};
use serde_json::value::RawValue;

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "prepared_insertion")]
struct PreparedArgs {
    value: i64,
}

/// Wraps arguments in an envelope, keeping one that's already there as a
/// hook that wraps arguments must, and records what it saw.
#[derive(Clone, Default)]
struct EnvelopeHook {
    decoded: Arc<AtomicUsize>,
    seen: Arc<Mutex<Vec<String>>>,
}

impl Hook for EnvelopeHook {
    async fn insert_begin(&self, insert: &mut InsertContext) -> Result<(), BoxError> {
        tokio::task::yield_now().await;
        let args = insert.encoded_args.get().to_owned();
        self.seen.lock().unwrap().push(args.clone());
        if !args.starts_with(r#"{"envelope":"#) {
            insert.encoded_args = RawValue::from_string(format!(r#"{{"envelope":{args}}}"#))?;
        }
        Ok(())
    }

    async fn decode_insert_result(&self, job: &mut JobRow) -> Result<(), BoxError> {
        tokio::task::yield_now().await;
        self.decoded.fetch_add(1, Ordering::SeqCst);
        let mut outer: std::collections::HashMap<String, Box<RawValue>> = job.decode_args()?;
        if let Some(inner) = outer.remove("envelope") {
            job.encoded_args = inner;
        }
        Ok(())
    }
}

/// Counts insertions.
#[derive(Clone, Default)]
struct CountingMiddleware(Arc<AtomicUsize>);

impl InsertMiddleware for CountingMiddleware {
    async fn insert_many(
        &self,
        jobs: Vec<InsertContext>,
        next: InsertNext<'_>,
    ) -> Result<InsertedJobs, Error> {
        self.0.fetch_add(jobs.len(), Ordering::SeqCst);
        next.run(jobs).await
    }
}

fn prepared(row: &JobRow, encoded_args: Box<RawValue>) -> PreparedInsertParams {
    PreparedInsertParams {
        created_at: row.created_at,
        encoded_args,
        kind: row.kind.clone(),
        max_attempts: row.max_attempts,
        metadata: row.metadata.clone(),
        priority: row.priority,
        queue: row.queue.clone(),
        scheduled_at: row.scheduled_at,
        tags: row.tags.clone(),
        unique_key: row.unique_key.clone(),
        unique_states: row.unique_states.clone(),
    }
}

/// Inserts a job, deletes it, and inserts it again from its stored row.
async fn assert_stored_jobs_insert_like_ordinary_jobs(
    builder: impl Fn() -> riverqueue::ClientBuilder,
    delete: impl AsyncFn(i64),
    stored_args: impl AsyncFn(i64) -> String,
) {
    let hook = EnvelopeHook::default();
    let middleware = CountingMiddleware::default();
    let client = builder()
        .hook(hook.clone())
        .insert_middleware(middleware.clone())
        .build()
        .unwrap();
    let scheduled_at = chrono::Utc::now() + chrono::Duration::hours(1);
    let original = client
        .insert(PreparedArgs { value: 7 })
        .opts(
            InsertOpts::default()
                .with_metadata(
                    serde_json::json!({"source": true})
                        .as_object()
                        .unwrap()
                        .clone(),
                )
                .with_scheduled_at(scheduled_at)
                .with_tags(["prepared"])
                .with_unique(UniqueOpts::new().with_by_args(true)),
        )
        .await
        .unwrap()
        .job
        .row;
    let stored = stored_args(original.id).await;
    let row = client.jobs().get(original.id).await.unwrap();
    delete(original.id).await;
    // SQLite may reuse the highest deleted ID, so take a newer one first.
    let sentinel = client.insert(PreparedArgs { value: 8 }).await.unwrap().id();
    let (seen, decoded, inserted) = (
        hook.seen.lock().unwrap().len(),
        hook.decoded.load(Ordering::SeqCst),
        middleware.0.load(Ordering::SeqCst),
    );

    let reinserted = ExtensionClient::new(&client)
        .insert_prepared(vec![prepared(
            &row,
            RawValue::from_string(stored.clone()).unwrap(),
        )])
        .await
        .unwrap()
        .remove(0);

    // Each step ran once, the hook saw the stored arguments, and nothing
    // decoded the stored row before inserting it.
    assert_eq!(middleware.0.load(Ordering::SeqCst), inserted + 1);
    let seen_now = hook.seen.lock().unwrap().clone();
    assert_eq!(seen_now.len(), seen + 1);
    assert_eq!(seen_now.last().unwrap(), &stored);
    assert_eq!(hook.decoded.load(Ordering::SeqCst), decoded + 1);
    // The hook kept its envelope, so the stored arguments are unchanged.
    assert_eq!(stored_args(reinserted.job.id).await, stored);
    assert!(reinserted.job.id > sentinel);
    assert_eq!(reinserted.job.created_at, original.created_at);
    assert_eq!(reinserted.job.scheduled_at, original.scheduled_at);
    assert_eq!(reinserted.job.unique_key, original.unique_key);
    assert_eq!(reinserted.job.unique_states, original.unique_states);
    assert_eq!(reinserted.job.tags, original.tags);
    assert_eq!(
        reinserted.job.metadata.get::<bool>("source").unwrap(),
        Some(true)
    );
    assert_eq!(reinserted.job.attempt, 0);
    assert!(reinserted.job.errors.is_empty());

    // Arguments may be any JSON value.
    for args in ["[1,2]", "null"] {
        let mut params = prepared(&row, RawValue::from_string(args.to_owned()).unwrap());
        params.unique_key = None;
        params.unique_states = None;
        let inserted = ExtensionClient::new(&client)
            .insert_prepared(vec![params])
            .await
            .unwrap();
        assert_eq!(inserted.len(), 1, "{args}");
    }
    assert!(
        ExtensionClient::new(&client)
            .insert_prepared(Vec::new())
            .await
            .unwrap()
            .is_empty()
    );
}

/// Two available stored jobs to insert again, without uniqueness.
fn available_params() -> Vec<PreparedInsertParams> {
    let now = chrono::Utc::now();
    (1..=2)
        .map(|value| PreparedInsertParams {
            created_at: now,
            encoded_args: RawValue::from_string(format!(r#"{{"value":{value}}}"#)).unwrap(),
            kind: PreparedArgs::KIND.to_owned(),
            max_attempts: 25,
            metadata: riverqueue::JobMetadata::default(),
            priority: 1,
            queue: "prepared".to_owned(),
            scheduled_at: now - chrono::Duration::seconds(1),
            tags: Vec::new(),
            unique_key: None,
            unique_states: None,
        })
        .collect()
}

#[cfg(feature = "postgres-tests")]
mod postgres {
    use riverqueue::database::PostgresDatabase;

    use super::*;
    use crate::support::PostgresSchema;

    #[tokio::test(flavor = "multi_thread")]
    async fn stored_jobs_insert_like_ordinary_jobs() {
        let schema = PostgresSchema::new("prepared_insert").await;
        let table = schema.table("river_job");
        let pool = schema.pool.clone();
        assert_stored_jobs_insert_like_ordinary_jobs(
            || {
                Client::builder(
                    PostgresDatabase::new(schema.pool.clone()).with_schema(schema.schema.clone()),
                )
            },
            async |id| {
                sqlx::query(sqlx::AssertSqlSafe(format!(
                    "DELETE FROM {table} WHERE id = $1"
                )))
                .bind(id)
                .execute(&pool)
                .await
                .unwrap();
            },
            async |id| {
                sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                    "SELECT args::text FROM {table} WHERE id = $1"
                )))
                .bind(id)
                .fetch_one(&pool)
                .await
                .unwrap()
            },
        )
        .await;
        schema.cleanup().await;
    }

    /// Stored jobs inserted again notify their queue once committed, on the
    /// client's pool or in a caller's transaction, and a rolled-back
    /// transaction keeps neither the jobs nor the notification.
    #[tokio::test(flavor = "multi_thread")]
    async fn prepared_insertions_notify_on_commit_only() {
        let schema = PostgresSchema::new("prepared_notify").await;
        let client = Client::builder(
            PostgresDatabase::new(schema.pool.clone()).with_schema(schema.schema.clone()),
        )
        .build()
        .unwrap();
        let channel = format!("{}.river_insert", schema.schema.as_deref().unwrap());
        let mut listener = sqlx::postgres::PgListener::connect_with(&schema.pool)
            .await
            .unwrap();
        listener.listen(&channel).await.unwrap();
        // Returns the queues notified before a marker sent now.
        let mut notified = async || {
            sqlx::query("SELECT pg_notify($1, 'marker')")
                .bind(&channel)
                .execute(&schema.pool)
                .await
                .unwrap();
            let mut payloads = Vec::new();
            loop {
                let notification = listener.recv().await.unwrap();
                if notification.payload() == "marker" {
                    return payloads;
                }
                payloads.push(notification.payload().to_owned());
            }
        };
        let extension = ExtensionClient::new(&client);
        let count = async || -> i64 {
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT count(*) FROM {}",
                schema.table("river_job")
            )))
            .fetch_one(&schema.pool)
            .await
            .unwrap()
        };

        assert_eq!(
            extension
                .insert_prepared(available_params())
                .await
                .unwrap()
                .len(),
            2
        );
        assert_eq!(notified().await, [r#"{"queue": "prepared"}"#]);

        let mut transaction = schema.pool.begin().await.unwrap();
        extension
            .insert_prepared_tx(&mut transaction, available_params())
            .await
            .unwrap();
        assert!(notified().await.is_empty(), "notified before commit");
        transaction.commit().await.unwrap();
        assert_eq!(notified().await, [r#"{"queue": "prepared"}"#]);
        assert_eq!(count().await, 4);

        let mut transaction = schema.pool.begin().await.unwrap();
        extension
            .insert_prepared_tx(&mut transaction, available_params())
            .await
            .unwrap();
        transaction.rollback().await.unwrap();
        assert!(notified().await.is_empty(), "rolled back but notified");
        assert_eq!(count().await, 4);

        drop(listener);
        schema.cleanup().await;
    }
}

#[cfg(feature = "sqlite")]
mod sqlite {
    use super::*;
    use crate::support::{sqlite_cleanup, sqlite_file_pool};

    #[tokio::test(flavor = "multi_thread")]
    async fn stored_jobs_insert_like_ordinary_jobs() {
        let (pool, path) = sqlite_file_pool(4).await;
        assert_stored_jobs_insert_like_ordinary_jobs(
            || Client::builder(pool.clone()),
            async |id| {
                sqlx::query("DELETE FROM river_job WHERE id = ?")
                    .bind(id)
                    .execute(&pool)
                    .await
                    .unwrap();
            },
            async |id| {
                sqlx::query_scalar("SELECT json(args) FROM river_job WHERE id = ?")
                    .bind(id)
                    .fetch_one(&pool)
                    .await
                    .unwrap()
            },
        )
        .await;
        sqlite_cleanup(pool, path).await;
    }

    /// Stored jobs inserted again write their queue's notification with the
    /// jobs, on the client's pool or in a caller's transaction, and a
    /// rolled-back transaction keeps neither.
    #[tokio::test(flavor = "multi_thread")]
    async fn prepared_insertions_notify_on_commit_only() {
        let (pool, path) = sqlite_file_pool(4).await;
        let client = Client::builder(pool.clone()).build().unwrap();
        let extension = ExtensionClient::new(&client);
        let counts = async || -> (i64, i64) {
            let jobs = sqlx::query_scalar("SELECT count(*) FROM river_job")
                .fetch_one(&pool)
                .await
                .unwrap();
            let notifications = sqlx::query_scalar(
                "SELECT count(*) FROM river_notification WHERE topic = 'river_insert' \
                 AND payload = '{\"queue\": \"prepared\"}'",
            )
            .fetch_one(&pool)
            .await
            .unwrap();
            (jobs, notifications)
        };

        assert_eq!(
            extension
                .insert_prepared(available_params())
                .await
                .unwrap()
                .len(),
            2
        );
        assert_eq!(counts().await, (2, 1));

        let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
        extension
            .insert_prepared_tx(&mut transaction, available_params())
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        assert_eq!(counts().await, (4, 2));

        let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
        extension
            .insert_prepared_tx(&mut transaction, available_params())
            .await
            .unwrap();
        transaction.rollback().await.unwrap();
        assert_eq!(counts().await, (4, 2));

        sqlite_cleanup(pool, path).await;
    }
}
