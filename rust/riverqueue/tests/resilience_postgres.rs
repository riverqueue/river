//! PostgreSQL runtime behavior under malformed rows and database faults.
//!
//! These tests require `RIVER_RUST_DATABASE_URL` and fail when it is missing.
//! Each test migrates a uniquely named schema so concurrent runs against one
//! database cannot interfere.

#![cfg(feature = "postgres-tests")]

use std::{
    convert::Infallible,
    future::Future,
    sync::atomic::{AtomicUsize, Ordering},
    time::Duration,
};

use riverqueue::{
    Client, InsertOpts, Job, JobArgs, JobState, QueueConfig, WorkContext, WorkOutcome,
    WorkerRegistry,
    database::{PostgresDatabase, SchemaName},
};
use riverqueue_migrate::PostgresMigrator;
use serde::{Deserialize, Serialize};
use sqlx::{AssertSqlSafe, PgPool};

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_postgres_resilience")]
struct ResilienceArgs {}

fn database_url() -> String {
    std::env::var("RIVER_RUST_DATABASE_URL")
        .expect("RIVER_RUST_DATABASE_URL must point at a disposable PostgreSQL test database")
}

/// A migrated schema with a unique name, dropped by [`TestSchema::drop`].
struct TestSchema {
    name: String,
    pool: PgPool,
    schema: SchemaName,
}

impl TestSchema {
    async fn new(label: &str) -> Self {
        static SCHEMA_NONCE: AtomicUsize = AtomicUsize::new(0);
        let name = format!(
            "river_res_{label}_{}_{}",
            std::process::id(),
            SCHEMA_NONCE.fetch_add(1, Ordering::Relaxed)
        );
        let pool = PgPool::connect(&database_url()).await.unwrap();
        sqlx::raw_sql(AssertSqlSafe(format!("CREATE SCHEMA {name}")))
            .execute(&pool)
            .await
            .unwrap();
        let schema = SchemaName::new(name.clone()).unwrap();
        PostgresMigrator::new(pool.clone())
            .with_schema(schema.clone())
            .migrate_up()
            .await
            .unwrap();
        Self { name, pool, schema }
    }

    fn database(&self) -> PostgresDatabase {
        PostgresDatabase::new(self.pool.clone()).schema(self.schema.clone())
    }

    fn table(&self) -> String {
        format!("{}.river_job", self.name)
    }

    async fn execute(&self, sql: String) {
        sqlx::raw_sql(AssertSqlSafe(sql))
            .execute(&self.pool)
            .await
            .unwrap();
    }

    async fn job_state(&self, id: i64) -> String {
        sqlx::query_scalar(AssertSqlSafe(format!(
            "SELECT state::text FROM {} WHERE id = $1",
            self.table()
        )))
        .bind(id)
        .fetch_one(&self.pool)
        .await
        .unwrap()
    }

    async fn drop(self) {
        self.execute(format!("DROP SCHEMA {} CASCADE", self.name))
            .await;
        self.pool.close().await;
    }
}

fn completing_workers() -> WorkerRegistry {
    let mut workers = WorkerRegistry::new();
    workers
        .register_fn(|_context: WorkContext, _job: Job<ResilienceArgs>| async {
            Ok::<_, Infallible>(WorkOutcome::Complete)
        })
        .unwrap();
    workers
}

fn fast_queue() -> QueueConfig {
    QueueConfig::new(4)
        .with_fetch_cooldown(Duration::from_millis(1))
        .with_fetch_poll_interval(Duration::from_millis(20))
}

/// Polls `condition` until it holds, failing the test after `timeout`.
async fn wait_until<F, Fut>(timeout: Duration, description: &str, mut condition: F)
where
    F: FnMut() -> Fut,
    Fut: Future<Output = bool>,
{
    let deadline = tokio::time::Instant::now() + timeout;
    while !condition().await {
        assert!(
            tokio::time::Instant::now() < deadline,
            "timed out waiting for {description}"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn claimed_rows_decode_individually_and_leniently() {
    let schema = TestSchema::new("decode").await;
    let client = Client::builder(schema.database())
        .id("postgres-resilience-decode")
        .without_notifications()
        .workers(completing_workers())
        .queue("default", fast_queue())
        .build()
        .unwrap();

    // River Go decodes attempt errors with `encoding/json`, which tolerates
    // missing and unknown fields.
    let sparse_errors = client.insert(ResilienceArgs {}).await.unwrap();
    schema
        .execute(format!(
            "UPDATE {} SET errors = ARRAY['{{\"error\": \"go\", \"extra\": 1}}'::jsonb] \
             WHERE id = {}",
            schema.table(),
            sparse_errors.job.row.id
        ))
        .await;
    // Array metadata cannot become a `JobRow`. Claiming it with the others
    // must record a failure for it alone.
    let malformed = client
        .insert_with(
            ResilienceArgs {},
            InsertOpts::default().with_max_attempts(1),
        )
        .await
        .unwrap();
    schema
        .execute(format!(
            "UPDATE {} SET metadata = '[1]'::jsonb WHERE id = {}",
            schema.table(),
            malformed.job.row.id
        ))
        .await;
    let ordinary = client.insert(ResilienceArgs {}).await.unwrap();

    let run = client.start().unwrap();
    for id in [sparse_errors.job.row.id, ordinary.job.row.id] {
        wait_until(
            Duration::from_secs(10),
            "decodable job completion",
            || async { schema.job_state(id).await == "completed" },
        )
        .await;
    }
    wait_until(Duration::from_secs(10), "malformed job failure", || async {
        schema.job_state(malformed.job.row.id).await == "discarded"
    })
    .await;
    run.shutdown().await.unwrap();

    let sparse_errors = client.job_get(sparse_errors.job.row.id).await.unwrap();
    assert_eq!(sparse_errors.state, JobState::Completed);
    assert_eq!(sparse_errors.errors.len(), 1);
    assert_eq!(sparse_errors.errors[0].error, "go");
    assert_eq!(sparse_errors.errors[0].attempt, 0);

    let (attempt, errors): (i16, Vec<serde_json::Value>) = sqlx::query_as(AssertSqlSafe(format!(
        "SELECT attempt, errors FROM {} WHERE id = $1",
        schema.table()
    )))
    .bind(malformed.job.row.id)
    .fetch_one(&schema.pool)
    .await
    .unwrap();
    assert_eq!(attempt, 1);
    assert_eq!(errors.len(), 1);
    assert_eq!(errors[0]["attempt"], 1);
    assert!(
        errors[0]["error"]
            .as_str()
            .unwrap()
            .starts_with("River could not decode the job row"),
        "{errors:?}"
    );

    schema.drop().await;
}
