//! Exact-version seams that add-on crates build on: filtered finalized-job
//! deletion, batched insertion interception, and extension insert options.
//!
//! PostgreSQL scenarios run in a unique schema and fail rather than skip when
//! `RIVER_RUST_DATABASE_URL` is unset; SQLite scenarios use temporary files.

#![cfg(any(feature = "postgres-tests", feature = "sqlite"))]

mod support;

use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use riverqueue::__private::{
    ClientBuilderExt, DatabaseConnection, InsertOptsExt, Pilot, PilotError,
};
use riverqueue::{Client, InsertOpts, JobArgs};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "extension_seams")]
struct SeamArgs {
    value: i64,
}

/// Finalized jobs seeded for deletion: `(queue, state)`, all finalized an
/// hour ago.
const FINALIZED_SEEDS: [(&str, &str); 6] = [
    ("alpha", "completed"),
    ("alpha", "cancelled"),
    ("alpha", "discarded"),
    ("beta", "completed"),
    ("gamma", "completed"),
    ("gamma", "discarded"),
];

/// Deletion filters and the seeds (indexes into [`FINALIZED_SEEDS`]) each
/// leaves in place, applied in order to the same rows.
fn finalized_deletions() -> Vec<(riverqueue::__private::FinalizedJobDeleteParams, Vec<usize>)> {
    use riverqueue::__private::FinalizedJobDeleteParams;

    let before = chrono::Utc::now();
    let mut none = FinalizedJobDeleteParams::new(100);
    none.queues_included = Some(vec!["alpha".to_owned()]);

    let mut alpha_completed = FinalizedJobDeleteParams::new(100);
    alpha_completed.completed_before = Some(before);
    alpha_completed.queues_included = Some(vec!["alpha".to_owned()]);

    let mut not_gamma = FinalizedJobDeleteParams::new(100);
    not_gamma.completed_before = Some(before);
    not_gamma.discarded_before = Some(before);
    not_gamma.queues_excluded = vec!["gamma".to_owned()];

    let mut limited = FinalizedJobDeleteParams::new(1);
    limited.completed_before = Some(before);
    limited.discarded_before = Some(before);

    vec![
        // No horizon keeps everything, like the cleaner's `None` retention.
        (none, vec![0, 1, 2, 3, 4, 5]),
        (alpha_completed, vec![1, 2, 3, 4, 5]),
        (not_gamma, vec![1, 4, 5]),
        // The lowest ID goes first.
        (limited, vec![1, 5]),
    ]
}

/// Records each batch [`Pilot::before_jobs_insert`] receives and tags its
/// jobs.
struct BatchInsertPilot {
    batches: Arc<Mutex<Vec<usize>>>,
}

#[async_trait]
impl Pilot for BatchInsertPilot {
    fn intercepts_insert(&self) -> bool {
        true
    }

    async fn before_jobs_insert(
        &self,
        _connection: DatabaseConnection<'_>,
        jobs: &mut [riverqueue::__private::JobInsertParams<'_>],
    ) -> Result<(), PilotError> {
        self.batches.lock().unwrap().push(jobs.len());
        for job in jobs {
            job.metadata.insert("batched", true)?;
        }
        Ok(())
    }
}

/// Checks that each insertion call reaches the extension as one batch.
async fn assert_batched_insert_interception(builder: impl Fn() -> riverqueue::ClientBuilder) {
    let batches = Arc::new(Mutex::new(Vec::new()));
    let client = builder()
        .pilot(BatchInsertPilot {
            batches: Arc::clone(&batches),
        })
        .build()
        .unwrap();

    let single = client.insert(SeamArgs { value: 1 }).await.unwrap();
    assert_eq!(
        single.job.row.metadata.get::<bool>("batched").unwrap(),
        Some(true)
    );
    let many = client
        .insert_many((2..=4).map(|value| SeamArgs { value }))
        .await
        .unwrap();
    assert!(
        many.iter()
            .all(|job| job.job.row.metadata.contains_key("batched"))
    );
    assert_eq!(*batches.lock().unwrap(), [1, 3]);
}

/// A job type that declares extension options and default metadata.
#[derive(Clone, Debug, Deserialize, Serialize)]
struct DeclaredArgs {
    value: i64,
}

impl JobArgs for DeclaredArgs {
    const KIND: &'static str = "extension_seams_declared";

    fn default_insert_opts() -> InsertOpts {
        InsertOpts::default()
            .with_metadata(json!({"team": "a"}).as_object().unwrap().clone())
            .with_extension_option("declared", json!({"type": true}))
            .with_extension_option("shared", json!("type"))
    }
}

/// The extension options and metadata JSON a job reached the insert hook
/// with.
type SeenInsert = (Map<String, Value>, String);

/// Records what each inserted job reaches the insert hook with.
#[derive(Clone, Default)]
struct OptionsPilot {
    seen: Arc<Mutex<Vec<SeenInsert>>>,
}

#[async_trait]
impl Pilot for OptionsPilot {
    fn intercepts_insert(&self) -> bool {
        true
    }

    async fn before_jobs_insert(
        &self,
        _connection: DatabaseConnection<'_>,
        jobs: &mut [riverqueue::__private::JobInsertParams<'_>],
    ) -> Result<(), PilotError> {
        let mut seen = self.seen.lock().unwrap();
        for job in jobs {
            seen.push((
                job.extension_options.clone(),
                job.metadata.as_raw().get().to_owned(),
            ));
        }
        Ok(())
    }
}

/// Checks that extension options reach the insert hook resolved key by key
/// and aren't persisted, while per-call metadata replaces the job type's
/// default metadata wholesale, as in Go.
async fn assert_extension_options_reach_the_insert_hook(
    builder: impl Fn() -> riverqueue::ClientBuilder,
) {
    let pilot = OptionsPilot::default();
    let client = builder().pilot(pilot.clone()).build().unwrap();

    let defaults = client.insert(DeclaredArgs { value: 1 }).await.unwrap();
    let overridden = client
        .insert(DeclaredArgs { value: 2 })
        .opts(
            InsertOpts::default()
                .with_metadata(json!({"call": 1}).as_object().unwrap().clone())
                .with_extension_option("shared", json!("call")),
        )
        .await
        .unwrap();

    let seen = pilot.seen.lock().unwrap().clone();
    assert_eq!(seen.len(), 2);
    assert_eq!(
        Value::Object(seen[0].0.clone()),
        json!({"declared": {"type": true}, "shared": "type"})
    );
    assert_eq!(seen[0].1, r#"{"team":"a"}"#);
    // A call's metadata replaces the defaults, but not the declared
    // extension options, which the call overrides key by key.
    assert_eq!(
        Value::Object(seen[1].0.clone()),
        json!({"declared": {"type": true}, "shared": "call"})
    );
    assert_eq!(seen[1].1, r#"{"call":1}"#);
    // Nothing about the extension options is persisted. (SQLite adds its
    // insert nonce.)
    for (id, expected) in [
        (defaults.id(), json!({"team": "a"})),
        (overridden.id(), json!({"call": 1})),
    ] {
        let mut metadata: Map<String, Value> =
            serde_json::from_str(client.jobs().get(id).await.unwrap().metadata.as_raw().get())
                .unwrap();
        metadata.remove("river:unique_nonce");
        assert_eq!(Value::Object(metadata), expected);
    }
}

#[cfg(feature = "postgres-tests")]
mod postgres {
    use riverqueue::database::PostgresDatabase;
    use sqlx::AssertSqlSafe;

    use super::*;
    use crate::support::PostgresSchema;

    fn builder(schema: &PostgresSchema) -> riverqueue::ClientBuilder {
        Client::builder(PostgresDatabase::new(schema.pool.clone()).schema(schema.schema.clone()))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn deletes_finalized_jobs_with_the_cleaner_filters() {
        use riverqueue::__private::{DatabaseConfig, delete_finalized_jobs};

        let schema = PostgresSchema::new("seam_finalized_delete").await;
        let mut ids = Vec::new();
        for (queue, state) in FINALIZED_SEEDS {
            let id: i64 = sqlx::query_scalar(AssertSqlSafe(format!(
                "INSERT INTO {} (args, finalized_at, kind, max_attempts, queue, state) \
                 VALUES ('{{}}', now() - interval '1 hour', 'extension_seams', 25, $1, \
                 $2::text::{}) RETURNING id",
                schema.table("river_job"),
                schema.table("river_job_state"),
            )))
            .bind(queue)
            .bind(state)
            .fetch_one(&schema.pool)
            .await
            .unwrap();
            ids.push(id);
        }
        let database = DatabaseConfig::Postgres {
            schema: schema.schema.clone(),
        };
        for (params, kept) in finalized_deletions() {
            let mut connection = schema.pool.acquire().await.unwrap();
            delete_finalized_jobs(
                DatabaseConnection::Postgres(&mut connection),
                &database,
                &params,
            )
            .await
            .unwrap();
            let remaining: Vec<i64> = sqlx::query_scalar(AssertSqlSafe(format!(
                "SELECT id FROM {} ORDER BY id",
                schema.table("river_job")
            )))
            .fetch_all(&schema.pool)
            .await
            .unwrap();
            let expected: Vec<i64> = kept.iter().map(|&index| ids[index]).collect();
            assert_eq!(remaining, expected, "{params:?}");
        }
        schema.cleanup().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn extension_options_reach_the_insert_hook() {
        let schema = PostgresSchema::new("seam_extension_options").await;
        assert_extension_options_reach_the_insert_hook(|| builder(&schema)).await;
        schema.cleanup().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn insertions_reach_the_extension_as_one_batch() {
        let schema = PostgresSchema::new("seam_batch_insert").await;
        assert_batched_insert_interception(|| builder(&schema)).await;
        schema.cleanup().await;
    }
}

#[cfg(feature = "sqlite")]
mod sqlite {
    use super::*;
    use crate::support::{sqlite_cleanup, sqlite_file_pool};

    #[tokio::test(flavor = "multi_thread")]
    async fn deletes_finalized_jobs_with_the_cleaner_filters() {
        use riverqueue::__private::{DatabaseConfig, delete_finalized_jobs};

        let (pool, path) = sqlite_file_pool(4).await;
        let mut ids = Vec::new();
        for (queue, state) in FINALIZED_SEEDS {
            let id: i64 = sqlx::query_scalar(
                "INSERT INTO river_job (args, finalized_at, kind, max_attempts, queue, state) \
                 VALUES (jsonb('{}'), strftime('%Y-%m-%d %H:%M:%f', 'now', '-1 hour'), \
                 'extension_seams', 25, ?, ?) RETURNING id",
            )
            .bind(queue)
            .bind(state)
            .fetch_one(&pool)
            .await
            .unwrap();
            ids.push(id);
        }
        for (params, kept) in finalized_deletions() {
            let mut connection = pool.acquire().await.unwrap();
            delete_finalized_jobs(
                DatabaseConnection::Sqlite(&mut connection),
                &DatabaseConfig::Sqlite,
                &params,
            )
            .await
            .unwrap();
            drop(connection);
            let remaining: Vec<i64> = sqlx::query_scalar("SELECT id FROM river_job ORDER BY id")
                .fetch_all(&pool)
                .await
                .unwrap();
            let expected: Vec<i64> = kept.iter().map(|&index| ids[index]).collect();
            assert_eq!(remaining, expected, "{params:?}");
        }
        sqlite_cleanup(pool, path).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn extension_options_reach_the_insert_hook() {
        let (pool, path) = sqlite_file_pool(4).await;
        assert_extension_options_reach_the_insert_hook(|| Client::builder(pool.clone())).await;
        sqlite_cleanup(pool, path).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn insertions_reach_the_extension_as_one_batch() {
        let (pool, path) = sqlite_file_pool(4).await;
        assert_batched_insert_interception(|| Client::builder(pool.clone())).await;
        sqlite_cleanup(pool, path).await;
    }
}
