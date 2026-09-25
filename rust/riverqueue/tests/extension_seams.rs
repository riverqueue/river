//! Exact-version seams that add-on crates build on: fetch claims rolled back
//! by a failed commit, filtered finalized-job deletion, reserved job-type
//! metadata, and batched insertion interception.
//!
//! PostgreSQL scenarios run in a unique schema and fail rather than skip when
//! `RIVER_RUST_DATABASE_URL` is unset; SQLite scenarios use temporary files.

#![cfg(any(feature = "postgres-tests", feature = "sqlite"))]

mod support;

use std::{
    convert::Infallible,
    sync::{Arc, Mutex},
    time::Duration,
};

use async_trait::async_trait;
use riverqueue::__private::{ClientBuilderExt, DatabaseConnection, FetchParams, Pilot, PilotError};
use riverqueue::{
    Client, Job, JobArgs, JobState, QueueConfig, WorkContext, WorkOutcome, WorkerRegistry,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "extension_seams")]
struct SeamArgs {
    value: i64,
}

fn workers() -> WorkerRegistry {
    let mut workers = WorkerRegistry::new();
    workers
        .register_fn(|_context: WorkContext, _job: Job<SeamArgs>| async {
            Ok::<_, Infallible>(WorkOutcome::Complete)
        })
        .unwrap();
    workers
}

fn fast_queue() -> QueueConfig {
    QueueConfig::new(1)
        .with_fetch_cooldown(Duration::from_millis(1))
        .with_fetch_poll_interval(Duration::from_millis(10))
}

/// How [`ClaimingPilot`] takes part in fetches.
#[derive(Clone, Copy)]
enum ClaimMode {
    /// Claims jobs itself through `claim_jobs` (PostgreSQL only here).
    #[cfg_attr(not(feature = "postgres-tests"), allow(dead_code))]
    Claim,
    /// Selects IDs for River to claim through `select_job_ids`.
    Select,
}

/// Claims or selects every available job and records the IDs River reports
/// as rolled back.
struct ClaimingPilot {
    mode: ClaimMode,
    rolled_back: Arc<Mutex<Vec<i64>>>,
}

#[async_trait]
impl Pilot for ClaimingPilot {
    fn intercepts_fetch(&self) -> bool {
        true
    }

    async fn claim_jobs(
        &self,
        connection: DatabaseConnection<'_>,
        params: &FetchParams,
    ) -> Result<Option<Vec<riverqueue::__private::ClaimedJob>>, PilotError> {
        if matches!(self.mode, ClaimMode::Select) {
            return Ok(None);
        }
        match connection {
            #[cfg(feature = "postgres")]
            DatabaseConnection::Postgres(connection) => {
                let table = params
                    .database
                    .postgres_schema()
                    .unwrap()
                    .qualify("river_job");
                let sql = format!(
                    "UPDATE {table} AS job SET state = 'running', attempt = job.attempt + 1, \
                     attempted_at = now(), attempted_by = array_append(job.attempted_by, $1) \
                     WHERE id IN (SELECT id FROM {table} WHERE state = 'available' AND queue = $2 \
                     ORDER BY id LIMIT $3 FOR UPDATE SKIP LOCKED) \
                     RETURNING {}, false AS unique_skipped_as_duplicate",
                    riverqueue::__private::postgres_job_projection("job")
                );
                let rows = sqlx::query(sqlx::AssertSqlSafe(sql))
                    .bind(&params.client_id)
                    .bind(&params.queue)
                    .bind(params.maximum)
                    .fetch_all(connection)
                    .await?;
                Ok(Some(
                    rows.iter()
                        .map(riverqueue::__private::claimed_postgres_job)
                        .collect(),
                ))
            }
            #[allow(unreachable_patterns)]
            _ => {
                let _ = params;
                Ok(None)
            }
        }
    }

    fn claim_jobs_rolled_back(&self, _params: &FetchParams, job_ids: &[i64]) {
        self.rolled_back.lock().unwrap().extend_from_slice(job_ids);
    }

    async fn select_job_ids(
        &self,
        connection: DatabaseConnection<'_>,
        params: &FetchParams,
    ) -> Result<Option<Vec<i64>>, PilotError> {
        let ids = match connection {
            #[cfg(feature = "postgres")]
            DatabaseConnection::Postgres(connection) => {
                let table = params
                    .database
                    .postgres_schema()
                    .unwrap()
                    .qualify("river_job");
                sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                    "SELECT id FROM {table} WHERE state = 'available' AND queue = $1 \
                     ORDER BY id LIMIT $2 FOR UPDATE SKIP LOCKED"
                )))
                .bind(&params.queue)
                .bind(params.maximum)
                .fetch_all(connection)
                .await?
            }
            #[cfg(feature = "sqlite")]
            DatabaseConnection::Sqlite(connection) => {
                sqlx::query_scalar(
                    "SELECT id FROM river_job WHERE state = 'available' AND queue = ? \
                     ORDER BY id LIMIT ?",
                )
                .bind(&params.queue)
                .bind(params.maximum)
                .fetch_all(connection)
                .await?
            }
            #[allow(unreachable_patterns)]
            _ => return Ok(None),
        };
        Ok(Some(ids))
    }
}

/// Waits until `rolled_back` reports `id`, failing after ten seconds.
async fn wait_for_rollback(rolled_back: &Mutex<Vec<i64>>, id: i64) {
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while !rolled_back.lock().unwrap().contains(&id) {
        assert!(
            tokio::time::Instant::now() < deadline,
            "claim of job {id} was never reported as rolled back"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Starts a client whose fetch commits fail until `unblock` runs, and checks
/// that the extension hears about every rolled-back claim and that the job
/// is worked once commits succeed.
async fn assert_claims_roll_back<F, Fut>(
    builder: riverqueue::ClientBuilder,
    mode: ClaimMode,
    unblock: F,
) where
    F: FnOnce() -> Fut,
    Fut: std::future::Future<Output = ()>,
{
    let rolled_back = Arc::new(Mutex::new(Vec::new()));
    let client = builder
        .pilot(ClaimingPilot {
            mode,
            rolled_back: Arc::clone(&rolled_back),
        })
        .queue("default", fast_queue())
        .workers(workers())
        .build()
        .unwrap();
    let id = client.insert(SeamArgs { value: 1 }).await.unwrap().id();
    let mut run = client.start().unwrap();
    run.wait_ready().await.unwrap();

    wait_for_rollback(&rolled_back, id).await;
    assert_eq!(
        client.jobs().get(id).await.unwrap().state,
        JobState::Available
    );

    unblock().await;
    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while client.jobs().get(id).await.unwrap().state != JobState::Completed {
        assert!(
            tokio::time::Instant::now() < deadline,
            "job never completed"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    run.shutdown().await.unwrap();
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
/// jobs, optionally leaving fast insertions to River.
struct BatchInsertPilot {
    batches: Arc<Mutex<Vec<usize>>>,
    intercepts_fast: bool,
}

#[async_trait]
impl Pilot for BatchInsertPilot {
    fn intercepts_insert(&self) -> bool {
        true
    }

    fn intercepts_fast_insert(&self) -> bool {
        self.intercepts_fast
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

/// Checks that each insertion call reaches the extension as one batch, and
/// that fast insertions skip it when the extension asks.
async fn assert_batched_insert_interception(builder: impl Fn() -> riverqueue::ClientBuilder) {
    for intercepts_fast in [true, false] {
        let batches = Arc::new(Mutex::new(Vec::new()));
        let client = builder()
            .pilot(BatchInsertPilot {
                batches: Arc::clone(&batches),
                intercepts_fast,
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
        let fast = client
            .insert_many((5..=6).map(|value| SeamArgs { value }))
            .fast()
            .await
            .unwrap();
        assert_eq!(fast, 2);

        let expected: &[usize] = if intercepts_fast { &[1, 3, 2] } else { &[1, 3] };
        assert_eq!(*batches.lock().unwrap(), expected);
        let listed = client
            .jobs()
            .list(riverqueue::JobListParams::default().limit(100))
            .await
            .unwrap()
            .jobs;
        let fast_batched = listed
            .iter()
            .filter(|job| {
                serde_json::from_str::<SeamArgs>(job.encoded_args.get())
                    .is_ok_and(|args| args.value >= 5)
            })
            .all(|job| job.metadata.contains_key("batched"));
        assert_eq!(fast_batched, intercepts_fast);
        client
            .jobs()
            .delete_many(riverqueue::JobDeleteManyParams::all())
            .await
            .unwrap();
    }
}

#[cfg(feature = "postgres-tests")]
mod postgres {
    use riverqueue::database::PostgresDatabase;
    use sqlx::AssertSqlSafe;

    use super::*;
    use crate::support::PostgresSchema;

    /// Makes every commit that leaves a job `running` fail, like a deferred
    /// constraint violated by the fetch.
    async fn fail_running_commits(schema: &PostgresSchema) {
        let name = schema.schema.as_deref().unwrap();
        sqlx::raw_sql(AssertSqlSafe(format!(
            "CREATE FUNCTION \"{name}\".fail_running_commit() RETURNS trigger LANGUAGE plpgsql AS $$ \
             BEGIN IF NEW.state = 'running' THEN RAISE EXCEPTION 'fetch commit failed on purpose'; \
             END IF; RETURN NULL; END $$; \
             CREATE CONSTRAINT TRIGGER fail_running_commit AFTER UPDATE ON {} \
             DEFERRABLE INITIALLY DEFERRED FOR EACH ROW \
             EXECUTE FUNCTION \"{name}\".fail_running_commit();",
            schema.table("river_job")
        )))
        .execute(&schema.pool)
        .await
        .unwrap();
    }

    async fn allow_running_commits(schema: &PostgresSchema) {
        sqlx::raw_sql(AssertSqlSafe(format!(
            "DROP TRIGGER fail_running_commit ON {}",
            schema.table("river_job")
        )))
        .execute(&schema.pool)
        .await
        .unwrap();
    }

    fn builder(schema: &PostgresSchema) -> riverqueue::ClientBuilder {
        Client::builder(PostgresDatabase::new(schema.pool.clone()).schema(schema.schema.clone()))
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn failed_fetch_commits_roll_back_extension_claims() {
        let schema = PostgresSchema::new("seam_claim_rollback").await;
        fail_running_commits(&schema).await;

        assert_claims_roll_back(builder(&schema), ClaimMode::Claim, || {
            allow_running_commits(&schema)
        })
        .await;
        schema.cleanup().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn failed_fetch_commits_roll_back_extension_selections() {
        let schema = PostgresSchema::new("seam_select_rollback").await;
        fail_running_commits(&schema).await;

        assert_claims_roll_back(builder(&schema), ClaimMode::Select, || {
            allow_running_commits(&schema)
        })
        .await;
        schema.cleanup().await;
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
    async fn failed_fetch_commits_roll_back_extension_selections() {
        let (pool, path) = sqlite_file_pool(4).await;
        // A deferred foreign key violated by every claim fails the COMMIT.
        sqlx::raw_sql(
            "CREATE TABLE guard_parent (id INTEGER PRIMARY KEY); \
             CREATE TABLE fetch_guard (parent_id INTEGER REFERENCES guard_parent (id) \
                 DEFERRABLE INITIALLY DEFERRED); \
             CREATE TRIGGER fail_running_commit AFTER UPDATE OF state ON river_job \
             WHEN NEW.state = 'running' BEGIN INSERT INTO fetch_guard VALUES (-1); END;",
        )
        .execute(&pool)
        .await
        .unwrap();

        let unblock_pool = pool.clone();
        assert_claims_roll_back(
            Client::builder(pool.clone()),
            ClaimMode::Select,
            || async move {
                sqlx::raw_sql("DROP TRIGGER fail_running_commit")
                    .execute(&unblock_pool)
                    .await
                    .unwrap();
            },
        )
        .await;
        sqlite_cleanup(pool, path).await;
    }

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
    async fn insertions_reach_the_extension_as_one_batch() {
        let (pool, path) = sqlite_file_pool(4).await;
        assert_batched_insert_interception(|| Client::builder(pool.clone())).await;
        sqlite_cleanup(pool, path).await;
    }
}
