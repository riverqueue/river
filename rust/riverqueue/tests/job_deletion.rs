//! Client deletions and the job cleaner run through the pilot's deletion
//! hooks, inside the deletion's transaction, like River Go's
//! `PilotJobDeleter`.
//!
//! Postgres scenarios run in a unique schema and fail rather than skip when
//! `RIVER_RUST_DATABASE_URL` is unset; SQLite scenarios use temporary files.

#![cfg(any(all(feature = "postgres", river_postgres_tests), feature = "sqlite"))]

mod support;

use std::{
    convert::Infallible,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use riverqueue::__private::{
    ClientBuilderExt, DatabaseConnection, JobDeleteBeforeContext, JobDeleteBeforeNext,
    JobDeleteContext, JobDeleteManyContext, JobDeleteManyNext, JobDeleteNext, Pilot, PilotError,
};
use riverqueue::{
    Client, Error, Job, JobArgs, JobDeleteManyParams, JobListParams, JobRow, MaintenanceConfig,
    QueueConfig, Retention, WorkContext, WorkOutcome, Workers,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "job_deletion")]
struct DeletionArgs {}

/// Runs River's standard deletion, then records the deleted IDs in a table
/// of its own on the same connection (zero for a cleaner batch), and fails
/// afterwards while `fail` is set.
struct DeletePilot {
    effect_table: String,
    fail: Arc<AtomicBool>,
}

impl DeletePilot {
    async fn record(
        &self,
        mut connection: DatabaseConnection<'_>,
        ids: &[i64],
    ) -> Result<(), PilotError> {
        for id in ids {
            let sql = sqlx::AssertSqlSafe(format!(
                "INSERT INTO {} (job_id) VALUES ({id})",
                self.effect_table
            ));
            match connection.reborrow() {
                #[cfg(feature = "postgres")]
                DatabaseConnection::Postgres(connection) => {
                    sqlx::query(sql).execute(connection).await?;
                }
                #[cfg(feature = "sqlite")]
                DatabaseConnection::Sqlite(connection) => {
                    sqlx::query(sql).execute(connection).await?;
                }
                #[allow(unreachable_patterns)]
                _ => unreachable!("built-in backends only"),
            }
        }
        if self.fail.load(Ordering::SeqCst) {
            return Err("extension step failed after its write".into());
        }
        Ok(())
    }
}

#[async_trait]
impl Pilot for DeletePilot {
    async fn job_delete(
        &self,
        mut connection: DatabaseConnection<'_>,
        _context: &JobDeleteContext,
        next: JobDeleteNext<'_>,
    ) -> Result<JobRow, PilotError> {
        let row = next.delete(connection.reborrow()).await?;
        self.record(connection, &[row.id]).await?;
        Ok(row)
    }

    async fn job_delete_before(
        &self,
        mut connection: DatabaseConnection<'_>,
        _context: &JobDeleteBeforeContext<'_>,
        next: JobDeleteBeforeNext<'_>,
    ) -> Result<u64, PilotError> {
        let count = next.delete(connection.reborrow()).await?;
        if count > 0 {
            self.record(connection, &[0]).await?;
        }
        Ok(count)
    }

    async fn job_delete_many(
        &self,
        mut connection: DatabaseConnection<'_>,
        _context: &JobDeleteManyContext<'_>,
        next: JobDeleteManyNext<'_>,
    ) -> Result<Vec<JobRow>, PilotError> {
        let rows = next.delete(connection.reborrow()).await?;
        let ids = rows.iter().map(|row| row.id).collect::<Vec<_>>();
        self.record(connection, &ids).await?;
        Ok(rows)
    }
}

fn ids(ids: &[i64]) -> JobDeleteManyParams {
    JobDeleteManyParams::matching(JobListParams::default().ids(ids.iter().copied()))
}

/// Defines each scenario for one backend's `Fixture`.
macro_rules! scenarios {
    () => {
        #[tokio::test(flavor = "multi_thread")]
        async fn default_hooks_delete_like_river() {
            // Without a pilot, the no-op pilot's default hooks run.
            let fixture = Fixture::new().await;
            let client = fixture.builder().build().unwrap();
            let single = client.insert(DeletionArgs {}).await.unwrap().id();
            let many = client.insert(DeletionArgs {}).await.unwrap().id();

            assert_eq!(client.jobs().delete(single).await.unwrap().id, single);
            assert_eq!(
                client.jobs().delete_many(ids(&[many])).await.unwrap()[0].id,
                many
            );
            assert!(fixture.job_ids().await.is_empty());
            fixture.cleanup().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn deletes_through_pilot_in_the_deletions_transaction() {
            let fixture = Fixture::new().await;
            let client = fixture.pilot_client(false);
            let single = client.insert(DeletionArgs {}).await.unwrap().id();
            let many = client.insert(DeletionArgs {}).await.unwrap().id();
            let rolled_back = client.insert(DeletionArgs {}).await.unwrap().id();
            let running = client.insert(DeletionArgs {}).await.unwrap().id();
            riverqueue::__private::claim_job_for_test(&client, running)
                .await
                .unwrap();

            assert_eq!(client.jobs().delete(single).await.unwrap().id, single);
            assert_eq!(
                client.jobs().delete_many(ids(&[many])).await.unwrap()[0].id,
                many
            );
            assert_eq!(fixture.effect_ids().await, [single, many]);
            // River's errors from `next` reach the caller unchanged.
            let error = client.jobs().delete(single).await.unwrap_err();
            assert!(matches!(error, Error::NotFound(_)), "{error}");
            let error = client.jobs().delete(running).await.unwrap_err();
            assert!(matches!(error, Error::JobRunning), "{error}");

            // The hook's write rolls back with the caller's transaction.
            let mut tx = fixture.begin().await;
            client.jobs().delete(rolled_back).tx(&mut tx).await.unwrap();
            tx.rollback().await.unwrap();
            assert_eq!(fixture.job_ids().await, [rolled_back, running]);
            assert_eq!(fixture.effect_ids().await, [single, many]);

            // The cleaner deletes a finalized job through the pilot.
            client.jobs().cancel(rolled_back).await.unwrap();
            let mut handle = client.start().unwrap();
            let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
            while fixture.job_ids().await != [running] {
                assert!(tokio::time::Instant::now() < deadline, "cleaner didn't run");
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
            handle.stop().await.unwrap();
            assert_eq!(fixture.effect_ids().await, [0, single, many]);
            fixture.cleanup().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn failed_hooks_roll_back_the_deletion() {
            let fixture = Fixture::new().await;
            let client = fixture.pilot_client(true);
            let single = client.insert(DeletionArgs {}).await.unwrap().id();
            let many = client.insert(DeletionArgs {}).await.unwrap().id();

            let error = client.jobs().delete(single).await.unwrap_err();
            assert!(matches!(error, Error::Extension { .. }), "{error}");
            let error = client.jobs().delete_many(ids(&[many])).await.unwrap_err();
            assert!(matches!(error, Error::Extension { .. }), "{error}");

            let mut tx = fixture.begin().await;
            let error = client.jobs().delete(single).tx(&mut tx).await.unwrap_err();
            assert!(matches!(error, Error::Extension { .. }), "{error}");
            tx.rollback().await.unwrap();

            assert_eq!(fixture.job_ids().await, [single, many]);
            assert!(fixture.effect_ids().await.is_empty());
            fixture.cleanup().await;
        }
    };
}

/// A worker for the started client's idle queue, which no test job uses.
fn workers() -> Workers {
    let mut workers = Workers::new();
    workers
        .add_fn(|_context: WorkContext, _job: Job<DeletionArgs>| async {
            Ok::<_, Infallible>(WorkOutcome::Complete)
        })
        .unwrap();
    workers
}

/// Maintenance that lets the cleaner delete a cancelled job at once.
fn maintenance() -> MaintenanceConfig {
    MaintenanceConfig::default()
        .with_cancelled_job_retention(Retention::DeleteAfter(Duration::from_millis(1)))
        .with_elect_interval(Duration::from_millis(20))
        .with_job_cleaner_interval(Duration::from_millis(20))
}

#[cfg(all(feature = "postgres", river_postgres_tests))]
mod postgres {
    use riverqueue::database::PostgresDatabase;
    use sqlx::{Postgres, Transaction};

    use super::*;
    use crate::support::PostgresSchema;

    struct Fixture {
        schema: PostgresSchema,
    }

    impl Fixture {
        async fn new() -> Self {
            let schema = PostgresSchema::new("river_job_deletion").await;
            sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                "CREATE TABLE {} (job_id bigint NOT NULL)",
                schema.table("delete_effect")
            )))
            .execute(&schema.pool)
            .await
            .unwrap();
            Self { schema }
        }

        fn builder(&self) -> riverqueue::ClientBuilder {
            Client::builder(
                PostgresDatabase::new(self.schema.pool.clone())
                    .with_schema(self.schema.schema.clone()),
            )
        }

        fn pilot_client(&self, fail: bool) -> Client {
            self.builder()
                .maintenance(maintenance())
                .queue("job_deletion_idle", QueueConfig::new(1))
                .workers(workers())
                .pilot(DeletePilot {
                    effect_table: self.schema.table("delete_effect"),
                    fail: Arc::new(AtomicBool::new(fail)),
                })
                .build()
                .unwrap()
        }

        async fn begin(&self) -> Transaction<'static, Postgres> {
            self.schema.pool.begin().await.unwrap()
        }

        async fn ids(&self, table: &str, column: &str) -> Vec<i64> {
            sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT {column} FROM {} ORDER BY {column}",
                self.schema.table(table)
            )))
            .fetch_all(&self.schema.pool)
            .await
            .unwrap()
        }

        async fn job_ids(&self) -> Vec<i64> {
            self.ids("river_job", "id").await
        }

        async fn effect_ids(&self) -> Vec<i64> {
            self.ids("delete_effect", "job_id").await
        }

        async fn cleanup(self) {
            self.schema.cleanup().await;
        }
    }

    scenarios!();
}

#[cfg(feature = "sqlite")]
mod sqlite {
    use sqlx::{Sqlite, SqlitePool, Transaction};

    use super::*;
    use crate::support::{sqlite_cleanup, sqlite_file_pool};

    struct Fixture {
        path: std::path::PathBuf,
        pool: SqlitePool,
    }

    impl Fixture {
        async fn new() -> Self {
            let (pool, path) = sqlite_file_pool(4).await;
            sqlx::raw_sql("CREATE TABLE delete_effect (job_id INTEGER NOT NULL)")
                .execute(&pool)
                .await
                .unwrap();
            Self { path, pool }
        }

        fn builder(&self) -> riverqueue::ClientBuilder {
            Client::builder(self.pool.clone())
        }

        fn pilot_client(&self, fail: bool) -> Client {
            self.builder()
                .maintenance(maintenance())
                .queue("job_deletion_idle", QueueConfig::new(1))
                .workers(workers())
                .pilot(DeletePilot {
                    effect_table: "delete_effect".to_owned(),
                    fail: Arc::new(AtomicBool::new(fail)),
                })
                .build()
                .unwrap()
        }

        async fn begin(&self) -> Transaction<'static, Sqlite> {
            self.pool.begin_with("BEGIN IMMEDIATE").await.unwrap()
        }

        async fn ids(&self, sql: &'static str) -> Vec<i64> {
            sqlx::query_scalar(sql).fetch_all(&self.pool).await.unwrap()
        }

        async fn job_ids(&self) -> Vec<i64> {
            self.ids("SELECT id FROM river_job ORDER BY id").await
        }

        async fn effect_ids(&self) -> Vec<i64> {
            self.ids("SELECT job_id FROM delete_effect ORDER BY job_id")
                .await
        }

        async fn cleanup(self) {
            sqlite_cleanup(self.pool, self.path).await;
        }
    }

    scenarios!();
}
