//! Intercepted operations on a caller-managed transaction are atomic.
//!
//! An extension step or insert middleware can fail after River's own write
//! succeeded. The caller then gets the error and may still commit its
//! transaction; neither River's write nor the extension's own writes may
//! survive that commit, and the transaction stays usable.
//!
//! PostgreSQL scenarios run in a unique schema and fail rather than skip when
//! `RIVER_RUST_DATABASE_URL` is unset; SQLite scenarios use temporary files.

#![cfg(any(all(feature = "postgres", river_postgres_tests), feature = "sqlite"))]

mod support;

use std::sync::{
    Arc,
    atomic::{AtomicBool, Ordering},
};

use async_trait::async_trait;
use riverqueue::__private::{
    ClientBuilderExt, DatabaseConnection, JobSetStateParams, JobUpdatedParams, JobsInsertedParams,
    Pilot, PilotError,
};
use riverqueue::{
    BoxError, Client, Error, Hook, InsertContext, InsertMiddleware, InsertNext, InsertOpts,
    InsertedJobs, JobArgs, JobRow, JobState,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "operation_scope")]
struct ScopeArgs {
    name: String,
}

fn args(name: &str) -> ScopeArgs {
    ScopeArgs {
        name: name.to_owned(),
    }
}

/// Writes a row of its own after each intercepted operation and then fails
/// while `fail` is set.
struct EffectPilot {
    effect_table: String,
    fail: Arc<AtomicBool>,
}

impl EffectPilot {
    async fn effect(&self, connection: DatabaseConnection<'_>, id: i64) -> Result<(), PilotError> {
        let sql = sqlx::AssertSqlSafe(format!(
            "INSERT INTO {} (job_id) VALUES ({id})",
            self.effect_table
        ));
        match connection {
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
        if self.fail.load(Ordering::SeqCst) {
            return Err("extension step failed after its write".into());
        }
        Ok(())
    }
}

#[async_trait]
impl Pilot for EffectPilot {
    fn intercepts_insert(&self) -> bool {
        true
    }

    fn intercepts_job_cancel_retry(&self) -> bool {
        true
    }

    fn intercepts_job_set_state(&self) -> bool {
        true
    }

    async fn after_jobs_inserted(
        &self,
        connection: DatabaseConnection<'_>,
        params: &JobsInsertedParams<'_>,
    ) -> Result<(), PilotError> {
        self.effect(connection, params.jobs[0].id).await
    }

    async fn after_job_cancel(
        &self,
        connection: DatabaseConnection<'_>,
        params: &JobUpdatedParams,
    ) -> Result<(), PilotError> {
        self.effect(connection, params.job.id).await
    }

    async fn after_job_retry(
        &self,
        connection: DatabaseConnection<'_>,
        params: &JobUpdatedParams,
    ) -> Result<(), PilotError> {
        self.effect(connection, params.job.id).await
    }

    async fn after_jobs_set_state(
        &self,
        connection: DatabaseConnection<'_>,
        params: &JobSetStateParams,
    ) -> Result<(), PilotError> {
        self.effect(connection, params.job_ids[0]).await
    }
}

/// Fails every insertion after River wrote it.
struct FailAfterWrite;

impl InsertMiddleware for FailAfterWrite {
    async fn insert_many(
        &self,
        jobs: Vec<InsertContext>,
        next: InsertNext<'_>,
    ) -> Result<InsertedJobs, Error> {
        next.run(jobs).await?;
        Err(Error::extension(
            riverqueue::ExtensionPhase::InsertMiddleware,
            std::io::Error::other("middleware failed after the write"),
        ))
    }
}

/// Fails to decode every inserted row.
struct FailingDecode;

impl Hook for FailingDecode {
    fn decode_insert_result(
        &self,
        _job: &mut JobRow,
    ) -> impl Future<Output = Result<(), BoxError>> + Send {
        std::future::ready(Err("decode failed on purpose".into()))
    }
}

/// Arguments that serialize but never deserialize, so decoding the inserted
/// row's arguments fails after the write.
#[derive(Clone, Debug, Serialize, JobArgs)]
#[river(kind = "operation_scope_undecodable")]
struct UndecodableArgs {
    name: String,
}

impl<'de> Deserialize<'de> for UndecodableArgs {
    fn deserialize<D: serde::Deserializer<'de>>(_deserializer: D) -> Result<Self, D::Error> {
        Err(serde::de::Error::custom("never decodes"))
    }
}

fn assert_extension_error(error: &Error) {
    assert!(matches!(error, Error::Extension { .. }), "{error}");
}

/// Defines each scenario for one backend's `Fixture`.
macro_rules! scenarios {
    () => {
        #[tokio::test(flavor = "multi_thread")]
        async fn failed_insert_steps_leave_nothing_to_commit() {
            let fixture = Fixture::new().await;
            let fail = Arc::new(AtomicBool::new(true));
            let client = fixture.client(&fail);

            let mut tx = fixture.begin().await;
            let error = client.insert(args("single")).tx(&mut tx).await.unwrap_err();
            assert_extension_error(&error);
            let error = client
                .insert_many([args("many_1"), args("many_2")])
                .tx(&mut tx)
                .await
                .unwrap_err();
            assert_extension_error(&error);
            // The caller's transaction is still usable after the failures.
            fail.store(false, Ordering::SeqCst);
            let kept = client.insert(args("kept")).tx(&mut tx).await.unwrap().id();
            tx.commit().await.unwrap();

            assert_eq!(fixture.job_ids().await, vec![kept]);
            assert_eq!(fixture.effect_ids().await, vec![kept]);
            fixture.cleanup().await;
        }

        // Decode hooks and argument decoding run after the write, but an
        // error from either still means nothing was written, with or
        // without a caller transaction.
        #[tokio::test(flavor = "multi_thread")]
        async fn failed_result_decoding_leaves_nothing_written() {
            let fixture = Fixture::new().await;
            let hooked = fixture.builder().hook(FailingDecode).build().unwrap();
            let plain = fixture.builder().build().unwrap();

            let error = hooked.insert(args("pool")).await.unwrap_err();
            assert_extension_error(&error);
            let error = plain
                .insert(UndecodableArgs {
                    name: "pool".to_owned(),
                })
                .await
                .unwrap_err();
            assert!(matches!(error, Error::Json(_)), "{error}");
            let mut tx = fixture.begin().await;
            let error = hooked.insert(args("tx")).tx(&mut tx).await.unwrap_err();
            assert_extension_error(&error);
            let error = plain
                .insert(UndecodableArgs {
                    name: "tx".to_owned(),
                })
                .tx(&mut tx)
                .await
                .unwrap_err();
            assert!(matches!(error, Error::Json(_)), "{error}");
            tx.commit().await.unwrap();

            assert!(fixture.job_ids().await.is_empty());
            fixture.cleanup().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn failed_insert_middleware_leaves_nothing_to_commit() {
            let fixture = Fixture::new().await;
            let client = fixture
                .builder()
                .insert_middleware(FailAfterWrite)
                .build()
                .unwrap();

            let mut tx = fixture.begin().await;
            let error = client.insert(args("single")).tx(&mut tx).await.unwrap_err();
            assert_extension_error(&error);
            tx.commit().await.unwrap();

            assert!(fixture.job_ids().await.is_empty());
            fixture.cleanup().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn failed_cancel_and_retry_steps_leave_nothing_to_commit() {
            let fixture = Fixture::new().await;
            let fail = Arc::new(AtomicBool::new(false));
            let client = fixture.client(&fail);
            let available = client.insert(args("available")).await.unwrap().id();
            let scheduled = client
                .insert(args("scheduled"))
                .opts(
                    InsertOpts::default()
                        .with_scheduled_at(chrono::Utc::now() + chrono::Duration::hours(1)),
                )
                .await
                .unwrap()
                .id();
            fixture.clear_effects().await;
            fail.store(true, Ordering::SeqCst);

            let mut tx = fixture.begin().await;
            let error = client
                .jobs()
                .cancel(available)
                .tx(&mut tx)
                .await
                .unwrap_err();
            assert_extension_error(&error);
            let error = client
                .jobs()
                .retry(scheduled)
                .tx(&mut tx)
                .await
                .unwrap_err();
            assert_extension_error(&error);
            tx.commit().await.unwrap();

            assert_eq!(fixture.state(available).await, JobState::Available);
            assert_eq!(fixture.state(scheduled).await, JobState::Scheduled);
            assert!(fixture.effect_ids().await.is_empty());
            fixture.cleanup().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn failed_completion_steps_leave_nothing_to_commit() {
            let fixture = Fixture::new().await;
            let fail = Arc::new(AtomicBool::new(false));
            let client = fixture.client(&fail);
            let id = client.insert(args("running")).await.unwrap().id();
            let (_, context) = riverqueue::__private::claim_job_for_test(&client, id)
                .await
                .unwrap();
            fixture.clear_effects().await;
            fail.store(true, Ordering::SeqCst);

            let mut tx = fixture.begin().await;
            let error = client.jobs().complete(id).tx(&mut tx).await.unwrap_err();
            assert_extension_error(&error);
            let error = context.job_complete_tx(&mut tx).await.unwrap_err();
            assert_extension_error(&error);
            tx.commit().await.unwrap();

            assert_eq!(fixture.state(id).await, JobState::Running);
            assert!(fixture.effect_ids().await.is_empty());

            fail.store(false, Ordering::SeqCst);
            let mut tx = fixture.begin().await;
            client.jobs().complete(id).tx(&mut tx).await.unwrap();
            tx.commit().await.unwrap();
            assert_eq!(fixture.state(id).await, JobState::Completed);
            assert_eq!(fixture.effect_ids().await, vec![id]);
            fixture.cleanup().await;
        }
    };
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
            let schema = PostgresSchema::new("river_op_scope").await;
            sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                "CREATE TABLE {} (job_id bigint NOT NULL)",
                schema.table("scope_effect")
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

        fn client(&self, fail: &Arc<AtomicBool>) -> Client {
            self.builder()
                .pilot(EffectPilot {
                    effect_table: self.schema.table("scope_effect"),
                    fail: Arc::clone(fail),
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
            self.ids("scope_effect", "job_id").await
        }

        async fn clear_effects(&self) {
            sqlx::raw_sql(sqlx::AssertSqlSafe(format!(
                "DELETE FROM {}",
                self.schema.table("scope_effect")
            )))
            .execute(&self.schema.pool)
            .await
            .unwrap();
        }

        async fn state(&self, id: i64) -> JobState {
            let state: String = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT state::text FROM {} WHERE id = $1",
                self.schema.table("river_job")
            )))
            .bind(id)
            .fetch_one(&self.schema.pool)
            .await
            .unwrap();
            state.parse().unwrap()
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
            sqlx::raw_sql("CREATE TABLE scope_effect (job_id INTEGER NOT NULL)")
                .execute(&pool)
                .await
                .unwrap();
            Self { path, pool }
        }

        fn builder(&self) -> riverqueue::ClientBuilder {
            Client::builder(self.pool.clone())
        }

        fn client(&self, fail: &Arc<AtomicBool>) -> Client {
            self.builder()
                .pilot(EffectPilot {
                    effect_table: "scope_effect".to_owned(),
                    fail: Arc::clone(fail),
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
            self.ids("SELECT job_id FROM scope_effect ORDER BY job_id")
                .await
        }

        async fn clear_effects(&self) {
            sqlx::raw_sql("DELETE FROM scope_effect")
                .execute(&self.pool)
                .await
                .unwrap();
        }

        async fn state(&self, id: i64) -> JobState {
            let state: String = sqlx::query_scalar("SELECT state FROM river_job WHERE id = ?")
                .bind(id)
                .fetch_one(&self.pool)
                .await
                .unwrap();
            state.parse().unwrap()
        }

        async fn cleanup(self) {
            sqlite_cleanup(self.pool, self.path).await;
        }
    }

    scenarios!();
}
