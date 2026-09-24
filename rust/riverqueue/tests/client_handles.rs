//! Behavior of the client's scoped operation handles on every backend.
//!
//! Each scenario runs against PostgreSQL (in a unique schema, failing rather
//! than skipping when `RIVER_RUST_DATABASE_URL` is unset) and SQLite (in a
//! temporary file).

mod support;

use riverqueue::{
    Client, Error, JobArgs, JobDeleteManyParams, JobListParams, JobState, JobUpdateParams,
    QueueListParams, QueueSelector, QueueUpdateParams,
};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "client_handles")]
struct HandleArgs {
    name: String,
}

fn args(name: &str) -> HandleArgs {
    HandleArgs {
        name: name.to_owned(),
    }
}

/// Defines each scenario for one backend's `Fixture`.
macro_rules! scenarios {
    () => {
        #[tokio::test(flavor = "multi_thread")]
        async fn job_requests_take_effect_only_when_the_transaction_commits() {
            let fixture = Fixture::new().await;
            let client = &fixture.client;
            let jobs = client.jobs();
            let cancelled = client.insert(args("cancel")).await.unwrap().id();
            let deleted = client.insert(args("delete")).await.unwrap().id();
            let updated = client.insert(args("update")).await.unwrap().id();

            // Every write is rolled back with the caller's transaction.
            let mut tx = fixture.begin().await;
            let row = jobs.cancel(cancelled).tx(&mut tx).await.unwrap();
            assert_eq!(row.state, JobState::Cancelled);
            let row = jobs.delete(deleted).tx(&mut tx).await.unwrap();
            assert_eq!(row.id, deleted);
            let row = jobs
                .update(
                    updated,
                    JobUpdateParams::default().with_output(serde_json::json!("rolled back")),
                )
                .tx(&mut tx)
                .await
                .unwrap();
            assert_eq!(row.metadata["output"], "rolled back");
            // Reads in the transaction see its uncommitted writes.
            assert!(matches!(
                jobs.get(deleted).tx(&mut tx).await,
                Err(Error::NotFound)
            ));
            let inserted = client
                .insert(args("uncommitted"))
                .tx(&mut tx)
                .await
                .unwrap()
                .id();
            let listed = jobs
                .list(JobListParams::default().with_ids([inserted]))
                .tx(&mut tx)
                .await
                .unwrap();
            assert_eq!(listed.jobs.len(), 1);
            tx.rollback().await.unwrap();

            assert_eq!(
                jobs.get(cancelled).await.unwrap().state,
                JobState::Available
            );
            assert!(jobs.get(deleted).await.is_ok());
            assert!(
                !jobs
                    .get(updated)
                    .await
                    .unwrap()
                    .metadata
                    .contains_key("output")
            );
            let listed = jobs
                .list(JobListParams::default().with_ids([inserted]))
                .await
                .unwrap();
            assert!(listed.jobs.is_empty());
            assert!(listed.last_cursor.is_none());

            // The same requests persist once the transaction commits.
            let mut tx = fixture.begin().await;
            jobs.cancel(cancelled).tx(&mut tx).await.unwrap();
            jobs.delete_many(JobDeleteManyParams::matching(
                JobListParams::default().with_ids([deleted]),
            ))
            .tx(&mut tx)
            .await
            .unwrap();
            let retried = jobs.retry(cancelled).tx(&mut tx).await.unwrap();
            assert_eq!(retried.state, JobState::Available);
            tx.commit().await.unwrap();

            assert_eq!(
                jobs.get(cancelled).await.unwrap().state,
                JobState::Available
            );
            assert!(matches!(jobs.get(deleted).await, Err(Error::NotFound)));

            fixture.cleanup().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn job_requests_run_on_the_pool_without_a_transaction() {
            let fixture = Fixture::new().await;
            let jobs = fixture.client.jobs();
            let first = fixture.client.insert(args("first")).await.unwrap().id();
            let second = fixture.client.insert(args("second")).await.unwrap().id();

            let page = jobs
                .list(
                    JobListParams::default()
                        .with_ids([first, second])
                        .with_limit(1),
                )
                .await
                .unwrap();
            assert_eq!(
                page.jobs.iter().map(|job| job.id).collect::<Vec<_>>(),
                [first]
            );
            let cursor = page.last_cursor.expect("a nonempty page has a cursor");
            let page = jobs
                .list(
                    JobListParams::default()
                        .with_ids([first, second])
                        .with_limit(1)
                        .with_after(cursor),
                )
                .await
                .unwrap();
            assert_eq!(
                page.jobs.iter().map(|job| job.id).collect::<Vec<_>>(),
                [second]
            );

            assert_eq!(jobs.cancel(first).await.unwrap().state, JobState::Cancelled);
            assert_eq!(jobs.retry(first).await.unwrap().state, JobState::Available);
            assert_eq!(jobs.delete(second).await.unwrap().id, second);
            assert!(matches!(jobs.delete(second).await, Err(Error::NotFound)));
            assert!(matches!(jobs.cancel(i64::MAX).await, Err(Error::NotFound)));
            assert!(matches!(jobs.retry(i64::MAX).await, Err(Error::NotFound)));

            fixture.cleanup().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn queue_requests_take_effect_only_when_the_transaction_commits() {
            let fixture = Fixture::new().await;
            let queues = fixture.client.queues();
            fixture.insert_queue("alpha").await;
            fixture.insert_queue("beta").await;
            let owner = serde_json::Map::from_iter([("owner".to_owned(), "rust".into())]);

            let mut tx = fixture.begin().await;
            queues.pause(QueueSelector::All).tx(&mut tx).await.unwrap();
            assert!(
                queues
                    .get("alpha")
                    .tx(&mut tx)
                    .await
                    .unwrap()
                    .paused_at
                    .is_some()
            );
            let beta = queues
                .update("beta", QueueUpdateParams::new().metadata(owner.clone()))
                .tx(&mut tx)
                .await
                .unwrap();
            assert_eq!(beta.metadata, owner);
            tx.rollback().await.unwrap();
            assert!(paused(&fixture.client).await.is_empty());
            assert!(queues.get("beta").await.unwrap().metadata.is_empty());

            let mut tx = fixture.begin().await;
            queues.pause(QueueSelector::All).tx(&mut tx).await.unwrap();
            queues
                .update("beta", QueueUpdateParams::new().metadata(owner.clone()))
                .tx(&mut tx)
                .await
                .unwrap();
            tx.commit().await.unwrap();
            assert_eq!(paused(&fixture.client).await, ["alpha", "beta"]);
            assert_eq!(queues.get("beta").await.unwrap().metadata, owner);

            queues.resume("alpha").await.unwrap();
            assert_eq!(paused(&fixture.client).await, ["beta"]);
            queues.resume(QueueSelector::All).await.unwrap();
            assert!(paused(&fixture.client).await.is_empty());

            fixture.cleanup().await;
        }

        #[tokio::test(flavor = "multi_thread")]
        async fn queue_selectors_match_names_literally() {
            let fixture = Fixture::new().await;
            let queues = fixture.client.queues();

            // Selecting every queue succeeds when there are none, like Go.
            queues.pause(QueueSelector::All).await.unwrap();
            queues.resume(QueueSelector::All).await.unwrap();

            fixture.insert_queue("alpha").await;
            assert_eq!(
                QueueSelector::from("alpha"),
                QueueSelector::Named("alpha".to_owned())
            );
            assert_eq!(
                QueueSelector::from("*".to_owned()),
                QueueSelector::Named("*".to_owned())
            );
            // `*` is only a name, and no queue can have it.
            assert!(matches!(queues.pause("*").await, Err(Error::NotFound)));
            assert!(matches!(queues.resume("*").await, Err(Error::NotFound)));
            assert!(paused(&fixture.client).await.is_empty());
            assert!(matches!(
                queues.pause("missing").await,
                Err(Error::NotFound)
            ));
            assert!(matches!(queues.get("missing").await, Err(Error::NotFound)));
            assert!(matches!(
                queues.update("missing", QueueUpdateParams::new()).await,
                Err(Error::NotFound)
            ));

            // Updating without metadata keeps it while refreshing the record.
            let owner = serde_json::Map::from_iter([("owner".to_owned(), "rust".into())]);
            let before = queues
                .update("alpha", QueueUpdateParams::new().metadata(owner.clone()))
                .await
                .unwrap();
            let after = queues
                .update("alpha", QueueUpdateParams::new())
                .await
                .unwrap();
            assert_eq!(after.metadata, owner);
            assert!(after.updated_at >= before.updated_at);

            fixture.cleanup().await;
        }
    };
}

/// Returns the names of paused queues.
async fn paused(client: &Client) -> Vec<String> {
    client
        .queues()
        .list(QueueListParams::default())
        .await
        .unwrap()
        .into_iter()
        .filter(|queue| queue.paused_at.is_some())
        .map(|queue| queue.name)
        .collect()
}

#[cfg(feature = "postgres")]
mod postgres {
    use sqlx::{PgPool, Postgres, Transaction};

    use super::*;
    use crate::support::PostgresSchema;

    struct Fixture {
        client: Client,
        pool: PgPool,
        schema: PostgresSchema,
    }

    impl Fixture {
        async fn new() -> Self {
            let schema = PostgresSchema::new("river_handles").await;
            let client = Client::builder(
                riverqueue::database::PostgresDatabase::new(schema.pool.clone())
                    .schema(schema.schema.clone()),
            )
            .build()
            .unwrap();
            Self {
                client,
                pool: schema.pool.clone(),
                schema,
            }
        }

        async fn begin(&self) -> Transaction<'static, Postgres> {
            self.pool.begin().await.unwrap()
        }

        async fn insert_queue(&self, name: &str) {
            sqlx::query(sqlx::AssertSqlSafe(format!(
                "INSERT INTO {} (name, created_at, metadata, updated_at) \
                 VALUES ($1, now(), '{{}}', now())",
                self.schema.table("river_queue")
            )))
            .bind(name)
            .execute(&self.pool)
            .await
            .unwrap();
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
        client: Client,
        path: std::path::PathBuf,
        pool: SqlitePool,
    }

    impl Fixture {
        async fn new() -> Self {
            let (pool, path) = sqlite_file_pool(4).await;
            let client = Client::builder(pool.clone()).build().unwrap();
            Self { client, path, pool }
        }

        async fn begin(&self) -> Transaction<'static, Sqlite> {
            self.pool.begin_with("BEGIN IMMEDIATE").await.unwrap()
        }

        async fn insert_queue(&self, name: &str) {
            sqlx::query("INSERT INTO river_queue (name, metadata) VALUES (?, jsonb('{}'))")
                .bind(name)
                .execute(&self.pool)
                .await
                .unwrap();
        }

        async fn cleanup(self) {
            sqlite_cleanup(self.pool, self.path).await;
        }
    }

    scenarios!();
}
