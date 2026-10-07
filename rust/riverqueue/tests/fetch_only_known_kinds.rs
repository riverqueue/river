//! Clients built with `fetch_only_known_kinds`, on every backend.
//!
//! PostgreSQL tests run in a unique schema and fail rather than skip when
//! `RIVER_RUST_DATABASE_URL` is unset; SQLite tests use a temporary file.

#![cfg(any(all(feature = "postgres", river_postgres_tests), feature = "sqlite"))]

mod support;

use std::{collections::HashSet, convert::Infallible, time::Duration};

use riverqueue::{
    __private::Database, Client, EventKind, Job, JobArgs, JobState, QueueConfig, WorkContext,
    WorkOutcome, Workers,
};
use serde::{Deserialize, Serialize};

/// Every wait in these tests is bounded by this timeout.
const TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_fetch_known", aliases("rust_fetch_known_old"))]
struct KnownArgs {}

/// Inserts jobs under the known kind's alias.
#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_fetch_known_old")]
struct AliasArgs {}

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_fetch_unknown")]
struct UnknownArgs {}

/// A migrated database on one backend.
enum Backend {
    #[cfg(all(feature = "postgres", river_postgres_tests))]
    Postgres(support::PostgresSchema),
    #[cfg(feature = "sqlite")]
    Sqlite(sqlx::SqlitePool, std::path::PathBuf),
}

impl Backend {
    fn database(&self) -> Database {
        match self {
            #[cfg(all(feature = "postgres", river_postgres_tests))]
            Self::Postgres(schema) => Database::from_source(
                riverqueue::database::PostgresDatabase::new(schema.pool.clone())
                    .with_schema(schema.schema.clone()),
            ),
            #[cfg(feature = "sqlite")]
            Self::Sqlite(pool, _) => Database::from_source(pool.clone()),
        }
    }

    async fn cleanup(self) {
        match self {
            #[cfg(all(feature = "postgres", river_postgres_tests))]
            Self::Postgres(schema) => schema.cleanup().await,
            #[cfg(feature = "sqlite")]
            Self::Sqlite(pool, path) => support::sqlite_cleanup(pool, path).await,
        }
    }
}

/// Works jobs of the registered kind and its alias, and leaves a job of
/// another kind available without using an attempt, even though it's
/// first in the queue.
async fn claims_only_registered_kinds(backend: Backend) {
    let inserter = Client::builder(backend.database()).build().unwrap();
    let unknown = inserter.insert(UnknownArgs {}).await.unwrap().job.row.id;
    let known = inserter.insert(KnownArgs {}).await.unwrap().job.row.id;
    let alias = inserter.insert(AliasArgs {}).await.unwrap().job.row.id;

    let mut workers = Workers::new();
    workers
        .add_fn(|_context: WorkContext, _job: Job<KnownArgs>| async {
            Ok::<_, Infallible>(WorkOutcome::Complete)
        })
        .unwrap();
    let client = Client::builder(backend.database())
        .fetch_only_known_kinds(true)
        .queue(riverqueue::QUEUE_DEFAULT, QueueConfig::new(10))
        .without_leader_election()
        .workers(workers)
        .build()
        .unwrap();
    let mut events = client
        .subscribe(&[EventKind::JobCompleted, EventKind::JobFailed])
        .unwrap();
    let mut run = client.start().unwrap();
    run.wait_ready().await.unwrap();

    let mut completed = HashSet::new();
    while completed.len() < 2 {
        let event = tokio::time::timeout(TIMEOUT, events.recv())
            .await
            .expect("known jobs should complete")
            .unwrap();
        let job = &event.as_job().expect("a job event").job;
        assert_eq!(job.state, JobState::Completed, "{job:?}");
        completed.insert(job.id);
    }
    assert_eq!(completed, HashSet::from([known, alias]));

    tokio::time::timeout(TIMEOUT, run.stop())
        .await
        .expect("the client should stop")
        .unwrap();
    let unknown = client.jobs().get(unknown).await.unwrap();
    assert_eq!(unknown.state, JobState::Available);
    assert_eq!(unknown.attempt, 0);
    assert_eq!(unknown.attempted_by, Vec::<String>::new());

    backend.cleanup().await;
}

#[cfg(all(feature = "postgres", river_postgres_tests))]
mod postgres {
    use super::*;

    #[tokio::test]
    async fn claims_only_registered_kinds() {
        super::claims_only_registered_kinds(Backend::Postgres(
            support::PostgresSchema::new("river_known_kinds").await,
        ))
        .await;
    }
}

#[cfg(feature = "sqlite")]
mod sqlite {
    use super::*;

    #[tokio::test]
    async fn claims_only_registered_kinds() {
        let (pool, path) = support::sqlite_file_pool(4).await;
        super::claims_only_registered_kinds(Backend::Sqlite(pool, path)).await;
    }
}
