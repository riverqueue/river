//! PostgreSQL-compatible servers without `xmax` or `LISTEN`/`NOTIFY`, like
//! YugabyteDB, simulated on PostgreSQL the way River Go's tests do.
//!
//! A test schema shadows `version()` and `current_setting(text, boolean)`
//! ahead of `pg_catalog` on the connections' `search_path`, so River detects
//! a Yugabyte version and notification setting. When notifications are off,
//! it also shadows `pg_notify` with a function that raises, so any attempt
//! to notify fails. This exercises detection and River's fallbacks, not
//! Yugabyte's storage or transaction semantics.
//!
//! These tests fail rather than skip when `RIVER_RUST_DATABASE_URL` is unset.

#![cfg(all(feature = "postgres", river_postgres_tests))]

mod support;

use std::{convert::Infallible, sync::Arc, time::Duration};

use riverqueue::{
    Client, EventKind, InsertOpts, Job, JobArgs, JobState, QueueConfig, QueueSelector, UniqueOpts,
    WorkContext, WorkOutcome, WorkerRegistry, database::PostgresDatabase,
};
use serde::{Deserialize, Serialize};
use sqlx::{
    AssertSqlSafe, PgPool,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use tokio::sync::Semaphore;

/// Every wait in these tests is bounded by this timeout. It covers a few of
/// the two-second polls for cancellation requests.
const TIMEOUT: Duration = Duration::from_secs(10);

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_yugabyte")]
struct YugabyteArgs {
    value: i32,
}

/// Which server a test schema simulates.
#[derive(Clone, Copy, Debug)]
enum Server {
    /// PostgreSQL 17, before `RETURNING OLD`.
    Postgres17,
    /// YugabyteDB before 2025.2.3, without `yb_enable_listen_notify`.
    YugabyteUnavailable,
    /// YugabyteDB with `yb_enable_listen_notify` off.
    YugabyteDisabled,
    /// YugabyteDB with `yb_enable_listen_notify` on.
    YugabyteEnabled,
}

impl Server {
    const ALL: [Self; 4] = [
        Self::Postgres17,
        Self::YugabyteUnavailable,
        Self::YugabyteDisabled,
        Self::YugabyteEnabled,
    ];

    const fn listen_notify(self) -> bool {
        matches!(self, Self::Postgres17 | Self::YugabyteEnabled)
    }

    const fn yugabyte(self) -> bool {
        !matches!(self, Self::Postgres17)
    }
}

/// A migrated test schema and a pool whose connections see the simulated
/// server.
struct Simulated {
    pool: PgPool,
    schema: support::PostgresSchema,
}

impl Simulated {
    async fn new(server: Server) -> Self {
        let schema = support::PostgresSchema::new("river_yugabyte").await;
        let name = schema.schema.as_deref().unwrap().to_owned();
        let functions = match server {
            Server::Postgres17 => format!(
                "CREATE FUNCTION \"{name}\".current_setting(setting_name text) RETURNS text \
                 LANGUAGE sql AS $$ SELECT CASE WHEN setting_name = 'server_version_num' \
                 THEN '170004' ELSE pg_catalog.current_setting(setting_name) END $$;"
            ),
            Server::YugabyteUnavailable | Server::YugabyteDisabled | Server::YugabyteEnabled => {
                let (version, setting) = match server {
                    Server::YugabyteUnavailable => ("2025.2.1.0", "NULL::text"),
                    Server::YugabyteDisabled => ("2025.2.3.0", "'off'::text"),
                    _ => ("2025.2.3.0", "'on'::text"),
                };
                format!(
                    "CREATE FUNCTION \"{name}\".version() RETURNS text LANGUAGE sql AS $$ \
                     SELECT 'PostgreSQL 15.12-YB-{version}-b1'::text $$; \
                     CREATE FUNCTION \"{name}\".current_setting(setting_name text, missing_ok boolean) \
                     RETURNS text LANGUAGE sql AS $$ SELECT CASE WHEN setting_name = \
                     'yb_enable_listen_notify' THEN {setting} ELSE \
                     pg_catalog.current_setting(setting_name, missing_ok) END $$;"
                )
            }
        };
        sqlx::raw_sql(AssertSqlSafe(functions))
            .execute(&schema.pool)
            .await
            .unwrap();
        if !server.listen_notify() {
            sqlx::raw_sql(AssertSqlSafe(format!(
                "CREATE FUNCTION \"{name}\".pg_notify(text, text) RETURNS void LANGUAGE plpgsql \
                 AS $$ BEGIN RAISE EXCEPTION 'LISTEN/NOTIFY is unavailable'; END $$;"
            )))
            .execute(&schema.pool)
            .await
            .unwrap();
        }
        let url = std::env::var("RIVER_RUST_DATABASE_URL").unwrap();
        let options = url
            .parse::<PgConnectOptions>()
            .unwrap()
            .options([("search_path", format!("{name},pg_catalog"))]);
        let pool = PgPoolOptions::new()
            .max_connections(8)
            .connect_with(options)
            .await
            .unwrap();
        Self { pool, schema }
    }

    fn database(&self) -> PostgresDatabase {
        PostgresDatabase::new(self.pool.clone()).with_schema(self.schema.schema.clone())
    }

    async fn cleanup(self) {
        self.pool.close().await;
        self.schema.cleanup().await;
    }
}

/// An insert-only client detects the server as it goes: unique inserts tell
/// a duplicate from a new row with a nonce on Yugabyte and `xmax` before
/// PostgreSQL 18, and notifications, cancellation, queue changes, and
/// resignation requests work without `pg_notify` when it's unavailable.
#[tokio::test]
async fn detects_the_server_without_starting() {
    for server in Server::ALL {
        let simulated = Simulated::new(server).await;
        let client = Client::builder(simulated.database()).build().unwrap();
        let unique = InsertOpts::default().with_unique(UniqueOpts::new().with_by_args(true));

        let first = client
            .insert(YugabyteArgs { value: 1 })
            .opts(unique.clone())
            .await
            .unwrap();
        assert!(!first.unique_skipped_as_duplicate, "{server:?}");
        let second = client
            .insert(YugabyteArgs { value: 1 })
            .opts(unique.clone())
            .await
            .unwrap();
        assert!(second.unique_skipped_as_duplicate, "{server:?}");
        assert_eq!(second.job.row.id, first.job.row.id, "{server:?}");
        let other = client
            .insert(YugabyteArgs { value: 2 })
            .opts(unique)
            .await
            .unwrap();
        assert!(!other.unique_skipped_as_duplicate, "{server:?}");
        // Like River Go, the nonce stays in the stored metadata.
        let nonce = first
            .job
            .row
            .metadata
            .get::<String>("river:unique_nonce")
            .unwrap();
        assert_eq!(nonce.is_some(), server.yugabyte(), "{server:?}");

        let cancelled = client.jobs().cancel(other.job.row.id).await.unwrap();
        assert_eq!(cancelled.state, JobState::Cancelled, "{server:?}");
        client.queues().pause(QueueSelector::All).await.unwrap();
        client.request_resign().await.unwrap();

        simulated.cleanup().await;
    }
}

/// A client of a server without `LISTEN`/`NOTIFY` works jobs and hears a
/// cancellation from another client by polling, without being configured
/// as poll-only.
#[tokio::test]
async fn polls_without_listen_notify() {
    for server in [Server::YugabyteUnavailable, Server::YugabyteDisabled] {
        let simulated = Simulated::new(server).await;
        let started = Arc::new(Semaphore::new(0));
        let worker_started = Arc::clone(&started);
        let mut workers = WorkerRegistry::new();
        workers
            .register_fn(move |context: WorkContext, job: Job<YugabyteArgs>| {
                let started = Arc::clone(&worker_started);
                async move {
                    if job.args.value == 0 {
                        return Ok::<_, Infallible>(WorkOutcome::Complete);
                    }
                    started.add_permits(1);
                    context.cancellation_token().cancelled().await;
                    // Any outcome but completion becomes the cancellation.
                    Ok(WorkOutcome::Snooze(Duration::from_hours(1)))
                }
            })
            .unwrap();
        let client = Client::builder(simulated.database())
            .queue(
                riverqueue::QUEUE_DEFAULT,
                QueueConfig::new(2).with_fetch_poll_interval(Duration::from_millis(100)),
            )
            .workers(workers)
            .build()
            .unwrap();
        let other = Client::builder(simulated.database()).build().unwrap();
        let mut events = client
            .subscribe(&[EventKind::JobCompleted, EventKind::JobCancelled])
            .unwrap();
        let mut run = client.start().unwrap();
        run.wait_ready().await.unwrap();

        let completed = other.insert(YugabyteArgs { value: 0 }).await.unwrap();
        let event = tokio::time::timeout(TIMEOUT, events.recv())
            .await
            .expect("the job should complete")
            .unwrap();
        let job = &event.as_job().expect("a job event").job;
        assert_eq!(job.id, completed.job.row.id, "{server:?}");
        assert_eq!(job.state, JobState::Completed, "{server:?}");

        let cancellable = other.insert(YugabyteArgs { value: 1 }).await.unwrap();
        tokio::time::timeout(TIMEOUT, started.acquire())
            .await
            .expect("the job should start")
            .unwrap()
            .forget();
        other.jobs().cancel(cancellable.job.row.id).await.unwrap();
        let event = tokio::time::timeout(TIMEOUT, events.recv())
            .await
            .expect("the job should be cancelled")
            .unwrap();
        let job = &event.as_job().expect("a job event").job;
        assert_eq!(job.id, cancellable.job.row.id, "{server:?}");
        assert_eq!(job.state, JobState::Cancelled, "{server:?}");

        tokio::time::timeout(TIMEOUT, run.stop())
            .await
            .expect("the client should stop")
            .unwrap();
        simulated.cleanup().await;
    }
}

/// Like River Go's check that YugabyteDB-incompatible system columns only
/// appear where the unique insert mode replaces them.
#[test]
fn system_columns_appear_only_in_unique_insert_modes() {
    let source = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
    let mut violations = Vec::new();
    let mut directories = vec![source];
    while let Some(directory) = directories.pop() {
        for entry in std::fs::read_dir(&directory).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                directories.push(path);
                continue;
            }
            if path.extension().is_none_or(|extension| extension != "rs")
                || path.ends_with("database/postgres_capabilities.rs")
            {
                continue;
            }
            let contents = std::fs::read_to_string(&path).unwrap();
            for (index, line) in contents.lines().enumerate() {
                if line.trim_start().starts_with("//") {
                    continue;
                }
                let has_column = line
                    .split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
                    .any(|word| ["cmax", "cmin", "ctid", "xmax", "xmin"].contains(&word));
                if has_column {
                    violations.push(format!("{}:{}: {line}", path.display(), index + 1));
                }
            }
        }
    }
    assert!(
        violations.is_empty(),
        "system columns YugabyteDB lacks must only appear in the unique insert modes: {violations:#?}"
    );
}
