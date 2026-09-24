//! PostgreSQL parity tests for maintenance, leadership, and storage semantics
//! that mirror the Go implementation.

#![cfg(feature = "postgres-tests")]

mod support;

use std::{convert::Infallible, time::Duration};

use riverqueue::{
    Client, Error, Job, JobArgs, JobState, QueueConfig, WorkContext, WorkOutcome, WorkerRegistry,
    database::PostgresDatabase,
};
use serde::{Deserialize, Serialize};

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
        client.queues().pause("missing").await,
        Err(Error::NotFound)
    ));
    assert!(matches!(
        client.queues().resume("missing").await,
        Err(Error::NotFound)
    ));
    client
        .queues()
        .pause(riverqueue::QueueSelector::All)
        .await
        .unwrap();
    client
        .queues()
        .resume(riverqueue::QueueSelector::All)
        .await
        .unwrap();

    // Go accepts `|` as a queue-name separator.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {} (name, created_at, metadata, updated_at) VALUES ('tenant|emails', now(), '{{}}', now())",
        database.table("river_queue")
    )))
    .execute(&database.pool)
    .await
    .unwrap();
    client.queues().pause("tenant|emails").await.unwrap();
    assert!(
        client
            .queues()
            .get("tenant|emails")
            .await
            .unwrap()
            .paused_at
            .is_some()
    );
    // Pausing an already paused queue still addresses an existing row.
    client.queues().pause("tenant|emails").await.unwrap();
    client.queues().resume("tenant|emails").await.unwrap();
    client.queues().resume("tenant|emails").await.unwrap();
    assert!(
        client
            .queues()
            .get("tenant|emails")
            .await
            .unwrap()
            .paused_at
            .is_none()
    );

    database.cleanup().await;
}

/// Inserts a raw job row and returns its ID.
async fn insert_raw_job(
    database: &PostgresSchema,
    state: &str,
    finalized_ago_secs: Option<i64>,
) -> i64 {
    let sql = format!(
        "INSERT INTO {} (args, kind, max_attempts, state, attempt, attempted_at, finalized_at) \
         VALUES ('{{}}', 'parity_raw', 25, $1::text::{}, \
                 CASE WHEN $1 = 'running' THEN 1 ELSE 0 END, \
                 CASE WHEN $1 = 'running' THEN now() END, \
                 now() - make_interval(secs => $2::bigint)) \
         RETURNING id",
        database.table("river_job"),
        database.schema.qualify("river_job_state"),
    );
    sqlx::query_scalar(sqlx::AssertSqlSafe(sql))
        .bind(state)
        .bind(finalized_ago_secs)
        .fetch_one(&database.pool)
        .await
        .unwrap()
}

#[tokio::test(flavor = "multi_thread")]
async fn job_delete_many() {
    let database = PostgresSchema::new("rpp_delete_many").await;
    let client = insert_only_client(&database);

    let running = insert_raw_job(&database, "running", None).await;
    let first = insert_raw_job(&database, "available", None).await;
    let locked = insert_raw_job(&database, "completed", Some(1)).await;
    let last = insert_raw_job(&database, "cancelled", Some(1)).await;

    // Running jobs are excluded before the limit, so a limit of two deletes two
    // non-running rows even though the lowest ID is running.
    let mut blocker = database.pool.begin().await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT id FROM {} WHERE id = $1 FOR UPDATE",
        database.table("river_job")
    )))
    .bind(locked)
    .execute(&mut *blocker)
    .await
    .unwrap();

    // A row locked by another transaction is skipped rather than waited on.
    let deleted = tokio::time::timeout(
        std::time::Duration::from_secs(5),
        client
            .jobs()
            .delete_many(riverqueue::JobDeleteManyParams::matching(
                riverqueue::JobListParams::default()
                    .with_ids([running, first, locked, last])
                    .with_limit(2),
            )),
    )
    .await
    .expect("bulk delete must skip locked rows instead of blocking")
    .unwrap();
    assert_eq!(
        deleted.iter().map(|job| job.id).collect::<Vec<_>>(),
        vec![first, last]
    );
    blocker.rollback().await.unwrap();

    let remaining = client
        .jobs()
        .delete_many(riverqueue::JobDeleteManyParams::all())
        .await
        .unwrap();
    assert_eq!(
        remaining.iter().map(|job| job.id).collect::<Vec<_>>(),
        vec![locked]
    );
    assert_eq!(
        client.jobs().get(running).await.unwrap().state,
        riverqueue::JobState::Running
    );

    database.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn job_list_single_finalized_state_by_time() {
    use riverqueue::{JobListOrderBy, JobListParams, JobState, SortDirection};

    let database = PostgresSchema::new("rpp_list_final").await;
    let client = insert_only_client(&database);

    let oldest = insert_raw_job(&database, "completed", Some(30)).await;
    let newest = insert_raw_job(&database, "completed", Some(10)).await;
    let middle = insert_raw_job(&database, "completed", Some(20)).await;
    let _other_state = insert_raw_job(&database, "discarded", Some(15)).await;

    for (direction, expected) in [
        (SortDirection::Ascending, vec![oldest, middle, newest]),
        (SortDirection::Descending, vec![newest, middle, oldest]),
    ] {
        let mut params = JobListParams::default()
            .with_order_by(JobListOrderBy::Time)
            .with_limit(2);
        params.states = vec![JobState::Completed];
        params.direction = direction;
        let first_page = client.jobs().list(params.clone()).await.unwrap();
        assert_eq!(
            first_page.jobs.iter().map(|job| job.id).collect::<Vec<_>>(),
            expected[..2]
        );
        let cursor = first_page.last_cursor.unwrap();
        let second_page = client
            .jobs()
            .list(params.clone().with_after(cursor))
            .await
            .unwrap()
            .jobs;
        assert_eq!(
            second_page.iter().map(|job| job.id).collect::<Vec<_>>(),
            expected[2..]
        );
    }

    // Multiple states keep the generic predicate and still filter correctly.
    let mut params = JobListParams::default().with_order_by(JobListOrderBy::FinalizedAt);
    params.states = vec![JobState::Completed, JobState::Discarded];
    let both = client.jobs().list(params).await.unwrap().jobs;
    assert_eq!(both.len(), 4);

    database.cleanup().await;
}

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "parity_noop")]
struct NoopArgs {}

fn noop_workers() -> WorkerRegistry {
    let mut workers = WorkerRegistry::new();
    workers
        .register_fn(|_context: WorkContext, _job: Job<NoopArgs>| async {
            Ok::<_, Infallible>(WorkOutcome::Complete)
        })
        .unwrap();
    workers
}

async fn wait_for_job_state(client: &Client, id: i64, state: JobState) -> riverqueue::JobRow {
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let row = client.jobs().get(id).await.unwrap();
            if row.state == state {
                return row;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap_or_else(|_| panic!("job {id} did not reach {state:?}"))
}

#[tokio::test(flavor = "multi_thread")]
async fn schema_names_are_quoted_like_go() {
    // Go quotes any schema with `SafeIdentifier`; a hyphenated mixed-case
    // schema must migrate, notify, elect, and work jobs from Rust.
    let database = PostgresSchema::new("Rpp-Mixed-Schema").await;
    assert!(database.schema.as_deref().unwrap().contains('-'));
    let client = Client::builder(
        PostgresDatabase::new(database.pool.clone()).schema(database.schema.clone()),
    )
    .queue(
        "default",
        QueueConfig::new(1).with_fetch_poll_interval(Duration::from_secs(60)),
    )
    .workers(noop_workers())
    .build()
    .unwrap();
    let mut handle = client.start().unwrap();
    handle.wait_ready().await.unwrap();

    let inserted = client.insert(NoopArgs {}).await.unwrap();
    wait_for_job_state(&client, inserted.job.row.id, JobState::Completed).await;

    handle.shutdown().await.unwrap();
    database.cleanup().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn extension_notify_many_is_transactional() {
    use riverqueue::__private::{
        DatabaseConfig, DatabaseConnection, NotificationTopic, notify_many,
    };

    let database = PostgresSchema::new("rpp_notify_many").await;
    let config = DatabaseConfig::Postgres {
        schema: database.schema.clone(),
    };
    let mut listener = sqlx::postgres::PgListener::connect_with(&database.pool)
        .await
        .unwrap();
    listener
        .listen(&database.schema.notification_topic("river_insert"))
        .await
        .unwrap();

    // A rolled-back transaction delivers nothing; the committed batch that
    // follows is therefore the first thing the listener receives.
    let mut rolled_back = database.pool.begin().await.unwrap();
    notify_many(
        DatabaseConnection::Postgres(&mut rolled_back),
        &config,
        NotificationTopic::Insert,
        &[r#"{"queue":"rolled_back"}"#.to_owned()],
    )
    .await
    .unwrap();
    rolled_back.rollback().await.unwrap();

    let mut committed = database.pool.begin().await.unwrap();
    notify_many(
        DatabaseConnection::Postgres(&mut committed),
        &config,
        NotificationTopic::Insert,
        &[
            r#"{"queue":"first"}"#.to_owned(),
            r#"{"queue":"second"}"#.to_owned(),
        ],
    )
    .await
    .unwrap();
    committed.commit().await.unwrap();

    for expected in [r#"{"queue":"first"}"#, r#"{"queue":"second"}"#] {
        let notification = tokio::time::timeout(Duration::from_secs(5), listener.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(notification.payload(), expected);
    }
    // The listener holds a pooled connection that must be returned first.
    drop(listener);
    database.cleanup().await;
}

#[tokio::test]
async fn rescue_after_defaults_and_validation_match_go() {
    use riverqueue::MaintenanceConfig;

    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy("postgres://localhost/unused")
        .unwrap();
    // Like Go's `RescueStuckJobsAfter`, a rescue age shorter than the job
    // timeout is rejected, while an equal one is accepted.
    let error = Client::builder(pool.clone())
        .job_timeout(Some(Duration::from_mins(5)))
        .maintenance(MaintenanceConfig::default().with_rescue_after(Duration::from_mins(4)))
        .build()
        .unwrap_err();
    assert!(error.to_string().contains("rescue after"), "{error}");
    Client::builder(pool.clone())
        .job_timeout(Some(Duration::from_mins(5)))
        .maintenance(MaintenanceConfig::default().with_rescue_after(Duration::from_mins(5)))
        .build()
        .unwrap();
    assert_eq!(MaintenanceConfig::default().rescue_after(), None);
}

#[tokio::test(flavor = "multi_thread")]
async fn leader_renews_while_maintenance_is_blocked() {
    use riverqueue::MaintenanceConfig;

    let database = PostgresSchema::new("rpp_slow_maintenance").await;
    let expired = insert_raw_job(&database, "completed", Some(48 * 3_600)).await;

    // Hold the expired row so the job cleaner's delete blocks on it.
    let mut blocker = database.pool.begin().await.unwrap();
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "SELECT id FROM {} WHERE id = $1 FOR UPDATE",
        database.table("river_job")
    )))
    .bind(expired)
    .execute(&mut *blocker)
    .await
    .unwrap();

    let client = Client::builder(
        PostgresDatabase::new(database.pool.clone()).schema(database.schema.clone()),
    )
    .maintenance(
        MaintenanceConfig::default()
            .with_elect_interval(Duration::from_millis(50))
            .with_job_cleaner_interval(Duration::from_millis(50)),
    )
    .queue("default", QueueConfig::new(1))
    .workers(noop_workers())
    .build()
    .unwrap();
    let mut handle = client.start().unwrap();

    let blocked_deletes = || {
        let pool = database.pool.clone();
        async move {
            sqlx::query_scalar::<_, i64>(
                "SELECT count(*) FROM pg_stat_activity \
                 WHERE datname = current_database() AND wait_event_type = 'Lock' \
                   AND query LIKE 'DELETE FROM%river_job%'",
            )
            .fetch_one(&pool)
            .await
            .unwrap()
        }
    };
    let lease = || {
        let pool = database.pool.clone();
        let table = database.table("river_leader");
        async move {
            sqlx::query_as::<_, (chrono::DateTime<chrono::Utc>, chrono::DateTime<chrono::Utc>)>(
                sqlx::AssertSqlSafe(format!("SELECT elected_at, expires_at FROM {table}")),
            )
            .fetch_optional(&pool)
            .await
            .unwrap()
        }
    };
    tokio::time::timeout(Duration::from_secs(10), async {
        while blocked_deletes().await == 0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("the job cleaner should block on the locked row");

    // The lease keeps being renewed within the same term while maintenance is
    // stuck, rather than waiting for the blocked service.
    let (elected_at, mut expires_at) = lease().await.unwrap();
    for _ in 0..3 {
        tokio::time::timeout(Duration::from_secs(10), async {
            loop {
                let (current_elected_at, current_expires_at) = lease().await.unwrap();
                assert_eq!(current_elected_at, elected_at);
                if current_expires_at > expires_at {
                    expires_at = current_expires_at;
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("the leader should renew while maintenance is blocked");
    }
    assert!(blocked_deletes().await > 0);

    // Shutdown cancels the blocked statement server-side instead of waiting.
    tokio::time::timeout(Duration::from_secs(10), handle.shutdown())
        .await
        .expect("shutdown should cancel blocked maintenance")
        .unwrap();
    assert_eq!(blocked_deletes().await, 0);
    blocker.rollback().await.unwrap();
    database.cleanup().await;
}
