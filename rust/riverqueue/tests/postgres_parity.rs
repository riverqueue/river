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
        client.queue_pause("missing").await,
        Err(Error::NotFound)
    ));
    assert!(matches!(
        client.queue_resume("missing").await,
        Err(Error::NotFound)
    ));
    client.queue_pause("*").await.unwrap();
    client.queue_resume("*").await.unwrap();

    // Go accepts `|` as a queue-name separator.
    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {} (name, created_at, metadata, updated_at) VALUES ('tenant|emails', now(), '{{}}', now())",
        database.table("river_queue")
    )))
    .execute(&database.pool)
    .await
    .unwrap();
    client.queue_pause("tenant|emails").await.unwrap();
    assert!(
        client
            .queue_get("tenant|emails")
            .await
            .unwrap()
            .paused_at
            .is_some()
    );
    // Pausing an already paused queue still addresses an existing row.
    client.queue_pause("tenant|emails").await.unwrap();
    client.queue_resume("tenant|emails").await.unwrap();
    client.queue_resume("tenant|emails").await.unwrap();
    assert!(
        client
            .queue_get("tenant|emails")
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
        client.job_delete_many(&riverqueue::JobDeleteManyParams::matching(
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
        .job_delete_many(&riverqueue::JobDeleteManyParams::all())
        .await
        .unwrap();
    assert_eq!(
        remaining.iter().map(|job| job.id).collect::<Vec<_>>(),
        vec![locked]
    );
    assert_eq!(
        client.job_get(running).await.unwrap().state,
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
        let first_page = client.job_list(&params).await.unwrap();
        assert_eq!(
            first_page.iter().map(|job| job.id).collect::<Vec<_>>(),
            expected[..2]
        );
        let cursor =
            riverqueue::JobListCursor::from_job(first_page.last().unwrap(), &params).unwrap();
        let second_page = client
            .job_list(&params.clone().with_after(cursor))
            .await
            .unwrap();
        assert_eq!(
            second_page.iter().map(|job| job.id).collect::<Vec<_>>(),
            expected[2..]
        );
    }

    // Multiple states keep the generic predicate and still filter correctly.
    let mut params = JobListParams::default().with_order_by(JobListOrderBy::FinalizedAt);
    params.states = vec![JobState::Completed, JobState::Discarded];
    let both = client.job_list(&params).await.unwrap();
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
            let row = client.job_get(id).await.unwrap();
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
