use chrono::{DateTime, Duration, SubsecRound, Utc};
use riverqueue::{
    Client, Error, InsertBatch, JobArgs, JobDeleteManyParams, JobListCursor, JobListOrderBy,
    JobListParams, JobState, JobUpdateParams, QueueListParams, QueueSelector, QueueUpdateParams,
    SortDirection,
};
use riverqueue_migrate::SqliteMigrator;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};
use sqlx::{Row, SqlitePool, sqlite::SqlitePoolOptions};

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "sqlite_empty_batch")]
struct EmptyBatchArgs {
    value: i32,
}

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "sqlite_unencodable")]
struct UnencodableArgs {
    // JSON object keys must be strings.
    pairs: std::collections::BTreeMap<(i32, i32), i32>,
}

#[tokio::test]
async fn batch_encoding_errors_surface_at_insert_without_inserting() {
    let (client, pool) = setup().await;

    let mut batch = InsertBatch::new();
    batch
        .push(EmptyBatchArgs { value: 1 })
        .push(UnencodableArgs {
            pairs: std::collections::BTreeMap::from([((1, 2), 3)]),
        });
    let error = client.insert_batch(batch).await.unwrap_err();

    assert!(matches!(error, Error::Json(_)), "{error:?}");
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM river_job")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert_eq!(count, 0);

    pool.close().await;
}

#[tokio::test]
async fn empty_batches_are_rejected_before_database_work() {
    let (client, pool) = setup().await;

    let empty_many = client
        .insert_many(Vec::<EmptyBatchArgs>::new())
        .await
        .unwrap_err();
    assert_eq!(
        empty_many.to_string(),
        "invalid job: job: no jobs to insert"
    );
    let empty_batch = client.insert_batch(InsertBatch::new()).await.unwrap_err();
    assert_eq!(
        empty_batch.to_string(),
        "invalid job: job: no jobs to insert"
    );

    let mut transaction = pool.begin().await.unwrap();
    let empty_many_tx = client
        .insert_many(Vec::<EmptyBatchArgs>::new())
        .tx(&mut transaction)
        .await
        .unwrap_err();
    assert_eq!(
        empty_many_tx.to_string(),
        "invalid job: job: no jobs to insert"
    );
    let empty_batch_tx = client
        .insert_batch(InsertBatch::new())
        .tx(&mut transaction)
        .await
        .unwrap_err();
    assert_eq!(
        empty_batch_tx.to_string(),
        "invalid job: job: no jobs to insert"
    );
    transaction.commit().await.unwrap();

    pool.close().await;
}

#[tokio::test]
async fn job_list_time_without_states_uses_schedule_and_finalized_requires_states() {
    let (client, pool) = setup().await;
    let now = Utc::now();
    let first = insert_job(
        &pool,
        JobSeed {
            scheduled_at: now + Duration::hours(1),
            ..JobSeed::default()
        },
    )
    .await;
    let second = insert_job(
        &pool,
        JobSeed {
            scheduled_at: now - Duration::hours(1),
            ..JobSeed::default()
        },
    )
    .await;

    let params = JobListParams::default()
        .ids([first, second])
        .order_by(JobListOrderBy::Time);
    let rows = client.jobs().list(params.clone()).await.unwrap().jobs;
    assert_eq!(
        rows.iter().map(|row| row.id).collect::<Vec<_>>(),
        [second, first]
    );
    let cursor = JobListCursor::from_job(&rows[0], &params).unwrap();
    let page = client
        .jobs()
        .list(params.clone().after(cursor))
        .await
        .unwrap()
        .jobs;
    assert_eq!(page.iter().map(|row| row.id).collect::<Vec<_>>(), [first]);

    let error = client
        .jobs()
        .list(JobListParams::default().order_by(JobListOrderBy::FinalizedAt))
        .await
        .unwrap_err();
    assert!(matches!(error, Error::InvalidJob(_)));
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one lifecycle test keeps ordered CRUD state transitions and rollback assertions together"
)]
async fn job_crud_preserves_sqlite_semantics() {
    let (client, pool) = setup().await;
    let now = Utc::now();
    let delete_id = insert_job(
        &pool,
        JobSeed {
            metadata: json!({"original": true}),
            scheduled_at: now - Duration::minutes(3),
            tags: json!(["delete-me", "shared"]),
            ..JobSeed::default()
        },
    )
    .await;
    let retry_id = insert_job(
        &pool,
        JobSeed {
            attempt: 3,
            max_attempts: 3,
            scheduled_at: now - Duration::minutes(2),
            state: JobState::Retryable,
            tags: json!(["retry-me", "shared"]),
            ..JobSeed::default()
        },
    )
    .await;
    let running_id = insert_job(
        &pool,
        JobSeed {
            scheduled_at: now - Duration::minutes(1),
            state: JobState::Running,
            tags: json!(["running", "shared"]),
            ..JobSeed::default()
        },
    )
    .await;

    let running = client.jobs().get(running_id).await.unwrap();
    assert_eq!(running.state, JobState::Running);
    assert_eq!(running.tags, ["running", "shared"]);

    let list_params = JobListParams::default()
        .ids([delete_id, retry_id, running_id])
        .limit(2)
        .order_by(JobListOrderBy::ScheduledAt)
        .direction(SortDirection::Descending);
    let first_page = client.jobs().list(list_params.clone()).await.unwrap();
    assert_eq!(
        first_page.jobs.iter().map(|job| job.id).collect::<Vec<_>>(),
        [running_id, retry_id]
    );
    let last_cursor = first_page.last_cursor.unwrap();
    assert_eq!(
        last_cursor.encode(),
        JobListCursor::from_job(&first_page.jobs[1], &list_params)
            .unwrap()
            .encode()
    );
    let second_page = client
        .jobs()
        .list(list_params.after(last_cursor))
        .await
        .unwrap()
        .jobs;
    assert_eq!(
        second_page.iter().map(|job| job.id).collect::<Vec<_>>(),
        [delete_id]
    );

    let updated = client
        .jobs()
        .update(
            delete_id,
            JobUpdateParams::default()
                .with_metadata(Map::from_iter([("added".to_owned(), json!(42))]))
                .with_output(json!({"ok": true})),
        )
        .await
        .unwrap();
    assert_eq!(
        updated.metadata.get::<bool>("original").unwrap(),
        Some(true)
    );
    assert_eq!(updated.metadata.get::<i64>("added").unwrap(), Some(42));
    assert_eq!(
        updated.output().map(serde_json::value::RawValue::get),
        Some(r#"{"ok":true}"#)
    );

    let mut transaction = pool.begin().await.unwrap();
    let transaction_update = client
        .jobs()
        .update(
            delete_id,
            JobUpdateParams::default()
                .with_metadata(Map::from_iter([("rolled_back".to_owned(), json!(true))])),
        )
        .tx(&mut transaction)
        .await
        .unwrap();
    assert_eq!(
        transaction_update
            .metadata
            .get::<bool>("rolled_back")
            .unwrap(),
        Some(true)
    );
    assert_eq!(
        client
            .jobs()
            .get(delete_id)
            .tx(&mut transaction)
            .await
            .unwrap()
            .metadata
            .get::<bool>("rolled_back")
            .unwrap(),
        Some(true)
    );
    transaction.rollback().await.unwrap();
    assert!(
        !client
            .jobs()
            .get(delete_id)
            .await
            .unwrap()
            .metadata
            .contains_key("rolled_back")
    );

    let mut transaction = pool.begin().await.unwrap();
    let completed = client
        .jobs()
        .complete(running_id)
        .tx(&mut transaction)
        .await
        .unwrap();
    assert_eq!(completed.state, JobState::Completed);
    transaction.rollback().await.unwrap();
    assert_eq!(
        client.jobs().get(running_id).await.unwrap().state,
        JobState::Running
    );
    let mut transaction = pool.begin().await.unwrap();
    client
        .jobs()
        .complete(running_id)
        .tx(&mut transaction)
        .await
        .unwrap();
    transaction.commit().await.unwrap();
    let completed = client.jobs().get(running_id).await.unwrap();
    assert_eq!(completed.state, JobState::Completed);
    assert!(completed.finalized_at.is_some());

    let retried = client.jobs().retry(retry_id).await.unwrap();
    assert_eq!(retried.state, JobState::Available);
    assert_eq!(retried.max_attempts, 4);
    let insert_notification: String = sqlx::query_scalar(
        "SELECT payload FROM river_notification WHERE topic = 'river_insert' ORDER BY id DESC LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&insert_notification).unwrap(),
        json!({"queue": "default"})
    );

    let deleted = client.jobs().delete(delete_id).await.unwrap();
    assert_eq!(deleted.id, delete_id);
    assert!(matches!(
        client.jobs().get(delete_id).await,
        Err(Error::NotFound)
    ));

    let still_running_id = insert_job(
        &pool,
        JobSeed {
            state: JobState::Running,
            ..JobSeed::default()
        },
    )
    .await;
    assert!(matches!(
        client.jobs().delete(still_running_id).await,
        Err(Error::JobRunning)
    ));

    pool.close().await;
}

#[tokio::test]
async fn job_delete_many_is_atomic_and_skips_running_jobs() {
    let (client, pool) = setup().await;
    let first = insert_job(&pool, JobSeed::default()).await;
    let second = insert_job(
        &pool,
        JobSeed {
            state: JobState::Pending,
            ..JobSeed::default()
        },
    )
    .await;
    let running = insert_job(
        &pool,
        JobSeed {
            state: JobState::Running,
            ..JobSeed::default()
        },
    )
    .await;

    assert!(matches!(
        client
            .jobs()
            .delete_many(JobDeleteManyParams::matching(JobListParams::default()))
            .await,
        Err(Error::InvalidJob(_))
    ));
    let params =
        JobDeleteManyParams::matching(JobListParams::default().ids([first, second, running]));
    let mut transaction = pool.begin().await.unwrap();
    let rolled_back = client
        .jobs()
        .delete_many(params.clone())
        .tx(&mut transaction)
        .await
        .unwrap();
    assert_eq!(
        rolled_back.iter().map(|job| job.id).collect::<Vec<_>>(),
        [first, second]
    );
    transaction.rollback().await.unwrap();
    assert!(client.jobs().get(first).await.is_ok());
    assert!(client.jobs().get(second).await.is_ok());

    let deleted = client.jobs().delete_many(params).await.unwrap();
    assert_eq!(
        deleted.iter().map(|job| job.id).collect::<Vec<_>>(),
        [first, second]
    );
    assert_eq!(
        client.jobs().get(running).await.unwrap().state,
        JobState::Running
    );

    pool.close().await;
}

#[tokio::test]
async fn queue_crud_and_notifications_share_the_caller_transaction() {
    let (client, pool) = setup().await;
    insert_queue(&pool, "alpha").await;
    insert_queue(&pool, "beta").await;

    assert_eq!(client.queues().get("alpha").await.unwrap().name, "alpha");
    assert_eq!(
        client
            .queues()
            .list(QueueListParams::default())
            .await
            .unwrap()
            .iter()
            .map(|queue| queue.name.as_str())
            .collect::<Vec<_>>(),
        ["alpha", "beta"]
    );

    client.queues().pause("alpha").await.unwrap();
    assert!(
        client
            .queues()
            .get("alpha")
            .await
            .unwrap()
            .paused_at
            .is_some()
    );
    let initial_notification_count = notification_count(&pool).await;

    let mut transaction = pool.begin().await.unwrap();
    client
        .queues()
        .resume("alpha")
        .tx(&mut transaction)
        .await
        .unwrap();
    assert!(
        client
            .queues()
            .get("alpha")
            .tx(&mut transaction)
            .await
            .unwrap()
            .paused_at
            .is_none()
    );
    transaction.rollback().await.unwrap();
    assert!(
        client
            .queues()
            .get("alpha")
            .await
            .unwrap()
            .paused_at
            .is_some()
    );
    assert_eq!(notification_count(&pool).await, initial_notification_count);

    let mut transaction = pool.begin().await.unwrap();
    let updated = client
        .queues()
        .update(
            "alpha",
            QueueUpdateParams::new()
                .metadata(Map::from_iter([("owner".to_owned(), json!("rust"))])),
        )
        .tx(&mut transaction)
        .await
        .unwrap();
    assert_eq!(updated.metadata["owner"], "rust");
    transaction.commit().await.unwrap();
    assert_eq!(
        client.queues().get("alpha").await.unwrap().metadata["owner"],
        "rust"
    );
    assert_eq!(
        notification_count(&pool).await,
        initial_notification_count + 1
    );

    client.queues().resume("alpha").await.unwrap();
    client.queues().pause(QueueSelector::All).await.unwrap();
    assert!(
        client
            .queues()
            .list(QueueListParams::default())
            .await
            .unwrap()
            .iter()
            .all(|queue| queue.paused_at.is_some())
    );

    let payload: String = sqlx::query_scalar(
        "SELECT payload FROM river_notification WHERE topic = 'river_control' ORDER BY id DESC LIMIT 1",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(
        serde_json::from_str::<Value>(&payload).unwrap(),
        json!({"action": "pause", "queue": "*"})
    );

    pool.close().await;
}

#[derive(Clone)]
struct JobSeed {
    attempt: i16,
    kind: &'static str,
    max_attempts: i16,
    metadata: Value,
    queue: &'static str,
    scheduled_at: DateTime<Utc>,
    state: JobState,
    tags: Value,
}

impl Default for JobSeed {
    fn default() -> Self {
        Self {
            attempt: 0,
            kind: "sqlite_storage_test",
            max_attempts: 25,
            metadata: json!({}),
            queue: "default",
            scheduled_at: Utc::now(),
            state: JobState::Available,
            tags: json!([]),
        }
    }
}

async fn insert_job(pool: &SqlitePool, seed: JobSeed) -> i64 {
    let now = Utc::now();
    let finalized_at = matches!(
        seed.state,
        JobState::Cancelled | JobState::Completed | JobState::Discarded
    )
    .then_some(now);
    sqlx::query_scalar(
        "INSERT INTO river_job (args, attempt, attempted_at, attempted_by, created_at, errors, \
         finalized_at, kind, max_attempts, metadata, priority, queue, scheduled_at, state, tags) \
         VALUES (jsonb(?), ?, ?, jsonb(?), ?, jsonb(?), ?, ?, ?, jsonb(?), 1, ?, ?, ?, jsonb(?)) \
         RETURNING id",
    )
    .bind(r#"{"message":"hello"}"#)
    .bind(seed.attempt)
    .bind(
        (seed.state == JobState::Running)
            .then_some(now)
            .map(sqlite_time),
    )
    .bind("[]")
    .bind(sqlite_time(now))
    .bind("[]")
    .bind(finalized_at.map(sqlite_time))
    .bind(seed.kind)
    .bind(seed.max_attempts)
    .bind(seed.metadata.to_string())
    .bind(seed.queue)
    .bind(sqlite_time(seed.scheduled_at))
    .bind(seed.state.as_str())
    .bind(seed.tags.to_string())
    .fetch_one(pool)
    .await
    .unwrap()
}

fn sqlite_time(time: DateTime<Utc>) -> String {
    time.round_subsecs(3)
        .format("%Y-%m-%d %H:%M:%S%.3f")
        .to_string()
}

async fn insert_queue(pool: &SqlitePool, name: &str) {
    sqlx::query("INSERT INTO river_queue (name, metadata) VALUES (?, jsonb('{}'))")
        .bind(name)
        .execute(pool)
        .await
        .unwrap();
}

async fn notification_count(pool: &SqlitePool) -> i64 {
    sqlx::query("SELECT count(*) AS count FROM river_notification")
        .fetch_one(pool)
        .await
        .unwrap()
        .get("count")
}

async fn setup() -> (Client, SqlitePool) {
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    SqliteMigrator::new(pool.clone())
        .migrate_up()
        .await
        .unwrap();
    let client = Client::builder(pool.clone()).build().unwrap();
    (client, pool)
}
