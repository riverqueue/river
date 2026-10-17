use std::{
    collections::HashSet,
    str::FromStr,
    sync::atomic::{AtomicU64, Ordering},
};

use riverqueue_migrate::{Direction, MigrateOpts, MySqlMigrator};
use serde::{Deserialize, Serialize};
use serde_json::json;
use sqlx::{
    MySqlPool,
    mysql::{MySqlConnectOptions, MySqlPoolOptions},
};

use super::*;
use crate::{Client, InsertOpts, JobArgs, JobListParams, UniqueOpts};

mod conformance;

#[derive(Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "mysql_test")]
struct Args {
    value: i64,
}

struct TestDatabase {
    admin: MySqlPool,
    client: Client,
    name: String,
    pool: MySqlPool,
}

impl TestDatabase {
    async fn new() -> Self {
        static NEXT: AtomicU64 = AtomicU64::new(0);
        let url = std::env::var("RIVER_RUST_MYSQL_URL")
            .expect("RIVER_RUST_MYSQL_URL is required for MySQL tests");
        let options = MySqlConnectOptions::from_str(&url).unwrap();
        let admin = MySqlPoolOptions::new()
            .max_connections(1)
            .connect_with(options.clone())
            .await
            .unwrap();
        let name = format!(
            "river_rust_{}_{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        );
        sqlx::query(AssertSqlSafe(format!("CREATE DATABASE `{name}`")))
            .execute(&admin)
            .await
            .unwrap();
        let pool = MySqlPoolOptions::new()
            .max_connections(12)
            .connect_with(options.database(&name))
            .await
            .unwrap();
        MySqlMigrator::new(pool.clone()).migrate_up().await.unwrap();
        let client = Client::builder(pool.clone()).build().unwrap();
        Self {
            admin,
            client,
            name,
            pool,
        }
    }

    async fn close(self) {
        self.pool.close().await;
        sqlx::query(AssertSqlSafe(format!("DROP DATABASE `{}`", self.name)))
            .execute(&self.admin)
            .await
            .unwrap();
        self.admin.close().await;
    }
}

#[tokio::test]
async fn cleanup_skips_locked_jobs() {
    let db = TestDatabase::new().await;
    let running = db
        .client
        .insert(Args { value: 1 })
        .await
        .unwrap()
        .job
        .row
        .id;
    let locked = db
        .client
        .insert(Args { value: 2 })
        .await
        .unwrap()
        .job
        .row
        .id;
    let expired = db
        .client
        .insert(Args { value: 3 })
        .await
        .unwrap()
        .job
        .row
        .id;
    sqlx::query(
        "UPDATE river_job SET state='completed', finalized_at='1970-01-01' WHERE id IN (?, ?)",
    )
    .bind(locked)
    .bind(expired)
    .execute(&db.pool)
    .await
    .unwrap();
    sqlx::query("UPDATE river_job SET state='running' WHERE id=?")
        .bind(running)
        .execute(&db.pool)
        .await
        .unwrap();
    let mut lock = crate::database::begin_mysql_write(&db.pool).await.unwrap();
    sqlx::query("SELECT id FROM river_job WHERE id IN (?, ?) FOR UPDATE")
        .bind(running)
        .bind(locked)
        .fetch_all(&mut *lock)
        .await
        .unwrap();
    let mut connection = db.pool.acquire().await.unwrap();
    let deleted = tokio::time::timeout(
        Duration::from_secs(2),
        cleanup_jobs(
            &mut connection,
            &CleanupJobs {
                cancelled_before: None,
                completed_before: Some(Utc::now()),
                discarded_before: None,
                limit: 100,
                queues_excluded: &[],
                queues_included: None,
            },
        ),
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(deleted, 1);
    assert!(db.client.jobs().get(expired).await.is_err());
    assert_eq!(
        db.client.jobs().get(locked).await.unwrap().state,
        JobState::Completed
    );
    assert_eq!(
        db.client.jobs().get(running).await.unwrap().state,
        JobState::Running
    );
    drop(connection);
    lock.rollback().await.unwrap();
    db.close().await;
}

#[tokio::test]
async fn migrations_are_serialized_and_reversible() {
    let db = TestDatabase::new().await;
    let migrator = MySqlMigrator::new(db.pool.clone());
    assert!(migrator.validate(None).await.unwrap().is_valid());
    migrator
        .migrate(Direction::Down, MigrateOpts::new().with_target_version(-1))
        .await
        .unwrap();
    assert_eq!(migrator.existing_versions().await.unwrap(), [] as [i64; 0]);
    let (left, right) = tokio::join!(migrator.migrate_up(), migrator.migrate_up());
    assert_eq!(left.unwrap().len() + right.unwrap().len(), 8);
    assert_eq!(
        migrator.existing_versions().await.unwrap(),
        (1..=8).collect::<Vec<_>>()
    );
    db.close().await;
}

#[tokio::test]
async fn insertion_and_completion_use_the_callers_transaction() {
    let db = TestDatabase::new().await;
    let now = DateTime::parse_from_rfc3339("2026-01-02T03:04:05.123456Z")
        .unwrap()
        .with_timezone(&Utc);
    let mut tx = crate::database::begin_mysql_write(&db.pool).await.unwrap();
    let inserted = db
        .client
        .insert(Args {
            value: 9_007_199_254_740_993,
        })
        .opts(
            InsertOpts::default()
                .with_max_attempts(40_000)
                .with_scheduled_at(now),
        )
        .tx(&mut tx)
        .await
        .unwrap();
    assert_eq!(inserted.job.args.value, 9_007_199_254_740_993);
    assert_eq!(inserted.job.row.scheduled_at, now);
    assert_eq!(inserted.job.row.max_attempts, 40_000);
    tx.rollback().await.unwrap();
    assert!(db.client.jobs().get(inserted.job.row.id).await.is_err());

    let inserted = db.client.insert(Args { value: 1 }).await.unwrap();
    let id = inserted.job.row.id;
    sqlx::query("UPDATE river_job SET state = 'running', attempt = 1, attempted_at = UTC_TIMESTAMP(6) WHERE id = ?").bind(id).execute(&db.pool).await.unwrap();
    let mut tx = crate::database::begin_mysql_write(&db.pool).await.unwrap();
    db.client.jobs().complete(id).tx(&mut tx).await.unwrap();
    tx.rollback().await.unwrap();
    assert_eq!(
        db.client.jobs().get(id).await.unwrap().state,
        JobState::Running
    );
    db.close().await;
}

#[tokio::test]
async fn concurrent_unique_insertion_returns_one_job() {
    let db = TestDatabase::new().await;
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..12 {
        let client = db.client.clone();
        tasks.spawn(async move {
            client
                .insert(Args { value: 1 })
                .opts(InsertOpts::default().with_unique(UniqueOpts::new().with_by_args(true)))
                .await
                .unwrap()
        });
    }
    let mut ids = HashSet::new();
    let mut inserted = 0;
    while let Some(result) = tasks.join_next().await {
        let result = result.unwrap();
        ids.insert(result.job.row.id);
        inserted += usize::from(!result.unique_skipped_as_duplicate);
    }
    assert_eq!(ids.len(), 1);
    assert_eq!(inserted, 1);
    db.close().await;
}

#[tokio::test]
async fn concurrent_claims_do_not_duplicate_jobs_and_respect_paused_queues() {
    let db = TestDatabase::new().await;
    for value in 0..40 {
        db.client.insert(Args { value }).await.unwrap();
    }
    let mut tx = crate::database::begin_mysql_write(&db.pool).await.unwrap();
    queue_upsert(&mut tx, "paused", &Map::new(), Some(Utc::now()), Utc::now())
        .await
        .unwrap();
    tx.commit().await.unwrap();
    db.client
        .insert(Args { value: 0 })
        .opts(InsertOpts::default().with_queue("paused"))
        .await
        .unwrap();
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..4 {
        let pool = db.pool.clone();
        tasks.spawn(async move {
            let mut tx = crate::database::begin_mysql_write(&pool).await.unwrap();
            let rows = claim(
                &mut tx,
                &ClaimJobs {
                    client_id: "worker",
                    kinds: None,
                    limit: 10,
                    max_attempted_by: 100,
                    now: Utc::now(),
                    queue: "default",
                },
            )
            .await
            .unwrap();
            tx.commit().await.unwrap();
            rows.into_iter()
                .map(|row| row.unwrap().id)
                .collect::<Vec<_>>()
        });
    }
    let mut ids = HashSet::new();
    while let Some(result) = tasks.join_next().await {
        for id in result.unwrap() {
            assert!(ids.insert(id));
        }
    }
    let mut tx = crate::database::begin_mysql_write(&db.pool).await.unwrap();
    let params = ClaimJobs {
        client_id: "worker",
        kinds: None,
        limit: 50,
        max_attempted_by: 100,
        now: Utc::now(),
        queue: "default",
    };
    for row in claim(&mut tx, &params).await.unwrap() {
        assert!(ids.insert(row.unwrap().id));
    }
    assert_eq!(ids.len(), 40);
    assert!(
        claim(
            &mut tx,
            &ClaimJobs {
                queue: "paused",
                ..params
            }
        )
        .await
        .unwrap()
        .is_empty()
    );
    tx.commit().await.unwrap();
    db.close().await;
}

#[tokio::test]
async fn cancellation_wins_over_retry_and_metadata_preserves_nulls() {
    let db = TestDatabase::new().await;
    let inserted = db.client.insert(Args { value: 1 }).await.unwrap();
    let id = inserted.job.row.id;
    let mut tx = crate::database::begin_mysql_write(&db.pool).await.unwrap();
    claim_selected(
        &mut tx,
        &ClaimJobs {
            client_id: "worker",
            kinds: None,
            limit: 1,
            max_attempted_by: 100,
            now: Utc::now(),
            queue: "default",
        },
        &[id],
    )
    .await
    .unwrap();
    cancel(&mut tx, id, Utc::now()).await.unwrap();
    let metadata = Map::from_iter([("output".into(), Value::Null)]);
    let row = complete(
        &mut tx,
        &CompleteJob {
            attempt: Some(0),
            error: None,
            finalized_at: None,
            id,
            metadata_updates: Some(&metadata),
            now: Utc::now(),
            scheduled_at: Some(Utc::now()),
            state: JobState::Available,
        },
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(row.state, JobState::Cancelled);
    assert_eq!(row.attempt, 1);
    assert_eq!(
        row.metadata.get::<Value>("output").unwrap(),
        Some(Value::Null)
    );
    assert!(row.finalized_at.is_some());
    tx.commit().await.unwrap();
    db.close().await;
}

#[tokio::test]
async fn metadata_containment_preserves_structure_and_exact_queue_names() {
    let db = TestDatabase::new().await;
    let wanted = db
        .client
        .insert(Args { value: 1 })
        .opts(
            InsertOpts::default().with_metadata(
                json!({"obj":{"a":1,"b":2},"array":[[1,2]],"nil":null})
                    .as_object()
                    .unwrap()
                    .clone(),
            ),
        )
        .await
        .unwrap();
    let other = db.client.insert(Args { value: 2 }).await.unwrap();
    sqlx::query("UPDATE river_job SET queue = 'default ' WHERE id = ?")
        .bind(other.job.row.id)
        .execute(&db.pool)
        .await
        .unwrap();
    for (fragment, matches) in [
        (json!({"obj":{"a":1}}), true),
        (json!({"obj":{"a":2}}), false),
        (json!({"array":[[1]]}), true),
        (json!({"array":[1]}), false),
        (json!({"nil":null}), true),
        (json!({"absent":null}), false),
    ] {
        let result = db
            .client
            .jobs()
            .list(JobListParams::default().metadata(fragment.as_object().unwrap().clone()))
            .await
            .unwrap();
        assert_eq!(
            result.jobs.iter().map(|job| job.id).collect::<Vec<_>>(),
            if matches {
                vec![wanted.job.row.id]
            } else {
                vec![]
            },
            "{fragment}"
        );
    }
    assert_eq!(
        db.client
            .jobs()
            .list(JobListParams::default().queues(["default"]))
            .await
            .unwrap()
            .jobs
            .len(),
        1
    );
    db.close().await;
}

#[tokio::test]
async fn outbox_waits_for_lower_uncommitted_ids() {
    let db = TestDatabase::new().await;
    let mut listener = NotificationListener::default();
    let mut connection = db.pool.acquire().await.unwrap();
    listener.connect(&mut connection).await.unwrap();
    listener
        .listen(&mut connection, "river_leadership")
        .await
        .unwrap();
    let mut tx = crate::database::begin_mysql_write(&db.pool).await.unwrap();
    notification_insert(
        &mut tx,
        &[NotificationInput {
            topic: "river_leadership",
            payload: "first",
        }],
    )
    .await
    .unwrap();
    db.client.request_resign().await.unwrap();
    let read = tokio::spawn(async move {
        let first = listener.next(&mut connection).await.unwrap().unwrap();
        let second = listener.next(&mut connection).await.unwrap().unwrap();
        (first, second)
    });
    // Wait for the actual blocked scan, then verify it holds its locks in
    // an explicit transaction instead of relying on autocommit semantics.
    let autocommit = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let autocommit: Option<String> = sqlx::query_scalar(
                "SELECT t.AUTOCOMMIT FROM performance_schema.data_lock_waits w \
                 JOIN performance_schema.data_locks l \
                 ON l.ENGINE_LOCK_ID=w.REQUESTING_ENGINE_LOCK_ID AND l.ENGINE=w.ENGINE \
                 JOIN performance_schema.events_transactions_current t \
                 ON t.THREAD_ID=w.REQUESTING_THREAD_ID \
                 WHERE l.OBJECT_SCHEMA=? AND l.OBJECT_NAME='river_notification'",
            )
            .bind(&db.name)
            .fetch_optional(&db.admin)
            .await
            .unwrap();
            if let Some(autocommit) = autocommit {
                break autocommit;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(autocommit, "NO");
    tx.commit().await.unwrap();
    let (first, second) = tokio::time::timeout(Duration::from_secs(5), read)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(first.payload, "first");
    assert_eq!(
        serde_json::from_str::<Value>(&second.payload).unwrap()["action"],
        "request_resign"
    );
    assert!(first.id < second.id);
    db.close().await;
}

#[tokio::test]
async fn maintenance_schedules_rescues_and_cleans_jobs() {
    let db = TestDatabase::new().await;
    let inserted = db
        .client
        .insert(Args { value: 1 })
        .opts(InsertOpts::default().with_scheduled_at(Utc::now() + chrono::Duration::hours(1)))
        .await
        .unwrap();
    let now = Utc::now() + chrono::Duration::hours(2);
    let mut tx = crate::database::begin_mysql_write(&db.pool).await.unwrap();
    let leader = leader_elect(&mut tx, "leader", now, Duration::from_secs(30))
        .await
        .unwrap()
        .unwrap();
    assert!(
        leader_elect(&mut tx, "other", now, Duration::from_secs(30))
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        leader_reelect(
            &mut tx,
            "leader",
            leader.elected_at,
            now,
            Duration::from_mins(1)
        )
        .await
        .unwrap()
        .is_some()
    );
    assert!(
        !leader_resign(&mut tx, "other", leader.elected_at)
            .await
            .unwrap()
    );
    assert!(
        leader_resign(&mut tx, "leader", leader.elected_at)
            .await
            .unwrap()
    );
    let candidates = schedule_candidates(&mut tx, now, 10).await.unwrap();
    assert_eq!(candidates.len(), 1);
    schedule_set_available(&mut tx, &[candidates[0].id])
        .await
        .unwrap();
    claim(
        &mut tx,
        &ClaimJobs {
            client_id: "worker",
            kinds: None,
            limit: 10,
            max_attempted_by: 100,
            now,
            queue: "default",
        },
    )
    .await
    .unwrap();
    let rescue_time = now + chrono::Duration::hours(1);
    let error = AttemptError {
        at: rescue_time,
        attempt: 1,
        error: "rescued".into(),
        trace: String::new(),
    };
    let rescued = rescue(
        &mut tx,
        &RescueJob {
            error: &error,
            finalized_at: Some(rescue_time),
            id: inserted.job.row.id,
            scheduled_at: now,
            state: JobState::Discarded,
            stuck_horizon: rescue_time,
        },
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(rescued.state, JobState::Discarded);
    assert_eq!(rescued.errors.len(), 1);
    assert_eq!(
        cleanup_jobs(
            &mut tx,
            &CleanupJobs {
                cancelled_before: None,
                completed_before: None,
                discarded_before: Some(rescue_time + chrono::Duration::seconds(1)),
                limit: 10,
                queues_excluded: &[],
                queues_included: None
            }
        )
        .await
        .unwrap(),
        1
    );
    tx.commit().await.unwrap();
    db.close().await;
}

async fn wait_state(client: &Client, id: i64, state: JobState) -> JobRow {
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

#[tokio::test]
async fn workers_receive_peer_notifications_and_run_maintenance() {
    use crate::{Job, MaintenanceConfig, QueueConfig, WorkContext, WorkOutcome, Workers};
    let db = TestDatabase::new().await;
    // Use a fresh peer for each notification so insert cooldowns cannot suppress it.
    // Make the job due before commit so client/server clock skew cannot consume
    // the notification before the job is eligible for a claim.
    let insert_available = async |value| {
        let peer = Client::builder(db.pool.clone()).build().unwrap();
        let mut tx = crate::database::begin_mysql_write(&db.pool).await.unwrap();
        let inserted = peer.insert(Args { value }).tx(&mut tx).await.unwrap();
        sqlx::query("UPDATE river_job SET scheduled_at = ? WHERE id = ?")
            .bind(mysql_time(Utc::now() - chrono::Duration::seconds(1)))
            .bind(inserted.job.row.id)
            .execute(&mut *tx)
            .await
            .unwrap();
        tx.commit().await.unwrap();
        inserted
    };
    let mut workers = Workers::new();
    workers
        .add_fn(|context: WorkContext, job: Job<Args>| async move {
            if job.args.value < 0 {
                context.cancellation_token().cancelled().await;
                return Ok::<_, crate::BoxError>(WorkOutcome::Cancel);
            }
            context.record_output(json!({"value": job.args.value}))?;
            Ok(WorkOutcome::Complete)
        })
        .unwrap();
    let worker = Client::builder(db.pool.clone())
        .id("mysql-worker")
        .workers(workers)
        .queue(
            "default",
            QueueConfig::new(4)
                .with_fetch_cooldown(Duration::from_millis(1))
                .with_fetch_poll_interval(Duration::from_mins(1)),
        )
        .maintenance(
            MaintenanceConfig::default()
                .with_elect_interval(Duration::from_millis(50))
                .with_scheduler_interval(Duration::from_millis(50)),
        )
        .build()
        .unwrap();
    let mut run = worker.start().unwrap();
    run.wait_ready().await.unwrap();
    let inserted = insert_available(42).await;
    let completed = wait_state(&db.client, inserted.job.row.id, JobState::Completed).await;
    assert_eq!(
        completed.metadata.get::<Value>("output").unwrap(),
        Some(json!({"value":42}))
    );

    let cancellation = insert_available(-1).await;
    wait_state(&db.client, cancellation.job.row.id, JobState::Running).await;
    db.client
        .jobs()
        .cancel(cancellation.job.row.id)
        .await
        .unwrap();
    wait_state(&db.client, cancellation.job.row.id, JobState::Cancelled).await;

    let scheduled = db
        .client
        .insert(Args { value: 5 })
        .opts(InsertOpts::default().with_scheduled_at(Utc::now() + chrono::Duration::hours(1)))
        .await
        .unwrap();
    sqlx::query(
        "UPDATE river_job SET scheduled_at = UTC_TIMESTAMP(6) - INTERVAL 1 SECOND WHERE id = ?",
    )
    .bind(scheduled.job.row.id)
    .execute(&db.pool)
    .await
    .unwrap();
    wait_state(&db.client, scheduled.job.row.id, JobState::Completed).await;

    let term: DateTime<Utc> =
        sqlx::query_scalar("SELECT elected_at FROM river_leader WHERE name = 'default'")
            .fetch_one(&db.pool)
            .await
            .unwrap();
    db.client.request_resign().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let current: Option<DateTime<Utc>> =
                sqlx::query_scalar("SELECT elected_at FROM river_leader WHERE name = 'default'")
                    .fetch_optional(&db.pool)
                    .await
                    .unwrap();
            if current != Some(term) {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    run.stop().await.unwrap();
    db.close().await;
}

#[tokio::test]
async fn shutdown_interrupts_a_blocked_notification_poll() {
    use crate::{Job, QueueConfig, WorkContext, WorkOutcome, Workers};
    let db = TestDatabase::new().await;
    let mut workers = Workers::new();
    workers
        .add_fn(|_: WorkContext, _: Job<Args>| async {
            Ok::<_, crate::BoxError>(WorkOutcome::Complete)
        })
        .unwrap();
    let worker = Client::builder(db.pool.clone())
        .workers(workers)
        .queue("default", QueueConfig::new(1))
        .build()
        .unwrap();
    let mut run = worker.start().unwrap();
    run.wait_ready().await.unwrap();
    let mut tx = db.pool.begin().await.unwrap();
    sqlx::query("INSERT INTO river_notification (topic, payload) VALUES ('river_insert', '{}')")
        .execute(&mut *tx)
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            let waiting: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM performance_schema.data_lock_waits w JOIN performance_schema.data_locks l ON l.ENGINE_LOCK_ID=w.REQUESTING_ENGINE_LOCK_ID AND l.ENGINE=w.ENGINE WHERE l.OBJECT_SCHEMA=? AND l.OBJECT_NAME='river_notification'")
                .bind(&db.name).fetch_one(&db.admin).await.unwrap();
            if waiting > 0 { break; }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    }).await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), run.stop())
        .await
        .unwrap()
        .unwrap();
    tx.rollback().await.unwrap();
    db.close().await;
}
