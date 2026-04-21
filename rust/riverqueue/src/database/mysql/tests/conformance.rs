//! Persist Go-generated protocol values through the real MySQL backend.

use super::*;
use crate::{Job, QueueConfig, QueueUpdateParams, WorkContext, WorkOutcome, Workers};

#[derive(Deserialize)]
struct NotificationFixture {
    name: String,
    payload: Value,
    topic: String,
}

#[derive(Deserialize)]
struct Protocol {
    attempt_error: AttemptError,
    job_states: Vec<StateFixture>,
    metadata_keys: HashMap<String, String>,
    notifications: Vec<NotificationFixture>,
}

#[derive(Deserialize)]
struct SnoozeFixture {
    expected_snoozes: i64,
    metadata: JobMetadata,
    name: String,
}

#[derive(Deserialize)]
struct Snoozes {
    snooze_counters: Vec<SnoozeFixture>,
}

#[derive(Deserialize)]
struct StateFixture {
    state: JobState,
    unique_bit: u8,
}

fn protocol() -> Protocol {
    serde_json::from_str(&crate::conformance::read_fixture("protocol_values.json")).unwrap()
}

#[tokio::test]
async fn emits_go_notification_payloads() {
    let db = TestDatabase::new().await;
    let mut connection = db.pool.acquire().await.unwrap();
    queue_upsert(&mut connection, "priority", &Map::new(), None, Utc::now())
        .await
        .unwrap();

    for fixture in protocol().notifications {
        match fixture.name.as_str() {
            "cancel" => {
                let job = db
                    .client
                    .insert(Args { value: 1 })
                    .opts(InsertOpts::default().with_queue("priority"))
                    .await
                    .unwrap();
                sqlx::query("UPDATE river_job SET id=42, state='running' WHERE id=?")
                    .bind(job.job.row.id)
                    .execute(&db.pool)
                    .await
                    .unwrap();
                db.client.jobs().cancel(42).await.unwrap();
            }
            "insert" => {
                // A fresh client has no local insert-notification cooldown.
                Client::builder(db.pool.clone())
                    .build()
                    .unwrap()
                    .insert(Args { value: 2 })
                    .opts(InsertOpts::default().with_queue("priority"))
                    .await
                    .unwrap();
            }
            "metadata_changed" => {
                db.client
                    .queues()
                    .update(
                        "priority",
                        QueueUpdateParams::new()
                            .metadata(Map::from_iter([("owner".into(), json!("candidate"))])),
                    )
                    .await
                    .unwrap();
            }
            "pause" => db.client.queues().pause("priority").await.unwrap(),
            "request_resign" => db.client.request_resign().await.unwrap(),
            "resigned" => {
                // Like Go's MySQL/SQLite drivers, Rust does not emit a
                // resignation notification. Receiving it is tested below.
                let before = notification_last_id(&mut connection).await.unwrap();
                let leader = leader_elect(
                    &mut connection,
                    "client-1",
                    Utc::now(),
                    Duration::from_secs(60),
                )
                .await
                .unwrap()
                .unwrap();
                assert!(
                    leader_resign(&mut connection, "client-1", leader.elected_at)
                        .await
                        .unwrap()
                );
                assert_eq!(notification_last_id(&mut connection).await.unwrap(), before);
                continue;
            }
            "resume" => db.client.queues().resume("priority").await.unwrap(),
            name => panic!("unhandled Go notification {name}"),
        }
        let (topic, payload): (String, String) = sqlx::query_as(
            "SELECT topic, CAST(payload AS CHAR) FROM river_notification ORDER BY id DESC LIMIT 1",
        )
        .fetch_one(&db.pool)
        .await
        .unwrap();
        assert_eq!(topic, fixture.topic, "{}", fixture.name);
        assert_eq!(
            serde_json::from_str::<Value>(&payload).unwrap(),
            fixture.payload,
            "{}",
            fixture.name
        );
    }
    drop(connection);
    db.close().await;
}

#[tokio::test]
async fn persists_go_snooze_counters_through_workers() {
    let db = TestDatabase::new().await;
    let fixture: Snoozes =
        serde_json::from_str(&crate::conformance::read_fixture("snooze_counters.json")).unwrap();
    assert!(!fixture.snooze_counters.is_empty());
    let mut workers = Workers::new();
    workers
        .add_fn(|_: WorkContext, _: Job<Args>| async {
            Ok::<_, crate::BoxError>(WorkOutcome::Snooze(Duration::from_secs(3600)))
        })
        .unwrap();
    let worker = Client::builder(db.pool.clone())
        .workers(workers)
        .queue("default", QueueConfig::new(1))
        .build()
        .unwrap();
    let mut run = worker.start().unwrap();
    run.wait_ready().await.unwrap();
    for case in fixture.snooze_counters {
        let inserted = db
            .client
            .insert(Args { value: 1 })
            .opts(InsertOpts::default().with_metadata(case.metadata))
            .await
            .unwrap();
        let stored = wait_state(&db.client, inserted.job.row.id, JobState::Scheduled).await;
        assert_eq!(stored.attempt, 0, "{}", case.name);
        assert_eq!(
            stored.metadata.get::<i64>("snoozes").unwrap(),
            Some(case.expected_snoozes),
            "{}",
            case.name
        );
    }
    run.stop().await.unwrap();
    db.close().await;
}

#[tokio::test]
async fn receives_go_notifications_after_commit_and_ignores_rollback() {
    let db = TestDatabase::new().await;
    let mut connection = db.pool.acquire().await.unwrap();
    let mut listener = NotificationListener::default();
    listener.connect(&mut connection).await.unwrap();
    let fixture = protocol();
    for notification in &fixture.notifications {
        listener
            .listen(&mut connection, &notification.topic)
            .await
            .unwrap();
    }
    let mut tx = crate::database::begin_mysql_write(&db.pool).await.unwrap();
    for notification in &fixture.notifications {
        sqlx::query("INSERT INTO river_notification(topic,payload) VALUES (?,?)")
            .bind(&notification.topic)
            .bind(notification.payload.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    tx.rollback().await.unwrap();
    assert!(listener.next(&mut connection).await.unwrap().is_none());
    let mut tx = crate::database::begin_mysql_write(&db.pool).await.unwrap();
    for notification in &fixture.notifications {
        sqlx::query("INSERT INTO river_notification(topic,payload) VALUES (?,?)")
            .bind(&notification.topic)
            .bind(notification.payload.to_string())
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    for expected in fixture.notifications {
        let received = listener.next(&mut connection).await.unwrap().unwrap();
        assert_eq!(received.topic, expected.topic);
        assert_eq!(
            serde_json::from_str::<Value>(&received.payload).unwrap(),
            expected.payload
        );
    }
    assert!(listener.next(&mut connection).await.unwrap().is_none());
    drop(connection);
    db.close().await;
}

#[tokio::test]
async fn round_trips_go_errors_and_persists_rescue_and_output_metadata() {
    let db = TestDatabase::new().await;
    let fixture = protocol();
    let keys = &fixture.metadata_keys;
    let inserted = db
        .client
        .insert(Args { value: 1 })
        .opts(
            InsertOpts::default()
                .with_metadata(Map::from_iter([(keys["rescue_count"].clone(), json!(2))])),
        )
        .await
        .unwrap();
    let id = inserted.job.row.id;
    let nonce = inserted
        .job
        .row
        .metadata
        .get::<String>(&keys["unique_nonce"])
        .unwrap()
        .unwrap();
    assert_ne!(nonce, "");
    sqlx::query("UPDATE river_job SET state='running', attempt=3, attempted_at=UTC_TIMESTAMP(6), errors=JSON_ARRAY(CAST(? AS JSON)) WHERE id=?")
        .bind(serde_json::to_string(&fixture.attempt_error).unwrap()).bind(id).execute(&db.pool).await.unwrap();
    assert_eq!(
        db.client.jobs().get(id).await.unwrap().errors,
        vec![fixture.attempt_error.clone()]
    );
    let now = Utc::now();
    let mut tx = crate::database::begin_mysql_write(&db.pool).await.unwrap();
    let rescued = rescue(
        &mut tx,
        &RescueJob {
            error: &fixture.attempt_error,
            finalized_at: None,
            id,
            scheduled_at: now,
            state: JobState::Retryable,
            stuck_horizon: now + chrono::Duration::hours(1),
        },
    )
    .await
    .unwrap()
    .unwrap();
    assert_eq!(
        // MySQL JSON arithmetic returns a double, as in Go's driver.
        rescued.metadata.get::<f64>(&keys["rescue_count"]).unwrap(),
        Some(3.0)
    );
    tx.commit().await.unwrap();
    let mut workers = Workers::new();
    workers
        .add_fn(|context: WorkContext, _: Job<Args>| async move {
            context.record_output(json!({"ok": true}))?;
            Ok::<_, crate::BoxError>(WorkOutcome::Complete)
        })
        .unwrap();
    let worker = Client::builder(db.pool.clone())
        .workers(workers)
        .queue("default", QueueConfig::new(1))
        .build()
        .unwrap();
    db.client.jobs().retry(id).await.unwrap();
    let mut run = worker.start().unwrap();
    run.wait_ready().await.unwrap();
    let stored = wait_state(&db.client, id, JobState::Completed).await;
    assert_eq!(
        stored.metadata.get::<Value>(&keys["output"]).unwrap(),
        Some(json!({"ok": true}))
    );
    assert_eq!(
        stored.metadata.get::<f64>(&keys["rescue_count"]).unwrap(),
        Some(3.0)
    );
    assert_eq!(
        stored
            .metadata
            .get::<String>(&keys["unique_nonce"])
            .unwrap(),
        Some(nonce)
    );
    assert_eq!(
        stored.errors,
        vec![fixture.attempt_error.clone(), fixture.attempt_error]
    );
    run.stop().await.unwrap();
    db.close().await;
}

#[tokio::test]
async fn stored_states_and_unique_bits_match_go() {
    let db = TestDatabase::new().await;
    let opts = InsertOpts::default().with_unique(
        UniqueOpts::new()
            .with_by_args(true)
            .with_by_state(JobState::ALL.iter().copied()),
    );
    let inserted = db
        .client
        .insert(Args { value: 1 })
        .opts(opts.clone())
        .await
        .unwrap();
    for fixture in protocol().job_states {
        let finalized = matches!(
            fixture.state,
            JobState::Cancelled | JobState::Completed | JobState::Discarded
        );
        sqlx::query("UPDATE river_job SET state=?, unique_states=?, finalized_at=IF(?, UTC_TIMESTAMP(6), NULL) WHERE id=?")
            .bind(fixture.state.as_str()).bind(fixture.unique_bit).bind(finalized).bind(inserted.job.row.id).execute(&db.pool).await.unwrap();
        let stored = db.client.jobs().get(inserted.job.row.id).await.unwrap();
        assert_eq!(stored.state, fixture.state);
        assert_eq!(stored.unique_states, Some(vec![fixture.state]));
        let duplicate = db
            .client
            .insert(Args { value: 1 })
            .opts(opts.clone())
            .await
            .unwrap();
        assert!(duplicate.unique_skipped_as_duplicate, "{}", fixture.state);
        assert_eq!(duplicate.job.row.id, inserted.job.row.id);
        let bit: i16 = sqlx::query_scalar("SELECT unique_states FROM river_job WHERE id=?")
            .bind(inserted.job.row.id)
            .fetch_one(&db.pool)
            .await
            .unwrap();
        assert_eq!(bit, i16::from(fixture.unique_bit));
    }
    db.close().await;
}
