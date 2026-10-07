//! River's notifications use the topics and payload fields River Go's do, per
//! the Go-generated `protocol_values.json` fixture.
//!
//! Postgres scenarios capture real notifications in a unique schema and
//! fail rather than skip when `RIVER_RUST_DATABASE_URL` is unset; SQLite
//! scenarios read the notification outbox of a temporary file.

#![cfg(any(all(feature = "postgres", river_postgres_tests), feature = "sqlite"))]

mod support;

use std::{collections::BTreeSet, io::ErrorKind, path::Path};

use riverqueue::{Client, InsertOpts, JobArgs, QueueUpdateParams};
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value, json};

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "notification_fixtures")]
struct NotifyArgs {}

#[derive(Deserialize)]
struct Fixture {
    notifications: Vec<NotificationFixture>,
}

#[derive(Deserialize)]
struct FieldFixture {
    name: String,
    omitempty: bool,
}

#[derive(Deserialize)]
struct NotificationFixture {
    fields: Vec<FieldFixture>,
    name: String,
    payload: Map<String, Value>,
    topic: String,
}

/// Reads `name` from `conformance/testdata`, where `make generate/fixtures`
/// writes fixtures produced by River's Go implementation. A missing fixture
/// fails the test rather than skipping it.
fn read_fixture(name: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../conformance/testdata")
        .join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|error| match error.kind() {
        ErrorKind::NotFound => panic!(
            "missing conformance fixture {}; run `make generate/fixtures` from the repository root",
            path.display()
        ),
        _ => panic!(
            "error reading conformance fixture {}: {error}",
            path.display()
        ),
    })
}

fn go_notifications() -> Vec<NotificationFixture> {
    serde_json::from_str::<Fixture>(&read_fixture("protocol_values.json"))
        .unwrap()
        .notifications
}

/// Asserts that each `(topic, payload)` notification has the topic and
/// payload fields of Go's notification of the same name, and returns the
/// names seen.
fn assert_notifications_match_go(notifications: &[(String, String)]) -> BTreeSet<String> {
    let go = go_notifications();
    let mut seen = BTreeSet::new();
    for (topic, payload) in notifications {
        let payload: Map<String, Value> = serde_json::from_str(payload).unwrap();
        // Only insert notifications have no action.
        let name = match payload.get("action") {
            Some(action) => action.as_str().unwrap().to_owned(),
            None => "insert".to_owned(),
        };
        let expected = go
            .iter()
            .find(|notification| notification.name == name)
            .unwrap_or_else(|| panic!("Go sends no {name} notification: {payload:?}"));
        assert_eq!(topic, &expected.topic, "{name} topic");

        let keys = payload.keys().map(String::as_str).collect::<BTreeSet<_>>();
        let fields = expected
            .fields
            .iter()
            .map(|field| field.name.as_str())
            .collect::<BTreeSet<_>>();
        assert!(
            keys.is_subset(&fields),
            "{name} payload fields {keys:?} not all in Go's {fields:?}"
        );
        for field in expected.fields.iter().filter(|field| !field.omitempty) {
            assert!(
                keys.contains(field.name.as_str()),
                "{name} payload {keys:?} lacks Go's required {:?}",
                field.name
            );
        }
        // Each scenario matches the one that produced Go's sample payload,
        // so the same optional fields are set.
        assert_eq!(
            keys,
            expected
                .payload
                .keys()
                .map(String::as_str)
                .collect::<BTreeSet<_>>(),
            "{name} payload fields"
        );
        seen.insert(name);
    }
    seen
}

/// Names of Go's notifications, except `excluded`.
fn go_names(excluded: &[&str]) -> BTreeSet<String> {
    go_notifications()
        .into_iter()
        .map(|notification| notification.name)
        .filter(|name| !excluded.contains(&name.as_str()))
        .collect()
}

/// Sends every notification a client sends outside leader election, on the
/// existing queue `priority`.
async fn send_notifications(client: &Client) {
    let id = client
        .insert(NotifyArgs {})
        .opts(InsertOpts::default().with_queue("priority"))
        .await
        .unwrap()
        .id();
    riverqueue::__private::claim_job_for_test(client, id)
        .await
        .unwrap();
    client.jobs().cancel(id).await.unwrap();
    client
        .queues()
        .update(
            "priority",
            QueueUpdateParams::new()
                .metadata(Map::from_iter([("owner".to_owned(), json!("candidate"))])),
        )
        .await
        .unwrap();
    client.queues().pause("priority").await.unwrap();
    client.queues().resume("priority").await.unwrap();
    client.request_resign().await.unwrap();
}

#[cfg(all(feature = "postgres", river_postgres_tests))]
#[tokio::test(flavor = "multi_thread")]
async fn postgres_notifications_match_go() {
    use std::{convert::Infallible, time::Duration};

    use riverqueue::{
        Job, MaintenanceConfig, QueueConfig, WorkContext, WorkOutcome, Workers,
        database::PostgresDatabase,
        protocol::{
            NOTIFICATION_TOPIC_CONTROL, NOTIFICATION_TOPIC_INSERT, NOTIFICATION_TOPIC_LEADERSHIP,
        },
    };
    use sqlx::postgres::PgListener;

    let mut workers = Workers::new();
    workers
        .add_fn(|_context: WorkContext, _job: Job<NotifyArgs>| async {
            Ok::<_, Infallible>(WorkOutcome::Complete)
        })
        .unwrap();

    let schema = support::PostgresSchema::new("river_notify_fields").await;
    let database = || PostgresDatabase::new(schema.pool.clone()).with_schema(schema.schema.clone());
    let prefix = format!("{}.", schema.schema.as_deref().unwrap());
    let mut listener = PgListener::connect_with(&schema.pool).await.unwrap();
    listener
        .listen_all(
            [
                NOTIFICATION_TOPIC_CONTROL,
                NOTIFICATION_TOPIC_INSERT,
                NOTIFICATION_TOPIC_LEADERSHIP,
            ]
            .map(|topic| format!("{prefix}{topic}"))
            .iter()
            .map(String::as_str),
        )
        .await
        .unwrap();

    // A leader resigns when it stops.
    let leader = Client::builder(database())
        .id("client-1")
        .maintenance(MaintenanceConfig::default().with_elect_interval(Duration::from_millis(50)))
        .queue("leader", QueueConfig::new(1))
        .workers(workers)
        .build()
        .unwrap();
    let mut run = leader.start().unwrap();
    run.wait_ready().await.unwrap();
    tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let leaders: i64 = sqlx::query_scalar(sqlx::AssertSqlSafe(format!(
                "SELECT count(*) FROM {}",
                schema.table("river_leader")
            )))
            .fetch_one(&schema.pool)
            .await
            .unwrap();
            if leaders == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .expect("client-1 was not elected leader");
    run.stop().await.unwrap();

    sqlx::query(sqlx::AssertSqlSafe(format!(
        "INSERT INTO {} (name, updated_at) VALUES ('priority', now())",
        schema.table("river_queue")
    )))
    .execute(&schema.pool)
    .await
    .unwrap();
    let client = Client::builder(database()).build().unwrap();
    send_notifications(&client).await;

    let expected = go_names(&[]);
    let mut seen = BTreeSet::new();
    while seen != expected {
        let notification = tokio::time::timeout(Duration::from_secs(5), listener.recv())
            .await
            .unwrap_or_else(|_| panic!("saw only {seen:?} of Go's {expected:?}"))
            .unwrap();
        let topic = notification
            .channel()
            .strip_prefix(&prefix)
            .unwrap()
            .to_owned();
        seen.extend(assert_notifications_match_go(&[(
            topic,
            notification.payload().to_owned(),
        )]));
    }

    // The listener holds a pooled connection that must be returned first.
    drop(listener);
    schema.cleanup().await;
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread")]
async fn sqlite_notifications_match_go() {
    let (pool, path) = support::sqlite_file_pool(4).await;
    sqlx::query("INSERT INTO river_queue (name, metadata) VALUES ('priority', jsonb('{}'))")
        .execute(&pool)
        .await
        .unwrap();
    let client = Client::builder(pool.clone()).build().unwrap();
    send_notifications(&client).await;

    let notifications: Vec<(String, String)> =
        sqlx::query_as("SELECT topic, payload FROM river_notification ORDER BY id")
            .fetch_all(&pool)
            .await
            .unwrap();
    // Like Go's SQLite driver, resigning writes no outbox row.
    assert_eq!(
        assert_notifications_match_go(&notifications),
        go_names(&["resigned"])
    );
    assert_eq!(notifications.len(), go_names(&["resigned"]).len());

    support::sqlite_cleanup(pool, path).await;
}
