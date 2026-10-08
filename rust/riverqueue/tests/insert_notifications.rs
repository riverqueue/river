//! A client notifies a queue of inserted jobs at most once per fetch
//! cooldown, like River Go's insert notification limiter: every insertion
//! path shares one window per queue, a rolled-back transaction still starts
//! it, and each client keeps its own.
//!
//! Postgres scenarios run in a unique schema and fail rather than skip when
//! `RIVER_RUST_DATABASE_URL` is unset; SQLite scenarios use temporary files.

#![cfg(any(all(feature = "postgres", river_postgres_tests), feature = "sqlite"))]

mod support;

use std::time::Duration;

use riverqueue::{Client, InsertBatch, InsertManyItem, InsertOpts, JobArgs};
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "insert_notification")]
struct NotificationArgs {}

fn queue(name: &str) -> InsertOpts {
    InsertOpts::default().with_queue(name)
}

fn unique() -> InsertOpts {
    InsertOpts::default().with_unique(riverqueue::UniqueOpts::new().with_by_args(true))
}

fn batch(queues: &[&str]) -> InsertBatch {
    let mut batch = InsertBatch::new();
    for name in queues {
        batch.push_with(NotificationArgs {}, queue(name));
    }
    batch
}

fn many(queues: &[&str]) -> Vec<InsertManyItem<NotificationArgs>> {
    queues
        .iter()
        .map(|name| InsertManyItem::new(NotificationArgs {}, queue(name)))
        .collect()
}

/// The client's fetch cooldown is validated like Go's and is the default for
/// queues without their own.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn fetch_cooldown_validates_against_queue_poll_intervals() {
    use riverqueue::{Error, QueueConfig, WorkContext, WorkOutcome, Workers};

    let (pool, path) = support::sqlite_file_pool(1).await;
    let workers = || {
        let mut workers = Workers::new();
        workers
            .add_fn(
                |_context: WorkContext, _job: riverqueue::Job<NotificationArgs>| async {
                    Ok::<_, std::convert::Infallible>(WorkOutcome::Complete)
                },
            )
            .unwrap();
        workers
    };
    let builder = || Client::builder(pool.clone()).workers(workers());

    assert_eq!(QueueConfig::new(1).fetch_cooldown(), None);
    assert_eq!(
        QueueConfig::new(1)
            .with_fetch_cooldown(Duration::from_millis(5))
            .fetch_cooldown(),
        Some(Duration::from_millis(5))
    );
    for builder in [
        builder().fetch_cooldown(Duration::ZERO),
        builder().fetch_cooldown(Duration::from_micros(999)),
        // A queue's poll interval (one second by default) can't be
        // shorter than the client's cooldown it inherits...
        builder()
            .fetch_cooldown(Duration::from_secs(2))
            .queue("default", QueueConfig::new(1)),
        // ...or than its own.
        builder().queue(
            "default",
            QueueConfig::new(1).with_fetch_cooldown(Duration::from_secs(2)),
        ),
        builder().queue(
            "default",
            QueueConfig::new(1).with_fetch_cooldown(Duration::ZERO),
        ),
    ] {
        let error = builder.build().unwrap_err();
        assert!(matches!(error, Error::Configuration(_)), "{error}");
    }

    builder()
        .fetch_cooldown(Duration::from_millis(1))
        .build()
        .unwrap();
    builder()
        .fetch_cooldown(Duration::from_secs(2))
        .queue(
            "overridden",
            QueueConfig::new(1).with_fetch_cooldown(Duration::from_millis(100)),
        )
        .queue(
            "slow",
            QueueConfig::new(1).with_fetch_poll_interval(Duration::from_secs(2)),
        )
        .build()
        .unwrap();

    // Queues added at runtime are checked against the client's cooldown too.
    let client = builder()
        .fetch_cooldown(Duration::from_secs(2))
        .build()
        .unwrap();
    let error = client
        .local_queues()
        .add("default", QueueConfig::new(1))
        .unwrap_err();
    assert!(matches!(error, Error::Configuration(_)), "{error}");
    client
        .local_queues()
        .add(
            "default",
            QueueConfig::new(1).with_fetch_poll_interval(Duration::from_secs(2)),
        )
        .unwrap();
    support::sqlite_cleanup(pool, path).await;
}

#[cfg(all(feature = "postgres", river_postgres_tests))]
mod postgres {
    use std::time::Duration;

    use riverqueue::Client;
    use riverqueue::database::PostgresDatabase;
    use sqlx::postgres::PgListener;

    use super::support::PostgresSchema;
    use super::{NotificationArgs, batch, many, queue, unique};

    /// Listens to a schema's insert channel.
    struct Notifications {
        channel: String,
        listener: PgListener,
    }

    impl Notifications {
        async fn listen(schema: &PostgresSchema) -> Self {
            let channel = format!("{}.river_insert", schema.schema.as_deref().unwrap());
            let mut listener = PgListener::connect_with(&schema.pool).await.unwrap();
            listener.listen(&channel).await.unwrap();
            Self { channel, listener }
        }

        /// Returns the queues notified since the last call, in order.
        async fn next(&mut self, schema: &PostgresSchema) -> Vec<String> {
            sqlx::query("SELECT pg_notify($1, 'marker')")
                .bind(&self.channel)
                .execute(&schema.pool)
                .await
                .unwrap();
            let mut queues = Vec::new();
            loop {
                let notification = self.listener.recv().await.unwrap();
                if notification.payload() == "marker" {
                    return queues;
                }
                let payload: serde_json::Value =
                    serde_json::from_str(notification.payload()).unwrap();
                queues.push(payload["queue"].as_str().unwrap().to_owned());
            }
        }
    }

    fn client(schema: &PostgresSchema, cooldown: Duration) -> Client {
        Client::builder(
            PostgresDatabase::new(schema.pool.clone()).with_schema(schema.schema.clone()),
        )
        .fetch_cooldown(cooldown)
        .build()
        .unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn insert_notifications_resume_after_the_fetch_cooldown() {
        let schema = PostgresSchema::new("insert_notify_resume").await;
        let mut notifications = Notifications::listen(&schema).await;
        let client = client(&schema, Duration::from_millis(50));

        // Suppression is covered separately with a long cooldown; database
        // round trips can exceed this short window on a busy runner.
        client.insert(NotificationArgs {}).await.unwrap();
        assert_eq!(notifications.next(&schema).await, ["default"]);
        tokio::time::sleep(Duration::from_millis(60)).await;
        client.insert(NotificationArgs {}).await.unwrap();
        assert_eq!(notifications.next(&schema).await, ["default"]);
        // The listener holds a pool connection, which closing the pool awaits.
        drop(notifications);
        schema.cleanup().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn insert_notifications_wait_for_the_fetch_cooldown() {
        let schema = PostgresSchema::new("insert_notify_cooldown").await;
        let mut notifications = Notifications::listen(&schema).await;
        let client = client(&schema, Duration::from_hours(1));

        client
            .insert(NotificationArgs {})
            .opts(queue("a"))
            .await
            .unwrap();
        assert_eq!(notifications.next(&schema).await, ["a"]);

        client
            .insert(NotificationArgs {})
            .opts(queue("a"))
            .await
            .unwrap();
        client.insert_many(many(&["a", "b", "b"])).await.unwrap();
        client.insert_batch(batch(&["b", "c"])).await.unwrap();
        assert_eq!(notifications.next(&schema).await, ["b", "c"]);

        // Scheduled jobs send nothing and leave the queue's window alone.
        client
            .insert(NotificationArgs {})
            .opts(queue("d").with_scheduled_at(chrono::Utc::now() + chrono::Duration::hours(1)))
            .await
            .unwrap();
        assert_eq!(notifications.next(&schema).await, Vec::<String>::new());

        let mut transaction = schema.pool.begin().await.unwrap();
        client
            .insert(NotificationArgs {})
            .opts(queue("d"))
            .tx(&mut transaction)
            .await
            .unwrap();
        client
            .insert_many(many(&["d", "e"]))
            .tx(&mut transaction)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        assert_eq!(notifications.next(&schema).await, ["d", "e"]);

        // A rolled-back transaction delivers nothing but still starts its
        // queue's window.
        let mut transaction = schema.pool.begin().await.unwrap();
        client
            .insert_batch(batch(&["f"]))
            .tx(&mut transaction)
            .await
            .unwrap();
        transaction.rollback().await.unwrap();
        client
            .insert(NotificationArgs {})
            .opts(queue("f"))
            .await
            .unwrap();
        assert_eq!(notifications.next(&schema).await, Vec::<String>::new());

        // Each client has its own windows.
        let other = self::client(&schema, Duration::from_hours(1));
        other
            .insert(NotificationArgs {})
            .opts(queue("a"))
            .await
            .unwrap();
        assert_eq!(notifications.next(&schema).await, ["a"]);
        // The listener holds a pool connection, which closing the pool awaits.
        drop(notifications);
        schema.cleanup().await;
    }

    /// Like Go, a job skipped as a unique duplicate still notifies its queue.
    #[tokio::test(flavor = "multi_thread")]
    async fn unique_duplicates_notify_their_queue() {
        let schema = PostgresSchema::new("insert_notify_duplicate").await;
        let mut notifications = Notifications::listen(&schema).await;
        let client = client(&schema, Duration::from_millis(1));

        let first = client
            .insert(NotificationArgs {})
            .opts(unique())
            .await
            .unwrap();
        assert!(!first.unique_skipped_as_duplicate);
        assert_eq!(notifications.next(&schema).await, ["default"]);
        tokio::time::sleep(Duration::from_millis(2)).await;
        let duplicate = client
            .insert(NotificationArgs {})
            .opts(unique())
            .await
            .unwrap();
        assert!(duplicate.unique_skipped_as_duplicate);
        assert_eq!(notifications.next(&schema).await, ["default"]);
        // The listener holds a pool connection, which closing the pool awaits.
        drop(notifications);
        schema.cleanup().await;
    }
}

#[cfg(feature = "sqlite")]
mod sqlite {
    use std::time::Duration;

    use riverqueue::Client;
    use sqlx::SqlitePool;

    use super::support::{sqlite_cleanup, sqlite_file_pool};
    use super::{NotificationArgs, batch, many, queue, unique};

    /// Reads the insert notifications written to the outbox.
    #[derive(Default)]
    struct Notifications {
        after_id: i64,
    }

    impl Notifications {
        /// Returns the queues notified since the last call, in order.
        async fn next(&mut self, pool: &SqlitePool) -> Vec<String> {
            let rows: Vec<(i64, String)> = sqlx::query_as(
                "SELECT id, json_extract(payload, '$.queue') FROM river_notification \
                 WHERE topic = 'river_insert' AND id > ? ORDER BY id",
            )
            .bind(self.after_id)
            .fetch_all(pool)
            .await
            .unwrap();
            if let Some((id, _)) = rows.last() {
                self.after_id = *id;
            }
            rows.into_iter().map(|(_, queue)| queue).collect()
        }
    }

    fn client(pool: &SqlitePool, cooldown: Duration) -> Client {
        Client::builder(pool.clone())
            .fetch_cooldown(cooldown)
            .build()
            .unwrap()
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn insert_notifications_resume_after_the_fetch_cooldown() {
        let (pool, path) = sqlite_file_pool(4).await;
        let mut notifications = Notifications::default();
        let client = client(&pool, Duration::from_millis(50));

        // Suppression is covered separately with a long cooldown; database
        // round trips can exceed this short window on a busy runner.
        client.insert(NotificationArgs {}).await.unwrap();
        assert_eq!(notifications.next(&pool).await, ["default"]);
        tokio::time::sleep(Duration::from_millis(60)).await;
        client.insert(NotificationArgs {}).await.unwrap();
        assert_eq!(notifications.next(&pool).await, ["default"]);
        sqlite_cleanup(pool, path).await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn insert_notifications_wait_for_the_fetch_cooldown() {
        let (pool, path) = sqlite_file_pool(4).await;
        let mut notifications = Notifications::default();
        let client = client(&pool, Duration::from_hours(1));

        client
            .insert(NotificationArgs {})
            .opts(queue("a"))
            .await
            .unwrap();
        assert_eq!(notifications.next(&pool).await, ["a"]);

        client
            .insert(NotificationArgs {})
            .opts(queue("a"))
            .await
            .unwrap();
        client.insert_many(many(&["a", "b", "b"])).await.unwrap();
        client.insert_batch(batch(&["b", "c"])).await.unwrap();
        assert_eq!(notifications.next(&pool).await, ["b", "c"]);

        // Scheduled jobs send nothing and leave the queue's window alone.
        client
            .insert(NotificationArgs {})
            .opts(queue("d").with_scheduled_at(chrono::Utc::now() + chrono::Duration::hours(1)))
            .await
            .unwrap();
        assert_eq!(notifications.next(&pool).await, Vec::<String>::new());

        let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
        client
            .insert(NotificationArgs {})
            .opts(queue("d"))
            .tx(&mut transaction)
            .await
            .unwrap();
        client
            .insert_many(many(&["d", "e"]))
            .tx(&mut transaction)
            .await
            .unwrap();
        transaction.commit().await.unwrap();
        assert_eq!(notifications.next(&pool).await, ["d", "e"]);

        // A rolled-back transaction delivers nothing but still starts its
        // queue's window.
        let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
        client
            .insert_batch(batch(&["f"]))
            .tx(&mut transaction)
            .await
            .unwrap();
        transaction.rollback().await.unwrap();
        client
            .insert(NotificationArgs {})
            .opts(queue("f"))
            .await
            .unwrap();
        assert_eq!(notifications.next(&pool).await, Vec::<String>::new());

        // Each client has its own windows.
        let other = self::client(&pool, Duration::from_hours(1));
        other
            .insert(NotificationArgs {})
            .opts(queue("a"))
            .await
            .unwrap();
        assert_eq!(notifications.next(&pool).await, ["a"]);
        sqlite_cleanup(pool, path).await;
    }

    /// Like Go, a job skipped as a unique duplicate still notifies its queue.
    #[tokio::test(flavor = "multi_thread")]
    async fn unique_duplicates_notify_their_queue() {
        let (pool, path) = sqlite_file_pool(4).await;
        let mut notifications = Notifications::default();
        let client = client(&pool, Duration::from_millis(1));

        let first = client
            .insert(NotificationArgs {})
            .opts(unique())
            .await
            .unwrap();
        assert!(!first.unique_skipped_as_duplicate);
        assert_eq!(notifications.next(&pool).await, ["default"]);
        tokio::time::sleep(Duration::from_millis(2)).await;
        let duplicate = client
            .insert(NotificationArgs {})
            .opts(unique())
            .await
            .unwrap();
        assert!(duplicate.unique_skipped_as_duplicate);
        assert_eq!(notifications.next(&pool).await, ["default"]);
        sqlite_cleanup(pool, path).await;
    }
}
