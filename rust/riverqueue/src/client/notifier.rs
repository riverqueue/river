//! Notification listeners and wakeups.
//!
//! Notifications only shorten the time before producers notice new work,
//! cancellations, and leadership changes; producers keep polling regardless.
//! Like River Go's notifier, a listener therefore never stops the client when
//! the database becomes unavailable. It reconnects with exponential backoff,
//! resubscribes to every topic, and wakes all producers after reconnecting in
//! case notifications were missed while it was disconnected.

#[allow(clippy::wildcard_imports)]
use super::*;

/// Timeout for connecting and subscribing, matching River Go's
/// `listenerTimeout`.
#[cfg(feature = "postgres")]
const LISTENER_TIMEOUT: Duration = Duration::from_secs(10);
/// Idle time after which the listener pings its connection, matching River
/// Go's notifier. A dead connection is then detected even without traffic.
#[cfg(feature = "postgres")]
const LISTENER_PING_INTERVAL: Duration = Duration::from_secs(5);

#[derive(Deserialize)]
pub(super) struct ControlNotification {
    pub(super) action: String,
    pub(super) job_id: Option<i64>,
    pub(super) queue: Option<String>,
}

#[derive(Deserialize)]
pub(super) struct InsertNotification {
    pub(super) queue: String,
}

#[derive(Deserialize)]
pub(super) struct LeadershipNotification {
    pub(super) action: String,
    pub(super) leader_id: Option<String>,
}

#[derive(Clone, Debug)]
pub(crate) enum RuntimeNotification {
    Insert(String),
    LeadershipChanged,
    LeadershipRequestResign,
    QueueControl(String),
}

/// Readiness reported once by the notification path.
pub(super) type ReadySender = oneshot::Sender<Result<(), String>>;

fn report_ready(ready: &mut Option<ReadySender>) {
    if let Some(ready) = ready.take() {
        let _ = ready.send(Ok(()));
    }
}

/// Wakes every producer to fetch and refresh its queue state. Used after a
/// listener reconnects, when notifications may have been missed.
#[cfg(feature = "postgres")]
fn wake_all_producers(queue_notifications: &broadcast::Sender<RuntimeNotification>) {
    let _ = queue_notifications.send(RuntimeNotification::Insert("*".to_owned()));
    let _ = queue_notifications.send(RuntimeNotification::QueueControl("*".to_owned()));
}

/// Routes one River notification to local producers and services.
fn dispatch_notification(
    inner: &ClientInner,
    queue_notifications: &broadcast::Sender<RuntimeNotification>,
    topic: &str,
    payload: &str,
) {
    match topic {
        crate::NOTIFICATION_TOPIC_INSERT => {
            if let Ok(payload) = serde_json::from_str::<InsertNotification>(payload) {
                let _ = queue_notifications.send(RuntimeNotification::Insert(payload.queue));
            }
        }
        crate::NOTIFICATION_TOPIC_LEADERSHIP => {
            if let Ok(payload) = serde_json::from_str::<LeadershipNotification>(payload) {
                if payload.action == "resigned"
                    && payload.leader_id.as_deref() == Some(inner.id.as_str())
                {
                    return;
                }
                let notification = if payload.action == "request_resign" {
                    RuntimeNotification::LeadershipRequestResign
                } else {
                    RuntimeNotification::LeadershipChanged
                };
                let _ = queue_notifications.send(notification);
            }
        }
        crate::NOTIFICATION_TOPIC_CONTROL => {
            let Ok(payload) = serde_json::from_str::<ControlNotification>(payload) else {
                warn!(payload, "ignored invalid River control notification");
                return;
            };
            match payload.action.as_str() {
                "cancel" => {
                    if let Some(job_id) = payload.job_id {
                        signal_running_attempt(
                            &inner.running,
                            &inner.pending_cancellations,
                            &inner.fetch_registration_windows,
                            job_id,
                        );
                    }
                }
                "pause" | "resume" => {
                    if let Some(queue) = payload.queue {
                        let _ = queue_notifications.send(RuntimeNotification::QueueControl(queue));
                    }
                }
                _ => debug!(
                    action = payload.action,
                    "ignored unknown River control action"
                ),
            }
        }
        _ => {}
    }
}

/// Listens for PostgreSQL notifications until cancelled.
///
/// The listener uses a dedicated connection opened with the client pool's
/// connect options, like River Go's hijacked listener connection, so it never
/// occupies one of the caller's pool slots. Connection and subscription
/// failures are retried with River's service backoff for as long as the client
/// runs; `ready` is reported once the first subscription succeeds.
#[cfg(feature = "postgres")]
pub(super) async fn run_notifications(
    inner: Arc<ClientInner>,
    cancel: CancellationToken,
    queue_notifications: broadcast::Sender<RuntimeNotification>,
    mut ready: Option<ReadySender>,
) -> Result<(), Error> {
    let mut attempt = 0;
    let mut missed_notifications = false;
    let mut schema = None;
    loop {
        let result = tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            result = listen_until_error(
                &inner,
                &cancel,
                &queue_notifications,
                &mut ready,
                &mut schema,
                &mut attempt,
                missed_notifications,
            ) => result,
        };
        let Err(listener_error) = result else {
            return Ok(());
        };
        missed_notifications = true;
        attempt += 1;
        let sleep = exponential_backoff(attempt);
        error!(
            attempt,
            error = %listener_error,
            sleep_duration = ?sleep,
            "River notification listener failed (will reconnect after backoff); producers keep polling"
        );
        tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            () = tokio::time::sleep(sleep) => {}
        }
    }
}

/// Connects, subscribes, and dispatches notifications until the connection
/// fails. Returns `Ok` only when cancelled.
#[cfg(feature = "postgres")]
async fn listen_until_error(
    inner: &ClientInner,
    cancel: &CancellationToken,
    queue_notifications: &broadcast::Sender<RuntimeNotification>,
    ready: &mut Option<ReadySender>,
    schema: &mut Option<String>,
    attempt: &mut u32,
    missed_notifications: bool,
) -> Result<(), Error> {
    let pool = inner
        .postgres_pool()
        .expect("PostgreSQL notifications require a PostgreSQL pool");
    let schema = if let Some(schema) = schema {
        schema.clone()
    } else {
        let resolved = match inner.schema.as_deref() {
            Some(schema) => schema.to_owned(),
            None => tokio::time::timeout(
                LISTENER_TIMEOUT,
                sqlx::query_scalar::<_, Option<String>>("SELECT current_schema()").fetch_one(pool),
            )
            .await
            .map_err(|_| {
                Error::runtime_context(
                    "notification listener",
                    "timed out resolving the current schema".to_owned(),
                )
            })??
            .ok_or_else(|| Error::invalid_job("PostgreSQL current_schema() is null".to_owned()))?,
        };
        schema.insert(resolved).clone()
    };
    let topics = [
        crate::NOTIFICATION_TOPIC_CONTROL,
        crate::NOTIFICATION_TOPIC_INSERT,
        crate::NOTIFICATION_TOPIC_LEADERSHIP,
    ]
    .map(|topic| (format!("{schema}.{topic}"), topic));

    // A private one-connection pool lets `PgListener` reconnect by itself
    // without borrowing from, or being limited by, the caller's pool.
    let listener_pool = sqlx::postgres::PgPoolOptions::new()
        .acquire_timeout(LISTENER_TIMEOUT)
        .idle_timeout(None)
        .max_connections(1)
        .max_lifetime(None)
        .min_connections(0)
        .connect_lazy_with((*pool.connect_options()).clone());
    let mut listener = tokio::time::timeout(LISTENER_TIMEOUT, async {
        let mut listener = PgListener::connect_with(&listener_pool).await?;
        listener
            .listen_all(topics.iter().map(|(channel, _)| channel.as_str()))
            .await?;
        Ok::<_, sqlx::Error>(listener)
    })
    .await
    .map_err(|_| {
        Error::runtime_context(
            "notification listener",
            "timed out connecting and subscribing".to_owned(),
        )
    })??;
    debug!("River notification listener healthy");
    *attempt = 0;
    report_ready(ready);
    if missed_notifications {
        wake_all_producers(queue_notifications);
    }

    loop {
        let received = tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            received = tokio::time::timeout(LISTENER_PING_INTERVAL, listener.try_recv()) => received,
        };
        match received {
            Ok(Ok(Some(notification))) => {
                let Some((_, topic)) = topics
                    .iter()
                    .find(|(channel, _)| channel == notification.channel())
                else {
                    continue;
                };
                dispatch_notification(inner, queue_notifications, topic, notification.payload());
            }
            // The connection dropped and `PgListener` reconnected and
            // resubscribed before returning. Anything sent in between was
            // lost, so producers must look for themselves.
            Ok(Ok(None)) => {
                warn!("River notification listener reconnected; waking producers");
                wake_all_producers(queue_notifications);
            }
            Ok(Err(listener_error)) => return Err(listener_error.into()),
            Err(_) => {
                tokio::time::timeout(
                    LISTENER_TIMEOUT,
                    sqlx::query("SELECT 1").execute(&mut listener),
                )
                .await
                .map_err(|_| {
                    Error::runtime_context(
                        "notification listener",
                        "health check timed out".to_owned(),
                    )
                })??;
            }
        }
    }
}

/// Polls SQLite's notification outbox until cancelled.
///
/// Poll failures (for example `database is locked` while another process holds
/// the write lock) are logged and retried with River's service backoff. The
/// cursor is kept, so no durable notification is skipped.
#[cfg(feature = "sqlite")]
pub(super) async fn run_sqlite_notifications(
    inner: Arc<ClientInner>,
    cancel: CancellationToken,
    queue_notifications: broadcast::Sender<RuntimeNotification>,
    mut ready: Option<ReadySender>,
) -> Result<(), Error> {
    let pool = inner
        .sqlite_pool()
        .expect("SQLite notifications require a SQLite pool");
    let mut attempt = 0;
    let mut after_id = None;
    let mut notification_tick =
        tokio::time::interval(crate::database::sqlite::DEFAULT_NOTIFICATION_POLL_INTERVAL);
    notification_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            _ = notification_tick.tick() => {}
        }
        let polled = async {
            let mut connection = pool.acquire().await?;
            let last_id = match after_id {
                Some(last_id) => last_id,
                None => crate::database::sqlite::notification_last_id(&mut connection)
                    .await
                    .map_err(sqlite_backend_error)?,
            };
            after_id = Some(last_id);
            crate::database::sqlite::notification_poll(&mut connection, last_id, 1_000)
                .await
                .map_err(sqlite_backend_error)
        }
        .await;
        let notifications = match polled {
            Ok(notifications) => notifications,
            Err(poll_error) => {
                attempt += 1;
                let sleep = exponential_backoff(attempt);
                error!(
                    attempt,
                    error = %poll_error,
                    sleep_duration = ?sleep,
                    "River notification poll failed (will retry after backoff); producers keep polling"
                );
                tokio::select! {
                    () = cancel.cancelled() => return Ok(()),
                    () = tokio::time::sleep(sleep) => {}
                }
                continue;
            }
        };
        attempt = 0;
        report_ready(&mut ready);
        for notification in notifications {
            after_id = Some(notification.id);
            dispatch_notification(
                &inner,
                &queue_notifications,
                &notification.topic,
                &notification.payload,
            );
        }
    }
}
