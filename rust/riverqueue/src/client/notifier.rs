//! Notification listeners and wakeups.

#[allow(clippy::wildcard_imports)]
use super::*;

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

#[cfg(feature = "postgres")]
pub(super) async fn run_notifications(
    inner: Arc<ClientInner>,
    cancel: CancellationToken,
    queue_notifications: broadcast::Sender<RuntimeNotification>,
    ready: oneshot::Sender<Result<(), String>>,
) -> Result<(), Error> {
    let schema = match inner.schema.as_deref() {
        Some(schema) => schema.to_owned(),
        None => sqlx::query_scalar::<_, Option<String>>("SELECT current_schema()")
            .fetch_one(
                inner
                    .postgres_pool()
                    .expect("PostgreSQL notifications require a PostgreSQL pool"),
            )
            .await?
            .ok_or_else(|| Error::invalid_job("PostgreSQL current_schema() is null".to_owned()))?,
    };
    let control_topic = format!("{schema}.{}", crate::NOTIFICATION_TOPIC_CONTROL);
    let insert_topic = format!("{schema}.{}", crate::NOTIFICATION_TOPIC_INSERT);
    let leadership_topic = format!("{schema}.{}", crate::NOTIFICATION_TOPIC_LEADERSHIP);
    let listener_result = async {
        let mut listener = PgListener::connect_with(
            inner
                .postgres_pool()
                .expect("PostgreSQL notifications require a PostgreSQL pool"),
        )
        .await?;
        listener.listen(&control_topic).await?;
        listener.listen(&insert_topic).await?;
        listener.listen(&leadership_topic).await?;
        Ok::<_, sqlx::Error>(listener)
    }
    .await;
    let mut listener = match listener_result {
        Ok(listener) => {
            let _ = ready.send(Ok(()));
            listener
        }
        Err(listener_error) => {
            let _ = ready.send(Err(listener_error.to_string()));
            return Err(listener_error.into());
        }
    };

    loop {
        let notification = tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            notification = listener.recv() => notification?,
        };
        if notification.channel() == insert_topic {
            if let Ok(payload) = serde_json::from_str::<InsertNotification>(notification.payload())
            {
                let _ = queue_notifications.send(RuntimeNotification::Insert(payload.queue));
            }
            continue;
        }
        if notification.channel() == leadership_topic {
            if let Ok(payload) =
                serde_json::from_str::<LeadershipNotification>(notification.payload())
            {
                if payload.action == "resigned"
                    && payload.leader_id.as_deref() == Some(inner.id.as_str())
                {
                    continue;
                }
                let notification = if payload.action == "request_resign" {
                    RuntimeNotification::LeadershipRequestResign
                } else {
                    RuntimeNotification::LeadershipChanged
                };
                let _ = queue_notifications.send(notification);
            }
            continue;
        }

        let Ok(payload) = serde_json::from_str::<ControlNotification>(notification.payload())
        else {
            warn!(
                payload = notification.payload(),
                "ignored invalid River control notification"
            );
            continue;
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
}

#[cfg(feature = "sqlite")]
#[allow(
    clippy::too_many_lines,
    reason = "keeps SQLite outbox topic decoding and dispatch in one ordered polling loop"
)]
pub(super) async fn run_sqlite_notifications(
    inner: Arc<ClientInner>,
    cancel: CancellationToken,
    queue_notifications: broadcast::Sender<RuntimeNotification>,
    ready: oneshot::Sender<Result<(), String>>,
) -> Result<(), Error> {
    let pool = inner
        .sqlite_pool()
        .expect("SQLite notifications require a SQLite pool");
    let initial = async {
        let mut connection = pool.acquire().await?;
        crate::database::sqlite::notification_last_id(&mut connection)
            .await
            .map_err(sqlite_backend_error)
    }
    .await;
    let mut after_id = match initial {
        Ok(last_id) => {
            let _ = ready.send(Ok(()));
            last_id
        }
        Err(error) => {
            let _ = ready.send(Err(error.to_string()));
            return Err(error);
        }
    };
    let mut notification_tick =
        tokio::time::interval(crate::database::sqlite::DEFAULT_NOTIFICATION_POLL_INTERVAL);
    notification_tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            () = cancel.cancelled() => return Ok(()),
            _ = notification_tick.tick() => {}
        }
        let notifications = {
            let mut connection = pool.acquire().await?;
            crate::database::sqlite::notification_poll(&mut connection, after_id, 1_000)
                .await
                .map_err(sqlite_backend_error)?
        };
        for notification in notifications {
            after_id = notification.id;
            match notification.topic.as_str() {
                crate::NOTIFICATION_TOPIC_INSERT => {
                    if let Ok(payload) =
                        serde_json::from_str::<InsertNotification>(&notification.payload)
                    {
                        let _ =
                            queue_notifications.send(RuntimeNotification::Insert(payload.queue));
                    }
                }
                crate::NOTIFICATION_TOPIC_LEADERSHIP => {
                    if let Ok(payload) =
                        serde_json::from_str::<LeadershipNotification>(&notification.payload)
                    {
                        if payload.action == "resigned"
                            && payload.leader_id.as_deref() == Some(inner.id.as_str())
                        {
                            continue;
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
                    let Ok(payload) =
                        serde_json::from_str::<ControlNotification>(&notification.payload)
                    else {
                        warn!(
                            payload = notification.payload,
                            "ignored invalid River control notification"
                        );
                        continue;
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
                                let _ = queue_notifications
                                    .send(RuntimeNotification::QueueControl(queue));
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
    }
}
