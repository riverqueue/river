//! Queue producers that fetch and dispatch jobs.

use std::collections::HashSet;

#[allow(clippy::wildcard_imports)]
use super::*;

/// Runs one producer per configured queue and reconciles them with runtime
/// queue changes.
///
/// A reconfigured or removed queue stops fetching at once, but its
/// replacement starts only after the old producer's jobs have finished, so a
/// queue never runs more than `max_workers` jobs. A producer that stops
/// unexpectedly (for example after a panic) is restarted with backoff.
///
/// `queues_ready` is sent once every queue configured at startup has created
/// or refreshed its `river_queue` row, as Go's `Client.Start` does before
/// returning, so a peer can pause or inspect those queues right away.
pub(super) async fn run_dynamic_queues(
    inner: Arc<ClientInner>,
    completion_sender: mpsc::Sender<CompletionUpdate>,
    fetch_cancel: CancellationToken,
    work_cancel: CancellationToken,
    notifications: broadcast::Sender<RuntimeNotification>,
    mut changes: watch::Receiver<u64>,
    queues_ready: oneshot::Sender<()>,
) -> Result<(), Error> {
    let (registered_sender, mut registered) = mpsc::unbounded_channel();
    let mut producers = Producers {
        active: HashMap::new(),
        completion_sender,
        draining: HashMap::new(),
        fetch_cancel: fetch_cancel.clone(),
        inner,
        next_generation: 0,
        notifications,
        registered: registered_sender,
        restarts: HashMap::new(),
        task_queues: HashMap::new(),
        tasks: JoinSet::new(),
        work_cancel,
    };
    producers.reconcile()?;
    let mut startup = Some((
        producers.active.keys().cloned().collect::<HashSet<_>>(),
        queues_ready,
    ));
    producers.report_startup(&mut startup);

    loop {
        tokio::select! {
            () = fetch_cancel.cancelled() => break,
            change_result = changes.changed() => {
                if change_result.is_err() {
                    break;
                }
                producers.reconcile()?;
            }
            joined = producers.tasks.join_next_with_id(), if !producers.tasks.is_empty() => {
                if let Some(joined) = joined {
                    producers.finish(joined);
                }
                producers.reconcile()?;
            }
            Some(queue) = registered.recv(), if startup.is_some() => {
                if let Some((pending, _)) = &mut startup {
                    pending.remove(&queue);
                }
            }
        }
        producers.report_startup(&mut startup);
    }

    for (_, queue_cancel, _) in producers.active.values() {
        queue_cancel.cancel();
    }
    while let Some(joined) = producers.tasks.join_next_with_id().await {
        producers.finish(joined);
    }
    Ok(())
}

type ProducerOutcome = (String, u64, CancellationToken, Result<(), Error>);

struct Producers {
    active: HashMap<String, (QueueConfig, CancellationToken, u64)>,
    completion_sender: mpsc::Sender<CompletionUpdate>,
    /// Producers stopped by reconfiguration whose jobs are still finishing.
    draining: HashMap<String, u64>,
    fetch_cancel: CancellationToken,
    inner: Arc<ClientInner>,
    next_generation: u64,
    notifications: broadcast::Sender<RuntimeNotification>,
    /// Receives each queue name once its producer has registered the queue.
    registered: mpsc::UnboundedSender<String>,
    restarts: HashMap<String, u32>,
    task_queues: HashMap<tokio::task::Id, (String, u64)>,
    tasks: JoinSet<ProducerOutcome>,
    work_cancel: CancellationToken,
}

impl Producers {
    fn finish(
        &mut self,
        joined: Result<(tokio::task::Id, ProducerOutcome), tokio::task::JoinError>,
    ) {
        let (task_id, name, generation, failure) = match joined {
            Ok((task_id, (name, generation, queue_cancel, result))) => {
                let failure = match result {
                    Err(queue_error) => Some(queue_error.to_string()),
                    Ok(()) if !queue_cancel.is_cancelled() => {
                        Some("producer exited unexpectedly".to_owned())
                    }
                    Ok(()) => None,
                };
                (task_id, name, generation, failure)
            }
            Err(join_error) => {
                let Some((name, generation)) = self.task_queues.get(&join_error.id()).cloned()
                else {
                    error!(error = %join_error, "River queue producer failed");
                    return;
                };
                (
                    join_error.id(),
                    name,
                    generation,
                    Some(join_error.to_string()),
                )
            }
        };
        self.task_queues.remove(&task_id);
        if self.draining.get(&name) == Some(&generation) {
            self.draining.remove(&name);
        }
        if self
            .active
            .get(&name)
            .is_some_and(|(_, _, active_generation)| *active_generation == generation)
        {
            self.active.remove(&name);
            if let Some(failure) = failure
                && !self.fetch_cancel.is_cancelled()
            {
                let restarts = self.restarts.entry(name.clone()).or_default();
                *restarts += 1;
                error!(
                    queue = %name,
                    error = %failure,
                    attempt = *restarts,
                    "River queue producer failed; restarting it after backoff"
                );
            }
        }
    }

    fn reconcile(&mut self) -> Result<(), Error> {
        if self.fetch_cancel.is_cancelled() {
            return Ok(());
        }
        let configured = self
            .inner
            .queues
            .read()
            .map_err(|_| Error::runtime("queue configuration lock poisoned".to_owned()))?
            .clone();
        let stale = self
            .active
            .iter()
            .filter(|(name, (running_config, _, _))| configured.get(*name) != Some(running_config))
            .map(|(name, _)| name.clone())
            .collect::<Vec<_>>();
        for name in stale {
            if let Some((_, queue_cancel, generation)) = self.active.remove(&name) {
                queue_cancel.cancel();
                self.restarts.remove(&name);
                self.draining.insert(name, generation);
            }
        }

        for (name, config) in configured {
            if self.active.contains_key(&name) || self.draining.contains_key(&name) {
                continue;
            }
            let start_delay = self
                .restarts
                .get(&name)
                .map_or(Duration::ZERO, |restarts| exponential_backoff(*restarts));
            let queue_cancel = self.fetch_cancel.child_token();
            self.next_generation = self.next_generation.wrapping_add(1);
            let generation = self.next_generation;
            self.active.insert(
                name.clone(),
                (config.clone(), queue_cancel.clone(), generation),
            );
            let inner = Arc::clone(&self.inner);
            let completion_sender = self.completion_sender.clone();
            let notifications = self.notifications.subscribe();
            let registered = self.registered.clone();
            let task_cancel = queue_cancel.clone();
            let task_name = name.clone();
            let work_cancel = self.work_cancel.child_token();
            let handle = self.tasks.spawn(async move {
                if !start_delay.is_zero() {
                    tokio::select! {
                        () = task_cancel.cancelled() => {
                            return (task_name, generation, task_cancel, Ok(()));
                        }
                        () = tokio::time::sleep(start_delay) => {}
                    }
                }
                let result = run_queue(
                    inner,
                    completion_sender,
                    task_name.clone(),
                    config,
                    task_cancel.clone(),
                    work_cancel,
                    notifications,
                    registered,
                )
                .await;
                (task_name, generation, task_cancel, result)
            });
            self.task_queues.insert(handle.id(), (name, generation));
        }
        Ok(())
    }

    /// Reports startup readiness once every startup queue that is still
    /// configured has registered.
    fn report_startup(&self, startup: &mut Option<(HashSet<String>, oneshot::Sender<()>)>) {
        let Some((pending, _)) = startup else {
            return;
        };
        pending
            .retain(|queue| self.active.contains_key(queue) || self.draining.contains_key(queue));
        if pending.is_empty()
            && let Some((_, queues_ready)) = startup.take()
        {
            let _ = queues_ready.send(());
        }
    }
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(super) async fn run_queue(
    inner: Arc<ClientInner>,
    completion_sender: mpsc::Sender<CompletionUpdate>,
    queue: String,
    config: QueueConfig,
    fetch_cancel: CancellationToken,
    work_cancel: CancellationToken,
    mut notifications: broadcast::Receiver<RuntimeNotification>,
    registered: mpsc::UnboundedSender<String>,
) -> Result<(), Error> {
    // Short write contention (common on SQLite) clears quickly. Longer
    // outages back off like River's other services; the producer keeps trying
    // for as long as the client runs rather than stopping the client.
    const START_FAST_RETRY_INTERVAL: Duration = Duration::from_millis(10);
    const START_FAST_RETRY_WINDOW: Duration = Duration::from_secs(10);

    let start_time = tokio::time::Instant::now();
    let mut start_attempt = 0;
    let initial_queue = loop {
        match crate::storage::touch_queue(&inner, &queue).await {
            Ok(queue_row) => break queue_row,
            Err(queue_error) => {
                let sleep = if start_time.elapsed() < START_FAST_RETRY_WINDOW {
                    debug!(error = %queue_error, "River queue startup failed; retrying");
                    START_FAST_RETRY_INTERVAL
                } else {
                    start_attempt += 1;
                    let sleep = exponential_backoff(start_attempt);
                    error!(
                        queue = %queue,
                        error = %queue_error,
                        sleep_duration = ?sleep,
                        "River queue startup failed (will retry after backoff)"
                    );
                    sleep
                };
                tokio::select! {
                    () = fetch_cancel.cancelled() => return Ok(()),
                    () = tokio::time::sleep(sleep) => {}
                }
            }
        }
    };
    let _ = registered.send(queue.clone());
    let mut paused = initial_queue.paused_at.is_some();
    let mut metadata = initial_queue.metadata.clone();
    notify_queue_metadata(&inner, &queue, &metadata).await;
    let permits = Arc::new(Semaphore::new(config.max_workers));
    let mut jobs = JoinSet::new();
    let mut last_fetch = tokio::time::Instant::now() - config.fetch_cooldown;
    let mut heartbeat = tokio::time::interval(QUEUE_HEARTBEAT_INTERVAL);
    let mut poll = tokio::time::interval(config.fetch_poll_interval);
    let mut queue_config_poll = tokio::time::interval(QUEUE_CONFIG_POLL_INTERVAL);
    heartbeat.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    queue_config_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        let (mut should_fetch, refresh_queue_state) = tokio::select! {
            () = fetch_cancel.cancelled() => break,
            _ = heartbeat.tick() => {
                if let Err(queue_error) = crate::storage::touch_queue(&inner, &queue).await {
                    error!(error = %queue_error, "River queue heartbeat failed; retrying");
                }
                (false, false)
            },
            _ = queue_config_poll.tick() => (false, true),
            _ = poll.tick() => (true, false),
            result = jobs.join_next(), if !jobs.is_empty() => {
                if let Some(Err(join_error)) = result {
                    error!(error = %join_error, "River queue task failed");
                }
                (true, false)
            },
            notification = notifications.recv() => match notification {
                Ok(RuntimeNotification::Insert(notification_queue)) => (
                    notification_queue == "*" || notification_queue == queue,
                    false,
                ),
                Ok(RuntimeNotification::QueueControl(notification_queue)) => (
                    false,
                    notification_queue == "*" || notification_queue == queue,
                ),
                Ok(
                    RuntimeNotification::LeadershipChanged
                        | RuntimeNotification::LeadershipRequestResign,
                )
                | Err(broadcast::error::RecvError::Closed) => (false, false),
                Err(broadcast::error::RecvError::Lagged(_)) => (true, true),
            },
        };

        if refresh_queue_state {
            match crate::storage::load_queue(&inner, &queue).await {
                Ok(Some(queue_row)) => {
                    if queue_row.metadata != metadata {
                        metadata.clone_from(&queue_row.metadata);
                        notify_queue_metadata(&inner, &queue, &metadata).await;
                    }
                    let next_paused = queue_row.paused_at.is_some();
                    if next_paused != paused {
                        paused = next_paused;
                        let event_kind = if paused {
                            QueueEventKind::Paused
                        } else {
                            QueueEventKind::Resumed
                        };
                        let _ = inner.events.send(Event::queue(event_kind, queue_row));
                        should_fetch |= !paused;
                    }
                }
                Ok(None) => {}
                Err(queue_error) => {
                    error!(error = %queue_error, "River queue state refresh failed; retrying");
                    continue;
                }
            }
        }

        if !should_fetch || paused {
            continue;
        }
        let since_fetch = last_fetch.elapsed();
        if let Some(remaining) = config.fetch_cooldown.checked_sub(since_fetch) {
            tokio::time::sleep(remaining).await;
        }
        // A stop can be requested while another branch above was selected or
        // during the cooldown. Go's fetch query fails once its context is
        // cancelled, so no jobs are claimed after a stop; match that.
        if fetch_cancel.is_cancelled() {
            break;
        }
        let available = permits.available_permits();
        if available == 0 {
            continue;
        }
        let registration_guard = FetchRegistrationGuard::new(&inner);
        let use_parallel_fetch = match inner.database.kind() {
            #[cfg(feature = "postgres")]
            DatabaseKind::Postgres => true,
            #[cfg(feature = "sqlite")]
            DatabaseKind::Sqlite => false,
        };
        let rows = if use_parallel_fetch
            && available >= PARALLEL_FETCH_MINIMUM
            && !inner.pilot.intercepts_fetch()
        {
            let first_maximum = available / 2;
            let second_maximum = available - first_maximum;
            let (first, second) = tokio::join!(
                fetch_jobs(&inner, &queue, first_maximum),
                fetch_jobs(&inner, &queue, second_maximum),
            );
            match (first, second) {
                (Ok(mut first), Ok(second)) => {
                    first.extend(second);
                    first
                }
                (Ok(rows), Err(fetch_error)) | (Err(fetch_error), Ok(rows)) => {
                    error!(
                        error = %fetch_error,
                        "one parallel River job fetch failed; working the successfully fetched jobs"
                    );
                    rows
                }
                (Err(fetch_error), Err(second_fetch_error)) => {
                    last_fetch = tokio::time::Instant::now();
                    error!(
                        error = %fetch_error,
                        secondary_error = %second_fetch_error,
                        "River job fetch failed; retrying"
                    );
                    continue;
                }
            }
        } else {
            match fetch_jobs(&inner, &queue, available).await {
                Ok(rows) => rows,
                Err(fetch_error) => {
                    last_fetch = tokio::time::Instant::now();
                    error!(error = %fetch_error, "River job fetch failed; retrying");
                    continue;
                }
            }
        };
        last_fetch = tokio::time::Instant::now();
        let FetchedJobs { rows, undecodable } = rows;
        // Like River Go, a claimed job whose row couldn't be fully decoded
        // gets an executor that fails its attempt with the decode error
        // instead of working it, so it's retried or discarded rather than
        // left running.
        let claimed = rows.into_iter().map(|row| (row, None)).chain(
            undecodable
                .into_iter()
                .filter_map(|UndecodableJob { error, row }| {
                    let Some(row) = row else {
                        error!(%error, "claimed River job row couldn't be identified; leaving it for the rescuer");
                        return None;
                    };
                    Some((*row, Some(error)))
                }),
        );
        for (row, decode_error) in claimed {
            let permit = Arc::clone(&permits)
                .acquire_owned()
                .await
                .map_err(|_| Error::invalid_job("queue worker semaphore closed".to_owned()))?;
            let hard_cancel = work_cancel.child_token();
            let cancellation = hard_cancel.child_token();
            register_running_attempt(
                &inner.running,
                &inner.pending_cancellations,
                row.id,
                &cancellation,
            );
            let inner = Arc::clone(&inner);
            let completion_sender = completion_sender.clone();
            jobs.spawn(async move {
                execute_job(
                    inner,
                    row,
                    decode_error,
                    hard_cancel,
                    cancellation,
                    completion_sender,
                    permit,
                )
                .await;
            });
        }
        drop(registration_guard);
        while let Some(result) = jobs.try_join_next() {
            if let Err(join_error) = result {
                error!(error = %join_error, "River queue task failed");
            }
        }
    }

    while let Some(result) = jobs.join_next().await {
        if let Err(join_error) = result {
            error!(error = %join_error, "River queue task failed during shutdown");
        }
    }
    Ok(())
}

#[allow(
    clippy::too_many_lines,
    reason = "keeps hooks and metrics identical across backend fetch paths"
)]
/// Tells an extension about a queue's metadata, like River Go's producer does
/// when it starts and when it's notified of a change.
async fn notify_queue_metadata(inner: &ClientInner, queue: &str, metadata: &Map<String, Value>) {
    let params = crate::__private::QueueMetadataChangedParams {
        database: inner.pilot_database_config(),
        metadata: metadata.clone(),
        pool: inner.pilot_database_pool(),
        queue: queue.to_owned(),
    };
    if let Err(hook_error) = inner.pilot.queue_metadata_changed(&params).await {
        error!(queue = %queue, error = %hook_error, "River extension queue metadata hook failed");
    }
}

/// Parameters passed to fetch extension hooks.
fn extension_fetch_params(inner: &ClientInner, queue: &str, maximum: i32) -> FetchParams {
    FetchParams {
        client_id: inner.id.clone(),
        database: inner.pilot_database_config(),
        kinds: inner
            .workers
            .kinds()
            .into_iter()
            .map(str::to_owned)
            .collect(),
        maximum,
        queue: queue.to_owned(),
    }
}

/// Wraps claimed rows and emits fetch metrics.
async fn finish_fetch(
    inner: &ClientInner,
    fetch_started: Option<std::time::Instant>,
    rows: Vec<DecodedJob>,
) -> FetchedJobs {
    let fetched = FetchedJobs::from_decoded(rows);
    if let Some(fetch_started) = fetch_started {
        for metric in [
            Metric::JobGetAvailableDuration(fetch_started.elapsed()),
            Metric::JobGetAvailableCount(u64::try_from(fetched.len()).unwrap_or(u64::MAX)),
        ] {
            for hook in &inner.hooks {
                if let Err(hook_error) = hook.metric_emit(metric).await {
                    error!(error = %hook_error, "River metric hook failed");
                }
            }
        }
    }
    fetched
}

#[allow(
    clippy::too_many_lines,
    reason = "each backend's claim and extension interception stay together until the backend rework"
)]
pub(super) async fn fetch_jobs(
    inner: &ClientInner,
    queue: &str,
    maximum: usize,
) -> Result<FetchedJobs, Error> {
    let fetch_started = (!inner.hooks.is_empty()).then(std::time::Instant::now);
    let maximum = i32::try_from(maximum)
        .map_err(|_| Error::invalid_job("fetch maximum exceeds i32".to_owned()))?;
    #[cfg(feature = "sqlite")]
    if let Some(pool) = inner.sqlite_pool() {
        let params = crate::database::sqlite::ClaimJobs {
            client_id: &inner.id,
            limit: maximum,
            max_attempted_by: ATTEMPTED_BY_MAX,
            now: Utc::now(),
            queue,
        };
        let rows = if inner.pilot.intercepts_fetch() {
            let mut transaction = crate::database::begin_sqlite_write(pool).await?;
            let fetch_params = extension_fetch_params(inner, queue, maximum);
            if let Some(claimed) = inner
                .pilot
                .claim_jobs(
                    PilotDatabaseConnection::Sqlite(&mut transaction),
                    &fetch_params,
                )
                .await
                .map_err(|source| Error::Extension {
                    phase: "fetch claim",
                    source,
                })?
            {
                transaction.commit().await?;
                return Ok(finish_fetch(
                    inner,
                    fetch_started,
                    claimed
                        .into_iter()
                        .map(crate::__private::ClaimedJob::into_decoded)
                        .collect(),
                )
                .await);
            }
            let selected_ids = inner
                .pilot
                .select_job_ids(
                    PilotDatabaseConnection::Sqlite(&mut transaction),
                    &fetch_params,
                )
                .await
                .map_err(|source| Error::Extension {
                    phase: "fetch selection",
                    source,
                })?;
            let rows = match selected_ids {
                Some(ids) => {
                    crate::database::sqlite::claim_selected(&mut transaction, &params, &ids).await
                }
                None => crate::database::sqlite::claim(&mut transaction, &params).await,
            }
            .map_err(sqlite_backend_error)?;
            transaction.commit().await?;
            rows
        } else {
            let mut connection = pool.acquire().await?;
            crate::database::sqlite::claim(&mut connection, &params)
                .await
                .map_err(sqlite_backend_error)?
        };
        return Ok(finish_fetch(inner, fetch_started, rows).await);
    }
    #[cfg(feature = "postgres")]
    {
        let table = inner.schema.qualify("river_job");
        let queue_table = inner.schema.qualify("river_queue");
        let oss_sql = format!(
            "WITH locked AS (\
            SELECT id FROM {table} WHERE state = 'available' AND queue = $1 AND scheduled_at <= now() \
                AND NOT EXISTS (SELECT 1 FROM {queue_table} WHERE name = $1 AND paused_at IS NOT NULL) \
            ORDER BY priority, scheduled_at, id LIMIT $2 FOR UPDATE SKIP LOCKED\
         ) UPDATE {table} AS job \
            SET state = 'running', attempt = job.attempt + 1, attempted_at = now(), \
                attempted_by = array_append(\
                    CASE WHEN array_length(job.attempted_by, 1) >= $4 \
                         THEN job.attempted_by[array_length(job.attempted_by, 1) + 2 - $4:] \
                         ELSE job.attempted_by END, $3) \
            FROM locked WHERE job.id = locked.id \
            RETURNING {}, false AS unique_skipped_as_duplicate",
            job_projection("job")
        );
        let records = if inner.pilot.intercepts_fetch() {
            let mut transaction = crate::database::begin_postgres(
                inner
                    .postgres_pool()
                    .expect("PostgreSQL fetch extension requires a PostgreSQL pool"),
            )
            .await?;
            let fetch_params = extension_fetch_params(inner, queue, maximum);
            if let Some(claimed) = inner
                .pilot
                .claim_jobs(
                    PilotDatabaseConnection::Postgres(&mut transaction),
                    &fetch_params,
                )
                .await
                .map_err(|source| Error::Extension {
                    phase: "fetch claim",
                    source,
                })?
            {
                transaction.commit().await?;
                return Ok(finish_fetch(
                    inner,
                    fetch_started,
                    claimed
                        .into_iter()
                        .map(crate::__private::ClaimedJob::into_decoded)
                        .collect(),
                )
                .await);
            }
            let selected_ids = inner
                .pilot
                .select_job_ids(
                    PilotDatabaseConnection::Postgres(&mut transaction),
                    &fetch_params,
                )
                .await
                .map_err(|source| Error::Extension {
                    phase: "fetch selection",
                    source,
                })?;
            let records = if let Some(selected_ids) = selected_ids {
                let sql = format!(
                    "UPDATE {table} AS job SET state = 'running', attempt = job.attempt + 1, \
                    attempted_at = now(), attempted_by = array_append(\
                        CASE WHEN array_length(job.attempted_by, 1) >= $3 \
                             THEN job.attempted_by[array_length(job.attempted_by, 1) + 2 - $3:] \
                             ELSE job.attempted_by END, $2) \
                WHERE id = ANY($1::bigint[]) AND state = 'available' \
                RETURNING {}, false AS unique_skipped_as_duplicate",
                    job_projection("job")
                );
                sqlx::query(AssertSqlSafe(sql))
                    .bind(selected_ids)
                    .bind(&inner.id)
                    .bind(ATTEMPTED_BY_MAX)
                    .fetch_all(&mut *transaction)
                    .await?
            } else {
                fetch_oss_records(&mut *transaction, oss_sql, queue, maximum, &inner.id).await?
            };
            transaction.commit().await?;
            records
        } else {
            fetch_oss_records(
                inner
                    .postgres_pool()
                    .expect("PostgreSQL fetch path requires a PostgreSQL pool"),
                oss_sql,
                queue,
                maximum,
                &inner.id,
            )
            .await?
        };
        return Ok(finish_fetch(
            inner,
            fetch_started,
            records.iter().map(decode_job_row).collect(),
        )
        .await);
    }
    #[allow(unreachable_code)]
    Err(Error::runtime(
        "database dispatch selected no supported backend".to_owned(),
    ))
}

#[cfg(feature = "postgres")]
pub(super) async fn fetch_oss_records<'executor, E>(
    executor: E,
    sql: String,
    queue: &str,
    maximum: i32,
    client_id: &str,
) -> Result<Vec<PgRow>, sqlx::Error>
where
    E: Executor<'executor, Database = Postgres>,
{
    sqlx::query(AssertSqlSafe(sql))
        .bind(queue)
        .bind(maximum)
        .bind(client_id)
        .bind(ATTEMPTED_BY_MAX)
        .fetch_all(executor)
        .await
}

/// Jobs claimed by one fetch. Claims commit before rows are decoded, so rows
/// that can't be fully decoded are returned separately to have their attempts
/// failed, instead of failing the whole fetch and stranding every claimed job.
#[derive(Default)]
pub(super) struct FetchedJobs {
    pub(super) rows: Vec<JobRow>,
    pub(super) undecodable: Vec<UndecodableJob>,
}

impl FetchedJobs {
    pub(super) fn from_decoded(decoded: Vec<DecodedJob>) -> Self {
        let mut fetched = Self::default();
        for row in decoded {
            match row {
                Ok(row) => fetched.rows.push(row),
                Err(undecodable) => fetched.undecodable.push(undecodable),
            }
        }
        fetched
    }

    pub(super) fn extend(&mut self, other: Self) {
        self.rows.extend(other.rows);
        self.undecodable.extend(other.undecodable);
    }

    pub(super) fn len(&self) -> usize {
        self.rows.len() + self.undecodable.len()
    }
}

pub(super) fn sort_claimed_jobs(rows: &mut [JobRow]) {
    rows.sort_by(|left, right| {
        left.priority
            .cmp(&right.priority)
            .then_with(|| left.scheduled_at.cmp(&right.scheduled_at))
            .then_with(|| left.id.cmp(&right.id))
    });
}
