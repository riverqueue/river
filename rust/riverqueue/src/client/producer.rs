//! Queue producers that fetch and dispatch jobs.

#[allow(clippy::wildcard_imports)]
use super::*;

pub(super) async fn run_dynamic_queues(
    inner: Arc<ClientInner>,
    completion_sender: mpsc::Sender<CompletionUpdate>,
    fetch_cancel: CancellationToken,
    work_cancel: CancellationToken,
    notifications: broadcast::Sender<RuntimeNotification>,
    mut changes: watch::Receiver<u64>,
) -> Result<(), Error> {
    let mut active = HashMap::<String, (QueueConfig, CancellationToken, u64)>::new();
    let mut next_generation = 0_u64;
    let mut tasks = JoinSet::new();
    reconcile_queues(
        &inner,
        &completion_sender,
        &fetch_cancel,
        &work_cancel,
        &notifications,
        &mut active,
        &mut tasks,
        &mut next_generation,
    )?;

    loop {
        tokio::select! {
            () = fetch_cancel.cancelled() => break,
            change_result = changes.changed() => {
                if change_result.is_err() {
                    break;
                }
                reconcile_queues(
                    &inner,
                    &completion_sender,
                    &fetch_cancel,
                    &work_cancel,
                    &notifications,
                    &mut active,
                    &mut tasks,
                    &mut next_generation,
                )?;
            }
            result = tasks.join_next(), if !tasks.is_empty() => {
                let (name, generation, queue_cancel, result) = result
                    .ok_or_else(|| Error::runtime("dynamic queue task set closed".to_owned()))?
                    .map_err(Error::from_join)?;
                if active
                    .get(&name)
                    .is_some_and(|(_, _, current_generation)| *current_generation == generation)
                {
                    active.remove(&name);
                }
                if let Err(queue_error) = result
                    && !queue_cancel.is_cancelled()
                {
                    return Err(queue_error);
                }
            }
        }
    }

    for (_, queue_cancel, _) in active.values() {
        queue_cancel.cancel();
    }
    while let Some(result) = tasks.join_next().await {
        let (_, _, _, queue_result) = result.map_err(Error::from_join)?;
        if let Err(queue_error) = queue_result {
            debug!(error = %queue_error, "dynamic queue stopped with an error during shutdown");
        }
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
pub(super) fn reconcile_queues(
    inner: &Arc<ClientInner>,
    completion_sender: &mpsc::Sender<CompletionUpdate>,
    fetch_cancel: &CancellationToken,
    work_cancel: &CancellationToken,
    notifications: &broadcast::Sender<RuntimeNotification>,
    active: &mut HashMap<String, (QueueConfig, CancellationToken, u64)>,
    tasks: &mut JoinSet<(String, u64, CancellationToken, Result<(), Error>)>,
    next_generation: &mut u64,
) -> Result<(), Error> {
    let configured = inner
        .queues
        .read()
        .map_err(|_| Error::runtime("queue configuration lock poisoned".to_owned()))?
        .clone();
    for (name, (running_config, queue_cancel, _)) in &*active {
        if configured.get(name) != Some(running_config) {
            queue_cancel.cancel();
        }
    }
    active.retain(|name, (running_config, _, _)| configured.get(name) == Some(running_config));

    for (name, config) in configured {
        if active.contains_key(&name) {
            continue;
        }
        let queue_cancel = fetch_cancel.child_token();
        *next_generation = next_generation.wrapping_add(1);
        let generation = *next_generation;
        active.insert(
            name.clone(),
            (config.clone(), queue_cancel.clone(), generation),
        );
        let inner = Arc::clone(inner);
        let completion_sender = completion_sender.clone();
        let notifications = notifications.subscribe();
        let task_cancel = queue_cancel.clone();
        let task_name = name.clone();
        let work_cancel = work_cancel.child_token();
        tasks.spawn(async move {
            let result = run_queue(
                inner,
                completion_sender,
                task_name.clone(),
                config,
                task_cancel.clone(),
                work_cancel,
                notifications,
            )
            .await;
            (task_name, generation, task_cancel, result)
        });
    }
    Ok(())
}

#[allow(clippy::too_many_lines)]
pub(super) async fn run_queue(
    inner: Arc<ClientInner>,
    completion_sender: mpsc::Sender<CompletionUpdate>,
    queue: String,
    config: QueueConfig,
    fetch_cancel: CancellationToken,
    work_cancel: CancellationToken,
    mut notifications: broadcast::Receiver<RuntimeNotification>,
) -> Result<(), Error> {
    const START_RETRY_INTERVAL: Duration = Duration::from_millis(10);
    const START_TIMEOUT: Duration = Duration::from_secs(10);

    let start_time = tokio::time::Instant::now();
    let initial_queue = loop {
        match crate::storage::touch_queue(&inner, &queue).await {
            Ok(queue_row) => break queue_row,
            Err(queue_error) if start_time.elapsed() < START_TIMEOUT => {
                debug!(error = %queue_error, "River queue startup failed; retrying");
                tokio::select! {
                    () = fetch_cancel.cancelled() => return Ok(()),
                    () = tokio::time::sleep(START_RETRY_INTERVAL) => {}
                }
            }
            Err(queue_error) => return Err(queue_error),
        }
    };
    let mut paused = initial_queue.paused_at.is_some();
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
        for row in rows {
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
pub(super) async fn fetch_jobs(
    inner: &ClientInner,
    queue: &str,
    maximum: usize,
) -> Result<Vec<JobRow>, Error> {
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
            let selected_ids = inner
                .pilot
                .select_job_ids(
                    PilotDatabaseConnection::Sqlite(&mut transaction),
                    &FetchParams {
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
                    },
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
        if let Some(fetch_started) = fetch_started {
            for metric in [
                Metric::JobGetAvailableDuration(fetch_started.elapsed()),
                Metric::JobGetAvailableCount(u64::try_from(rows.len()).unwrap_or(u64::MAX)),
            ] {
                for hook in &inner.hooks {
                    if let Err(hook_error) = hook.metric_emit(metric).await {
                        error!(error = %hook_error, "River metric hook failed");
                    }
                }
            }
        }
        return Ok(rows);
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
            let kinds = inner
                .workers
                .kinds()
                .into_iter()
                .map(str::to_owned)
                .collect::<Vec<_>>();
            let mut transaction = inner
                .postgres_pool()
                .expect("PostgreSQL fetch extension requires a PostgreSQL pool")
                .begin()
                .await?;
            let selected_ids = inner
                .pilot
                .select_job_ids(
                    PilotDatabaseConnection::Postgres(&mut transaction),
                    &FetchParams {
                        client_id: inner.id.clone(),
                        database: inner.pilot_database_config(),
                        kinds: kinds.clone(),
                        maximum,
                        queue: queue.to_owned(),
                    },
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
                sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
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
        let rows = records
            .into_iter()
            .map(JobRecord::into_job_row)
            .collect::<Result<Vec<_>, _>>()?;
        if let Some(fetch_started) = fetch_started {
            for metric in [
                Metric::JobGetAvailableDuration(fetch_started.elapsed()),
                Metric::JobGetAvailableCount(u64::try_from(rows.len()).unwrap_or(u64::MAX)),
            ] {
                for hook in &inner.hooks {
                    if let Err(hook_error) = hook.metric_emit(metric).await {
                        error!(error = %hook_error, "River metric hook failed");
                    }
                }
            }
        }
        return Ok(rows);
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
) -> Result<Vec<JobRecord>, sqlx::Error>
where
    E: Executor<'executor, Database = Postgres>,
{
    sqlx::query_as::<_, JobRecord>(AssertSqlSafe(sql))
        .bind(queue)
        .bind(maximum)
        .bind(client_id)
        .bind(ATTEMPTED_BY_MAX)
        .fetch_all(executor)
        .await
}

pub(super) fn sort_claimed_jobs(rows: &mut [JobRow]) {
    rows.sort_by(|left, right| {
        left.priority
            .cmp(&right.priority)
            .then_with(|| left.scheduled_at.cmp(&right.scheduled_at))
            .then_with(|| left.id.cmp(&right.id))
    });
}
