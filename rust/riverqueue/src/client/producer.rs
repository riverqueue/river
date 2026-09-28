//! Queue producers that fetch and dispatch jobs.

use std::collections::HashSet;

use futures_util::FutureExt as _;

use crate::pilot::{ProducerConfiguration, SharedProducer};

#[allow(clippy::wildcard_imports)]
use super::*;

/// Runs one producer per configured queue and reconciles them with runtime
/// queue changes.
///
/// A producer applies a changed configuration while it runs. A removed
/// queue stops claiming at once, drains its running jobs, and shuts down its
/// extension session before its name can be added again, so a queue never
/// runs under two producers. A producer that stops unexpectedly (for example
/// after a panic) is restarted with backoff.
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
        fatal: None,
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
    producers.reconcile();
    let mut startup = Some((
        producers.active.keys().cloned().collect::<HashSet<_>>(),
        queues_ready,
    ));
    producers.report_startup(&mut startup);

    loop {
        if producers.fatal.is_some() {
            break;
        }
        tokio::select! {
            () = fetch_cancel.cancelled() => break,
            change_result = changes.changed() => {
                if change_result.is_err() {
                    break;
                }
                producers.reconcile();
            }
            joined = producers.tasks.join_next_with_id(), if !producers.tasks.is_empty() => {
                if let Some(joined) = joined {
                    producers.finish(joined);
                }
                producers.reconcile();
            }
            Some(queue) = registered.recv(), if startup.is_some() => {
                if let Some((pending, _)) = &mut startup {
                    pending.remove(&queue);
                }
            }
        }
        producers.report_startup(&mut startup);
    }

    for active in producers.active.values() {
        active.cancel.cancel();
    }
    if producers.fatal.is_some() {
        producers.work_cancel.cancel();
    }
    while let Some(joined) = producers.tasks.join_next_with_id().await {
        producers.finish(joined);
    }
    producers.inner.live_queues.send_replace(HashSet::new());
    producers.fatal.map_or(Ok(()), Err)
}

type ProducerOutcome = (String, u64, CancellationToken, Result<(), Error>);

/// A queue's current producer generation.
struct ActiveProducer {
    cancel: CancellationToken,
    /// The configuration the producer applies while it runs.
    config: watch::Sender<QueueConfig>,
    generation: u64,
}

struct Producers {
    active: HashMap<String, ActiveProducer>,
    completion_sender: mpsc::Sender<CompletionUpdate>,
    /// Producers of removed queues whose jobs are still finishing.
    draining: HashMap<String, u64>,
    /// The first producer failure that stops the client.
    fatal: Option<Error>,
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
                    // A producer returns an error only for a failure that
                    // stops the client, such as a broken claim protocol.
                    Err(queue_error) => {
                        self.fatal.get_or_insert(queue_error);
                        None
                    }
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
            .is_some_and(|active| active.generation == generation)
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
        self.publish_live();
    }

    /// Publishes the queues whose producers are running or draining, which
    /// [`LocalQueues`] uses to keep a removed queue's name reserved until its
    /// producer stops.
    fn publish_live(&self) {
        let live = self
            .active
            .keys()
            .chain(self.draining.keys())
            .cloned()
            .collect::<HashSet<_>>();
        self.inner.live_queues.send_if_modified(|current| {
            if *current == live {
                return false;
            }
            *current = live;
            true
        });
    }

    fn reconcile(&mut self) {
        if self.fetch_cancel.is_cancelled() {
            return;
        }
        let configured = self
            .inner
            .queues
            .read()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .clone();
        let removed = self
            .active
            .keys()
            .filter(|name| !configured.contains_key(*name))
            .cloned()
            .collect::<Vec<_>>();
        for name in removed {
            if let Some(active) = self.active.remove(&name) {
                active.cancel.cancel();
                self.restarts.remove(&name);
                self.draining.insert(name, active.generation);
            }
        }

        for (name, config) in configured {
            if let Some(active) = self.active.get(&name) {
                active.config.send_if_modified(|running| {
                    if *running == config {
                        return false;
                    }
                    *running = config;
                    true
                });
                continue;
            }
            if self.draining.contains_key(&name) {
                continue;
            }
            let start_delay = self
                .restarts
                .get(&name)
                .map_or(Duration::ZERO, |restarts| exponential_backoff(*restarts));
            let queue_cancel = self.fetch_cancel.child_token();
            self.next_generation = self.next_generation.wrapping_add(1);
            let generation = self.next_generation;
            let (config_sender, config_receiver) = watch::channel(config);
            self.active.insert(
                name.clone(),
                ActiveProducer {
                    cancel: queue_cancel.clone(),
                    config: config_sender,
                    generation,
                },
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
                // Boxed: the producer loop's state, including an in-flight
                // fetch, is too large to embed in this task's future.
                let result = Box::pin(run_queue(
                    inner,
                    completion_sender,
                    task_name.clone(),
                    config_receiver,
                    task_cancel.clone(),
                    work_cancel,
                    notifications,
                    registered,
                ))
                .await;
                (task_name, generation, task_cancel, result)
            });
            self.task_queues.insert(handle.id(), (name, generation));
        }
        self.publish_live();
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

/// A queue producer's started generation: its persisted record and the
/// extension's session, if any.
struct Generation {
    queue: crate::Queue,
    session: Option<SharedProducer>,
}

/// The configuration an extension's session sees.
fn producer_configuration(config: &QueueConfig, queue: &crate::Queue) -> ProducerConfiguration {
    ProducerConfiguration {
        max_workers: config.max_workers,
        queue: queue.clone(),
        settings: config.extension_settings.clone(),
    }
}

/// Creates or refreshes the queue's record and starts the extension's
/// session for this generation.
async fn start_generation(
    inner: &ClientInner,
    queue: &str,
    config: &QueueConfig,
) -> Result<Generation, Error> {
    let queue_row = crate::storage::touch_queue(inner, queue).await?;
    let session = inner
        .pilot
        .start_producer(crate::__private::ProducerStartContext {
            client_id: inner.id.clone(),
            configuration: producer_configuration(config, &queue_row),
            database: inner.pilot_database(),
        })
        .await
        .map_err(|source| Error::Extension {
            phase: crate::ExtensionPhase::AddOnProducer,
            source,
        })?;
    Ok(Generation {
        queue: queue_row,
        session: session.map(SharedProducer::from),
    })
}

/// Runs one of an extension session's synchronous callbacks, turning a panic
/// into the error that stops the client, so the producer still drains and
/// shuts the session down in order.
fn session_callback(callback_name: &str, callback: impl FnOnce()) -> Result<(), Error> {
    std::panic::catch_unwind(std::panic::AssertUnwindSafe(callback)).map_err(|panic| {
        Error::Extension {
            phase: crate::ExtensionPhase::AddOnProducer,
            source: format!(
                "{callback_name} panicked: {}",
                crate::error::panic_message(&panic)
            )
            .into(),
        }
    })
}

/// The attempts a producer has running, and the claimed rows it reports to
/// the extension's session as each attempt exits.
struct Attempts {
    /// The first panic of the session's `job_finished`, which stops the
    /// client.
    failure: Option<Error>,
    rows: HashMap<tokio::task::Id, JobRow>,
    session: Option<SharedProducer>,
    tasks: JoinSet<()>,
}

impl Attempts {
    fn len(&self) -> usize {
        self.tasks.len()
    }

    /// Records that an attempt's task ended, however it ended.
    fn exited(&mut self, joined: Result<tokio::task::Id, tokio::task::JoinError>, stopping: bool) {
        let task_id = match joined {
            Ok(task_id) => task_id,
            Err(join_error) => {
                if stopping {
                    error!(error = %join_error, "River queue task failed during shutdown");
                } else {
                    error!(error = %join_error, "River queue task failed");
                }
                join_error.id()
            }
        };
        if let Some(row) = self.rows.remove(&task_id)
            && let Some(session) = &self.session
            && let Err(failure) = session_callback("job_finished", || session.job_finished(&row))
        {
            error!(error = %crate::error::Chain(&failure), "River extension producer callback panicked; stopping the client");
            self.failure.get_or_insert(failure);
        }
    }

    fn spawn(&mut self, row: &JobRow, task: impl Future<Output = ()> + Send + 'static) {
        let handle = self.tasks.spawn(task);
        if self.session.is_some() {
            self.rows.insert(handle.id(), row.clone());
        }
    }
}

/// Keeps a producer's queue record and extension session current until
/// `stop`, which the producer cancels only after its last attempt exits, so
/// reports continue while it drains.
///
/// Like Go's producer, the two reports run independently, so a slow one
/// never delays the other: the queue record is refreshed after up to a
/// second of jitter and then every [`QUEUE_HEARTBEAT_INTERVAL`], and the
/// session reports after its own jitter and then every producer report
/// interval. Each report runs one at a time and is dropped after
/// [`PRODUCER_REPORT_TIMEOUT`].
async fn run_reports(
    inner: Arc<ClientInner>,
    queue: String,
    session: Option<SharedProducer>,
    stop: CancellationToken,
) {
    let jitter = || crate::maintenance::random_duration(Duration::ZERO, Duration::from_secs(1));
    let heartbeat = async {
        let mut ticks = tokio::time::interval_at(
            tokio::time::Instant::now() + jitter() + QUEUE_HEARTBEAT_INTERVAL,
            QUEUE_HEARTBEAT_INTERVAL,
        );
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticks.tick().await;
            let touched = tokio::time::timeout(
                PRODUCER_REPORT_TIMEOUT,
                crate::storage::touch_queue(&inner, &queue),
            )
            .await;
            match touched {
                Ok(Ok(_)) => {}
                Ok(Err(queue_error)) => {
                    error!(queue = %queue, error = %crate::error::Chain(&queue_error), "River queue heartbeat failed; retrying");
                }
                Err(_) => {
                    error!(queue = %queue, timeout = ?PRODUCER_REPORT_TIMEOUT, "River queue heartbeat timed out; retrying");
                }
            }
        }
    };
    let keep_alive = async {
        let Some(session) = &session else {
            return std::future::pending().await;
        };
        let mut ticks = tokio::time::interval_at(
            tokio::time::Instant::now() + jitter(),
            inner.producer_report_interval,
        );
        ticks.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Delay);
        loop {
            ticks.tick().await;
            let stale_before = Utc::now()
                - chrono::Duration::from_std(PRODUCER_STALE_RETENTION)
                    .unwrap_or(chrono::Duration::MAX);
            let report = std::panic::AssertUnwindSafe(
                session.keep_alive(crate::__private::ProducerKeepAliveContext { stale_before }),
            )
            .catch_unwind();
            match tokio::time::timeout(PRODUCER_REPORT_TIMEOUT, report).await {
                Ok(Ok(Ok(()))) => {}
                Ok(Ok(Err(report_error))) => {
                    error!(queue = %queue, error = %crate::error::Chain(&*report_error), "River extension producer report failed; retrying at the next interval");
                }
                Ok(Err(panic)) => {
                    error!(queue = %queue, panic = crate::error::panic_message(&panic), "River extension producer report panicked; retrying at the next interval");
                }
                Err(_) => {
                    error!(queue = %queue, timeout = ?PRODUCER_REPORT_TIMEOUT, "River extension producer report timed out; retrying at the next interval");
                }
            }
        }
    };
    tokio::select! {
        () = stop.cancelled() => {}
        () = heartbeat => {}
        () = keep_alive => {}
    }
}

/// Shuts an extension's session down after its producer stopped, like Go's
/// `finalizeShutdown`: up to four attempts, one at a time, with deadlines of
/// 100 milliseconds growing fivefold.
async fn shut_down_session(queue: &str, session: &dyn crate::__private::PilotProducer) {
    const ATTEMPTS: u32 = 4;
    const BASE_TIMEOUT: Duration = Duration::from_millis(100);

    let mut timeout = BASE_TIMEOUT;
    for attempt in 1..=ATTEMPTS {
        let context = crate::__private::ProducerShutdownContext { attempt, timeout };
        let shutdown = std::panic::AssertUnwindSafe(session.shutdown(context)).catch_unwind();
        match tokio::time::timeout(timeout, shutdown).await {
            Ok(Ok(Ok(()))) => return,
            Ok(Ok(Err(shutdown_error))) => {
                error!(queue = %queue, attempt, ?timeout, error = %crate::error::Chain(&*shutdown_error), "River extension producer shutdown failed");
            }
            Ok(Err(panic)) => {
                error!(queue = %queue, attempt, ?timeout, panic = crate::error::panic_message(&panic), "River extension producer shutdown panicked");
            }
            Err(_) => {
                error!(queue = %queue, attempt, ?timeout, "River extension producer shutdown timed out");
            }
        }
        timeout *= 5;
    }
    warn!(queue = %queue, "River extension producer failed to shut down cleanly after all attempts");
}

#[allow(clippy::too_many_arguments, clippy::too_many_lines)]
pub(super) async fn run_queue(
    inner: Arc<ClientInner>,
    completion_sender: mpsc::Sender<CompletionUpdate>,
    queue: String,
    mut config_changes: watch::Receiver<QueueConfig>,
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

    let mut config = config_changes.borrow_and_update().clone();
    let start_time = tokio::time::Instant::now();
    let mut start_attempt = 0;
    let Generation {
        queue: mut queue_row,
        session,
    } = loop {
        let Some(started) =
            unless_cancelled(&fetch_cancel, start_generation(&inner, &queue, &config)).await
        else {
            return Ok(());
        };
        match started {
            Ok(generation) => break generation,
            Err(queue_error) => {
                let sleep = if start_time.elapsed() < START_FAST_RETRY_WINDOW {
                    debug!(error = %crate::error::Chain(&queue_error), "River queue startup failed; retrying");
                    START_FAST_RETRY_INTERVAL
                } else {
                    start_attempt += 1;
                    let sleep = exponential_backoff(start_attempt);
                    error!(
                        queue = %queue,
                        error = %crate::error::Chain(&queue_error),
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
    // Reports outlive claiming: they stop only once the last attempt exits.
    let reports_stop = CancellationToken::new();
    let reports = AbortOnDrop(tokio::spawn(run_reports(
        Arc::clone(&inner),
        queue.clone(),
        session.clone(),
        reports_stop.clone(),
    )));
    let mut paused = queue_row.paused_at.is_some();
    let claims_through_session = session
        .as_ref()
        .is_some_and(|session| session.intercepts_claim());
    let mut attempts = Attempts {
        failure: None,
        rows: HashMap::new(),
        session: session.clone(),
        tasks: JoinSet::new(),
    };
    // `None` until the first fetch, which needs no cooldown. Subtracting the
    // cooldown from now instead would panic for a cooldown longer than the
    // monotonic clock's age, as on a freshly booted macOS host.
    let mut last_fetch: Option<tokio::time::Instant> = None;
    let mut poll = tokio::time::interval(config.fetch_poll_interval);
    let mut queue_config_poll = tokio::time::interval(QUEUE_CONFIG_POLL_INTERVAL);
    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    queue_config_poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    let outcome = loop {
        if let Some(failure) = attempts.failure.take() {
            break Err(failure);
        }
        let (mut should_fetch, refresh_queue_state) = tokio::select! {
            () = fetch_cancel.cancelled() => break Ok(()),
            changed = config_changes.changed() => {
                if changed.is_err() {
                    break Ok(());
                }
                let updated = config_changes.borrow_and_update().clone();
                if updated.fetch_poll_interval != config.fetch_poll_interval {
                    poll = tokio::time::interval(updated.fetch_poll_interval);
                    poll.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
                }
                config = updated;
                if let Some(session) = &session {
                    let configuration = producer_configuration(&config, &queue_row);
                    if let Err(failure) = session_callback("configuration_changed", || {
                        session.configuration_changed(&configuration);
                    }) {
                        break Err(failure);
                    }
                }
                // More capacity may allow a claim now.
                (true, false)
            },
            _ = queue_config_poll.tick() => (false, true),
            _ = poll.tick() => (true, false),
            Some(joined) = attempts.tasks.join_next_with_id(), if !attempts.tasks.is_empty() => {
                attempts.exited(joined.map(|(task_id, ())| task_id), false);
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
                Err(broadcast::error::RecvError::Closed) => (false, false),
                Err(broadcast::error::RecvError::Lagged(_)) => (true, true),
            },
        };

        if refresh_queue_state {
            let Some(loaded) =
                unless_cancelled(&fetch_cancel, crate::storage::load_queue(&inner, &queue)).await
            else {
                break Ok(());
            };
            match loaded {
                Ok(Some(loaded)) => {
                    let changed = loaded.metadata != queue_row.metadata
                        || loaded.paused_at.is_some() != queue_row.paused_at.is_some();
                    queue_row = loaded;
                    if changed && let Some(session) = &session {
                        let configuration = producer_configuration(&config, &queue_row);
                        if let Err(failure) = session_callback("configuration_changed", || {
                            session.configuration_changed(&configuration);
                        }) {
                            break Err(failure);
                        }
                    }
                    let next_paused = queue_row.paused_at.is_some();
                    if next_paused != paused {
                        paused = next_paused;
                        let event_kind = if paused {
                            QueueEventKind::Paused
                        } else {
                            QueueEventKind::Resumed
                        };
                        let _ = inner
                            .events
                            .send(Event::queue(event_kind, queue_row.clone()));
                        should_fetch |= !paused;
                    }
                }
                Ok(None) => {}
                Err(queue_error) => {
                    error!(error = %crate::error::Chain(&queue_error), "River queue state refresh failed; retrying");
                    continue;
                }
            }
        }

        if !should_fetch || paused {
            continue;
        }
        if let Some(remaining) = last_fetch
            .and_then(|last_fetch| config.fetch_cooldown.checked_sub(last_fetch.elapsed()))
        {
            tokio::select! {
                () = fetch_cancel.cancelled() => break Ok(()),
                () = tokio::time::sleep(remaining) => {}
            }
        }
        // A stop can be requested while another branch above was selected or
        // during the cooldown. Go's fetch query fails once its context is
        // cancelled, so no jobs are claimed after a stop; match that.
        if fetch_cancel.is_cancelled() {
            break Ok(());
        }
        // Lowering `max_workers` stops claims until enough running jobs
        // finish; it never cancels them.
        let available = config.max_workers.saturating_sub(attempts.len());
        if available == 0 {
            continue;
        }
        let registration_guard = FetchRegistrationGuard::new(&inner);
        let fetched = match (&session, claims_through_session) {
            (Some(session), true) => {
                match claim_through_session(
                    &inner,
                    session.as_ref(),
                    &queue,
                    available,
                    &fetch_cancel,
                )
                .await
                {
                    Ok(fetched) => Ok(fetched),
                    Err(SessionClaimError::Claim(claim_error)) => Err(claim_error),
                    Err(SessionClaimError::Protocol(protocol_error)) => break Err(protocol_error),
                }
            }
            // Boxed: two concurrent PostgreSQL claims make a large future.
            _ => Box::pin(fetch_available(&inner, &queue, available, &fetch_cancel)).await,
        };
        last_fetch = Some(tokio::time::Instant::now());
        let FetchedJobs { rows, undecodable } = match fetched {
            Ok(fetched) => fetched,
            Err(fetch_error) => {
                error!(error = %crate::error::Chain(&fetch_error), "River job fetch failed; retrying");
                continue;
            }
        };
        // Like River Go, a claimed job whose row couldn't be fully decoded
        // gets an executor that fails its attempt with the decode error
        // instead of working it, so it's retried or discarded rather than
        // left running.
        let claimed = rows.into_iter().map(|row| (row, None)).chain(
            undecodable
                .into_iter()
                .filter_map(|UndecodableJob { error, row, .. }| {
                    let Some(row) = row else {
                        error!(%error, "claimed River job row couldn't be identified; leaving it for the rescuer");
                        return None;
                    };
                    Some((*row, Some(error)))
                }),
        );
        for (row, decode_error) in claimed {
            let hard_cancel = work_cancel.child_token();
            let cancellation = hard_cancel.child_token();
            register_running_attempt(
                &inner.running,
                &inner.pending_cancellations,
                row.id,
                &cancellation,
            );
            let task_inner = Arc::clone(&inner);
            let completion_sender = completion_sender.clone();
            let task_row = row.clone();
            let claim_stop = fetch_cancel.clone();
            attempts.spawn(&row, async move {
                execute_job(
                    task_inner,
                    task_row,
                    decode_error,
                    hard_cancel,
                    cancellation,
                    completion_sender,
                    claim_stop,
                )
                .await;
            });
        }
        drop(registration_guard);
        while let Some(joined) = attempts.tasks.try_join_next_with_id() {
            attempts.exited(joined.map(|(task_id, ())| task_id), false);
        }
    };

    if outcome.is_err() {
        // A protocol failure stops the client: cancel this queue's attempts
        // like a hard stop, then wait for them.
        work_cancel.cancel();
    }
    while let Some(joined) = attempts.tasks.join_next_with_id().await {
        attempts.exited(joined.map(|(task_id, ())| task_id), true);
    }
    let outcome = match (outcome, attempts.failure.take()) {
        (Ok(()), Some(failure)) => Err(failure),
        (outcome, _) => outcome,
    };
    reports_stop.cancel();
    let mut reports = reports;
    if let Err(join_error) = (&mut reports.0).await {
        error!(queue = %queue, error = %join_error, "River producer reports failed");
    }
    if let Some(session) = &session {
        shut_down_session(&queue, session.as_ref()).await;
    }
    outcome
}

/// Why a claim through an extension's session produced no jobs.
enum SessionClaimError {
    /// The session reported an error; River tries again later.
    Claim(Error),
    /// The session returned committed rows River can't accept.
    Protocol(Error),
}

/// Claims through an extension's session and checks what it returned.
async fn claim_through_session(
    inner: &ClientInner,
    session: &dyn crate::__private::PilotProducer,
    queue: &str,
    limit: usize,
    claim_stop: &CancellationToken,
) -> Result<FetchedJobs, SessionClaimError> {
    let fetch_started = (!inner.hooks.is_empty()).then(std::time::Instant::now);
    let database = inner.pilot_database();
    let claimed = session.claim(
        crate::__private::ProducerClaimContext {
            client_id: &inner.id,
            claim_stop,
            database: &database,
            limit,
            queue,
        },
        crate::__private::ProducerClaimNext::new(inner, queue, limit),
    );
    let claimed = std::panic::AssertUnwindSafe(claimed)
        .catch_unwind()
        .await
        .map_err(|panic| {
            SessionClaimError::Protocol(Error::Extension {
                phase: crate::ExtensionPhase::AddOnFetchClaim,
                source: format!("claim panicked: {}", crate::error::panic_message(&panic)).into(),
            })
        })?
        .map_err(|source| {
            SessionClaimError::Claim(Error::Extension {
                phase: crate::ExtensionPhase::AddOnFetchClaim,
                source,
            })
        })?;
    if let Err(violation) = crate::pilot::validate_claimed(&claimed, &inner.id, queue, limit) {
        error!(queue = %queue, error = %violation, "River extension claim broke the claim protocol; stopping the client");
        return Err(SessionClaimError::Protocol(Error::Extension {
            phase: crate::ExtensionPhase::AddOnFetchClaim,
            source: violation.into(),
        }));
    }
    let rows = claimed
        .into_iter()
        .map(crate::__private::ClaimedJob::into_decoded)
        .collect();
    Ok(finish_fetch(inner, fetch_started, rows).await)
}

/// Claims available jobs with River's own statements, splitting a large
/// PostgreSQL claim in two.
async fn fetch_available(
    inner: &ClientInner,
    queue: &str,
    available: usize,
    cancel: &CancellationToken,
) -> Result<FetchedJobs, Error> {
    let use_parallel_fetch = match inner.database.kind() {
        #[cfg(feature = "postgres")]
        DatabaseKind::Postgres => true,
        #[cfg(feature = "sqlite")]
        DatabaseKind::Sqlite => false,
    };
    if !use_parallel_fetch || available < PARALLEL_FETCH_MINIMUM {
        return fetch_jobs(inner, queue, available, cancel).await;
    }
    let first_maximum = available / 2;
    let second_maximum = available - first_maximum;
    let (first, second) = tokio::join!(
        fetch_jobs(inner, queue, first_maximum, cancel),
        fetch_jobs(inner, queue, second_maximum, cancel),
    );
    match (first, second) {
        (Ok(mut first), Ok(second)) => {
            first.extend(second);
            Ok(first)
        }
        (Ok(rows), Err(fetch_error)) | (Err(fetch_error), Ok(rows)) => {
            error!(
                error = %crate::error::Chain(&fetch_error),
                "one parallel River job fetch failed; working the successfully fetched jobs"
            );
            Ok(rows)
        }
        (Err(fetch_error), Err(second_fetch_error)) => {
            error!(
                secondary_error = %crate::error::Chain(&second_fetch_error),
                "the other parallel River job fetch failed too"
            );
            Err(fetch_error)
        }
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
                    error!(error = %crate::error::Chain(&hook_error), "River metric hook failed");
                }
            }
        }
    }
    fetched
}

/// Waits for `operation` unless `cancel` fires first.
///
/// A fetch only abandons connection acquisition and transaction begins,
/// which River's begin helpers make safe to drop. Like Go's fetch, which
/// runs under the fetch context, a stop then doesn't wait out the pool's
/// acquire timeout during a database outage. Nothing is claimed until the
/// claim itself runs, and that always completes.
async fn unless_cancelled<T>(
    cancel: &CancellationToken,
    operation: impl std::future::Future<Output = T>,
) -> Option<T> {
    tokio::select! {
        biased;
        () = cancel.cancelled() => None,
        output = operation => Some(output),
    }
}

/// Runs River's standard claim of up to `limit` available jobs from `queue`
/// on `connection`, the claim a fetch makes without an extension.
pub(crate) async fn standard_claim(
    inner: &ClientInner,
    connection: PilotDatabaseConnection<'_>,
    queue: &str,
    limit: usize,
) -> Result<Vec<DecodedJob>, Error> {
    let limit = i32::try_from(limit)
        .map_err(|_| Error::runtime_context("job fetch", "fetch maximum exceeds i32"))?;
    match connection {
        #[cfg(feature = "postgres")]
        PilotDatabaseConnection::Postgres(connection) => Ok(fetch_oss_records(
            connection,
            standard_claim_sql(inner),
            queue,
            limit,
            &inner.id,
        )
        .await?
        .iter()
        .map(decode_job_row)
        .collect()),
        #[cfg(feature = "sqlite")]
        PilotDatabaseConnection::Sqlite(connection) => {
            let params = crate::database::sqlite::ClaimJobs {
                client_id: &inner.id,
                limit,
                max_attempted_by: ATTEMPTED_BY_MAX,
                now: Utc::now(),
                queue,
            };
            crate::database::sqlite::claim(connection, &params)
                .await
                .map_err(sqlite_backend_error)
        }
    }
}

/// River's PostgreSQL claim statement for this client's schema.
#[cfg(feature = "postgres")]
fn standard_claim_sql(inner: &ClientInner) -> String {
    let table = inner.schema.qualify("river_job");
    let queue_table = inner.schema.qualify("river_queue");
    format!(
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
    )
}

/// Claims up to `maximum` jobs from `queue` with River's own statement on a
/// pooled connection. Returns no jobs when `cancel` fires before a
/// connection is available.
pub(super) async fn fetch_jobs(
    inner: &ClientInner,
    queue: &str,
    maximum: usize,
    cancel: &CancellationToken,
) -> Result<FetchedJobs, Error> {
    let fetch_started = (!inner.hooks.is_empty()).then(std::time::Instant::now);
    let rows = match inner.database.pool() {
        #[cfg(feature = "postgres")]
        DatabasePool::Postgres(pool) => {
            let Some(connection) = unless_cancelled(cancel, pool.acquire()).await else {
                return Ok(FetchedJobs::default());
            };
            let mut connection = connection?;
            standard_claim(
                inner,
                PilotDatabaseConnection::Postgres(&mut connection),
                queue,
                maximum,
            )
            .await?
        }
        #[cfg(feature = "sqlite")]
        DatabasePool::Sqlite(pool) => {
            let Some(connection) = unless_cancelled(cancel, pool.acquire()).await else {
                return Ok(FetchedJobs::default());
            };
            let mut connection = connection?;
            standard_claim(
                inner,
                PilotDatabaseConnection::Sqlite(&mut connection),
                queue,
                maximum,
            )
            .await?
        }
    };
    Ok(finish_fetch(inner, fetch_started, rows).await)
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
