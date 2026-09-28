//! Starts and stops leader-owned services on leadership transitions, a port
//! of Go's `QueueMaintainerLeader` and `QueueMaintainer`.

use std::{sync::Arc, time::Duration};

use tokio::{sync::mpsc, task::JoinSet};
use tokio_util::sync::CancellationToken;
use tracing::{debug, error};

use crate::{Client, client::ClientInner};

use super::{
    Breakers, MaintenanceError, STAGGER_MAX, cleaner, elector::Term, exponential_backoff,
    periodic_enqueuer, random_duration, rescuer, scheduler, sleep_cancellable,
};

/// Attempts to start the maintainer before requesting resignation (Go
/// `queueMaintainerMaxStartAttempts`).
const START_ATTEMPTS: u32 = 3;

/// Shared inputs of every service in one term.
pub(super) struct ServiceContext {
    pub(super) breakers: Arc<Breakers>,
    pub(super) cancel: CancellationToken,
    pub(super) inner: Arc<ClientInner>,
}

impl ServiceContext {
    pub(super) fn client(&self) -> Client {
        Client {
            inner: Arc::clone(&self.inner),
        }
    }
}

pub(super) struct Maintainer {
    breakers: Arc<Breakers>,
    inner: Arc<ClientInner>,
}

impl Maintainer {
    pub(super) fn new(inner: Arc<ClientInner>) -> Self {
        Self {
            breakers: Arc::new(Breakers::new(inner.maintenance.batch_sizes)),
            inner,
        }
    }

    /// Runs one term at a time. A new term starts only after every service of
    /// the previous term has returned, so services never overlap across terms.
    pub(super) async fn run(
        self,
        cancel: CancellationToken,
        mut terms: mpsc::UnboundedReceiver<Term>,
    ) {
        let mut previous: Option<tokio::task::JoinHandle<()>> = None;
        loop {
            let term = tokio::select! {
                biased;
                () = cancel.cancelled() => break,
                term = terms.recv() => match term {
                    Some(term) => term,
                    None => break,
                },
            };
            if let Some(handle) = previous.take() {
                join_term(handle).await;
            }
            if term.token.is_cancelled() {
                continue;
            }
            previous = Some(tokio::spawn(run_term(
                Arc::clone(&self.inner),
                Arc::clone(&self.breakers),
                term,
            )));
        }
        if let Some(handle) = previous {
            join_term(handle).await;
        }
    }
}

async fn join_term(handle: tokio::task::JoinHandle<()>) {
    if let Err(join_error) = handle.await {
        error!(error = %join_error, "River maintenance term task failed");
    }
}

/// Starts the term's services, retrying start failures and requesting
/// resignation once retries are exhausted, then waits for every service.
async fn run_term(inner: Arc<ClientInner>, breakers: Arc<Breakers>, term: Term) {
    let Term {
        elected_at,
        token: cancel,
    } = term;
    if !start_or_resign(&inner, &cancel).await {
        return;
    }

    let context = Arc::new(ServiceContext {
        breakers,
        cancel: cancel.clone(),
        inner: Arc::clone(&inner),
    });
    let mut services = JoinSet::new();
    services.spawn(periodic_enqueuer::run(Arc::clone(&context)));
    services.spawn(run_periodically(
        Arc::clone(&context),
        "job scheduler",
        inner.maintenance.scheduler_interval,
        |context| Box::pin(async move { scheduler::run_once(&context).await }),
    ));
    services.spawn(run_periodically(
        Arc::clone(&context),
        "job rescuer",
        inner.maintenance.rescuer_interval,
        |context| Box::pin(async move { rescuer::run_once(&context).await }),
    ));
    services.spawn(run_periodically(
        Arc::clone(&context),
        "job cleaner",
        inner.maintenance.job_cleaner_interval,
        |context| Box::pin(async move { cleaner::clean_jobs(&context).await }),
    ));
    services.spawn(run_periodically(
        Arc::clone(&context),
        "queue cleaner",
        inner.maintenance.queue_cleaner_interval,
        |context| Box::pin(async move { cleaner::clean_queues(&context).await }),
    ));
    #[cfg(feature = "sqlite")]
    if inner.sqlite_pool().is_some() {
        services.spawn(run_periodically(
            Arc::clone(&context),
            "SQLite notification cleaner",
            cleaner::NOTIFICATION_CLEANER_INTERVAL,
            |context| {
                Box::pin(async move { cleaner::clean_notifications(&context).await.map(|_| ()) })
            },
        ));
    }
    #[cfg(feature = "postgres")]
    if inner.postgres_pool().is_some() {
        services.spawn(super::reindexer::run(Arc::clone(&context)));
    }
    let term = crate::__private::LeaderTerm {
        elected_at,
        token: cancel.clone(),
    };
    for service in inner.pilot.maintenance_services() {
        services.spawn(supervise_extension_service(
            service,
            inner.pilot_database(),
            term.clone(),
        ));
    }

    while let Some(result) = services.join_next().await {
        if let Err(join_error) = result {
            error!(error = %join_error, "River maintenance service task failed");
        }
    }
    debug!("River maintenance services stopped");
}

/// Starts the term's maintenance, retrying start failures and requesting
/// resignation once retries are exhausted. Returns whether the term's
/// services should run.
async fn start_or_resign(inner: &Arc<ClientInner>, cancel: &CancellationToken) -> bool {
    for attempt in 1..=START_ATTEMPTS {
        match start(inner, cancel).await {
            Ok(()) => return true,
            Err(MaintenanceError::Cancelled) => return false,
            Err(start_error) => {
                error!(error = %crate::error::Chain(&start_error), attempt, "River maintenance start failed");
                if attempt < START_ATTEMPTS
                    && !sleep_cancellable(cancel, exponential_backoff(attempt, 7)).await
                {
                    return false;
                }
            }
        }
    }
    if cancel.is_cancelled() {
        return false;
    }
    error!("River maintenance failed to start after all attempts; requesting leader resignation");
    let client = Client {
        inner: Arc::clone(inner),
    };
    if let Err(resign_error) = client.request_resign().await {
        error!(error = %crate::error::Chain(&resign_error), "River could not request leader resignation");
    }
    false
}

/// Runs an extension's maintenance service for the whole term, restarting it
/// after River's service backoff when it fails, panics, or returns before the
/// term ends. Each run settles before the next starts, and the backoff starts
/// over after a long healthy run.
async fn supervise_extension_service(
    service: Arc<dyn crate::__private::MaintenanceService>,
    database: crate::__private::PilotDatabase,
    term: crate::__private::LeaderTerm,
) {
    let mut attempt = 0;
    loop {
        let started_at = tokio::time::Instant::now();
        // A task of its own, so a panic ends only this run.
        let mut run = JoinSet::new();
        let context = crate::__private::MaintenanceServiceContext {
            database: database.clone(),
            term: term.clone(),
        };
        let task_service = Arc::clone(&service);
        run.spawn(async move { task_service.run(context).await });
        let outcome = run.join_next().await;
        if term.token.is_cancelled() {
            return;
        }
        if started_at.elapsed() >= crate::client::SERVICE_RESTART_RESET_AFTER {
            attempt = 0;
        }
        attempt += 1;
        let delay = exponential_backoff(attempt, 7);
        let failure = match outcome {
            Some(Ok(Ok(()))) | None => "returned before its leadership term ended".to_owned(),
            Some(Ok(Err(service_error))) => crate::error::Chain(&*service_error).to_string(),
            Some(Err(join_error)) => join_error.to_string(),
        };
        error!(
            service = service.name(),
            attempt,
            error = %failure,
            sleep_duration = ?delay,
            "River extension maintenance service failed; restarting after backoff"
        );
        if !sleep_cancellable(&term.token, delay).await {
            return;
        }
    }
}

/// Mirrors the only fallible part of Go's `QueueMaintainer.Start`: the
/// periodic job enqueuer runs start hooks on every leadership gain.
async fn start(inner: &ClientInner, cancel: &CancellationToken) -> Result<(), MaintenanceError> {
    for hook in &inner.hooks {
        tokio::select! {
            biased;
            () = cancel.cancelled() => return Err(MaintenanceError::Cancelled),
            result = hook.periodic_jobs_start(&inner.periodic_jobs) => result?,
        }
    }
    Ok(())
}

type RunOnce = fn(
    Arc<ServiceContext>,
) -> std::pin::Pin<Box<dyn Future<Output = Result<(), MaintenanceError>> + Send>>;

/// Runs a service on an interval with an initial staggered tick, like Go's
/// `StaggerStart` plus `NewTickerWithInitialTick`. The stagger is capped by
/// the interval so short test intervals stay responsive.
async fn run_periodically(
    context: Arc<ServiceContext>,
    name: &'static str,
    interval: Duration,
    run_once: RunOnce,
) {
    let stagger = random_duration(Duration::ZERO, STAGGER_MAX.min(interval));
    if !sleep_cancellable(&context.cancel, stagger).await {
        return;
    }
    let mut ticker = tokio::time::interval(interval);
    ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
    loop {
        tokio::select! {
            biased;
            () = context.cancel.cancelled() => return,
            _ = ticker.tick() => {}
        }
        match run_once(Arc::clone(&context)).await {
            Ok(()) => {}
            Err(MaintenanceError::Cancelled) => return,
            Err(run_error) => {
                if context.cancel.is_cancelled() {
                    return;
                }
                error!(error = %crate::error::Chain(&run_error), service = name, "River maintenance service failed");
            }
        }
    }
}
