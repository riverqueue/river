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
    let cancel = term.token;
    let mut started = false;
    for attempt in 1..=START_ATTEMPTS {
        match start(&inner, &cancel).await {
            Ok(()) => {
                started = true;
                break;
            }
            Err(MaintenanceError::Cancelled) => return,
            Err(start_error) => {
                error!(error = %start_error, attempt, "River maintenance start failed");
                if attempt < START_ATTEMPTS
                    && !sleep_cancellable(&cancel, exponential_backoff(attempt, 7)).await
                {
                    return;
                }
            }
        }
    }
    if !started {
        if cancel.is_cancelled() {
            return;
        }
        error!(
            "River maintenance failed to start after all attempts; requesting leader resignation"
        );
        let client = Client {
            inner: Arc::clone(&inner),
        };
        if let Err(resign_error) = client.request_resign().await {
            error!(error = %resign_error, "River could not request leader resignation");
        }
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
            |context| Box::pin(async move { cleaner::clean_notifications(&context).await }),
        ));
    }
    #[cfg(feature = "postgres")]
    if inner.postgres_pool().is_some() {
        services.spawn(super::reindexer::run(Arc::clone(&context)));
    }
    for service in inner.pilot.maintenance_services() {
        let pool = inner.pilot_database_pool();
        let database = inner.pilot_database_config();
        let service_cancel = cancel.child_token();
        services.spawn(async move {
            if let Err(service_error) = service.run(pool, database, service_cancel).await {
                error!(error = %service_error, "River extension maintenance service failed");
            }
        });
    }

    while let Some(result) = services.join_next().await {
        if let Err(join_error) = result {
            error!(error = %join_error, "River maintenance service task failed");
        }
    }
    debug!("River maintenance services stopped");
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
                error!(error = %run_error, service = name, "River maintenance service failed");
            }
        }
    }
}
