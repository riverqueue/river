//! The worker client `start` builds: the built-in worker and its behaviors,
//! barriers, what the client observes, and its configuration.

use std::{
    collections::HashMap,
    sync::{
        Arc, Mutex, MutexGuard, PoisonError,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use chrono::{DateTime, Utc};
use riverqueue::{
    __private::{
        ClaimedJob, ClientBuilderExt, Pilot, PilotError, PilotProducer, ProducerClaimContext,
        ProducerClaimNext, ProducerStartContext,
    },
    BoxError, ClientBuilder, ErrorHandler, ErrorHandlerDecision, Event, EventKind, Hook,
    InsertOpts, IntervalSchedule, Job, JobArgs, JobCancelError, JobRow, MaintenanceConfig,
    PeriodicJob, PeriodicJobOpts, PeriodicJobs, QueueConfig, RetryPolicy, UniqueOpts,
    WorkCancelled, WorkContext, WorkError, WorkOutcome, WorkResult, Worker, Workers,
};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tokio_util::sync::CancellationToken;

use crate::protocol::{
    KIND_ECHO, KIND_ECHO_PEER, KIND_ECHO_RENAMED, PERIODIC_JOB_ID, PERIODIC_MARKER_JOB_ID,
    RpcError, StartParams, StatsResult,
};

/// Named barriers that jobs and claims wait on. A barrier exists from its
/// first use, a wait or a release, so releasing one early lets later waits
/// pass.
#[derive(Debug, Default)]
pub struct Barriers(Mutex<HashMap<String, CancellationToken>>);

impl Barriers {
    fn get(&self, name: &str) -> CancellationToken {
        let mut barriers = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        barriers.entry(name.to_owned()).or_default().clone()
    }

    pub fn release(&self, name: &str) {
        self.get(name).cancel();
    }

    pub async fn wait(&self, name: &str) {
        self.get(name).cancelled_owned().await;
    }
}

/// What a running client observed since it started.
#[derive(Debug, Default)]
pub struct Stats(Mutex<StatsResult>);

impl Stats {
    fn lock(&self) -> MutexGuard<'_, StatsResult> {
        self.0.lock().unwrap_or_else(PoisonError::into_inner)
    }

    pub fn record_event(&self, event: &Event) {
        let name = match event.kind() {
            EventKind::JobCancelled => "job_cancelled",
            EventKind::JobCompleted => "job_completed",
            EventKind::JobFailed => "job_failed",
            EventKind::JobSnoozed => "job_snoozed",
            EventKind::QueuePaused => "queue_paused",
            EventKind::QueueResumed => "queue_resumed",
            _ => return,
        };
        self.lock().events.push(name);
    }

    pub fn snapshot(&self) -> StatsResult {
        self.lock().clone()
    }
}

/// The events `stats` reports.
pub const EVENT_KINDS: [EventKind; 6] = [
    EventKind::JobCancelled,
    EventKind::JobCompleted,
    EventKind::JobFailed,
    EventKind::JobSnoozed,
    EventKind::QueuePaused,
    EventKind::QueueResumed,
];

/// The args of every job an adapter inserts. Every key is always present.
#[derive(Clone, Debug, Default, Deserialize, JobArgs, Serialize)]
#[river(kind = "conformance_echo")]
pub struct EchoArgs {
    pub behavior: String,
    pub duration_ms: i64,
    pub message: String,
}

/// The echo args under another kind, as a heterogeneous fleet's clients
/// each register only their own.
#[derive(Debug, Deserialize, Serialize)]
#[serde(transparent)]
struct PeerArgs(EchoArgs);

impl JobArgs for PeerArgs {
    const KIND: &'static str = KIND_ECHO_PEER;
}

/// The echo args after a safe rename, keeping the echo kind as an alias.
#[derive(Debug, Deserialize, Serialize)]
#[serde(transparent)]
struct RenamedArgs(EchoArgs);

impl JobArgs for RenamedArgs {
    const KIND: &'static str = KIND_ECHO_RENAMED;

    fn kind_aliases() -> &'static [&'static str] {
        &[KIND_ECHO]
    }
}

impl From<PeerArgs> for EchoArgs {
    fn from(args: PeerArgs) -> Self {
        args.0
    }
}

impl From<RenamedArgs> for EchoArgs {
    fn from(args: RenamedArgs) -> Self {
        args.0
    }
}

/// The built-in worker, which follows each job's behavior.
#[derive(Clone, Debug)]
struct EchoWorker {
    barriers: Arc<Barriers>,
    stats: Arc<Stats>,
}

impl<A: JobArgs + Into<EchoArgs>> Worker<A> for EchoWorker {
    type Error = BoxError;

    async fn work(&self, context: WorkContext, job: Job<A>) -> Result<WorkOutcome, BoxError> {
        let args: EchoArgs = job.args.into();
        let cancelled = context.cancellation_token().clone();
        match args.behavior.as_str() {
            "" => Ok(WorkOutcome::Complete),
            behavior @ ("barrier_output" | "barrier_wait") => {
                tokio::select! {
                    () = self.barriers.wait(&args.message) => {}
                    () = cancelled.cancelled() => return Err(WorkCancelled.into()),
                }
                if behavior == "barrier_output" {
                    context.record_output(json!({"race": "worker"}))?;
                }
                Ok(WorkOutcome::Complete)
            }
            "cancel" => Err(JobCancelError::new("cancelled by conformance worker").into()),
            "cooperative_cancel" => {
                if cancelled.is_cancelled() {
                    self.stats.lock().cancelled_at_start += 1;
                }
                cancelled.cancelled().await;
                Err(WorkCancelled.into())
            }
            "error" => Err("conformance retryable error".into()),
            "output" => {
                context.record_output(json!({"message": args.message}))?;
                Ok(WorkOutcome::Complete)
            }
            "resumable_cursor" => {
                resumable_cursor(&context, job.row.attempt).await?;
                Ok(WorkOutcome::Complete)
            }
            "sleep" => {
                let duration = Duration::from_millis(args.duration_ms.try_into().unwrap_or(0));
                tokio::select! {
                    () = tokio::time::sleep(duration) => Ok(WorkOutcome::Complete),
                    () = cancelled.cancelled() => Err(WorkCancelled.into()),
                }
            }
            "snooze_once" if job.row.metadata.contains_key("snoozes") => Ok(WorkOutcome::Complete),
            "snooze_once" => Ok(WorkOutcome::Snooze(Duration::from_millis(
                args.duration_ms.try_into().unwrap_or(0).max(1),
            ))),
            behavior => Err(format!("unknown behavior {behavior:?}").into()),
        }
    }
}

/// Three resumable steps: "first" records its attempt, "second" sets cursor
/// 7 and fails on attempt 1 and requires it afterwards, and "third" fails on
/// attempt 2.
async fn resumable_cursor(context: &WorkContext, attempt: i32) -> Result<(), BoxError> {
    context
        .resumable_step("first", || async {
            context.metadata_set("first_attempt", attempt)
        })
        .await?;
    context
        .resumable_step_with_cursor("second", |cursor: i64| async move {
            if attempt == 1 {
                context.resumable_set_cursor(&7)?;
                return Err(BoxError::from("retry with cursor"));
            }
            if cursor != 7 {
                return Err(format!("expected cursor 7, got {cursor}").into());
            }
            Ok(context.metadata_set("cursor_observed", cursor)?)
        })
        .await?;
    context
        .resumable_step("third", || async {
            if attempt == 2 {
                Err("retry after consuming cursor")
            } else {
                Ok(())
            }
        })
        .await?;
    Ok(())
}

/// Cancels every job whose attempt fails, counting its calls.
struct CancellingErrorHandler(Arc<Stats>);

impl ErrorHandler for CancellingErrorHandler {
    fn handle_error(
        &self,
        _context: &WorkContext,
        _job: &JobRow,
        _result: &WorkResult,
    ) -> impl Future<Output = Result<ErrorHandlerDecision, BoxError>> + Send {
        self.0.lock().error_handler_calls += 1;
        std::future::ready(Ok(ErrorHandlerDecision::Cancel))
    }
}

/// Retries every failed attempt after the same delay.
struct FixedRetryPolicy(Duration);

impl RetryPolicy for FixedRetryPolicy {
    fn next_retry(&self, _job: &JobRow, _error: &WorkError, _now: DateTime<Utc>) -> Duration {
        self.0
    }
}

/// Counts starts of the periodic job enqueuer.
struct PeriodicStartHook(Arc<Stats>);

impl Hook for PeriodicStartHook {
    fn periodic_jobs_start(
        &self,
        _jobs: &PeriodicJobs,
    ) -> impl Future<Output = Result<(), BoxError>> + Send {
        self.0.lock().periodic_starts += 1;
        std::future::ready(Ok(()))
    }
}

/// Holds the jobs of the client's first claim that returns any until the
/// named barrier is released. The claim has committed, so the jobs are
/// running without an executor while the client keeps handling
/// notifications, such as a cancellation.
#[derive(Clone, Debug)]
struct ClaimBarrier {
    barriers: Arc<Barriers>,
    name: String,
    waited: Arc<AtomicBool>,
}

#[async_trait]
impl Pilot for ClaimBarrier {
    async fn start_producer(
        &self,
        _context: ProducerStartContext,
    ) -> Result<Option<Box<dyn PilotProducer>>, PilotError> {
        Ok(Some(Box::new(self.clone())))
    }
}

#[async_trait]
impl PilotProducer for ClaimBarrier {
    fn intercepts_claim(&self) -> bool {
        true
    }

    async fn claim(
        &self,
        context: ProducerClaimContext<'_>,
        next: ProducerClaimNext<'_>,
    ) -> Result<Vec<ClaimedJob>, PilotError> {
        let mut transaction = context.database.begin().await?;
        let jobs = next.claim(transaction.connection()).await?;
        transaction.commit().await?;
        if !jobs.is_empty() && !self.waited.swap(true, Ordering::SeqCst) {
            // The jobs are claimed either way, so they're returned however
            // the wait ends. Stopping the client releases the barrier.
            tokio::select! {
                () = self.barriers.wait(&self.name) => {}
                () = context.claim_stop.cancelled() => {}
            }
        }
        Ok(jobs)
    }
}

/// Configures `builder` as `start`'s params ask.
pub fn configure(
    mut builder: ClientBuilder,
    params: &StartParams,
    barriers: &Arc<Barriers>,
    stats: &Arc<Stats>,
) -> Result<ClientBuilder, RpcError> {
    let worker = EchoWorker {
        barriers: Arc::clone(barriers),
        stats: Arc::clone(stats),
    };
    let mut workers = Workers::new();
    let kinds = if params.worker_kinds.is_empty() {
        &[KIND_ECHO.to_owned()][..]
    } else {
        &params.worker_kinds
    };
    for kind in kinds {
        match kind.as_str() {
            KIND_ECHO => workers.add::<EchoArgs, _>(worker.clone()),
            KIND_ECHO_PEER => workers.add::<PeerArgs, _>(worker.clone()),
            KIND_ECHO_RENAMED => workers.add::<RenamedArgs, _>(worker.clone()),
            _ => {
                return Err(RpcError::invalid_params(format!(
                    "unknown worker kind {kind:?}"
                )));
            }
        }?;
    }

    let mut queue = QueueConfig::new(if params.max_workers == 0 {
        4
    } else {
        params.max_workers
    });
    if params.fetch_poll_interval_ms > 0 {
        queue = queue.with_fetch_poll_interval(millis(params.fetch_poll_interval_ms));
    }
    if params.queues.is_empty() {
        builder = builder.queue(riverqueue::QUEUE_DEFAULT, queue);
    } else {
        for name in &params.queues {
            builder = builder.queue(name, queue.clone());
        }
    }

    let mut maintenance = MaintenanceConfig::default();
    if params.rescue_after_ms > 0 {
        maintenance = maintenance.with_rescue_after(millis(params.rescue_after_ms));
    }
    if let Some(tuning) = &params.tuning {
        if tuning.elect_interval_ms > 0 {
            maintenance = maintenance.with_elect_interval(millis(tuning.elect_interval_ms));
        }
        if tuning.rescuer_interval_ms > 0 {
            maintenance = maintenance.with_rescuer_interval(millis(tuning.rescuer_interval_ms));
        }
        if tuning.scheduler_interval_ms > 0 {
            maintenance = maintenance.with_scheduler_interval(millis(tuning.scheduler_interval_ms));
        }
    }

    builder = builder
        .workers(workers)
        .fetch_cooldown(Duration::from_millis(1))
        .fetch_only_known_kinds(params.fetch_only_known_kinds)
        .hook(PeriodicStartHook(Arc::clone(stats)))
        .maintenance(maintenance);
    if !params.client_id.is_empty() {
        builder = builder.id(&params.client_id);
    }
    match params.job_timeout_ms {
        0 => {}
        timeout if timeout < 0 => builder = builder.without_job_timeout(),
        timeout => builder = builder.job_timeout(millis(timeout.unsigned_abs())),
    }
    if params.leader_election_disabled {
        builder = builder.without_leader_election();
    }
    if params.poll_only {
        builder = builder.without_notifications();
    }
    if params.error_handler_cancel {
        builder = builder.error_handler(CancellingErrorHandler(Arc::clone(stats)));
    }
    if params.retry_delay_ms > 0 {
        builder = builder.retry_policy(FixedRetryPolicy(millis(params.retry_delay_ms)));
    }
    for job in periodic_jobs(params)? {
        builder = builder.periodic_job(job);
    }
    if !params.claim_barrier.is_empty() {
        builder = builder.pilot(ClaimBarrier {
            barriers: Arc::clone(barriers),
            name: params.claim_barrier.clone(),
            waited: Arc::default(),
        });
    }
    Ok(builder)
}

/// The periodic jobs `periodic_run_on_start` and `periodic_unique` configure.
fn periodic_jobs(params: &StartParams) -> Result<Vec<PeriodicJob>, RpcError> {
    if !params.periodic_run_on_start {
        if params.periodic_unique {
            return Err(RpcError::invalid_params(
                "periodic_unique requires periodic_run_on_start",
            ));
        }
        return Ok(Vec::new());
    }
    let job = |id: &str, message: &'static str, unique: Option<UniqueOpts>| {
        let metadata = serde_json::Map::from_iter([("periodic".to_owned(), true.into())]);
        let mut opts = InsertOpts::default().with_metadata(metadata);
        if let Some(unique) = unique {
            opts = opts.with_unique(unique);
        }
        let args = EchoArgs {
            message: message.to_owned(),
            ..EchoArgs::default()
        };
        Ok::<_, RpcError>(PeriodicJob::conditional_with_options(
            IntervalSchedule::new(Duration::from_hours(1))?,
            move || Some((args.clone(), opts.clone())),
            PeriodicJobOpts::new().with_id(id).with_run_on_start(true),
        ))
    };
    if !params.periodic_unique {
        return Ok(vec![job(PERIODIC_JOB_ID, "periodic run on start", None)?]);
    }
    let unique = UniqueOpts::new().with_by_args(true).with_by_queue(true);
    Ok(vec![
        job(PERIODIC_JOB_ID, "periodic run on start", Some(unique))?,
        // Configured after the unique job, so its insertion shows the unique
        // job's insertion was attempted.
        job(PERIODIC_MARKER_JOB_ID, "periodic marker", None)?,
    ])
}

const fn millis(value: u64) -> Duration {
    Duration::from_millis(value)
}
