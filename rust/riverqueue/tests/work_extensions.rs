//! Work middleware and work hooks: ordering, result replacement, unknown
//! kinds, and the job span around worker code.
//!
//! Extension behavior doesn't depend on the backend, so these tests use
//! temporary SQLite databases and need no external services.

use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex, OnceLock,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use riverqueue::{
    Client, EventKind, Hook, InsertOpts, Job, JobArgs, JobRow, JobState, QueueConfig, WorkContext,
    WorkError, WorkMiddleware, WorkNext, WorkOutcome, WorkerRegistry,
};
use riverqueue_migrate::SqliteMigrator;
use serde::{Deserialize, Serialize};
use sqlx::{
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_work_extensions")]
struct ExtensionArgs {
    fail: bool,
}

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_work_extensions_unknown")]
struct UnknownArgs {}

#[derive(Debug, thiserror::Error)]
#[error("worker failed on purpose")]
struct PurposefulFailure;

/// A migrated WAL database file that is removed when the test finishes.
struct TestDatabase {
    path: PathBuf,
    pool: SqlitePool,
}

impl TestDatabase {
    async fn new() -> Self {
        static DATABASE_NONCE: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "river-work-extensions-{}-{}.sqlite",
            std::process::id(),
            DATABASE_NONCE.fetch_add(1, Ordering::Relaxed)
        ));
        let pool = SqlitePoolOptions::new()
            .max_connections(4)
            .connect_with(
                SqliteConnectOptions::new()
                    .filename(&path)
                    .create_if_missing(true)
                    .journal_mode(SqliteJournalMode::Wal)
                    .busy_timeout(Duration::from_secs(5)),
            )
            .await
            .unwrap();
        SqliteMigrator::new(pool.clone())
            .migrate_up()
            .await
            .unwrap();
        Self { path, pool }
    }

    async fn close(self) {
        self.pool.close().await;
        for suffix in ["", "-shm", "-wal"] {
            let mut path = self.path.as_os_str().to_owned();
            path.push(suffix);
            let _ = std::fs::remove_file(path);
        }
    }
}

/// Records the order in which extensions and the worker run.
#[derive(Clone, Default)]
struct Trace(Arc<Mutex<Vec<String>>>);

impl Trace {
    fn push(&self, entry: impl Into<String>) {
        self.0.lock().unwrap().push(entry.into());
    }

    fn entries(&self) -> Vec<String> {
        self.0.lock().unwrap().clone()
    }
}

struct TracingMiddleware(Trace, &'static str);

impl WorkMiddleware for TracingMiddleware {
    async fn work(
        &self,
        _context: &WorkContext,
        job: JobRow,
        next: WorkNext<'_>,
    ) -> Result<WorkOutcome, WorkError> {
        self.0.push(format!("middleware {} before", self.1));
        let result = next.run(job).await;
        self.0.push(format!("middleware {} after", self.1));
        result
    }
}

/// Records work hooks and replaces a failure with a snooze, like a Go
/// `HookWorkEnd` that returns a different error.
struct TracingHook {
    snooze_failures: bool,
    trace: Trace,
}

#[allow(
    clippy::unused_async_trait_impl,
    reason = "the hook only records state synchronously"
)]
impl Hook for TracingHook {
    async fn work_begin(
        &self,
        _context: &WorkContext,
        _job: &mut JobRow,
    ) -> Result<(), riverqueue::BoxError> {
        self.trace.push("hook begin");
        Ok(())
    }

    async fn work_end(
        &self,
        _context: &WorkContext,
        _job: &JobRow,
        result: Result<WorkOutcome, WorkError>,
    ) -> Result<WorkOutcome, WorkError> {
        self.trace.push("hook end");
        match result {
            Err(error)
                if self.snooze_failures
                    && error
                        .source_ref()
                        .downcast_ref::<PurposefulFailure>()
                        .is_some() =>
            {
                Ok(WorkOutcome::Snooze(Duration::from_secs(60)))
            }
            result => result,
        }
    }
}

fn workers(trace: &Trace) -> WorkerRegistry {
    let trace = trace.clone();
    let mut workers = WorkerRegistry::new();
    workers
        .register_fn(move |_context: WorkContext, job: Job<ExtensionArgs>| {
            let trace = trace.clone();
            async move {
                trace.push("worker");
                // The worker runs inside River's span for the job.
                trace.push(format!(
                    "span {}",
                    tracing::Span::current()
                        .metadata()
                        .map_or("none", |metadata| metadata.name())
                ));
                if job.args.fail {
                    return Err(PurposefulFailure);
                }
                Ok(WorkOutcome::Complete)
            }
        })
        .unwrap();
    workers
}

fn client(database: &TestDatabase, trace: &Trace, snooze_failures: bool) -> Client {
    Client::builder(database.pool.clone())
        .queue(
            "default",
            QueueConfig::new(1)
                .with_fetch_cooldown(Duration::from_millis(1))
                .with_fetch_poll_interval(Duration::from_millis(10)),
        )
        .workers(workers(trace))
        .work_middleware(TracingMiddleware(trace.clone(), "outer"))
        .work_middleware(TracingMiddleware(trace.clone(), "inner"))
        .hook(TracingHook {
            snooze_failures,
            trace: trace.clone(),
        })
        .build()
        .unwrap()
}

/// Installs a subscriber that records spans, so `Span::current` identifies
/// River's job span inside workers on any runtime thread.
fn install_span_subscriber() {
    static INSTALLED: OnceLock<()> = OnceLock::new();
    INSTALLED.get_or_init(|| {
        tracing::subscriber::set_global_default(tracing_subscriber::registry())
            .expect("no other global subscriber in this test binary");
    });
}

/// Works `job_id` and returns its row from the first event of `kind`.
async fn work_until(client: &Client, kind: EventKind, job_id: i64) -> JobRow {
    // Subscribe before starting, so the event can't be missed.
    let mut events = client.subscribe(&[kind]).unwrap();
    let mut run = client.start().unwrap();
    let row = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let event = events.recv().await.unwrap();
            if let Some(event) = event.as_job()
                && event.job.id == job_id
            {
                return event.job.clone();
            }
        }
    })
    .await
    .expect("job event");
    run.shutdown().await.unwrap();
    row
}

#[tokio::test(flavor = "multi_thread")]
async fn hooks_run_inside_middleware_around_the_worker_like_go() {
    install_span_subscriber();
    let database = TestDatabase::new().await;
    let trace = Trace::default();
    let client = client(&database, &trace, false);
    let job = client.insert(ExtensionArgs { fail: false }).await.unwrap();
    let completed = work_until(&client, EventKind::JobCompleted, job.id()).await;

    assert_eq!(completed.state, JobState::Completed);
    assert_eq!(
        trace.entries(),
        [
            "middleware outer before",
            "middleware inner before",
            "hook begin",
            "worker",
            "span river_job",
            "hook end",
            "middleware inner after",
            "middleware outer after",
        ]
    );
    database.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn work_end_hooks_replace_the_workers_result() {
    let database = TestDatabase::new().await;
    let trace = Trace::default();
    let client = client(&database, &trace, true);
    let job = client
        .insert(ExtensionArgs { fail: true })
        .opts(InsertOpts::default().with_max_attempts(1))
        .await
        .unwrap();
    let snoozed = work_until(&client, EventKind::JobSnoozed, job.id()).await;

    // The hook turned the final failure into a snooze, which neither
    // records an error nor consumes the attempt.
    assert_eq!(snoozed.state, JobState::Scheduled);
    assert!(snoozed.errors.is_empty());
    database.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn unknown_kinds_fail_before_middleware_and_hooks() {
    let database = TestDatabase::new().await;
    let trace = Trace::default();
    let client = client(&database, &trace, false);
    // Insert with a client that knows the kind, then work it with one that
    // doesn't.
    let job = Client::builder(database.pool.clone())
        .build()
        .unwrap()
        .insert(UnknownArgs {})
        .await
        .unwrap();
    let failed = work_until(&client, EventKind::JobFailed, job.id()).await;

    // A short first retry stays available, as in River Go.
    assert!(
        matches!(failed.state, JobState::Available | JobState::Retryable),
        "{:?}",
        failed.state
    );
    assert_eq!(failed.errors.len(), 1);
    assert!(trace.entries().is_empty(), "{:?}", trace.entries());
    database.close().await;
}
