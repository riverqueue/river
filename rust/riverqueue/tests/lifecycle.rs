//! Client lifecycle: stopping from other tasks, graceful shutdown signals,
//! cancel safety, and idempotency.
//!
//! Lifecycle behavior doesn't depend on the backend, so these tests use
//! temporary SQLite databases and need no external services.

use std::{
    path::PathBuf,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use riverqueue::{
    Client, Job, JobArgs, JobState, QueueConfig, RunHandle, WorkCancelled, WorkContext,
    WorkOutcome, WorkerRegistry,
};
use riverqueue_migrate::SqliteMigrator;
use serde::{Deserialize, Serialize};
use sqlx::{
    SqlitePool,
    sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions},
};
use tokio::sync::{Semaphore, oneshot};
use tokio_util::sync::CancellationToken;

/// A job that runs until the test releases it or the client cancels it.
#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_lifecycle_gated")]
struct GatedArgs {}

/// How a gated job's worker ended.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Ending {
    Cancelled,
    Released,
}

/// Holds gated jobs inside their workers and records how each ended.
#[derive(Clone)]
struct Gate {
    endings: Arc<Mutex<Vec<(i64, Ending)>>>,
    release: Arc<Semaphore>,
    started: Arc<Semaphore>,
}

impl Gate {
    fn new() -> Self {
        Self {
            endings: Arc::new(Mutex::new(Vec::new())),
            release: Arc::new(Semaphore::new(0)),
            started: Arc::new(Semaphore::new(0)),
        }
    }

    fn ending(&self, id: i64) -> Option<Ending> {
        self.endings
            .lock()
            .unwrap()
            .iter()
            .find_map(|(job_id, ending)| (*job_id == id).then_some(*ending))
    }

    fn release(&self) {
        self.release.add_permits(1);
    }

    async fn wait_started(&self) {
        tokio::time::timeout(Duration::from_secs(10), self.started.acquire())
            .await
            .expect("gated job did not start")
            .unwrap()
            .forget();
    }

    fn workers(&self) -> WorkerRegistry {
        let gate = self.clone();
        let mut workers = WorkerRegistry::new();
        workers
            .register_fn(move |context: WorkContext, job: Job<GatedArgs>| {
                let gate = gate.clone();
                async move {
                    gate.started.add_permits(1);
                    // Cancellation wins when both are ready, so a stop that
                    // cancelled work is always observed.
                    let ending = tokio::select! {
                        biased;
                        () = context.cancellation_token().cancelled() => Ending::Cancelled,
                        permit = gate.release.acquire() => {
                            permit.unwrap().forget();
                            Ending::Released
                        }
                    };
                    gate.endings.lock().unwrap().push((job.id(), ending));
                    match ending {
                        Ending::Cancelled => Err(WorkCancelled),
                        Ending::Released => Ok(WorkOutcome::Complete),
                    }
                }
            })
            .unwrap();
        workers
    }
}

/// A migrated WAL database file that is removed when the test finishes.
struct TestDatabase {
    path: PathBuf,
    pool: SqlitePool,
}

impl TestDatabase {
    async fn new() -> Self {
        static DATABASE_NONCE: AtomicUsize = AtomicUsize::new(0);
        let path = std::env::temp_dir().join(format!(
            "river-lifecycle-{}-{}.sqlite",
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

    fn client(&self, gate: &Gate, max_workers: usize) -> Client {
        self.client_with(gate, max_workers, None)
    }

    fn client_with(
        &self,
        gate: &Gate,
        max_workers: usize,
        soft_stop_timeout: Option<Duration>,
    ) -> Client {
        Client::builder(self.pool.clone())
            .id("rust-lifecycle-test")
            .soft_stop_timeout(soft_stop_timeout)
            .workers(gate.workers())
            .queue(
                "default",
                QueueConfig::new(max_workers)
                    .with_fetch_cooldown(Duration::from_millis(1))
                    .with_fetch_poll_interval(Duration::from_millis(20)),
            )
            .build()
            .unwrap()
    }
}

impl Drop for TestDatabase {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.path);
        for suffix in ["-shm", "-wal"] {
            let mut path = self.path.as_os_str().to_owned();
            path.push(suffix);
            let _ = std::fs::remove_file(path);
        }
    }
}

async fn insert_gated(client: &Client) -> i64 {
    client.insert(GatedArgs {}).await.unwrap().id()
}

/// Waits for the client to stop, failing the test if it doesn't.
async fn wait_stopped(run: &mut RunHandle) {
    tokio::time::timeout(Duration::from_secs(10), run.wait())
        .await
        .expect("client did not stop")
        .unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_a_shutdown_future_keeps_the_soft_stop() {
    let database = TestDatabase::new().await;
    let gate = Gate::new();
    let client = database.client(&gate, 1);
    let running = insert_gated(&client).await;
    let unfetched = insert_gated(&client).await;

    let mut run = client.start().unwrap();
    gate.wait_started().await;
    // Poll `shutdown` once, which requests a soft stop, then drop it while
    // the gated job keeps it pending, as `tokio::time::timeout` would.
    tokio::select! {
        biased;
        result = run.shutdown() => panic!("shutdown finished while a job was held: {result:?}"),
        () = std::future::ready(()) => {}
    }
    gate.release();
    wait_stopped(&mut run).await;

    assert_eq!(gate.ending(running), Some(Ending::Released));
    assert_eq!(
        client.jobs().get(running).await.unwrap().state,
        JobState::Completed
    );
    // The soft stop requested by the dropped future stopped fetching.
    assert_eq!(
        client.jobs().get(unfetched).await.unwrap().state,
        JobState::Available
    );
    // The handle is still usable after the dropped future.
    run.shutdown_now().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn dropping_the_handle_requests_a_hard_stop() {
    let database = TestDatabase::new().await;
    let gate = Gate::new();
    let client = database.client(&gate, 1);
    let running = insert_gated(&client).await;

    let run = client.start().unwrap();
    gate.wait_started().await;
    drop(run);

    let deadline = tokio::time::Instant::now() + Duration::from_secs(10);
    while client.jobs().get(running).await.unwrap().state != JobState::Available {
        assert!(
            tokio::time::Instant::now() < deadline,
            "the job was not interrupted"
        );
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    assert_eq!(gate.ending(running), Some(Ending::Cancelled));
}

#[tokio::test(flavor = "multi_thread")]
async fn graceful_shutdown_signal_accepts_a_cancellation_token() {
    let database = TestDatabase::new().await;
    let client = database.client(&Gate::new(), 1);
    let token = CancellationToken::new();

    let mut run = client
        .start_with_graceful_shutdown(token.clone().cancelled_owned())
        .unwrap();
    run.wait_ready().await.unwrap();
    token.cancel();
    wait_stopped(&mut run).await;
}

#[tokio::test(flavor = "multi_thread")]
async fn graceful_shutdown_signal_stops_softly() {
    let database = TestDatabase::new().await;
    let gate = Gate::new();
    let client = database.client(&gate, 1);
    let running = insert_gated(&client).await;
    let (signal_sender, signal) = oneshot::channel::<()>();

    let mut run = client
        .start_with_graceful_shutdown(async move {
            let _ = signal.await;
        })
        .unwrap();
    gate.wait_started().await;
    signal_sender.send(()).unwrap();
    // The stop is applied asynchronously, so tests that also check that
    // fetching stops use a `Stopper`, which is equivalent and synchronous.
    gate.release();
    wait_stopped(&mut run).await;

    assert_eq!(gate.ending(running), Some(Ending::Released));
    assert_eq!(
        client.jobs().get(running).await.unwrap().state,
        JobState::Completed
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn lifecycle_methods_are_idempotent() {
    let database = TestDatabase::new().await;
    let gate = Gate::new();
    let client = database.client(&gate, 1);

    let mut run = client.start().unwrap();
    run.wait_ready().await.unwrap();
    let stopper = run.stopper();
    stopper.stop();
    stopper.stop();
    run.shutdown().await.unwrap();
    run.shutdown().await.unwrap();
    run.shutdown_now().await.unwrap();
    run.wait().await.unwrap();
    run.wait_ready().await.unwrap();
    stopper.stop_now();
    stopper.stop();

    // A stopper affects only its own run, not a restart of the client.
    let job = insert_gated(&client).await;
    let mut restarted = client.start().unwrap();
    gate.wait_started().await;
    stopper.stop_now();
    gate.release();
    let completed = async {
        while client.jobs().get(job).await.unwrap().state != JobState::Completed {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    };
    tokio::time::timeout(Duration::from_secs(10), completed)
        .await
        .expect("the restarted client did not complete its job");
    assert_eq!(gate.ending(job), Some(Ending::Released));
    restarted.shutdown().await.unwrap();
}

#[tokio::test(flavor = "multi_thread")]
async fn soft_stop_timeout_escalates_a_stop_from_a_stopper() {
    let database = TestDatabase::new().await;
    let gate = Gate::new();
    let client = database.client_with(&gate, 1, Some(Duration::from_millis(50)));
    let running = insert_gated(&client).await;

    let mut run = client.start().unwrap();
    gate.wait_started().await;
    run.stopper().stop();
    wait_stopped(&mut run).await;

    assert_eq!(gate.ending(running), Some(Ending::Cancelled));
    let job = client.jobs().get(running).await.unwrap();
    assert_eq!(job.state, JobState::Available);
    assert_eq!(job.attempt, 0, "an interrupted job keeps its attempt");
    assert!(job.errors.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn stop_from_another_task_while_waiting() {
    let database = TestDatabase::new().await;
    let gate = Gate::new();
    let client = database.client(&gate, 1);
    let running = insert_gated(&client).await;
    let unfetched = insert_gated(&client).await;

    let mut run = client.start().unwrap();
    let stopper = run.stopper();
    let stop_gate = gate.clone();
    let stop_task = tokio::spawn(async move {
        stop_gate.wait_started().await;
        stopper.stop();
        stop_gate.release();
    });
    wait_stopped(&mut run).await;
    stop_task.await.unwrap();

    assert_eq!(gate.ending(running), Some(Ending::Released));
    assert_eq!(
        client.jobs().get(running).await.unwrap().state,
        JobState::Completed
    );
    assert_eq!(
        client.jobs().get(unfetched).await.unwrap().state,
        JobState::Available
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn stop_now_from_another_task_interrupts_running_jobs() {
    let database = TestDatabase::new().await;
    let gate = Gate::new();
    let client = database.client(&gate, 2);
    let interrupted = insert_gated(&client).await;
    let cancel_attempted = insert_gated(&client).await;

    let mut run = client.start().unwrap();
    let stopper = run.stopper();
    let stop_gate = gate.clone();
    let pool = database.pool.clone();
    let stop_task = tokio::spawn(async move {
        stop_gate.wait_started().await;
        stop_gate.wait_started().await;
        // A cancellation whose notification never reached this client, as
        // `job_cancel` records it on a running job.
        sqlx::query(
            "UPDATE river_job SET metadata = jsonb_set(metadata, '$.cancel_attempted_at', \
             '2026-01-02T03:04:05Z') WHERE id = ?",
        )
        .bind(cancel_attempted)
        .execute(&pool)
        .await
        .unwrap();
        stopper.stop_now();
    });
    wait_stopped(&mut run).await;
    stop_task.await.unwrap();

    assert_eq!(gate.ending(interrupted), Some(Ending::Cancelled));
    let interrupted = client.jobs().get(interrupted).await.unwrap();
    assert_eq!(interrupted.state, JobState::Available);
    assert_eq!(
        interrupted.attempt, 0,
        "an interrupted job keeps its attempt"
    );
    assert!(interrupted.errors.is_empty());

    // Like Go, a hard stop finalizes a job whose cancellation was requested
    // instead of making it available again.
    assert_eq!(gate.ending(cancel_attempted), Some(Ending::Cancelled));
    let cancel_attempted = client.jobs().get(cancel_attempted).await.unwrap();
    assert_eq!(cancel_attempted.state, JobState::Cancelled);
    assert!(cancel_attempted.finalized_at.is_some());
}

#[tokio::test(flavor = "multi_thread")]
async fn wait_ready_waits_for_queue_registration() {
    let database = TestDatabase::new().await;
    let client = database.client(&Gate::new(), 1);

    let mut run = client.start().unwrap();
    run.wait_ready().await.unwrap();
    // Like Go's `Client.Start`, readiness means peers can manage the queue.
    let queues: i64 = sqlx::query_scalar("SELECT count(*) FROM river_queue WHERE name = 'default'")
        .fetch_one(&database.pool)
        .await
        .unwrap();
    assert_eq!(queues, 1);
    run.shutdown().await.unwrap();
}
