use std::{
    convert::Infallible,
    sync::{
        Arc, Mutex,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

use async_trait::async_trait;
use riverqueue::{
    BoxError, Client, ErrorHandler, ErrorHandlerDecision, EventKind, ExtensionClaimParams,
    ExtensionInsertParams, Hook, InsertBatch, InsertOpts, Job, JobArgs, JobRow, JobState,
    MaintenanceConfig, QueueConfig, UniqueOpts, WorkContext, WorkOutcome, WorkResult,
    WorkerRegistry, database::DatabaseKind,
};
use riverqueue_internal::{
    CompletionAction, CompletionParams, DatabaseConnection, FetchParams, JobInsertParams, Pilot,
    PilotError, RescueParams,
};
use riverqueue_internal::{DatabaseConfig, DatabasePool, MaintenanceService};
use riverqueue_migrate::SqliteMigrator;
use serde::{Deserialize, Serialize};
use sqlx::sqlite::{SqliteConnectOptions, SqliteJournalMode, SqlitePoolOptions};
use tokio::sync::Semaphore;

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_sqlite_runtime")]
struct RuntimeArgs {
    value: i64,
}

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_sqlite_cancel")]
struct CancelArgs {}

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_sqlite_cancel_ignored")]
struct CancelIgnoredArgs {}

#[derive(Clone, Debug, Deserialize, JobArgs, Serialize)]
#[river(kind = "rust_sqlite_unknown")]
struct UnknownArgs {}

#[derive(Debug, thiserror::Error)]
#[error("extension execution failed")]
struct ExtensionExecutionError;

#[derive(Default)]
struct CompletionObserver {
    error_job_ids: Mutex<Vec<i64>>,
    work_end_count: AtomicUsize,
}

#[derive(Clone)]
struct CompletionErrorHandler(Arc<CompletionObserver>);

#[derive(Clone)]
struct CompletionHook(Arc<CompletionObserver>);

struct WrapperTransformHook(&'static str);

#[async_trait]
impl ErrorHandler for CompletionErrorHandler {
    async fn handle_error(
        &self,
        _context: &WorkContext,
        job: &JobRow,
        result: &WorkResult,
    ) -> Result<ErrorHandlerDecision, riverqueue::Error> {
        let WorkResult::Failed(error) = result else {
            panic!("expected a failed extension result")
        };
        assert!(
            error
                .source_ref()
                .downcast_ref::<ExtensionExecutionError>()
                .is_some()
        );
        self.0.error_job_ids.lock().unwrap().push(job.id);
        Ok(ErrorHandlerDecision::Cancel)
    }
}

#[async_trait]
impl Hook for CompletionHook {
    async fn work_end(
        &self,
        _context: &WorkContext,
        _job: &JobRow,
        _result: &WorkResult,
    ) -> Result<(), riverqueue::Error> {
        self.0.work_end_count.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

#[async_trait]
impl Hook for WrapperTransformHook {
    async fn decode_insert_result(&self, job: &mut JobRow) -> Result<(), riverqueue::Error> {
        let wrapped = job
            .encoded_args
            .as_object_mut()
            .and_then(|object| object.remove(self.0))
            .ok_or_else(|| {
                riverqueue::Error::runtime(format!("missing outer insertion wrapper {:?}", self.0))
            })?;
        job.encoded_args = wrapped;
        Ok(())
    }

    async fn insert_begin(
        &self,
        insert: &mut riverqueue::InsertContext,
    ) -> Result<(), riverqueue::Error> {
        let inner = std::mem::replace(&mut insert.encoded_args, serde_json::Value::Null);
        insert.encoded_args =
            serde_json::Value::Object(serde_json::Map::from_iter([(self.0.to_owned(), inner)]));
        Ok(())
    }
}

#[derive(Clone, Copy)]
enum CompletionBehavior {
    Continue,
    Fail,
    FailFirst,
    Handled,
}

#[derive(Clone, Copy)]
enum SelectionBehavior {
    Fail,
    Success,
}

#[derive(Clone)]
struct SqlitePilot {
    completion: Option<CompletionBehavior>,
    completion_calls: Arc<AtomicUsize>,
    fetch: Option<SelectionBehavior>,
    fetch_calls: Arc<AtomicUsize>,
    insert: Option<SelectionBehavior>,
    insert_calls: Arc<AtomicUsize>,
    maintenance_service: Option<Arc<LeadershipServiceState>>,
    rescue: Option<SelectionBehavior>,
    rescue_calls: Arc<AtomicUsize>,
}

impl SqlitePilot {
    fn new() -> Self {
        Self {
            completion: None,
            completion_calls: Arc::new(AtomicUsize::new(0)),
            fetch: None,
            fetch_calls: Arc::new(AtomicUsize::new(0)),
            insert: None,
            insert_calls: Arc::new(AtomicUsize::new(0)),
            maintenance_service: None,
            rescue: None,
            rescue_calls: Arc::new(AtomicUsize::new(0)),
        }
    }
}

#[async_trait]
impl Pilot for SqlitePilot {
    fn intercepts_completion(&self) -> bool {
        self.completion.is_some()
    }

    fn intercepts_fetch(&self) -> bool {
        self.fetch.is_some()
    }

    fn intercepts_insert(&self) -> bool {
        self.insert.is_some()
    }

    fn intercepts_rescue(&self) -> bool {
        self.rescue.is_some()
    }

    fn maintenance_services(&self) -> Vec<Arc<dyn MaintenanceService>> {
        self.maintenance_service
            .as_ref()
            .map(|state| {
                vec![Arc::new(LeadershipService(Arc::clone(state))) as Arc<dyn MaintenanceService>]
            })
            .unwrap_or_default()
    }

    async fn before_job_completion(
        &self,
        connection: DatabaseConnection<'_>,
        params: &CompletionParams,
    ) -> Result<CompletionAction, PilotError> {
        self.completion_calls.fetch_add(1, Ordering::SeqCst);
        let connection = connection
            .into_sqlite()
            .ok_or_else(|| std::io::Error::other("expected SQLite completion connection"))?;
        sqlx::query("INSERT INTO pilot_effect (operation, job_id) VALUES ('completion', ?)")
            .bind(params.job_id)
            .execute(&mut *connection)
            .await?;
        match self.completion.expect("completion interception is enabled") {
            CompletionBehavior::Fail => {
                Err(std::io::Error::other("completion interception failed").into())
            }
            CompletionBehavior::FailFirst if self.completion_calls.load(Ordering::SeqCst) == 1 => {
                Err(std::io::Error::other("first completion interception failed").into())
            }
            CompletionBehavior::Continue | CompletionBehavior::FailFirst => {
                Ok(CompletionAction::Continue)
            }
            CompletionBehavior::Handled => {
                sqlx::query(
                    "UPDATE river_job SET state = 'completed', \
                     finalized_at = strftime('%Y-%m-%d %H:%M:%f', 'now'), \
                     metadata = jsonb_set(metadata, '$.pilot_handled', jsonb('true')) \
                     WHERE id = ?",
                )
                .bind(params.job_id)
                .execute(&mut *connection)
                .await?;
                Ok(CompletionAction::Handled)
            }
        }
    }

    async fn before_job_insert(
        &self,
        connection: DatabaseConnection<'_>,
        params: &mut JobInsertParams<'_>,
    ) -> Result<(), PilotError> {
        self.insert_calls.fetch_add(1, Ordering::SeqCst);
        let connection = connection
            .into_sqlite()
            .ok_or_else(|| std::io::Error::other("expected SQLite insertion connection"))?;
        let marker: String =
            sqlx::query_scalar("SELECT marker FROM pilot_insert_config WHERE queue = ?")
                .bind(&*params.queue)
                .fetch_one(&mut *connection)
                .await?;
        params
            .metadata
            .insert("pilot_insert".to_owned(), serde_json::json!(marker));
        sqlx::query("INSERT INTO pilot_effect (operation, job_id) VALUES ('insert', 0)")
            .execute(&mut *connection)
            .await?;
        if matches!(self.insert, Some(SelectionBehavior::Fail)) {
            return Err(std::io::Error::other("insert interception failed").into());
        }
        Ok(())
    }

    async fn select_job_ids(
        &self,
        connection: DatabaseConnection<'_>,
        params: &FetchParams,
    ) -> Result<Option<Vec<i64>>, PilotError> {
        self.fetch_calls.fetch_add(1, Ordering::SeqCst);
        let connection = connection
            .into_sqlite()
            .ok_or_else(|| std::io::Error::other("expected SQLite fetch connection"))?;
        sqlx::query("INSERT INTO pilot_effect (operation, job_id) VALUES ('fetch', 0)")
            .execute(&mut *connection)
            .await?;
        if matches!(self.fetch, Some(SelectionBehavior::Fail)) {
            return Err(std::io::Error::other("fetch interception failed").into());
        }
        let ids = sqlx::query_scalar(
            "SELECT id FROM river_job WHERE state = 'available' AND queue = ? \
             AND kind IN (SELECT value FROM json_each(?)) \
             ORDER BY priority, scheduled_at, id LIMIT ?",
        )
        .bind(&params.queue)
        .bind(serde_json::to_string(&params.kinds)?)
        .bind(params.maximum)
        .fetch_all(&mut *connection)
        .await?;
        Ok(Some(ids))
    }

    async fn select_rescue_job_ids(
        &self,
        connection: DatabaseConnection<'_>,
        params: &RescueParams,
    ) -> Result<Option<Vec<i64>>, PilotError> {
        self.rescue_calls.fetch_add(1, Ordering::SeqCst);
        let connection = connection
            .into_sqlite()
            .ok_or_else(|| std::io::Error::other("expected SQLite rescue connection"))?;
        let ids = sqlx::query_scalar(
            "SELECT id FROM river_job WHERE state = 'running' ORDER BY id LIMIT ?",
        )
        .bind(params.maximum)
        .fetch_all(&mut *connection)
        .await?;
        for id in &ids {
            sqlx::query("INSERT INTO pilot_effect (operation, job_id) VALUES ('rescue', ?)")
                .bind(id)
                .execute(&mut *connection)
                .await?;
        }
        if matches!(self.rescue, Some(SelectionBehavior::Fail)) {
            return Err(std::io::Error::other("rescue interception failed").into());
        }
        Ok(Some(ids))
    }
}

#[derive(Default)]
struct LeadershipServiceState {
    starts: AtomicUsize,
    stops: AtomicUsize,
}

struct LeadershipService(Arc<LeadershipServiceState>);

#[async_trait]
impl MaintenanceService for LeadershipService {
    async fn run(
        &self,
        _pool: DatabasePool,
        _database: DatabaseConfig,
        cancellation: tokio_util::sync::CancellationToken,
    ) -> Result<(), PilotError> {
        self.0.starts.fetch_add(1, Ordering::SeqCst);
        cancellation.cancelled().await;
        self.0.stops.fetch_add(1, Ordering::SeqCst);
        Ok(())
    }
}

async fn setup() -> sqlx::SqlitePool {
    let options = SqliteConnectOptions::new()
        .filename(":memory:")
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal);
    let pool = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    SqliteMigrator::new(pool.clone())
        .migrate_up()
        .await
        .unwrap();
    sqlx::query("CREATE TABLE pilot_effect (operation TEXT NOT NULL, job_id INTEGER NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("CREATE TABLE pilot_insert_config (queue TEXT PRIMARY KEY, marker TEXT NOT NULL)")
        .execute(&pool)
        .await
        .unwrap();
    sqlx::query("INSERT INTO pilot_insert_config (queue, marker) VALUES ('default', 'default')")
        .execute(&pool)
        .await
        .unwrap();
    pool
}

async fn setup_file_pool(busy_timeout: Duration) -> (sqlx::SqlitePool, std::path::PathBuf) {
    static DATABASE_NONCE: AtomicUsize = AtomicUsize::new(0);
    let database_path = std::env::temp_dir().join(format!(
        "river-sqlite-runtime-{}-{}.sqlite",
        std::process::id(),
        DATABASE_NONCE.fetch_add(1, Ordering::Relaxed)
    ));
    let options = SqliteConnectOptions::new()
        .filename(&database_path)
        .create_if_missing(true)
        .journal_mode(SqliteJournalMode::Wal)
        .busy_timeout(busy_timeout);
    let pool = SqlitePoolOptions::new()
        .max_connections(4)
        .connect_with(options)
        .await
        .unwrap();
    SqliteMigrator::new(pool.clone())
        .migrate_up()
        .await
        .unwrap();
    (pool, database_path)
}

fn remove_sqlite_files(database_path: &std::path::Path) {
    let _ = std::fs::remove_file(database_path);
    for suffix in ["-shm", "-wal"] {
        let mut path = database_path.as_os_str().to_owned();
        path.push(suffix);
        let _ = std::fs::remove_file(path);
    }
}

fn runtime_workers(worked: Arc<Semaphore>) -> WorkerRegistry {
    let mut workers = WorkerRegistry::new();
    workers
        .register_fn(move |_context: WorkContext, _job: Job<RuntimeArgs>| {
            let worked = Arc::clone(&worked);
            async move {
                worked.add_permits(1);
                Ok::<_, Infallible>(WorkOutcome::Complete)
            }
        })
        .unwrap();
    workers
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one end-to-end scenario covers every peer outcome and race"
)]
async fn extension_claimed_outcomes_use_canonical_completion_pipeline() {
    let pool = setup().await;
    let observer = Arc::new(CompletionObserver::default());
    let client = Client::builder(pool.clone())
        .id("sqlite-extension-completions")
        .error_handler(CompletionErrorHandler(Arc::clone(&observer)))
        .hook(CompletionHook(Arc::clone(&observer)))
        .workers(runtime_workers(Arc::new(Semaphore::new(0))))
        .queue(
            "default",
            QueueConfig::new(1)
                .with_fetch_cooldown(Duration::from_millis(1))
                .with_fetch_poll_interval(Duration::from_millis(10)),
        )
        .build()
        .unwrap();
    let mut events = client
        .subscribe(&[
            EventKind::JobCancelled,
            EventKind::JobCompleted,
            EventKind::JobFailed,
            EventKind::JobSnoozed,
        ])
        .unwrap();
    let mut run = client.start().unwrap();
    run.wait_ready().await.unwrap();

    let scheduled_at = chrono::Utc::now() + chrono::Duration::hours(1);
    let mut rows = Vec::new();
    for value in 1..=4 {
        let inserted = client
            .insert_with(
                RuntimeArgs { value },
                InsertOpts::default().with_scheduled_at(scheduled_at),
            )
            .await
            .unwrap();
        sqlx::query(
            "UPDATE river_job SET state = 'running', attempt = 1, \
             attempted_at = datetime('now', 'subsec') WHERE id = ?",
        )
        .bind(inserted.job.row.id)
        .execute(&pool)
        .await
        .unwrap();
        rows.push(client.job_get(inserted.job.row.id).await.unwrap());
    }
    sqlx::query(
        "UPDATE river_job SET state = 'discarded', finalized_at = '2026-01-02 03:05:00.000' \
         WHERE id = ?",
    )
    .bind(rows[2].id)
    .execute(&pool)
    .await
    .unwrap();
    sqlx::query(
        "UPDATE river_job SET metadata = jsonb_set(metadata, '$.cancel_attempted_at', \
         jsonb('\"2026-01-02T03:05:00Z\"')) WHERE id = ?",
    )
    .bind(rows[3].id)
    .execute(&pool)
    .await
    .unwrap();

    let execution_context = WorkContext::new(tokio_util::sync::CancellationToken::new());
    execution_context
        .metadata_set("shared_completion", serde_json::json!(true))
        .await;
    let failed_job_id = rows[1].id;
    client
        .extension_persist_claimed_outcomes(
            &execution_context,
            vec![
                (rows[0].clone(), Ok(WorkOutcome::Complete)),
                (
                    rows[1].clone(),
                    Err(Box::new(ExtensionExecutionError) as BoxError),
                ),
                (rows[2].clone(), Ok(WorkOutcome::Complete)),
                (
                    rows[3].clone(),
                    Ok(WorkOutcome::Snooze(Duration::from_hours(1))),
                ),
            ],
        )
        .await
        .unwrap();

    let mut received = std::collections::HashMap::new();
    for _ in 0..4 {
        let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
            .await
            .unwrap()
            .unwrap();
        let job_event = event.as_job().unwrap();
        assert!(job_event.statistics.is_some());
        assert_eq!(job_event.job.metadata["shared_completion"], true);
        received.insert(job_event.job.id, (event.kind(), job_event.job.state));
    }
    assert_eq!(
        received.get(&rows[0].id),
        Some(&(EventKind::JobCompleted, JobState::Completed))
    );
    assert_eq!(
        received.get(&rows[1].id),
        Some(&(EventKind::JobCancelled, JobState::Cancelled))
    );
    assert_eq!(
        received.get(&rows[2].id),
        Some(&(EventKind::JobFailed, JobState::Discarded))
    );
    assert_eq!(
        received.get(&rows[3].id),
        Some(&(EventKind::JobCancelled, JobState::Cancelled))
    );
    assert_eq!(*observer.error_job_ids.lock().unwrap(), [failed_job_id]);
    assert_eq!(observer.work_end_count.load(Ordering::SeqCst), 0);

    run.shutdown().await.unwrap();
}

#[tokio::test]
async fn extension_claimed_outcomes_continue_after_interception_error() {
    let pool = setup().await;
    let mut pilot = SqlitePilot::new();
    pilot.completion = Some(CompletionBehavior::FailFirst);
    let client = Client::builder(pool.clone())
        .id("sqlite-extension-completion-error")
        .pilot(pilot.clone())
        .workers(runtime_workers(Arc::new(Semaphore::new(0))))
        .queue(
            "default",
            QueueConfig::new(1).with_fetch_poll_interval(Duration::from_mins(1)),
        )
        .build()
        .unwrap();
    let mut events = client.subscribe(&[EventKind::JobCompleted]).unwrap();
    let mut run = client.start().unwrap();
    run.wait_ready().await.unwrap();

    let scheduled_at = chrono::Utc::now() + chrono::Duration::hours(1);
    let mut rows = Vec::new();
    for value in 1..=2 {
        let inserted = client
            .insert_with(
                RuntimeArgs { value },
                InsertOpts::default().with_scheduled_at(scheduled_at),
            )
            .await
            .unwrap();
        sqlx::query(
            "UPDATE river_job SET state = 'running', attempt = 1, \
             attempted_at = datetime('now', 'subsec') WHERE id = ?",
        )
        .bind(inserted.job.row.id)
        .execute(&pool)
        .await
        .unwrap();
        rows.push(client.job_get(inserted.job.row.id).await.unwrap());
    }
    let context = WorkContext::new(tokio_util::sync::CancellationToken::new());
    let error = client
        .extension_persist_claimed_outcomes(
            &context,
            rows.iter()
                .cloned()
                .map(|row| (row, Ok(WorkOutcome::Complete)))
                .collect(),
        )
        .await
        .unwrap_err();
    assert!(
        error
            .to_string()
            .contains("first completion interception failed")
    );
    assert_eq!(
        client.job_get(rows[0].id).await.unwrap().state,
        JobState::Running
    );
    assert_eq!(
        client.job_get(rows[1].id).await.unwrap().state,
        JobState::Completed
    );
    let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(event.as_job().unwrap().job.id, rows[1].id);
    assert_eq!(pilot.completion_calls.load(Ordering::SeqCst), 2);

    run.shutdown_now().await.unwrap();
}

#[tokio::test]
async fn sqlite_pilot_completion_continue_and_handled_are_atomic() {
    for behavior in [CompletionBehavior::Continue, CompletionBehavior::Handled] {
        let pool = setup().await;
        let worked = Arc::new(Semaphore::new(0));
        let mut pilot = SqlitePilot::new();
        pilot.completion = Some(behavior);
        pilot.fetch = Some(SelectionBehavior::Success);
        let client = Client::builder(pool.clone())
            .id("sqlite-pilot-completion")
            .pilot(pilot.clone())
            .workers(runtime_workers(Arc::clone(&worked)))
            .queue(
                "default",
                QueueConfig::new(1)
                    .with_fetch_cooldown(Duration::from_millis(1))
                    .with_fetch_poll_interval(Duration::from_millis(10)),
            )
            .build()
            .unwrap();
        let mut events = client.subscribe(&[EventKind::JobCompleted]).unwrap();
        let mut run = client.start().unwrap();
        run.wait_ready().await.unwrap();

        let inserted = client.insert(RuntimeArgs { value: 1 }).await.unwrap();
        tokio::time::timeout(Duration::from_secs(5), worked.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
        let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            event.as_job().map(|event| event.job.id),
            Some(inserted.job.row.id)
        );

        let row = client.job_get(inserted.job.row.id).await.unwrap();
        assert_eq!(row.state, JobState::Completed);
        assert_eq!(
            row.metadata
                .get("pilot_handled")
                .and_then(serde_json::Value::as_bool),
            matches!(behavior, CompletionBehavior::Handled).then_some(true)
        );
        let effects: i64 = sqlx::query_scalar(
            "SELECT count(*) FROM pilot_effect WHERE operation = 'completion' AND job_id = ?",
        )
        .bind(inserted.job.row.id)
        .fetch_one(&pool)
        .await
        .unwrap();
        assert_eq!(effects, 1);
        assert!(pilot.fetch_calls.load(Ordering::SeqCst) >= 1);
        assert_eq!(pilot.completion_calls.load(Ordering::SeqCst), 1);

        run.shutdown().await.unwrap();
    }
}

#[tokio::test]
async fn sqlite_pilot_completion_error_rolls_back_side_effects() {
    let pool = setup().await;
    let worked = Arc::new(Semaphore::new(0));
    let mut pilot = SqlitePilot::new();
    pilot.completion = Some(CompletionBehavior::Fail);
    let client = Client::builder(pool.clone())
        .id("sqlite-pilot-completion-error")
        .pilot(pilot.clone())
        .workers(runtime_workers(Arc::clone(&worked)))
        .queue(
            "default",
            QueueConfig::new(1)
                .with_fetch_cooldown(Duration::from_millis(1))
                .with_fetch_poll_interval(Duration::from_millis(10)),
        )
        .build()
        .unwrap();
    let mut run = client.start().unwrap();
    run.wait_ready().await.unwrap();

    let inserted = client.insert(RuntimeArgs { value: 1 }).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), worked.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    tokio::time::timeout(Duration::from_secs(5), async {
        while pilot.completion_calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let effects: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pilot_effect WHERE operation = 'completion' AND job_id = ?",
    )
    .bind(inserted.job.row.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(effects, 0);
    assert_eq!(
        client.job_get(inserted.job.row.id).await.unwrap().state,
        JobState::Running
    );

    run.shutdown_now().await.unwrap();
}

#[tokio::test]
async fn sqlite_pilot_fetch_error_rolls_back_selection_side_effects() {
    let pool = setup().await;
    let mut pilot = SqlitePilot::new();
    pilot.fetch = Some(SelectionBehavior::Fail);
    let client = Client::builder(pool.clone())
        .id("sqlite-pilot-fetch-error")
        .pilot(pilot.clone())
        .workers(runtime_workers(Arc::new(Semaphore::new(0))))
        .queue(
            "default",
            QueueConfig::new(1)
                .with_fetch_cooldown(Duration::from_millis(1))
                .with_fetch_poll_interval(Duration::from_millis(10)),
        )
        .build()
        .unwrap();
    let inserted = client.insert(RuntimeArgs { value: 1 }).await.unwrap();
    let mut run = client.start().unwrap();
    run.wait_ready().await.unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        while pilot.fetch_calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let effects: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pilot_effect WHERE operation = 'fetch'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(effects, 0);
    assert_eq!(
        client.job_get(inserted.job.row.id).await.unwrap().state,
        JobState::Available
    );

    let _ = run.shutdown_now().await;
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one backend regression verifies atomic selection, ordering, row updates, and decode rollback"
)]
async fn sqlite_extension_claim_returns_ordered_rows_and_rolls_back_decode_errors() {
    let pool = setup().await;
    let client = Client::builder(pool.clone())
        .id("sqlite-extension-claimer")
        .build()
        .unwrap();
    let now = chrono::Utc::now();
    let matches = serde_json::Map::from_iter([
        ("group".to_owned(), serde_json::json!("shared")),
        ("mode".to_owned(), serde_json::json!("open")),
    ]);
    let leader = client
        .insert_with(
            RuntimeArgs { value: 50 },
            InsertOpts::default().with_metadata(matches.clone()),
        )
        .await
        .unwrap();
    let mut expected = Vec::new();
    for (value, priority, scheduled_at) in [
        (51, 2, now - chrono::Duration::minutes(3)),
        (52, 1, now - chrono::Duration::minutes(1)),
        (53, 1, now - chrono::Duration::minutes(2)),
    ] {
        let inserted = client
            .insert_with(
                RuntimeArgs { value },
                InsertOpts::default()
                    .with_metadata(matches.clone())
                    .with_priority(priority),
            )
            .await
            .unwrap();
        sqlx::query("UPDATE river_job SET scheduled_at = ? WHERE id = ?")
            .bind(riverqueue::database::sqlite_timestamp(scheduled_at))
            .bind(inserted.job.row.id)
            .execute(&pool)
            .await
            .unwrap();
        expected.push(inserted);
    }
    client
        .insert_with(
            RuntimeArgs { value: 54 },
            InsertOpts::default().with_metadata(serde_json::Map::from_iter([
                ("group".to_owned(), serde_json::json!("shared")),
                ("mode".to_owned(), serde_json::json!("closed")),
            ])),
        )
        .await
        .unwrap();
    let future = client
        .insert_with(
            RuntimeArgs { value: 55 },
            InsertOpts::default().with_metadata(matches.clone()),
        )
        .await
        .unwrap();
    sqlx::query("UPDATE river_job SET scheduled_at = ? WHERE id = ?")
        .bind(riverqueue::database::sqlite_timestamp(
            now + chrono::Duration::hours(1),
        ))
        .bind(future.job.row.id)
        .execute(&pool)
        .await
        .unwrap();

    let rows = client
        .extension_claim_jobs(ExtensionClaimParams {
            excluded_job_id: leader.job.row.id,
            kind: RuntimeArgs::KIND.to_owned(),
            maximum: 3,
            metadata_matches: matches,
            metadata_updates: serde_json::Map::from_iter([(
                "claim".to_owned(),
                serde_json::json!("leader-50"),
            )]),
            queue: "default".to_owned(),
        })
        .await
        .unwrap();
    assert_eq!(
        rows.iter().map(|row| row.id).collect::<Vec<_>>(),
        [
            expected[2].job.row.id,
            expected[1].job.row.id,
            expected[0].job.row.id,
        ]
    );
    for row in &rows {
        assert_eq!(row.state, JobState::Running);
        assert_eq!(row.attempt, 1);
        assert_eq!(row.attempted_by, ["sqlite-extension-claimer"]);
        assert_eq!(row.metadata["claim"], "leader-50");
    }
    assert_eq!(
        client.job_get(leader.job.row.id).await.unwrap().state,
        JobState::Available
    );

    let invalid = client.insert(UnknownArgs {}).await.unwrap();
    sqlx::query("UPDATE river_job SET tags = jsonb('{}') WHERE id = ?")
        .bind(invalid.job.row.id)
        .execute(&pool)
        .await
        .unwrap();
    let error = client
        .extension_claim_jobs(ExtensionClaimParams {
            excluded_job_id: 0,
            kind: UnknownArgs::KIND.to_owned(),
            maximum: 1,
            metadata_matches: serde_json::Map::new(),
            metadata_updates: serde_json::Map::new(),
            queue: "default".to_owned(),
        })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("SQLite River JSON failed"));
    let (state, attempt): (String, i16) =
        sqlx::query_as("SELECT state, attempt FROM river_job WHERE id = ?")
            .bind(invalid.job.row.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(state, "available");
    assert_eq!(attempt, 0);
}

#[tokio::test]
async fn sqlite_transient_renewal_contention_preserves_leadership_services() {
    let (pool, database_path) = setup_file_pool(Duration::from_millis(5)).await;
    let service = Arc::new(LeadershipServiceState::default());
    let mut pilot = SqlitePilot::new();
    pilot.maintenance_service = Some(Arc::clone(&service));
    let client = Client::builder(pool.clone())
        .id("sqlite-contention-leader")
        .maintenance(MaintenanceConfig::default().with_elect_interval(Duration::from_millis(10)))
        .pilot(pilot)
        .queue(
            "default",
            QueueConfig::new(1).with_fetch_poll_interval(Duration::from_mins(1)),
        )
        .workers(runtime_workers(Arc::new(Semaphore::new(0))))
        .build()
        .unwrap();
    let mut run = client.start().unwrap();
    run.wait_ready().await.unwrap();
    let startup_deadline = tokio::time::Instant::now() + Duration::from_secs(2);
    while service.starts.load(Ordering::SeqCst) == 0 {
        assert!(tokio::time::Instant::now() < startup_deadline);
        client.request_resign().await.unwrap();
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    tokio::time::sleep(Duration::from_millis(50)).await;
    let starts_before_contention = service.starts.load(Ordering::SeqCst);
    let stops_before_contention = service.stops.load(Ordering::SeqCst);
    let mut writer = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    sqlx::query("UPDATE river_queue SET updated_at = updated_at WHERE name = 'default'")
        .execute(&mut *writer)
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        service.starts.load(Ordering::SeqCst),
        starts_before_contention
    );
    assert_eq!(
        service.stops.load(Ordering::SeqCst),
        stops_before_contention
    );
    writer.rollback().await.unwrap();

    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(
        service.starts.load(Ordering::SeqCst),
        starts_before_contention
    );
    assert_eq!(
        service.stops.load(Ordering::SeqCst),
        stops_before_contention
    );

    client.request_resign().await.unwrap();
    tokio::time::timeout(Duration::from_secs(2), async {
        while service.stops.load(Ordering::SeqCst) == stops_before_contention {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();

    run.shutdown_now().await.unwrap();
    assert!(service.stops.load(Ordering::SeqCst) >= 1);
    pool.close().await;
    remove_sqlite_files(&database_path);
}

#[tokio::test]
async fn sqlite_pilot_insert_uses_the_insertion_transaction() {
    let pool = setup().await;
    let mut pilot = SqlitePilot::new();
    pilot.insert = Some(SelectionBehavior::Success);
    let client = Client::builder(pool.clone())
        .pilot(pilot.clone())
        .build()
        .unwrap();

    let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    sqlx::query("UPDATE pilot_insert_config SET marker = 'uncommitted' WHERE queue = 'default'")
        .execute(&mut *transaction)
        .await
        .unwrap();
    let inserted = client
        .insert_tx(&mut transaction, RuntimeArgs { value: 31 })
        .await
        .unwrap();
    assert_eq!(inserted.job.row.metadata["pilot_insert"], "uncommitted");
    transaction.commit().await.unwrap();

    assert_eq!(pilot.insert_calls.load(Ordering::SeqCst), 1);
    let effects: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pilot_effect WHERE operation = 'insert'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(effects, 1);

    let mut failing_pilot = SqlitePilot::new();
    failing_pilot.insert = Some(SelectionBehavior::Fail);
    let failing_client = Client::builder(pool.clone())
        .pilot(failing_pilot.clone())
        .build()
        .unwrap();
    let error = failing_client
        .insert(RuntimeArgs { value: 32 })
        .await
        .unwrap_err();
    assert!(error.to_string().contains("insert interception failed"));
    let failed_jobs: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM river_job WHERE json_extract(args, '$.value') = 32",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(failed_jobs, 0);
    let effects_after_failure: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pilot_effect WHERE operation = 'insert'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(effects_after_failure, 1);
    assert_eq!(failing_pilot.insert_calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
#[allow(
    clippy::too_many_lines,
    reason = "one exact-version insertion regression verifies every preserved/reset wire field and hook phase"
)]
async fn sqlite_reinsert_preserves_wire_fields_and_runs_the_canonical_pipeline() {
    let pool = setup().await;
    let producer = Client::builder(pool.clone())
        .hook(WrapperTransformHook("A"))
        .hook(WrapperTransformHook("B"))
        .build()
        .unwrap();
    let mut pilot = SqlitePilot::new();
    pilot.insert = Some(SelectionBehavior::Success);
    let mut workers = WorkerRegistry::new();
    workers
        .register_fn(|_context: WorkContext, _job: Job<CancelArgs>| async move {
            Ok::<_, Infallible>(WorkOutcome::Complete)
        })
        .unwrap();
    let client = Client::builder(pool.clone())
        .hook(WrapperTransformHook("A"))
        .hook(WrapperTransformHook("B"))
        .pilot(pilot.clone())
        .workers(workers)
        .build()
        .unwrap();
    let scheduled_at = chrono::Utc::now() + chrono::Duration::hours(2);
    let raw = client
        .insert_raw(
            CancelArgs::KIND,
            &[],
            serde_json::json!({"raw": true}),
            InsertOpts::default().with_pending(true),
        )
        .await
        .unwrap();
    assert_eq!(raw.job.encoded_args, serde_json::json!({"raw": true}));
    let stored_raw_args: String =
        sqlx::query_scalar("SELECT json(args) FROM river_job WHERE id = ?")
            .bind(raw.job.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let stored_raw_args: serde_json::Value = serde_json::from_str(&stored_raw_args).unwrap();
    assert_eq!(stored_raw_args["B"]["A"]["raw"], true);
    let original = producer
        .insert_with(
            RuntimeArgs { value: 41 },
            InsertOpts::default()
                .with_metadata(serde_json::Map::from_iter([(
                    "source".to_owned(),
                    serde_json::json!(true),
                )]))
                .with_scheduled_at(scheduled_at)
                .with_tags(["dead-letter"])
                .with_unique(UniqueOpts::new().by_args()),
        )
        .await
        .unwrap()
        .job
        .row;
    assert_eq!(original.encoded_args, serde_json::json!({"value": 41}));
    let stored_source_args: String =
        sqlx::query_scalar("SELECT json(args) FROM river_job WHERE id = ?")
            .bind(original.id)
            .fetch_one(&pool)
            .await
            .unwrap();
    let stored_source_args: serde_json::Value = serde_json::from_str(&stored_source_args).unwrap();
    assert_eq!(stored_source_args["B"]["A"]["value"], 41);
    let sentinel = producer
        .insert_with(
            RuntimeArgs { value: 42 },
            InsertOpts::default().with_scheduled_at(scheduled_at),
        )
        .await
        .unwrap();

    let mut transaction = pool.begin_with("BEGIN IMMEDIATE").await.unwrap();
    sqlx::query("DELETE FROM river_job WHERE id = ?")
        .bind(original.id)
        .execute(&mut *transaction)
        .await
        .unwrap();
    let reinserted = client
        .extension_insert_tx(
            &mut transaction,
            ExtensionInsertParams {
                created_at: original.created_at,
                encoded_args: stored_source_args,
                kind: "x".to_owned(),
                max_attempts: original.max_attempts,
                metadata: original.metadata.clone(),
                priority: original.priority,
                queue: original.queue.clone(),
                scheduled_at: original.scheduled_at,
                tags: original.tags.clone(),
                unique_key: original.unique_key.clone(),
                unique_states: original.unique_states.clone(),
            },
        )
        .await
        .unwrap();
    transaction.commit().await.unwrap();

    assert_ne!(reinserted.job.id, original.id);
    assert!(reinserted.job.id > sentinel.job.row.id);
    assert_eq!(reinserted.job.attempt, 0);
    assert!(reinserted.job.attempted_at.is_none());
    assert!(reinserted.job.attempted_by.is_empty());
    assert_eq!(reinserted.job.created_at, original.created_at);
    assert!(reinserted.job.errors.is_empty());
    assert!(reinserted.job.finalized_at.is_none());
    assert_eq!(reinserted.job.scheduled_at, original.scheduled_at);
    assert_eq!(reinserted.job.state, JobState::Available);
    assert_eq!(reinserted.job.kind, "x");
    assert_eq!(reinserted.job.unique_key, original.unique_key);
    assert_eq!(reinserted.job.unique_states, original.unique_states);
    assert_eq!(reinserted.job.metadata["source"], true);
    assert_eq!(reinserted.job.metadata["pilot_insert"], "default");
    assert!(!reinserted.unique_skipped_as_duplicate);
    assert_eq!(pilot.insert_calls.load(Ordering::SeqCst), 2);

    assert_eq!(
        reinserted.job.encoded_args,
        serde_json::json!({"value": 41})
    );
    let stored_args: String = sqlx::query_scalar("SELECT json(args) FROM river_job WHERE id = ?")
        .bind(reinserted.job.id)
        .fetch_one(&pool)
        .await
        .unwrap();
    let stored_args: serde_json::Value = serde_json::from_str(&stored_args).unwrap();
    assert_eq!(stored_args["B"]["A"]["value"], 41);

    let notifications: i64 =
        sqlx::query_scalar("SELECT count(*) FROM river_notification WHERE topic = 'river_insert'")
            .fetch_one(&pool)
            .await
            .unwrap();
    assert_eq!(notifications, 1);
}

#[tokio::test]
async fn sqlite_pilot_rescue_selection_and_update_share_a_transaction() {
    let pool = setup().await;
    let mut pilot = SqlitePilot::new();
    pilot.rescue = Some(SelectionBehavior::Success);
    let client = Client::builder(pool.clone())
        .id("sqlite-pilot-rescue")
        .maintenance(
            MaintenanceConfig::default()
                .with_elect_interval(Duration::from_millis(10))
                .with_rescue_after(Duration::from_millis(10))
                .with_rescuer_interval(Duration::from_millis(10)),
        )
        .pilot(pilot.clone())
        .queue(
            "default",
            QueueConfig::new(1).with_fetch_poll_interval(Duration::from_mins(1)),
        )
        .workers(runtime_workers(Arc::new(Semaphore::new(0))))
        .build()
        .unwrap();
    let inserted = client.insert(RuntimeArgs { value: 1 }).await.unwrap();
    sqlx::query(
        "UPDATE river_job SET state = 'running', attempt = 1, \
         attempted_at = '2000-01-01 00:00:00.000' WHERE id = ?",
    )
    .bind(inserted.job.row.id)
    .execute(&pool)
    .await
    .unwrap();
    let mut run = client.start().unwrap();
    run.wait_ready().await.unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if client.job_get(inserted.job.row.id).await.unwrap().state == JobState::Retryable {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    let effects: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pilot_effect WHERE operation = 'rescue' AND job_id = ?",
    )
    .bind(inserted.job.row.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(effects, 1);
    assert!(pilot.rescue_calls.load(Ordering::SeqCst) >= 1);

    run.shutdown().await.unwrap();
}

#[tokio::test]
async fn sqlite_pilot_rescue_error_rolls_back_selection_side_effects() {
    let pool = setup().await;
    let mut pilot = SqlitePilot::new();
    pilot.rescue = Some(SelectionBehavior::Fail);
    let client = Client::builder(pool.clone())
        .id("sqlite-pilot-rescue-error")
        .maintenance(
            MaintenanceConfig::default()
                .with_elect_interval(Duration::from_millis(10))
                .with_rescue_after(Duration::from_millis(10))
                .with_rescuer_interval(Duration::from_millis(10)),
        )
        .pilot(pilot.clone())
        .queue(
            "default",
            QueueConfig::new(1).with_fetch_poll_interval(Duration::from_mins(1)),
        )
        .workers(runtime_workers(Arc::new(Semaphore::new(0))))
        .build()
        .unwrap();
    let inserted = client.insert(RuntimeArgs { value: 1 }).await.unwrap();
    sqlx::query(
        "UPDATE river_job SET state = 'running', attempt = 1, \
         attempted_at = '2000-01-01 00:00:00.000' WHERE id = ?",
    )
    .bind(inserted.job.row.id)
    .execute(&pool)
    .await
    .unwrap();
    let mut run = client.start().unwrap();
    run.wait_ready().await.unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        while pilot.rescue_calls.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let effects: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM pilot_effect WHERE operation = 'rescue' AND job_id = ?",
    )
    .bind(inserted.job.row.id)
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(effects, 0);
    assert_eq!(
        client.job_get(inserted.job.row.id).await.unwrap().state,
        JobState::Running
    );

    run.shutdown_now().await.unwrap();
}

#[tokio::test]
async fn sqlite_queue_events_are_emitted_once_per_transition() {
    let pool = setup().await;
    let client = Client::builder(pool)
        .workers(runtime_workers(Arc::new(Semaphore::new(0))))
        .queue(
            "default",
            QueueConfig::new(1)
                .with_fetch_cooldown(Duration::from_millis(1))
                .with_fetch_poll_interval(Duration::from_millis(10)),
        )
        .build()
        .unwrap();
    let mut events = client
        .subscribe(&[EventKind::QueuePaused, EventKind::QueueResumed])
        .unwrap();
    let mut run = client.start().unwrap();
    run.wait_ready().await.unwrap();

    client.queue_pause("default").await.unwrap();
    let paused = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(paused.kind(), EventKind::QueuePaused);
    assert!(paused.as_queue().unwrap().queue.paused_at.is_some());
    assert!(
        tokio::time::timeout(Duration::from_millis(300), events.recv())
            .await
            .is_err()
    );

    client.queue_pause("default").await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(300), events.recv())
            .await
            .is_err()
    );

    client.queue_resume("default").await.unwrap();
    let resumed = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(resumed.kind(), EventKind::QueueResumed);
    assert!(resumed.as_queue().unwrap().queue.paused_at.is_none());
    assert!(
        tokio::time::timeout(Duration::from_millis(300), events.recv())
            .await
            .is_err()
    );

    client.queue_resume("default").await.unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(300), events.recv())
            .await
            .is_err()
    );

    run.shutdown().await.unwrap();
}

#[tokio::test]
async fn sqlite_runs_jobs_and_persists_output() {
    let pool = setup().await;
    let worked = Arc::new(Semaphore::new(0));
    let worked_for_worker = Arc::clone(&worked);
    let mut workers = WorkerRegistry::new();
    workers
        .register_fn(move |context: WorkContext, job: Job<RuntimeArgs>| {
            let worked = Arc::clone(&worked_for_worker);
            async move {
                context
                    .record_output(&serde_json::json!({"doubled": job.args.value * 2}))
                    .await
                    .unwrap();
                worked.add_permits(1);
                Ok::<_, Infallible>(WorkOutcome::Complete)
            }
        })
        .unwrap();
    let client = Client::builder(pool.clone())
        .id("sqlite-runtime")
        .workers(workers)
        .queue(
            "default",
            QueueConfig::new(4)
                .with_fetch_cooldown(Duration::from_millis(1))
                .with_fetch_poll_interval(Duration::from_millis(10)),
        )
        .build()
        .unwrap();
    assert_eq!(client.database_kind(), DatabaseKind::Sqlite);
    let mut events = client.subscribe(&[EventKind::JobCompleted]).unwrap();
    let mut run = client.start().unwrap();
    run.wait_ready().await.unwrap();

    let inserted = client.insert(RuntimeArgs { value: 21 }).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), worked.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let event = tokio::time::timeout(Duration::from_secs(5), events.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        event.as_job().map(|job_event| job_event.job.id),
        Some(inserted.job.row.id)
    );
    let row = client.job_get(inserted.job.row.id).await.unwrap();
    assert_eq!(row.state, JobState::Completed);
    assert_eq!(row.output(), Some(&serde_json::json!({"doubled": 42})));

    run.shutdown().await.unwrap();
}

#[tokio::test]
async fn sqlite_transaction_insert_respects_rollback() {
    let pool = setup().await;
    let client = Client::builder(pool.clone()).build().unwrap();
    let mut transaction = pool.begin().await.unwrap();
    let inserted = client
        .insert_tx(&mut transaction, RuntimeArgs { value: 1 })
        .await
        .unwrap();
    transaction.rollback().await.unwrap();

    let error = client.job_get(inserted.job.row.id).await.unwrap_err();
    assert!(matches!(error, riverqueue::Error::NotFound));
}

#[tokio::test]
async fn sqlite_inserts_heterogeneous_batch_in_order() {
    let pool = setup().await;
    let client = Client::builder(pool).build().unwrap();
    let mut batch = InsertBatch::new();
    batch
        .push(RuntimeArgs { value: 7 })
        .unwrap()
        .push_with(
            CancelArgs {},
            InsertOpts::default().with_queue("heterogeneous-queue"),
        )
        .unwrap();

    let results = client.insert_batch(batch).await.unwrap();

    assert_eq!(results.len(), 2);
    assert_eq!(results[0].job.kind, RuntimeArgs::KIND);
    assert_eq!(results[1].job.kind, CancelArgs::KIND);
    assert_eq!(results[1].job.queue, "heterogeneous-queue");
    assert!(results[0].job.id < results[1].job.id);
}

#[tokio::test]
async fn sqlite_transaction_batches_roll_back_only_the_failed_batch() {
    let pool = setup().await;
    let client = Client::builder(pool.clone()).build().unwrap();

    let mut transaction = pool.begin().await.unwrap();
    client
        .insert_tx(&mut transaction, RuntimeArgs { value: 100 })
        .await
        .unwrap();
    let result = client
        .insert_many_tx_with(
            &mut transaction,
            [
                (
                    RuntimeArgs { value: 101 },
                    InsertOpts::default().with_tags(["ordinary-failed-batch"]),
                ),
                (
                    RuntimeArgs { value: 102 },
                    InsertOpts::default().with_priority(0),
                ),
            ],
        )
        .await;
    assert!(result.is_err());
    transaction.commit().await.unwrap();

    let control_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM river_job WHERE json_extract(args, '$.value') = 100",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let batch_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM river_job WHERE json_extract(args, '$.value') = 101",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(control_count, 1);
    assert_eq!(batch_count, 0);

    let unique = UniqueOpts::new().by_args();
    client
        .insert_with(
            RuntimeArgs { value: 200 },
            InsertOpts::default().with_unique(unique.clone()),
        )
        .await
        .unwrap();
    let mut transaction = pool.begin().await.unwrap();
    client
        .insert_tx(&mut transaction, RuntimeArgs { value: 201 })
        .await
        .unwrap();
    let result = client
        .insert_many_fast_tx_with(
            &mut transaction,
            [
                (
                    RuntimeArgs { value: 202 },
                    InsertOpts::default().with_tags(["fast-failed-batch"]),
                ),
                (
                    RuntimeArgs { value: 200 },
                    InsertOpts::default().with_unique(unique),
                ),
            ],
        )
        .await;
    assert!(result.is_err());
    transaction.commit().await.unwrap();

    let control_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM river_job WHERE json_extract(args, '$.value') = 201",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    let batch_count: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM river_job WHERE json_extract(args, '$.value') = 202",
    )
    .fetch_one(&pool)
    .await
    .unwrap();
    assert_eq!(control_count, 1);
    assert_eq!(batch_count, 0);
}

#[tokio::test]
async fn sqlite_outbox_cancels_work_from_another_client() {
    let pool = setup().await;
    let started = Arc::new(Semaphore::new(0));
    let started_for_worker = Arc::clone(&started);
    let mut workers = WorkerRegistry::new();
    workers
        .register_fn(move |context: WorkContext, _job: Job<CancelArgs>| {
            let started = Arc::clone(&started_for_worker);
            async move {
                started.add_permits(1);
                context.cancellation_token().cancelled().await;
                Ok::<_, Infallible>(WorkOutcome::Snooze(Duration::from_mins(1)))
            }
        })
        .unwrap();
    let worker_client = Client::builder(pool.clone())
        .id("sqlite-worker")
        .workers(workers)
        .queue(
            "default",
            QueueConfig::new(1)
                .with_fetch_cooldown(Duration::from_millis(1))
                .with_fetch_poll_interval(Duration::from_millis(10)),
        )
        .build()
        .unwrap();
    let cancelling_client = Client::builder(pool)
        .id("sqlite-canceller")
        .build()
        .unwrap();
    let mut cancelled = worker_client.subscribe(&[EventKind::JobCancelled]).unwrap();
    let mut run = worker_client.start().unwrap();
    run.wait_ready().await.unwrap();

    let inserted = worker_client.insert(CancelArgs {}).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    let running = worker_client.job_get(inserted.job.row.id).await.unwrap();
    assert_eq!(running.state, JobState::Running);
    cancelling_client.job_cancel(running.id).await.unwrap();
    let event = tokio::time::timeout(Duration::from_secs(5), cancelled.recv())
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        event.as_job().map(|job_event| job_event.job.id),
        Some(running.id)
    );
    assert_eq!(
        worker_client.job_get(running.id).await.unwrap().state,
        JobState::Cancelled
    );

    run.shutdown().await.unwrap();
}

#[tokio::test]
async fn sqlite_completion_event_follows_external_cancelled_state() {
    let pool = setup().await;
    let finish = Arc::new(Semaphore::new(0));
    let started = Arc::new(Semaphore::new(0));
    let mut workers = WorkerRegistry::new();
    workers
        .register_fn({
            let finish = Arc::clone(&finish);
            let started = Arc::clone(&started);
            move |_context: WorkContext, _job: Job<CancelIgnoredArgs>| {
                let finish = Arc::clone(&finish);
                let started = Arc::clone(&started);
                async move {
                    started.add_permits(1);
                    finish.acquire().await.unwrap().forget();
                    Ok::<_, Infallible>(WorkOutcome::Complete)
                }
            }
        })
        .unwrap();
    let worker_client = Client::builder(pool.clone())
        .id("sqlite-worker-cancel-ignored")
        .workers(workers)
        .queue(
            "default",
            QueueConfig::new(1)
                .with_fetch_cooldown(Duration::from_millis(1))
                .with_fetch_poll_interval(Duration::from_millis(10)),
        )
        .build()
        .unwrap();
    let mut cancelled = worker_client.subscribe(&[EventKind::JobCancelled]).unwrap();
    let mut run = worker_client.start().unwrap();
    run.wait_ready().await.unwrap();

    let inserted = worker_client.insert(CancelIgnoredArgs {}).await.unwrap();
    tokio::time::timeout(Duration::from_secs(5), started.acquire())
        .await
        .unwrap()
        .unwrap()
        .forget();
    sqlx::query(
        "UPDATE river_job SET state = 'cancelled', \
         finalized_at = strftime('%Y-%m-%d %H:%M:%f', 'now') WHERE id = ?",
    )
    .bind(inserted.job.row.id)
    .execute(&pool)
    .await
    .unwrap();
    finish.add_permits(1);
    let event = tokio::time::timeout(Duration::from_secs(5), cancelled.recv())
        .await
        .unwrap()
        .unwrap();
    let event = event.as_job().unwrap();
    assert_eq!(event.job.id, inserted.job.row.id);
    assert_eq!(event.job.state, JobState::Cancelled);

    run.shutdown().await.unwrap();
}

#[tokio::test]
async fn sqlite_fetches_and_discards_unregistered_kinds() {
    let pool = setup().await;
    let producer = Client::builder(pool.clone()).build().unwrap();
    let inserted = producer
        .insert_with(UnknownArgs {}, InsertOpts::default().with_max_attempts(1))
        .await
        .unwrap();
    let mut workers = WorkerRegistry::new();
    workers
        .register_fn(|_context: WorkContext, _job: Job<RuntimeArgs>| async move {
            Ok::<_, Infallible>(WorkOutcome::Complete)
        })
        .unwrap();
    let worker = Client::builder(pool)
        .workers(workers)
        .queue(
            "default",
            QueueConfig::new(1)
                .with_fetch_cooldown(Duration::from_millis(1))
                .with_fetch_poll_interval(Duration::from_millis(10)),
        )
        .build()
        .unwrap();
    let run = worker.start().unwrap();

    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if worker.job_get(inserted.job.row.id).await.unwrap().state == JobState::Discarded {
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();

    run.shutdown().await.unwrap();
}
