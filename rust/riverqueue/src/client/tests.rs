#[cfg(feature = "sqlite")]
use serde::Deserialize;
use serde_json::Map;

use super::attempts::{register_running_attempt, remove_running_attempt, signal_running_attempt};
#[cfg(feature = "postgres")]
use super::completer::CompletionBatcher;
#[cfg(feature = "sqlite")]
use super::completer::{
    COMPLETION_BACKLOG_LIMIT, COMPLETION_BATCH_SIZE, CompletionTiming, run_completion_batcher,
};
use super::completer::{persisted_completion_event_kind, with_completion_retries};
use super::executor::scheduled_after;
#[cfg(feature = "sqlite")]
use super::notifier::dispatch_notification;
use super::*;
use crate::{AttemptError, JobEventKind, JobRow, JobState, WorkError, WorkResult};
#[cfg(feature = "sqlite")]
use crate::{InsertOpts, InsertParams, WorkContext, WorkOutcome};
#[cfg(feature = "sqlite")]
use crate::{Job, JobArgs};

#[test]
fn completion_events_follow_persisted_state() {
    let cases = [
        (
            JobState::Available,
            JobEventKind::Failed,
            JobEventKind::Failed,
        ),
        (
            JobState::Available,
            JobEventKind::Interrupted,
            JobEventKind::Interrupted,
        ),
        (
            JobState::Available,
            JobEventKind::Cancelled,
            JobEventKind::Failed,
        ),
        (
            JobState::Available,
            JobEventKind::Completed,
            JobEventKind::Failed,
        ),
        (
            JobState::Available,
            JobEventKind::Snoozed,
            JobEventKind::Snoozed,
        ),
        (
            JobState::Cancelled,
            JobEventKind::Failed,
            JobEventKind::Cancelled,
        ),
        (
            JobState::Completed,
            JobEventKind::Failed,
            JobEventKind::Completed,
        ),
        (
            JobState::Discarded,
            JobEventKind::Completed,
            JobEventKind::Failed,
        ),
        (
            JobState::Retryable,
            JobEventKind::Completed,
            JobEventKind::Failed,
        ),
        (
            JobState::Scheduled,
            JobEventKind::Failed,
            JobEventKind::Snoozed,
        ),
    ];

    for (state, requested, expected) in cases {
        assert_eq!(
            persisted_completion_event_kind(state, requested),
            Some(expected)
        );
    }
    // A row moved back to a non-final state by someone else reports nothing
    // rather than failing the completer.
    for state in [JobState::Pending, JobState::Running] {
        assert_eq!(
            persisted_completion_event_kind(state, JobEventKind::Completed),
            None
        );
    }
}

#[test]
fn completion_cleanup_preserves_newer_attempt() {
    let job_id = 42;
    let first = CancellationToken::new();
    let second = CancellationToken::new();
    let running = Mutex::new(HashMap::from([(job_id, first.clone())]));

    running
        .lock()
        .unwrap_or_else(std::sync::PoisonError::into_inner)
        .insert(job_id, second.clone());

    remove_running_attempt(&running, job_id, &first);
    assert_eq!(
        running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&job_id),
        Some(&second)
    );

    remove_running_attempt(&running, job_id, &second);
    assert!(
        running
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .get(&job_id)
            .is_none()
    );
}

#[test]
fn pending_cancellation_reaches_fetched_attempt() {
    let job_id = 42;
    let cancellation = CancellationToken::new();
    let fetch_registration_windows = AtomicU64::new(1);
    let pending_cancellations = Mutex::new(HashMap::new());
    let running = Mutex::new(HashMap::new());

    signal_running_attempt(
        &running,
        &pending_cancellations,
        &fetch_registration_windows,
        job_id,
    );
    register_running_attempt(&running, &pending_cancellations, job_id, &cancellation);

    assert!(cancellation.is_cancelled());
    assert!(
        pending_cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    );
}

#[test]
fn unmatched_cancellation_is_not_retained_without_fetch() {
    let fetch_registration_windows = AtomicU64::new(0);
    let pending_cancellations = Mutex::new(HashMap::new());
    let running = Mutex::new(HashMap::new());

    signal_running_attempt(
        &running,
        &pending_cancellations,
        &fetch_registration_windows,
        42,
    );

    assert!(
        pending_cancellations
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .is_empty()
    );
}

fn retry_row(error_count: usize) -> JobRow {
    let now = DateTime::parse_from_rfc3339("2026-01-02T03:04:05Z")
        .unwrap()
        .with_timezone(&Utc);
    JobRow {
        attempt: i16::try_from(error_count).unwrap_or(i16::MAX),
        attempted_at: Some(now),
        attempted_by: vec!["test".to_owned()],
        created_at: now,
        encoded_args: serde_json::value::to_raw_value(&serde_json::json!({})).unwrap(),
        errors: vec![
            AttemptError {
                at: now,
                attempt: 1,
                error: "failed".to_owned(),
                trace: String::new(),
            };
            error_count
        ],
        finalized_at: None,
        id: 42,
        kind: "retry_test".to_owned(),
        max_attempts: 1_000,
        metadata: Map::new().into(),
        priority: 1,
        queue: "default".to_owned(),
        scheduled_at: now,
        state: JobState::Retryable,
        tags: Vec::new(),
        unique_key: None,
        unique_states: None,
    }
}

#[test]
fn retry_delay_is_seeded_bounded_and_capped() {
    let now = Utc::now();
    let row = retry_row(0);
    let first = default_retry_delay(&row, now, 123);
    assert_eq!(first, default_retry_delay(&row, now, 123));
    assert_ne!(first, default_retry_delay(&row, now, 456));
    assert!(first >= Duration::from_millis(900));
    assert!(first <= Duration::from_millis(1_100));

    assert_eq!(
        default_retry_delay(&retry_row(309), now, 123),
        Duration::from_nanos(i64::MAX as u64)
    );
    // Just below the cap, upward jitter must not exceed it.
    for seed in 0..64 {
        assert!(
            default_retry_delay(&retry_row(308), now, seed)
                <= Duration::from_nanos(i64::MAX as u64)
        );
    }
}

#[tokio::test]
async fn worker_failures_record_the_error_chain_and_panic_value() {
    #[derive(Debug, thiserror::Error)]
    #[error("charging card")]
    struct ChargeError(#[source] std::io::Error);

    let failure = super::executor::worker_join_result(Ok(Err(WorkError::new(ChargeError(
        std::io::Error::other("card declined"),
    )))))
    .unwrap_err();
    assert_eq!(failure.error, "charging card: card declined");

    let join_error = tokio::spawn(async { panic!("boom") }).await.unwrap_err();
    let failure = super::executor::worker_join_result(Err(join_error)).unwrap_err();
    assert_eq!(failure.error, "boom");
    let WorkResult::Panicked(panic) = super::executor::public_work_result(&Err(failure)) else {
        panic!("expected a panic result");
    };
    assert_eq!(panic.message(), "boom");
    assert_eq!(panic.to_string(), "worker panicked: boom");
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn extension_client_finds_only_the_installed_pilot() {
    #[derive(Debug)]
    struct InstalledPilot;
    impl Pilot for InstalledPilot {}

    #[derive(Debug)]
    struct OtherPilot;
    impl Pilot for OtherPilot {}

    let pool = sqlx::SqlitePool::connect_lazy("sqlite::memory:").unwrap();
    let client = Client::builder(pool)
        .with_pilot(InstalledPilot)
        .build()
        .unwrap();
    let extension = crate::__private::ExtensionClient::new(&client);
    assert!(extension.pilot::<InstalledPilot>().is_some());
    assert!(extension.pilot::<OtherPilot>().is_none());
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn erased_transactions_run_requests_in_the_callers_transaction() {
    #[derive(Deserialize, serde::Serialize)]
    struct ErasedArgs {}

    impl JobArgs for ErasedArgs {
        const KIND: &'static str = "erased";
    }

    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    riverqueue_migrate::SqliteMigrator::new(pool.clone())
        .migrate_up()
        .await
        .unwrap();
    let client = Client::builder(pool.clone()).build().unwrap();
    let id = client.insert(ErasedArgs {}).await.unwrap().id();

    let mut transaction = crate::database::begin_sqlite_write(&pool).await.unwrap();
    let mut erased = client.inner.database.transaction(&mut transaction).unwrap();
    let cancelled = client.jobs().cancel(id).tx(&mut erased).await.unwrap();
    assert_eq!(cancelled.state, JobState::Cancelled);
    transaction.rollback().await.unwrap();
    assert_eq!(
        client.jobs().get(id).await.unwrap().state,
        JobState::Available
    );
}

#[tokio::test]
async fn completion_retries_recover_from_a_transient_error() {
    let attempts = AtomicU64::new(0);
    let result = with_completion_retries("test completion", || async {
        if attempts.fetch_add(1, Ordering::SeqCst) == 0 {
            Err(Error::from(sqlx::Error::PoolTimedOut))
        } else {
            Ok("persisted")
        }
    })
    .await;
    assert_eq!(result.unwrap(), "persisted");
    assert_eq!(attempts.load(Ordering::SeqCst), 2);
}

#[tokio::test]
async fn completion_retries_stop_immediately_for_a_closed_pool() {
    let attempts = AtomicU64::new(0);
    let result = with_completion_retries("test completion", || async {
        attempts.fetch_add(1, Ordering::SeqCst);
        Err::<(), _>(Error::from(sqlx::Error::PoolClosed))
    })
    .await;
    assert!(result.is_err());
    assert_eq!(attempts.load(Ordering::SeqCst), 1);
}

#[test]
fn go_time_json_matches_go_rfc3339_nano() {
    use chrono::TimeZone as _;

    let base = Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();
    assert_eq!(go_time_json(base), "2026-01-02T03:04:05Z");
    assert_eq!(
        go_time_json(base + chrono::Duration::nanoseconds(120_000_000)),
        "2026-01-02T03:04:05.12Z"
    );
    assert_eq!(
        go_time_json(base + chrono::Duration::nanoseconds(123_456_789)),
        "2026-01-02T03:04:05.123456789Z"
    );
}

#[test]
fn schedule_delays_clamp_like_go_durations() {
    use chrono::TimeZone as _;

    let now = Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();
    assert_eq!(
        scheduled_after(now, Duration::from_secs(90)),
        now + chrono::Duration::seconds(90)
    );
    let clamped = scheduled_after(now, Duration::MAX);
    assert_eq!(
        clamped,
        now + chrono::Duration::nanoseconds(i64::MAX),
        "delays saturate at Go's maximum time.Duration"
    );
}

#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread")]
async fn subscription_forwarder_stops_when_the_receiver_drops() {
    #[derive(Clone, Debug, serde::Deserialize, crate::JobArgs, serde::Serialize)]
    #[river(kind = "subscription_forwarder_test")]
    struct ForwarderArgs {}

    let mut workers = WorkerRegistry::new();
    workers
        .register_fn(|_context: WorkContext, _job: Job<ForwarderArgs>| async {
            Ok::<_, std::convert::Infallible>(WorkOutcome::Complete)
        })
        .unwrap();
    let pool = sqlx::SqlitePool::connect_lazy("sqlite::memory:").unwrap();
    let client = Client::builder(pool)
        .workers(workers)
        .queue("default", QueueConfig::new(1))
        .build()
        .unwrap();
    let baseline = client.inner.events.receiver_count();
    let receiver = client.subscribe(&[EventKind::JobCompleted]).unwrap();
    assert_eq!(client.inner.events.receiver_count(), baseline + 1);

    drop(receiver);
    let deadline = tokio::time::Instant::now() + Duration::from_secs(5);
    while client.inner.events.receiver_count() > baseline {
        assert!(
            tokio::time::Instant::now() < deadline,
            "forwarder outlived its receiver"
        );
        tokio::time::sleep(Duration::from_millis(5)).await;
    }
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn intercepting_extensions_can_only_lower_completion_concurrency() {
    #[derive(Clone, Copy)]
    struct ConcurrencyPilot(usize);

    #[async_trait::async_trait]
    impl crate::__private::Pilot for ConcurrencyPilot {
        fn intercepts_job_set_state(&self) -> bool {
            true
        }

        fn job_set_state_concurrency(&self) -> usize {
            self.0
        }
    }

    let pool = sqlx::PgPool::connect_lazy("postgres://localhost/unused").unwrap();
    let concurrency = |pilot: Option<ConcurrencyPilot>| {
        let builder = Client::builder(pool.clone());
        let client = match pilot {
            Some(pilot) => builder.with_pilot(pilot),
            None => builder,
        }
        .build()
        .unwrap();
        CompletionBatcher::new(Arc::clone(&client.inner)).concurrency()
    };
    assert_eq!(concurrency(None), 2);
    assert_eq!(concurrency(Some(ConcurrencyPilot(0))), 1);
    assert_eq!(concurrency(Some(ConcurrencyPilot(1))), 1);
    assert_eq!(concurrency(Some(ConcurrencyPilot(8))), 2);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn periodic_jobs_run_at_their_target_unless_scheduled_explicitly() {
    let pool = sqlx::SqlitePool::connect_lazy("sqlite::memory:").unwrap();
    let client = Client::builder(pool).build().unwrap();
    let now = Utc::now();
    let target = now - chrono::Duration::milliseconds(5);
    let prepare = |opts: InsertParams| {
        client
            .prepare_periodic(
                "periodic_test",
                &[],
                serde_json::value::to_raw_value(&serde_json::json!({})).unwrap(),
                opts,
                target,
                now,
            )
            .unwrap()
    };
    let defaults = || {
        InsertOpts::resolve(
            MAX_ATTEMPTS_DEFAULT,
            InsertOpts::default(),
            InsertOpts::default(),
        )
    };

    // A due job runs immediately at its target, as Go's enqueuer inserts it.
    let due = prepare(defaults());
    assert_eq!(due.state, JobState::Available);
    assert_eq!(due.opts.scheduled_at, Some(target));

    // An explicit schedule from the constructor is kept and waits.
    let later = now + chrono::Duration::minutes(5);
    let mut explicit = defaults();
    explicit.scheduled_at = Some(later);
    let explicit = prepare(explicit);
    assert_eq!(explicit.state, JobState::Scheduled);
    assert_eq!(explicit.opts.scheduled_at, Some(later));

    // A pending job stays pending.
    let mut pending = defaults();
    pending.pending = true;
    assert_eq!(prepare(pending).state, JobState::Pending);
}

/// Like River Go's completer stop path, a client stopping during an outage
/// gives up on its unwritten completions after the first failed batch, even
/// when the backlog is too full for the batcher to receive the end of its
/// channel, instead of retrying every batch until the database returns.
#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread")]
async fn completer_abandons_its_backlog_when_a_batch_fails_during_shutdown() {
    // Without River's tables, every completion write fails and is retried.
    let pool = sqlx::SqlitePool::connect_lazy("sqlite::memory:").unwrap();
    let client = Client::builder(pool).build().unwrap();
    let inner = Arc::clone(&client.inner);
    // More than a full backlog plus the batch in flight.
    let updates = COMPLETION_BACKLOG_LIMIT + 2 * COMPLETION_BATCH_SIZE;
    let (sender, receiver) = mpsc::channel(updates);
    let now = std::time::Instant::now();
    for job_id in 0..i64::try_from(updates).unwrap() {
        let cancellation = CancellationToken::new();
        inner
            .running
            .lock()
            .unwrap()
            .insert(job_id, cancellation.clone());
        sender
            .try_send(CompletionUpdate {
                attempt: None,
                cancellation,
                error: None,
                event_kind: JobEventKind::Completed,
                finalized_at: Some(Utc::now()),
                job_id,
                metadata: Map::new(),
                peer: None,
                scheduled_at: None,
                state: JobState::Completed,
                timing: CompletionTiming {
                    completion_started: now,
                    queue_wait_duration: Duration::ZERO,
                    run_duration: Duration::ZERO,
                },
            })
            .unwrap_or_else(|_| panic!("the channel has room"));
    }
    // Every producer has stopped.
    drop(sender);

    // One batch's retry cycle sleeps about three seconds.
    tokio::time::timeout(
        Duration::from_secs(30),
        run_completion_batcher(Arc::clone(&inner), receiver),
    )
    .await
    .expect("the completer stops after the first failed batch")
    .unwrap();
    assert!(inner.running.lock().unwrap().is_empty());
}

/// A notification listener that panics is restarted, and the restarted
/// listener reports the client ready rather than the client appearing to
/// have stopped before becoming ready.
#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread")]
async fn readiness_survives_a_notification_listener_panic() {
    #[derive(Deserialize, serde::Serialize)]
    struct ReadinessArgs {}

    impl JobArgs for ReadinessArgs {
        const KIND: &'static str = "readiness";
    }

    let path = std::env::temp_dir().join(format!(
        "river-readiness-{}-{}.sqlite",
        std::process::id(),
        Utc::now().timestamp_nanos_opt().unwrap_or_default()
    ));
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(4)
        .connect_with(
            sqlx::sqlite::SqliteConnectOptions::new()
                .filename(&path)
                .create_if_missing(true)
                .journal_mode(sqlx::sqlite::SqliteJournalMode::Wal),
        )
        .await
        .unwrap();
    riverqueue_migrate::SqliteMigrator::new(pool.clone())
        .migrate_up()
        .await
        .unwrap();
    let mut workers = WorkerRegistry::new();
    workers
        .register_fn(|_context: WorkContext, _job: Job<ReadinessArgs>| async {
            Ok::<_, std::convert::Infallible>(WorkOutcome::Complete)
        })
        .unwrap();
    let client = Client::builder(pool.clone())
        .without_leader_election()
        .workers(workers)
        .queue("default", QueueConfig::new(1))
        .build()
        .unwrap();
    client
        .inner
        .notifier_start_panics
        .store(1, Ordering::Release);

    let mut run = client.start().unwrap();
    tokio::time::timeout(Duration::from_secs(10), run.wait_ready())
        .await
        .expect("the restarted listener reports readiness")
        .unwrap();
    assert_eq!(
        client.inner.notifier_start_panics.load(Ordering::Acquire),
        0
    );
    run.shutdown().await.unwrap();
    pool.close().await;
    for suffix in ["", "-shm", "-wal"] {
        let mut file = path.as_os_str().to_owned();
        file.push(suffix);
        let _ = std::fs::remove_file(file);
    }
}

/// Insert wakeups are far more frequent than leadership events. A burst of
/// them must not push a resignation request out of the elector's channel, as
/// it could when both shared one lagging broadcast channel.
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn resign_requests_survive_a_burst_of_insert_notifications() {
    let pool = sqlx::SqlitePool::connect_lazy("sqlite::memory:").unwrap();
    let client = Client::builder(pool).build().unwrap();
    let inner = &client.inner;
    // The receiver the supervisor hands to maintenance.
    let mut leadership = inner.leadership_wakeups.subscribe();
    let mut producer = inner.queue_notifications.subscribe();

    dispatch_notification(
        inner,
        &inner.queue_notifications,
        crate::NOTIFICATION_TOPIC_LEADERSHIP,
        r#"{"action":"request_resign"}"#,
    );
    for _ in 0..4_096 {
        dispatch_notification(
            inner,
            &inner.queue_notifications,
            crate::NOTIFICATION_TOPIC_INSERT,
            r#"{"queue":"default"}"#,
        );
    }

    assert!(matches!(
        leadership.try_recv(),
        Ok(LeadershipWakeup::RequestResign)
    ));
    assert!(leadership.try_recv().is_err());
    // The producers' channel lagged, which producers recover from by
    // fetching and refreshing everything.
    assert!(matches!(
        producer.try_recv(),
        Err(broadcast::error::TryRecvError::Lagged(_))
    ));
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn typed_timeouts_and_retentions_validate_at_build() {
    let pool = sqlx::SqlitePool::connect_lazy("sqlite::memory:").unwrap();
    let builder = || Client::builder(pool.clone());

    let client = builder().build().unwrap();
    assert_eq!(client.inner.job_timeout, Some(JOB_TIMEOUT_DEFAULT));
    let client = builder().without_job_timeout().build().unwrap();
    assert_eq!(client.inner.job_timeout, None);
    let client = builder()
        .job_timeout(Duration::from_secs(5))
        .build()
        .unwrap();
    assert_eq!(client.inner.job_timeout, Some(Duration::from_secs(5)));
    for error in [
        builder().job_timeout(Duration::ZERO).build().unwrap_err(),
        builder()
            .job_stuck_threshold(Duration::ZERO)
            .build()
            .unwrap_err(),
        builder()
            .soft_stop_timeout(Duration::ZERO)
            .build()
            .unwrap_err(),
    ] {
        assert!(matches!(error, Error::Configuration(_)), "{error}");
    }

    let defaults = MaintenanceConfig::default();
    assert_eq!(
        defaults.completed_job_retention(),
        Retention::DeleteAfter(Duration::from_hours(24))
    );
    let keep = defaults.with_completed_job_retention(Retention::Keep);
    assert_eq!(keep.completed_job_retention(), Retention::Keep);
    assert_eq!(keep.completed_job_retention, None);
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn ending_maintenance_abnormally_cancels_its_terms() {
    let pool = sqlx::SqlitePool::connect_lazy("sqlite::memory:").unwrap();
    let client = Client::builder(pool).build().unwrap();
    let cancel = CancellationToken::new();
    let maintenance = tokio::spawn(crate::maintenance::run_maintenance(
        Arc::clone(&client.inner),
        cancel.clone(),
        client.inner.leadership_wakeups.subscribe(),
    ));
    tokio::task::yield_now().await;
    assert!(!cancel.is_cancelled());

    // As when the task panics: its terms' services run under child tokens,
    // and the supervisor restarts it with a new token.
    maintenance.abort();
    assert!(maintenance.await.unwrap_err().is_cancelled());
    assert!(cancel.is_cancelled());
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn reindexer_timeout_is_positive_or_disabled() {
    use crate::database::{PostgresDatabase, PostgresReindexConfig};

    let pool = sqlx::PgPool::connect_lazy("postgres://localhost/unused").unwrap();
    let build = |config: PostgresReindexConfig| {
        Client::builder(PostgresDatabase::new(pool.clone()).with_reindex(config)).build()
    };

    assert_eq!(
        PostgresReindexConfig::default().timeout(),
        Some(Duration::from_mins(1))
    );
    let disabled = PostgresReindexConfig::default().without_timeout();
    assert_eq!(disabled.timeout(), None);
    build(disabled).unwrap();
    let error = build(PostgresReindexConfig::default().with_timeout(Duration::ZERO)).unwrap_err();
    assert!(matches!(error, Error::Configuration(_)), "{error}");
}
