use super::*;

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
        metadata: Map::new(),
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
