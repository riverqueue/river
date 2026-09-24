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
        assert_eq!(persisted_completion_event_kind(state, requested), expected);
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
}
