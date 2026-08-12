use std::time::Duration;

use chrono::{DateTime, Utc};
use riverqueue::{
    AttemptError, DefaultRetryPolicy, JobRow, JobState, METADATA_KEY_OUTPUT,
    METADATA_KEY_PERIODIC_JOB_ID, METADATA_KEY_RESCUE_COUNT, METADATA_KEY_RESUMABLE_CURSOR,
    METADATA_KEY_RESUMABLE_STEP, METADATA_KEY_UNIQUE_NONCE, NOTIFICATION_TOPIC_CONTROL,
    NOTIFICATION_TOPIC_INSERT, NOTIFICATION_TOPIC_LEADERSHIP, RetryPolicy,
};
use serde::Deserialize;
use serde_json::{Map, Value};

#[derive(Deserialize)]
struct Fixture {
    attempt_error: AttemptError,
    job_states: Vec<StateFixture>,
    metadata_keys: Map<String, Value>,
    notifications: Vec<NotificationFixture>,
    retry_cases: Vec<RetryFixture>,
    topics: Map<String, Value>,
}

#[derive(Deserialize)]
struct NotificationFixture {
    name: String,
    payload: Map<String, Value>,
    topic: String,
}

#[derive(Deserialize)]
struct RetryFixture {
    error_count: usize,
    expected_delay_ns: u64,
    job_id: i64,
    now: DateTime<Utc>,
    seed: u64,
}

#[derive(Deserialize)]
struct StateFixture {
    state: JobState,
    unique_bit: u8,
}

#[test]
fn go_protocol_values_match_rust() {
    let fixture: Fixture = serde_json::from_str(include_str!(
        "../../../conformance/fixtures/protocol_values.json"
    ))
    .unwrap();

    assert_eq!(fixture.attempt_error.attempt, 3);
    assert!(fixture.attempt_error.error.contains("escaped"));
    assert_eq!(fixture.job_states.len(), JobState::ALL.len());
    for state in fixture.job_states {
        assert_eq!(state.unique_bit, state.state.unique_bit());
    }
    for (name, expected) in [
        ("output", METADATA_KEY_OUTPUT),
        ("periodic_job_id", METADATA_KEY_PERIODIC_JOB_ID),
        ("rescue_count", METADATA_KEY_RESCUE_COUNT),
        ("resumable_cursor", METADATA_KEY_RESUMABLE_CURSOR),
        ("resumable_step", METADATA_KEY_RESUMABLE_STEP),
        ("unique_nonce", METADATA_KEY_UNIQUE_NONCE),
    ] {
        assert_eq!(fixture.metadata_keys[name], expected);
    }
    assert_eq!(fixture.topics["control"], NOTIFICATION_TOPIC_CONTROL);
    assert_eq!(fixture.topics["insert"], NOTIFICATION_TOPIC_INSERT);
    assert_eq!(fixture.topics["leadership"], NOTIFICATION_TOPIC_LEADERSHIP);
    for notification in fixture.notifications {
        assert!(!notification.name.is_empty());
        assert!(notification.payload.contains_key("action") || notification.name == "insert");
        assert!(
            [
                NOTIFICATION_TOPIC_CONTROL,
                NOTIFICATION_TOPIC_INSERT,
                NOTIFICATION_TOPIC_LEADERSHIP,
            ]
            .contains(&notification.topic.as_str())
        );
    }

    for test_case in fixture.retry_cases {
        let row = retry_row(test_case.job_id, test_case.now, test_case.error_count - 1);
        let delay = DefaultRetryPolicy::with_seed(test_case.seed).next_retry(
            &row,
            "fixture failure",
            test_case.now,
        );
        assert_eq!(delay.as_nanos(), u128::from(test_case.expected_delay_ns));
    }
}

fn retry_row(id: i64, now: DateTime<Utc>, previous_errors: usize) -> JobRow {
    JobRow {
        attempt: i16::try_from(previous_errors + 1).unwrap(),
        attempted_at: Some(now),
        attempted_by: vec!["fixture".to_owned()],
        created_at: now,
        encoded_args: serde_json::json!({}),
        errors: vec![
            AttemptError {
                at: now,
                attempt: 1,
                error: "previous failure".to_owned(),
                trace: String::new(),
            };
            previous_errors
        ],
        finalized_at: None,
        id,
        kind: "fixture_retry".to_owned(),
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
fn retry_duration_cap_matches_go_time_duration() {
    assert_eq!(
        Duration::from_nanos(i64::MAX as u64).as_nanos(),
        9_223_372_036_854_775_807
    );
}
