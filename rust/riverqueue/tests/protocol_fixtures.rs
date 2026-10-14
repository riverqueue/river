use std::{io::ErrorKind, path::Path, time::Duration};

use chrono::{DateTime, Utc};
use riverqueue::{
    AttemptError, DefaultRetryPolicy, JobRow, JobState, METADATA_KEY_OUTPUT,
    METADATA_KEY_PERIODIC_JOB_ID, METADATA_KEY_RESCUE_COUNT, METADATA_KEY_RESUMABLE_CURSOR,
    METADATA_KEY_RESUMABLE_STEP, METADATA_KEY_UNIQUE_NONCE, RetryPolicy,
    protocol::{
        NOTIFICATION_TOPIC_CONTROL, NOTIFICATION_TOPIC_INSERT, NOTIFICATION_TOPIC_LEADERSHIP,
        unique_state_bit,
    },
};
use serde::Deserialize;
use serde_json::{Map, Value};

#[derive(Deserialize)]
struct Fixture {
    attempt_error: AttemptError,
    job_states: Vec<StateFixture>,
    metadata_keys: Map<String, Value>,
    retry_cases: Vec<RetryFixture>,
    topics: Map<String, Value>,
}

#[derive(Deserialize)]
struct RetryFixture {
    error_count: usize,
    job_id: i64,
    max_delay_ns: u64,
    min_delay_ns: u64,
    now: DateTime<Utc>,
    seed: u64,
}

#[derive(Deserialize)]
struct StateFixture {
    state: JobState,
    unique_bit: u8,
}

/// Reads `name` from `conformance/testdata`, where `make generate/fixtures`
/// writes fixtures produced by River's Go implementation. A missing fixture
/// fails the test rather than skipping it.
fn read_fixture(name: &str) -> String {
    let path = Path::new(env!("CARGO_MANIFEST_DIR"))
        .join("../../conformance/testdata")
        .join(name);
    std::fs::read_to_string(&path).unwrap_or_else(|error| match error.kind() {
        ErrorKind::NotFound => panic!(
            "missing conformance fixture {}; run `make generate/fixtures` from the repository root",
            path.display()
        ),
        _ => panic!(
            "error reading conformance fixture {}: {error}",
            path.display()
        ),
    })
}

#[test]
fn go_protocol_values_match_rust() {
    let fixture: Fixture = serde_json::from_str(&read_fixture("protocol_values.json")).unwrap();

    assert_eq!(fixture.attempt_error.attempt, 3);
    assert!(fixture.attempt_error.error.contains("escaped"));
    assert_eq!(fixture.job_states.len(), JobState::ALL.len());
    for state in fixture.job_states {
        assert_eq!(state.unique_bit, unique_state_bit(state.state));
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
    for test_case in fixture.retry_cases {
        let row = retry_row(test_case.job_id, test_case.now, test_case.error_count - 1);
        let delay = DefaultRetryPolicy::with_seed(test_case.seed).next_retry(
            &row,
            &riverqueue::WorkError::new("fixture failure"),
            test_case.now,
        );
        let delay = delay.as_nanos();
        assert!(
            (u128::from(test_case.min_delay_ns)..=u128::from(test_case.max_delay_ns))
                .contains(&delay),
            "error count {} delay {delay}ns outside Go's bounds",
            test_case.error_count
        );
    }
}

fn retry_row(id: i64, now: DateTime<Utc>, previous_errors: usize) -> JobRow {
    let mut row = JobRow::new(
        id,
        "fixture_retry",
        riverqueue::encoding::encode_args(&serde_json::json!({})).unwrap(),
        now,
    );
    row.attempt = i32::try_from(previous_errors + 1).unwrap();
    row.attempted_at = Some(now);
    row.attempted_by = vec!["fixture".to_owned()];
    row.errors = vec![AttemptError::new(now, 1, "previous failure"); previous_errors];
    row.max_attempts = 1_000;
    row.metadata = Map::new().into();
    row.state = JobState::Retryable;
    row
}

#[test]
fn retry_duration_cap_matches_go_time_duration() {
    assert_eq!(
        Duration::from_nanos(i64::MAX as u64).as_nanos(),
        9_223_372_036_854_775_807
    );
}
