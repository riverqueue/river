//! Go-compatible unique job hashing.

use std::time::Duration;

use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::{Map, Value};
use sha2::{Digest, Sha256};

use crate::{Error, JobArgs, UniqueOpts};

const SECONDS_FROM_YEAR_ONE_TO_UNIX_EPOCH: i128 = 62_135_596_800;

/// Inputs used to construct a unique key.
pub struct UniqueKeyInput<'a, A> {
    /// Typed job arguments.
    pub args: &'a A,
    /// Encoded arguments generated for insertion.
    pub encoded_args: &'a Value,
    /// Job insertion options.
    pub opts: &'a UniqueOpts,
    /// Queue name.
    pub queue: &'a str,
    /// Scheduled time or current time when absent.
    pub scheduled_at: Option<DateTime<Utc>>,
    /// Injectable current time.
    pub now: DateTime<Utc>,
}

/// Builds the SHA-256 key used by River's Go implementation.
pub fn build_unique_key<A: JobArgs>(
    input: &UniqueKeyInput<'_, A>,
) -> Result<Option<[u8; 32]>, Error> {
    build_unique_key_parts(
        A::KIND,
        A::unique_fields(),
        input.encoded_args,
        input.now,
        input.opts,
        input.queue,
        input.scheduled_at,
    )
}

pub(crate) fn build_unique_key_parts(
    kind: &str,
    unique_fields: &[&str],
    encoded_args: &Value,
    now: DateTime<Utc>,
    opts: &UniqueOpts,
    queue: &str,
    scheduled_at: Option<DateTime<Utc>>,
) -> Result<Option<[u8; 32]>, Error> {
    if opts.is_empty() {
        return Ok(None);
    }
    opts.validate().map_err(Error::invalid_job)?;

    let mut key = String::new();
    if !opts.exclude_kind {
        key.push_str("&kind=");
        key.push_str(kind);
    }
    if opts.by_args {
        key.push_str("&args=");
        let args = select_unique_args(encoded_args, unique_fields)?;
        key.push_str(&go_compatible_json(&args)?);
    }
    if let Some(period) = opts.by_period {
        key.push_str("&period=");
        let scheduled_at = scheduled_at.unwrap_or(now);
        key.push_str(
            &truncate_period(scheduled_at, period)?.to_rfc3339_opts(SecondsFormat::Secs, true),
        );
    }
    if opts.by_queue {
        key.push_str("&queue=");
        key.push_str(queue);
    }

    Ok(Some(Sha256::digest(key.as_bytes()).into()))
}

fn go_compatible_json(value: &Value) -> Result<String, Error> {
    let mut output = String::new();
    write_go_compatible_json(value, &mut output)?;
    Ok(output)
}

fn write_go_compatible_json(value: &Value, output: &mut String) -> Result<(), Error> {
    match value {
        Value::Null => output.push_str("null"),
        Value::Bool(value) => output.push_str(if *value { "true" } else { "false" }),
        Value::Number(value) => {
            let encoded = value.to_string();
            if encoded_json_number_is_zero(&encoded)
                && value.as_f64().is_some_and(f64::is_sign_negative)
            {
                output.push_str("-0");
            } else {
                output.push_str(&encoded);
            }
        }
        Value::String(value) => output.push_str(&go_compatible_json_string(value)?),
        Value::Array(values) => {
            output.push('[');
            for (index, value) in values.iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                write_go_compatible_json(value, output)?;
            }
            output.push(']');
        }
        Value::Object(values) => {
            output.push('{');
            let mut entries = values.iter().collect::<Vec<_>>();
            entries.sort_unstable_by_key(|(key, _)| *key);
            for (index, (key, value)) in entries.into_iter().enumerate() {
                if index > 0 {
                    output.push(',');
                }
                output.push_str(&go_compatible_json_string(key)?);
                output.push(':');
                write_go_compatible_json(value, output)?;
            }
            output.push('}');
        }
    }
    Ok(())
}

fn encoded_json_number_is_zero(encoded: &str) -> bool {
    let coefficient = encoded
        .strip_prefix('-')
        .unwrap_or(encoded)
        .split_once(['e', 'E'])
        .map_or(
            encoded.strip_prefix('-').unwrap_or(encoded),
            |(value, _)| value,
        );
    coefficient.bytes().all(|byte| matches!(byte, b'0' | b'.'))
}

fn go_compatible_json_string(value: &str) -> Result<String, Error> {
    Ok(serde_json::to_string(value)?
        .replace('<', "\\u003c")
        .replace('>', "\\u003e")
        .replace('&', "\\u0026")
        .replace('\u{2028}', "\\u2028")
        .replace('\u{2029}', "\\u2029"))
}

fn select_unique_args(value: &Value, paths: &[&str]) -> Result<Value, Error> {
    let object = value.as_object().ok_or_else(|| {
        Error::invalid_job_context(
            "job uniqueness",
            "job arguments must encode to a JSON object".to_owned(),
        )
    })?;
    if paths.is_empty() {
        let sorted = object
            .iter()
            .map(|(key, value)| (key.clone(), value.clone()))
            .collect::<Map<_, _>>();
        return Ok(Value::Object(sorted));
    }

    let mut selected = Map::new();
    for path in paths {
        if let Some(value) = value.pointer(&format!("/{}", path.replace('.', "/"))) {
            insert_path(&mut selected, path, value.clone());
        }
    }
    Ok(Value::Object(selected))
}

fn insert_path(target: &mut Map<String, Value>, path: &str, value: Value) {
    let mut segments = path.split('.');
    let Some(first) = segments.next() else {
        return;
    };
    let remainder = segments.collect::<Vec<_>>();
    if remainder.is_empty() {
        target.insert(first.to_owned(), value);
        return;
    }

    let child = target
        .entry(first.to_owned())
        .or_insert_with(|| Value::Object(Map::new()));
    if let Value::Object(child) = child {
        insert_path(child, &remainder.join("."), value);
    }
}

fn truncate_period(timestamp: DateTime<Utc>, period: Duration) -> Result<DateTime<Utc>, Error> {
    let period_nanos = i128::try_from(period.as_nanos()).map_err(|_| {
        Error::invalid_job_context("job uniqueness", "unique period is too large".to_owned())
    })?;
    let unix_nanos = i128::from(timestamp.timestamp()) * 1_000_000_000
        + i128::from(timestamp.timestamp_subsec_nanos());
    let absolute_nanos = unix_nanos + SECONDS_FROM_YEAR_ONE_TO_UNIX_EPOCH * 1_000_000_000;
    let truncated_absolute = absolute_nanos - absolute_nanos.rem_euclid(period_nanos);
    let truncated_unix = truncated_absolute - SECONDS_FROM_YEAR_ONE_TO_UNIX_EPOCH * 1_000_000_000;
    let seconds = i64::try_from(truncated_unix.div_euclid(1_000_000_000)).map_err(|_| {
        Error::invalid_job_context(
            "job uniqueness",
            "truncated timestamp is out of range".to_owned(),
        )
    })?;
    let nanos = u32::try_from(truncated_unix.rem_euclid(1_000_000_000)).map_err(|_| {
        Error::invalid_job_context(
            "job uniqueness",
            "truncated timestamp is out of range".to_owned(),
        )
    })?;
    DateTime::from_timestamp(seconds, nanos).ok_or_else(|| {
        Error::invalid_job_context(
            "job uniqueness",
            "truncated timestamp is out of range".to_owned(),
        )
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::JobArgs as JobArgsDerive;
    use chrono::TimeZone;
    use serde::{Deserialize, Serialize};

    #[derive(Deserialize)]
    struct Fixture {
        cases: Vec<FixtureCase>,
        protocol_revision: u32,
    }

    #[derive(Deserialize)]
    struct FixtureCase {
        args: Value,
        expected_sha256: String,
        expected_state_mask: u8,
        kind: String,
        name: String,
        now: DateTime<Utc>,
        options: FixtureOptions,
        queue: String,
        scheduled_at: Option<DateTime<Utc>>,
        selected_unique_paths: Option<Vec<String>>,
    }

    #[derive(Deserialize)]
    struct FixtureOptions {
        by_args: bool,
        by_period_nanos: u64,
        by_queue: bool,
        by_state: Option<Vec<crate::JobState>>,
        exclude_kind: bool,
    }

    #[derive(Deserialize, JobArgsDerive, Serialize)]
    #[river(kind = "unique_test")]
    struct Args {
        ignored: String,
        #[river(unique)]
        #[serde(rename = "selected_value")]
        selected: String,
    }

    #[derive(Deserialize, JobArgsDerive, Serialize)]
    #[river(kind = "optional_unique_test")]
    struct ArgsWithOptionalUnique {
        #[river(unique)]
        #[serde(skip_serializing_if = "Option::is_none")]
        selected: Option<String>,
    }

    #[derive(Deserialize, JobArgsDerive, Serialize)]
    #[serde(rename_all(serialize = "kebab-case", deserialize = "camelCase"))]
    #[river(kind = "serialize_name_test")]
    struct ArgsWithSerializationNames {
        #[river(unique)]
        first_value: String,
        #[river(unique)]
        #[serde(rename(serialize = "wire-name", deserialize = "inputName"))]
        r#type: String,
    }

    #[derive(Deserialize, JobArgsDerive, Serialize)]
    #[river(
        kind = "new_kind",
        aliases("old_kind", "older_kind"),
        max_attempts = 7,
        pending = true,
        priority = 3,
        queue = "critical_jobs"
    )]
    struct ArgsWithDefaults {}

    #[test]
    fn hashes_selected_args_with_go_json_escaping() {
        let args = Args {
            ignored: "ignored".to_owned(),
            selected: "<selected>".to_owned(),
        };
        let encoded = serde_json::to_value(&args).unwrap();
        let key = build_unique_key(&UniqueKeyInput {
            args: &args,
            encoded_args: &encoded,
            now: Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap(),
            opts: &UniqueOpts {
                by_args: true,
                ..UniqueOpts::default()
            },
            queue: "default",
            scheduled_at: None,
        })
        .unwrap()
        .unwrap();

        let expected = Sha256::digest(
            b"&kind=unique_test&args={\"selected_value\":\"\\u003cselected\\u003e\"}",
        );
        assert_eq!(key.as_slice(), expected.as_slice());
    }

    #[test]
    fn hashes_serde_serialization_names() {
        let args = ArgsWithSerializationNames {
            first_value: "first".to_owned(),
            r#type: "second".to_owned(),
        };
        let encoded = serde_json::to_value(&args).unwrap();
        let key = build_unique_key(&UniqueKeyInput {
            args: &args,
            encoded_args: &encoded,
            now: Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap(),
            opts: &UniqueOpts {
                by_args: true,
                ..UniqueOpts::default()
            },
            queue: "default",
            scheduled_at: None,
        })
        .unwrap()
        .unwrap();

        let expected = Sha256::digest(
            b"&kind=serialize_name_test&args={\"first-value\":\"first\",\"wire-name\":\"second\"}",
        );
        assert_eq!(key.as_slice(), expected.as_slice());
    }

    #[test]
    fn hashes_absent_optional_unique_fields_as_omitted() {
        let args = ArgsWithOptionalUnique { selected: None };
        let encoded = serde_json::to_value(&args).unwrap();
        let key = build_unique_key(&UniqueKeyInput {
            args: &args,
            encoded_args: &encoded,
            now: Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap(),
            opts: &UniqueOpts {
                by_args: true,
                ..UniqueOpts::default()
            },
            queue: "default",
            scheduled_at: None,
        })
        .unwrap()
        .unwrap();

        let expected = Sha256::digest(b"&kind=optional_unique_test&args={}");
        assert_eq!(key.as_slice(), expected.as_slice());
    }

    #[test]
    fn job_args_derive_provides_aliases_and_insert_defaults() {
        assert_eq!(ArgsWithDefaults::kind_aliases(), ["old_kind", "older_kind"]);
        let opts = ArgsWithDefaults::default_insert_opts();
        assert_eq!(opts.max_attempts(), Some(7));
        assert_eq!(opts.pending(), Some(true));
        assert_eq!(opts.priority(), Some(3));
        assert_eq!(opts.queue(), Some("critical_jobs"));
    }

    #[test]
    fn matches_go_generated_golden_keys() {
        // `serde_json`'s arbitrary-precision parser normalizes the integer
        // token `-0` to `0`. Typed Rust arguments retain negative zero, so keep
        // the fixture on that production path while preserving its sign.
        let fixture_source = include_str!("../../../conformance/fixtures/unique_keys.json")
            .replace("\"zero\": -0,", "\"zero\": -0.0,");
        let fixture: Fixture = serde_json::from_str(&fixture_source).unwrap();
        assert_eq!(fixture.protocol_revision, 1);

        for case in fixture.cases {
            let unique_paths = case.selected_unique_paths.unwrap_or_default();
            let unique_path_refs = unique_paths.iter().map(String::as_str).collect::<Vec<_>>();
            let opts = UniqueOpts {
                by_args: case.options.by_args,
                by_period: (case.options.by_period_nanos > 0)
                    .then(|| Duration::from_nanos(case.options.by_period_nanos)),
                by_queue: case.options.by_queue,
                by_state: case.options.by_state,
                exclude_kind: case.options.exclude_kind,
            };
            let actual = build_unique_key_parts(
                &case.kind,
                &unique_path_refs,
                &case.args,
                case.now,
                &opts,
                &case.queue,
                case.scheduled_at,
            )
            .unwrap()
            .unwrap();
            assert_eq!(
                hex_to_bytes(&case.expected_sha256),
                actual,
                "fixture {}",
                case.name
            );
            assert_eq!(
                case.expected_state_mask,
                opts.state_bitmask(),
                "fixture {}",
                case.name
            );
        }
    }

    #[test]
    fn preserves_negative_zero_for_go_json() {
        let value = serde_json::to_value(-0.0_f64).unwrap();
        assert_eq!(go_compatible_json(&value).unwrap(), "-0");
    }

    fn hex_to_bytes(encoded: &str) -> [u8; 32] {
        assert_eq!(encoded.len(), 64);
        let mut decoded = [0_u8; 32];
        for (index, byte) in decoded.iter_mut().enumerate() {
            *byte = u8::from_str_radix(&encoded[index * 2..index * 2 + 2], 16).unwrap();
        }
        decoded
    }

    #[test]
    fn state_bitmask_matches_postgres_function() {
        assert_eq!(UniqueOpts::default().state_bitmask(), 0b1111_0101);
    }

    #[test]
    fn truncates_from_go_time_zero() {
        let timestamp = Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();
        let truncated = truncate_period(timestamp, Duration::from_mins(1)).unwrap();
        assert_eq!(
            truncated,
            Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 0).unwrap()
        );
    }
}
