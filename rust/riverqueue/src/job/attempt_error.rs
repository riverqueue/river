//! Lenient decoding of persisted attempt errors.
//!
//! This follows River Go's `AttemptError.UnmarshalJSON`. River always writes
//! attempt errors in the shape [`AttemptError`] serializes to, but elements
//! written by other tools or edited by hand might not match it. Because a job
//! row can't be read or worked unless every one of its attempt errors
//! decodes, any element that's valid JSON decodes on a best effort basis
//! instead of failing.

use std::{borrow::Cow, fmt};

use chrono::{DateTime, NaiveDate, Utc};
use serde::{
    Deserialize, Deserializer,
    de::{self, MapAccess, Visitor},
};
use serde_json::value::RawValue;

use super::AttemptError;
use crate::client::saturating_i16;

impl<'de> Deserialize<'de> for AttemptError {
    /// Decodes an attempt error leniently, like River Go.
    ///
    /// Only a JSON deserializer (such as [`serde_json`]'s) can decode an
    /// attempt error, since fields in an unexpected shape are kept as their
    /// JSON text.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        Self::from_json_lenient(raw.get()).map_err(de::Error::custom)
    }
}

impl AttemptError {
    /// Decodes one persisted attempt error exactly like River Go's
    /// `AttemptError.UnmarshalJSON`.
    ///
    /// Elements in the shape River writes decode as they would with Go's
    /// `encoding/json` defaults: fields match case-insensitively, a repeated
    /// field takes its last value, and missing, `null`, and unknown fields are
    /// accepted. Any other valid JSON decodes on a best effort basis:
    ///
    /// * `at` accepts RFC 3339 timestamps, along with timestamps that use a
    ///   space instead of `T`, a numeric UTC offset without a colon or
    ///   minutes (as in PostgreSQL's text output), or no offset at all (taken
    ///   to be UTC). Any other value leaves Go's zero time.
    /// * `attempt` accepts integers, numbers with an integral value, and
    ///   strings containing either. Any other value leaves zero.
    /// * `error` and `trace` accept strings. Any other non-null value is kept
    ///   as its compacted JSON text.
    /// * An element that's a JSON string instead of an object is used as
    ///   `error`, and any other element that isn't an object is kept as its
    ///   JSON text in `error`.
    ///
    /// Only text that isn't valid JSON is an error.
    pub(crate) fn from_json_lenient(json: &str) -> Result<Self, serde_json::Error> {
        let json = json.trim_matches(is_json_whitespace);
        if json.starts_with('{') {
            let fields: LenientFields = serde_json::from_str(json)?;
            return Ok(Self {
                at: fields
                    .at
                    .map_or_else(go_zero_time, |raw| lenient_time(raw.get())),
                attempt: fields.attempt.map_or(0, |raw| lenient_attempt(raw.get())),
                error: fields
                    .error
                    .map(|raw| lenient_string(raw.get()))
                    .unwrap_or_default(),
                trace: fields
                    .trace
                    .map(|raw| lenient_string(raw.get()))
                    .unwrap_or_default(),
            });
        }

        // Valid JSON, but not an object. `null` leaves every field empty.
        let raw: Box<RawValue> = serde_json::from_str(json)?;
        Ok(Self::new(go_zero_time(), 0, lenient_string(raw.get())))
    }
}

/// Go's zero `time.Time`, which Go leaves in an attempt error without a
/// usable `at`.
fn go_zero_time() -> DateTime<Utc> {
    NaiveDate::from_ymd_opt(1, 1, 1)
        .and_then(|date| date.and_hms_opt(0, 0, 0))
        .expect("Go's zero time is a valid date")
        .and_utc()
}

/// An attempt error object's fields as raw JSON, matched the way Go's
/// `encoding/json` matches struct fields.
#[derive(Default)]
struct LenientFields {
    at: Option<Box<RawValue>>,
    attempt: Option<Box<RawValue>>,
    error: Option<Box<RawValue>>,
    trace: Option<Box<RawValue>>,
}

impl<'de> Deserialize<'de> for LenientFields {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FieldsVisitor;

        impl<'de> Visitor<'de> for FieldsVisitor {
            type Value = LenientFields;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an attempt error object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut fields = LenientFields::default();
                while let Some(key) = map.next_key::<String>()? {
                    let value = map.next_value::<Box<RawValue>>()?;
                    // Go folds case when matching field names. Only ASCII
                    // letters fold to the letters of these four names.
                    let field = if key.eq_ignore_ascii_case("at") {
                        &mut fields.at
                    } else if key.eq_ignore_ascii_case("attempt") {
                        &mut fields.attempt
                    } else if key.eq_ignore_ascii_case("error") {
                        &mut fields.error
                    } else if key.eq_ignore_ascii_case("trace") {
                        &mut fields.trace
                    } else {
                        continue;
                    };
                    *field = Some(value);
                }
                Ok(fields)
            }
        }

        deserializer.deserialize_map(FieldsVisitor)
    }
}

fn is_json_whitespace(character: char) -> bool {
    matches!(character, ' ' | '\t' | '\n' | '\r')
}

/// The largest magnitude up to which every integer is exactly representable
/// as an `f64`, 2^53.
const MAX_EXACT_FLOAT_INTEGER: f64 = 9_007_199_254_740_992.0;

/// Decodes `attempt` like Go: integers, and numbers or numeric strings with an
/// integral value no larger in magnitude than 2^53. `attempt` is narrower
/// than Go's `int`, so values beyond `i16` saturate like other persisted
/// attempt counts. Unlike Go, a string in hexadecimal floating point notation
/// (such as `"0x1p4"`) isn't recognized and decodes as zero.
#[allow(
    clippy::float_cmp,
    reason = "an exact comparison checks for an integral value"
)]
fn lenient_attempt(raw: &str) -> i16 {
    let number: Cow<'_, str> = if raw.starts_with('"') {
        match serde_json::from_str::<String>(raw) {
            Ok(text) => Cow::Owned(text.trim().to_owned()),
            Err(_) => return 0,
        }
    } else if raw.starts_with(|character: char| character == '-' || character.is_ascii_digit()) {
        Cow::Borrowed(raw)
    } else {
        return 0;
    };

    if let Ok(integer) = number.parse::<i64>() {
        return saturating_i16(integer);
    }
    match number.parse::<f64>() {
        Ok(float) if float == float.trunc() && float.abs() <= MAX_EXACT_FLOAT_INTEGER =>
        {
            #[expect(
                clippy::cast_possible_truncation,
                reason = "the float is integral and within the exact integer range"
            )]
            saturating_i16(float as i64)
        }
        _ => 0,
    }
}

/// Decodes `error` and `trace` like Go: a string is used as is, `null` is
/// empty, and any other value is kept as its compacted JSON text.
fn lenient_string(raw: &str) -> String {
    if raw == "null" {
        return String::new();
    }
    if raw.starts_with('"')
        && let Ok(text) = serde_json::from_str::<String>(raw)
    {
        return text;
    }
    compact_json(raw)
}

/// Removes insignificant whitespace from valid JSON text without otherwise
/// changing it, like Go's `json.Compact`.
fn compact_json(raw: &str) -> String {
    let mut compacted = String::with_capacity(raw.len());
    let mut in_string = false;
    let mut escaped = false;
    for character in raw.chars() {
        if in_string {
            compacted.push(character);
            if escaped {
                escaped = false;
            } else if character == '\\' {
                escaped = true;
            } else if character == '"' {
                in_string = false;
            }
        } else if !is_json_whitespace(character) {
            in_string = character == '"';
            compacted.push(character);
        }
    }
    compacted
}

/// Decodes `at` like Go, which leaves the zero time for anything other than a
/// string in one of the accepted layouts.
fn lenient_time(raw: &str) -> DateTime<Utc> {
    serde_json::from_str::<String>(raw)
        .ok()
        .and_then(|text| parse_go_time(text.trim()))
        .unwrap_or_else(go_zero_time)
}

/// Parses a timestamp in any of the layouts River Go accepts for an attempt
/// error's `at`, with the same validation as Go's `time.Parse`:
///
/// `YYYY-MM-DD`, `T` or a space, a one or two digit hour, `:MM:SS`, an
/// optional fraction introduced by `.` or `,` (digits past nanoseconds are
/// ignored), and then `Z`, `±hh:mm`, `±hhmm`, `±hh`, or nothing for UTC.
/// Offsets may be as large as Go allows, 24 hours and 60 minutes.
fn parse_go_time(text: &str) -> Option<DateTime<Utc>> {
    let mut parser = TimeParser(text.as_bytes());
    let year = parser.digits(4)?;
    parser.expect(b"-")?;
    let month = parser.digits(2)?;
    parser.expect(b"-")?;
    let day = parser.digits(2)?;
    parser.expect(b"T").or_else(|| parser.expect(b" "))?;
    // Go's `15` hour takes one digit when a second one doesn't follow.
    let hour = parser.digits(2).or_else(|| parser.digits(1))?;
    parser.expect(b":")?;
    let minute = parser.digits(2)?;
    parser.expect(b":")?;
    let second = parser.digits(2)?;
    let nanosecond = parser.fraction();
    let offset_seconds = parser.offset()?;
    if !parser.0.is_empty() || hour > 23 || minute > 59 || second > 59 {
        return None;
    }

    let local = NaiveDate::from_ymd_opt(i32::try_from(year).ok()?, month, day)?
        .and_hms_nano_opt(hour, minute, second, nanosecond)?
        .and_utc();
    local.checked_sub_signed(chrono::Duration::seconds(offset_seconds))
}

/// The unparsed remainder of a timestamp.
struct TimeParser<'a>(&'a [u8]);

impl TimeParser<'_> {
    /// Consumes exactly `count` ASCII digits.
    fn digits(&mut self, count: usize) -> Option<u32> {
        let digits = self.0.get(..count)?;
        if !digits.iter().all(u8::is_ascii_digit) {
            return None;
        }
        self.0 = &self.0[count..];
        Some(
            digits
                .iter()
                .fold(0, |value, digit| value * 10 + u32::from(digit - b'0')),
        )
    }

    fn expect(&mut self, literal: &[u8]) -> Option<()> {
        self.0 = self.0.strip_prefix(literal)?;
        Some(())
    }

    /// Consumes an optional fractional second, returning nanoseconds.
    fn fraction(&mut self) -> u32 {
        let [b'.' | b',', first, ..] = self.0 else {
            return 0;
        };
        if !first.is_ascii_digit() {
            return 0;
        }
        let digit_count = self.0[1..]
            .iter()
            .take_while(|byte| byte.is_ascii_digit())
            .count();
        let digits = &self.0[1..=digit_count];
        self.0 = &self.0[1 + digit_count..];
        digits
            .iter()
            .chain(std::iter::repeat(&b'0'))
            .take(9)
            .fold(0, |nanoseconds, digit| {
                nanoseconds * 10 + u32::from(digit - b'0')
            })
    }

    /// Consumes an optional UTC offset, returning it in seconds east of UTC.
    fn offset(&mut self) -> Option<i64> {
        let sign = match self.0.first() {
            None => return Some(0),
            Some(b'Z') => {
                self.0 = &self.0[1..];
                return Some(0);
            }
            Some(b'+') => 1,
            Some(b'-') => -1,
            Some(_) => return None,
        };
        self.0 = &self.0[1..];
        let hours = self.digits(2)?;
        let minutes = if self.expect(b":").is_some() {
            self.digits(2)?
        } else {
            self.digits(2).unwrap_or(0)
        };
        if hours > 24 || minutes > 60 {
            return None;
        }
        Some(sign * (i64::from(hours) * 3_600 + i64::from(minutes) * 60))
    }
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, NaiveDate, Utc};

    use super::{AttemptError, go_zero_time};

    fn attempt_at() -> DateTime<Utc> {
        NaiveDate::from_ymd_opt(2024, 1, 2)
            .and_then(|date| date.and_hms_micro_opt(3, 4, 5, 123_456))
            .unwrap()
            .and_utc()
    }

    fn attempt_error(at: DateTime<Utc>, attempt: i16, error: &str, trace: &str) -> AttemptError {
        AttemptError::new(at, attempt, error).with_trace(trace)
    }

    #[test]
    fn invalid_json_is_an_error() {
        assert!(AttemptError::from_json_lenient(r#"{"at":"#).is_err());
        assert!(serde_json::from_str::<AttemptError>(r#"{"at":"#).is_err());
    }

    // The cases of River Go's `TestAttemptError_UnmarshalJSON/Lenient`.
    #[test]
    fn lenient_like_go() {
        let zero = go_zero_time();
        for (name, json, expected) in [
            (
                "AtInvalid",
                r#"{"at":"not a time","attempt":2,"error":"err"}"#,
                attempt_error(zero, 2, "err", ""),
            ),
            (
                "AtNoOffset",
                r#"{"at":"2024-01-02T03:04:05.123456","attempt":2}"#,
                attempt_error(attempt_at(), 2, "", ""),
            ),
            (
                "AtNumber",
                r#"{"at":1704164645,"attempt":2}"#,
                attempt_error(zero, 2, "", ""),
            ),
            (
                "AtPostgresText",
                r#"{"at":"2024-01-02 03:04:05.123456+00","attempt":2}"#,
                attempt_error(attempt_at(), 2, "", ""),
            ),
            (
                "AtSpaceNoOffset",
                r#"{"at":"2024-01-02 03:04:05.123456","attempt":2}"#,
                attempt_error(attempt_at(), 2, "", ""),
            ),
            (
                "AttemptFloat",
                r#"{"attempt":3.0,"error":"err"}"#,
                attempt_error(zero, 3, "err", ""),
            ),
            (
                "AttemptFractional",
                r#"{"attempt":3.5,"error":"err"}"#,
                attempt_error(zero, 0, "err", ""),
            ),
            (
                "AttemptObject",
                r#"{"attempt":{},"error":"err"}"#,
                attempt_error(zero, 0, "err", ""),
            ),
            (
                "AttemptString",
                r#"{"attempt":" 3 ","error":"err"}"#,
                attempt_error(zero, 3, "err", ""),
            ),
            (
                "AttemptStringInvalid",
                r#"{"attempt":"three","error":"err"}"#,
                attempt_error(zero, 0, "err", ""),
            ),
            (
                "ElementArray",
                r#"[1, "two"]"#,
                attempt_error(zero, 0, r#"[1,"two"]"#, ""),
            ),
            ("ElementNumber", "123", attempt_error(zero, 0, "123", "")),
            (
                "ElementString",
                r#""job failed""#,
                attempt_error(zero, 0, "job failed", ""),
            ),
            (
                "ErrorObject",
                r#"{"attempt":1,"error":{"message": "boom", "code": 7}}"#,
                attempt_error(zero, 1, r#"{"message":"boom","code":7}"#, ""),
            ),
            (
                "TraceArray",
                r#"{"attempt":1,"error":"err","trace":["frame1", "frame2"]}"#,
                attempt_error(zero, 1, "err", r#"["frame1","frame2"]"#),
            ),
            (
                "TraceNullWithInvalidField",
                r#"{"attempt":"x","error":null,"trace":null}"#,
                attempt_error(zero, 0, "", ""),
            ),
        ] {
            assert_eq!(
                serde_json::from_str::<AttemptError>(json).unwrap(),
                expected,
                "{name}"
            );
        }
    }

    fn assert_decodes<const N: usize>(cases: [(&str, &str, AttemptError); N]) {
        for (name, json, expected) in cases {
            assert_eq!(
                serde_json::from_str::<AttemptError>(json).unwrap(),
                expected,
                "{name}"
            );
        }
    }

    fn whole_second() -> DateTime<Utc> {
        NaiveDate::from_ymd_opt(2024, 1, 2)
            .and_then(|date| date.and_hms_opt(3, 4, 5))
            .unwrap()
            .and_utc()
    }

    // Behavior of Go's `encoding/json` that the Go test cases don't reach.
    #[test]
    fn lenient_fields_edges_like_go() {
        let zero = go_zero_time();
        let whole = whole_second();
        assert_decodes([
            (
                "CaseInsensitiveLastWins",
                r#"{"error":"first","ERROR":"second","Attempt":2}"#,
                attempt_error(zero, 2, "second", ""),
            ),
            (
                "EscapedKey",
                r#"{"at":"2024-01-02T03:04:05Z"}"#,
                attempt_error(whole, 0, "", ""),
            ),
            ("ElementNull", "null", attempt_error(zero, 0, "", "")),
            ("ElementTrue", "true", attempt_error(zero, 0, "true", "")),
            (
                "ErrorKeepsNumberAndEscapeText",
                r#"{"error":{"n": 1.50, "s": "a \"b\"\n c"}}"#,
                attempt_error(zero, 0, r#"{"n":1.50,"s":"a \"b\"\n c"}"#, ""),
            ),
            (
                "AttemptExponentString",
                r#"{"attempt":"1e1"}"#,
                attempt_error(zero, 10, "", ""),
            ),
            (
                "AttemptSignedString",
                r#"{"attempt":"+4"}"#,
                attempt_error(zero, 4, "", ""),
            ),
            (
                "AttemptBeyondExactFloat",
                r#"{"attempt":1e20}"#,
                attempt_error(zero, 0, "", ""),
            ),
            (
                "AttemptSaturates",
                r#"{"attempt":40000}"#,
                attempt_error(zero, i16::MAX, "", ""),
            ),
            (
                "AttemptBool",
                r#"{"attempt":true}"#,
                attempt_error(zero, 0, "", ""),
            ),
        ]);
    }

    // Behavior of Go's `time.Parse` with the accepted layouts that the Go test
    // cases don't reach.
    #[test]
    fn lenient_time_edges_like_go() {
        let zero = go_zero_time();
        let whole = whole_second();
        assert_decodes([
            (
                "AtOneDigitHour",
                r#"{"at":"2024-01-02T3:04:05Z"}"#,
                attempt_error(whole, 0, "", ""),
            ),
            (
                "AtCommaFraction",
                r#"{"at":"2024-01-02T03:04:05,123456Z"}"#,
                attempt_error(attempt_at(), 0, "", ""),
            ),
            (
                "AtFractionBeyondNanoseconds",
                r#"{"at":" 2024-01-02T03:04:05.1234560009Z "}"#,
                attempt_error(attempt_at(), 0, "", ""),
            ),
            (
                "AtOffsetWithoutColon",
                r#"{"at":"2024-01-02T05:34:05+0230"}"#,
                attempt_error(whole, 0, "", ""),
            ),
            (
                "AtOffsetWithColon",
                r#"{"at":"2024-01-02 00:04:05-03:00"}"#,
                attempt_error(whole, 0, "", ""),
            ),
            (
                "AtOffsetLargestGoAccepts",
                r#"{"at":"2024-01-03T04:04:05+24:60"}"#,
                attempt_error(whole, 0, "", ""),
            ),
            (
                "AtOffsetHoursOnly",
                r#"{"at":"2024-01-02T08:04:05+05"}"#,
                attempt_error(whole, 0, "", ""),
            ),
            (
                "AtOffsetHourOutOfRange",
                r#"{"at":"2024-01-02T03:04:05+25:00"}"#,
                attempt_error(zero, 0, "", ""),
            ),
            (
                "AtInvalidDay",
                r#"{"at":"2023-02-29T03:04:05Z"}"#,
                attempt_error(zero, 0, "", ""),
            ),
            (
                "AtLeapSecond",
                r#"{"at":"2024-01-02T03:04:60Z"}"#,
                attempt_error(zero, 0, "", ""),
            ),
            (
                "AtTrailingText",
                r#"{"at":"2024-01-02T03:04:05Zjunk"}"#,
                attempt_error(zero, 0, "", ""),
            ),
            (
                "AtLowercaseSeparator",
                r#"{"at":"2024-01-02t03:04:05Z"}"#,
                attempt_error(zero, 0, "", ""),
            ),
        ]);
    }

    #[test]
    fn serializes_at_like_go() {
        let at = NaiveDate::from_ymd_opt(2024, 1, 2)
            .unwrap()
            .and_hms_opt(3, 4, 5)
            .unwrap()
            .and_utc()
            + chrono::Duration::nanoseconds(678_900_000);
        let encoded = serde_json::to_string(&attempt_error(at, 1, "", "")).unwrap();
        assert_eq!(
            encoded,
            r#"{"at":"2024-01-02T03:04:05.6789Z","attempt":1,"error":"","trace":""}"#
        );
    }

    #[test]
    fn round_trip() {
        let attempt_error = attempt_error(attempt_at(), 3, "job failed", "frame one");
        let encoded = serde_json::to_string(&attempt_error).unwrap();
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&encoded).unwrap(),
            serde_json::json!({
                "at": "2024-01-02T03:04:05.123456Z",
                "attempt": 3,
                "error": "job failed",
                "trace": "frame one",
            })
        );
        assert_eq!(
            serde_json::from_str::<AttemptError>(&encoded).unwrap(),
            attempt_error
        );
        // Values decoded from `serde_json::Value` work the same way.
        assert_eq!(
            serde_json::from_value::<AttemptError>(serde_json::to_value(&attempt_error).unwrap())
                .unwrap(),
            attempt_error
        );
    }

    // One unexpected element doesn't prevent decoding the others.
    #[test]
    fn slice() {
        assert_eq!(
            serde_json::from_str::<Vec<AttemptError>>(
                r#"[{"at":"2024-01-02T03:04:05.123456Z","attempt":1,"error":"err1","trace":""},"err2"]"#
            )
            .unwrap(),
            vec![
                attempt_error(attempt_at(), 1, "err1", ""),
                attempt_error(go_zero_time(), 0, "err2", ""),
            ]
        );
    }

    // Missing fields and nulls decode the same as with Go's defaults.
    #[test]
    fn strict_shape_unchanged() {
        assert_eq!(
            serde_json::from_str::<AttemptError>(r#"{"attempt":2,"error":null}"#).unwrap(),
            attempt_error(go_zero_time(), 2, "", "")
        );
    }
}
