//! Decoding of attempt errors.
//!
//! [`AttemptError`] deserializes the way River Go's `encoding/json` decodes
//! `rivertype.AttemptError`. Persisted attempt errors are decoded leniently
//! instead, like River Go's driver reads: River always writes them in the
//! shape [`AttemptError`] serializes to, but elements written by other tools
//! or edited by hand might not match it, and a job row can't be read or worked
//! unless every one of its attempt errors decodes.

use std::{borrow::Cow, fmt};

use chrono::{DateTime, NaiveDate, Utc};
use serde::{
    Deserialize, Deserializer,
    de::{self, MapAccess, Visitor},
};
use serde_json::value::RawValue;

use super::AttemptError;

impl<'de> Deserialize<'de> for AttemptError {
    /// Decodes an attempt error like Go's `encoding/json`: fields match
    /// case-insensitively, a repeated field takes its last value, missing,
    /// `null`, and unknown fields are accepted, and `at` must be an RFC 3339
    /// timestamp. A field of any other type is an error.
    ///
    /// Only a JSON deserializer (such as [`serde_json`]'s) can decode an
    /// attempt error.
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let raw = Box::<RawValue>::deserialize(deserializer)?;
        Self::from_json_strict(raw.get()).map_err(de::Error::custom)
    }
}

impl AttemptError {
    /// Decodes an attempt error like Go's `encoding/json`. See
    /// [`AttemptError`]'s `Deserialize` implementation.
    fn from_json_strict(json: &str) -> Result<Self, String> {
        let json = json.trim_matches(is_json_whitespace);
        if json == "null" {
            return Ok(Self::new(go_zero_time(), 0, ""));
        }
        if !json.starts_with('{') {
            return Err(format!("cannot decode {json} into an attempt error"));
        }
        let fields: Fields = serde_json::from_str(json).map_err(|error| error.to_string())?;
        if let Some(invalid) = fields.invalid {
            return Err(invalid);
        }
        Ok(Self {
            at: strict_time(raw_or_null(fields.at.as_deref()))?,
            attempt: strict_attempt(raw_or_null(fields.attempt.as_deref()))?,
            error: strict_string(raw_or_null(fields.error.as_deref()))?,
            trace: strict_string(raw_or_null(fields.trace.as_deref()))?,
        })
    }

    /// Decodes one persisted attempt error exactly like River Go's
    /// `riverdriver.UnmarshalAttemptError`.
    ///
    /// Elements in the shape River writes decode as they would with Go's
    /// `encoding/json` defaults. Any other valid JSON decodes on a best
    /// effort basis:
    ///
    /// * `at` accepts only what Go's `time.Time` does, RFC 3339 timestamps.
    ///   Any other value leaves Go's zero time.
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
            let fields: Fields = serde_json::from_str(json)?;
            return Ok(Self {
                at: fields.at.map_or_else(go_zero_time, |raw| {
                    strict_time(raw.get()).unwrap_or_else(|_| go_zero_time())
                }),
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

    /// Decodes a persisted JSON array of attempt errors like River Go's
    /// `riverdriver.UnmarshalAttemptErrors`: each element decodes with
    /// [`from_json_lenient`](Self::from_json_lenient), `null` is empty, and
    /// anything other than an array is an error.
    pub(crate) fn from_json_array_lenient(json: &str) -> Result<Vec<Self>, serde_json::Error> {
        serde_json::from_str::<Option<Vec<Box<RawValue>>>>(json)?
            .unwrap_or_default()
            .iter()
            .map(|raw| Self::from_json_lenient(raw.get()))
            .collect()
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
struct Fields {
    at: Option<Box<RawValue>>,
    attempt: Option<Box<RawValue>>,
    error: Option<Box<RawValue>>,
    trace: Option<Box<RawValue>>,
    /// Why Go's `encoding/json` would reject a value, including one later
    /// replaced by a repeated field, which Go still reports.
    invalid: Option<String>,
}

impl<'de> Deserialize<'de> for Fields {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        struct FieldsVisitor;

        impl<'de> Visitor<'de> for FieldsVisitor {
            type Value = Fields;

            fn expecting(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
                formatter.write_str("an attempt error object")
            }

            fn visit_map<A>(self, mut map: A) -> Result<Self::Value, A::Error>
            where
                A: MapAccess<'de>,
            {
                let mut fields = Fields::default();
                while let Some(key) = map.next_key::<String>()? {
                    let value = map.next_value::<Box<RawValue>>()?;
                    // Go folds case when matching field names. Only ASCII
                    // letters fold to the letters of these four names.
                    let (field, valid) = if key.eq_ignore_ascii_case("at") {
                        (&mut fields.at, strict_time(value.get()).map(drop))
                    } else if key.eq_ignore_ascii_case("attempt") {
                        (&mut fields.attempt, strict_attempt(value.get()).map(drop))
                    } else if key.eq_ignore_ascii_case("error") {
                        (&mut fields.error, strict_string(value.get()).map(drop))
                    } else if key.eq_ignore_ascii_case("trace") {
                        (&mut fields.trace, strict_string(value.get()).map(drop))
                    } else {
                        continue;
                    };
                    *field = Some(value);
                    if let Err(invalid) = valid {
                        fields.invalid.get_or_insert(invalid);
                    }
                }
                Ok(fields)
            }
        }

        deserializer.deserialize_map(FieldsVisitor)
    }
}

/// A field's raw JSON, with a missing field read as `null` like Go does.
fn raw_or_null(field: Option<&RawValue>) -> &str {
    field.map_or("null", RawValue::get)
}

fn is_json_whitespace(character: char) -> bool {
    matches!(character, ' ' | '\t' | '\n' | '\r')
}

/// Decodes `at` like Go's `time.Time.UnmarshalJSON`: `null` is Go's zero
/// time, and a string must hold an RFC 3339 timestamp. Like Go 1.26, the
/// string isn't unescaped first.
fn strict_time(raw: &str) -> Result<DateTime<Utc>, String> {
    if raw == "null" {
        return Ok(go_zero_time());
    }
    raw.strip_prefix('"')
        .and_then(|text| text.strip_suffix('"'))
        .and_then(parse_rfc3339)
        .ok_or_else(|| format!("attempt error time {raw} isn't an RFC 3339 timestamp"))
}

/// Converts a JSON attempt number to `i32`, saturating at its bounds.
fn saturating_i32(value: i64) -> i32 {
    i32::try_from(value).unwrap_or(if value < 0 { i32::MIN } else { i32::MAX })
}

/// Decodes `attempt` like Go's `encoding/json` decodes an `int`: an integer
/// literal or `null`. `attempt` is narrower than Go's 64-bit `int`, so values
/// beyond `i32` saturate.
fn strict_attempt(raw: &str) -> Result<i32, String> {
    if raw == "null" {
        return Ok(0);
    }
    raw.parse::<i64>()
        .map(saturating_i32)
        .map_err(|_| format!("attempt error attempt {raw} isn't an integer"))
}

/// Decodes `error` or `trace` like Go's `encoding/json` decodes a `string`:
/// a string or `null`.
fn strict_string(raw: &str) -> Result<String, String> {
    if raw == "null" {
        return Ok(String::new());
    }
    if raw.starts_with('"') {
        return serde_json::from_str(raw).map_err(|error| error.to_string());
    }
    Err(format!("attempt error value {raw} isn't a string"))
}

/// The largest magnitude up to which every integer is exactly representable
/// as an `f64`, 2^53.
const MAX_EXACT_FLOAT_INTEGER: f64 = 9_007_199_254_740_992.0;

/// Decodes `attempt` like Go: integers, and numbers or numeric strings with an
/// integral value no larger in magnitude than 2^53. `attempt` is narrower
/// than Go's 64-bit `int`, so values beyond `i32` saturate. Unlike Go, a string in hexadecimal floating point notation
/// (such as `"0x1p4"`) isn't recognized and decodes as zero.
#[allow(
    clippy::float_cmp,
    reason = "an exact comparison checks for an integral value"
)]
fn lenient_attempt(raw: &str) -> i32 {
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
        return saturating_i32(integer);
    }
    match number.parse::<f64>() {
        Ok(float) if float == float.trunc() && float.abs() <= MAX_EXACT_FLOAT_INTEGER =>
        {
            #[expect(
                clippy::cast_possible_truncation,
                reason = "the float is integral and within the exact integer range"
            )]
            saturating_i32(float as i64)
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

/// Parses a timestamp exactly as Go's `time.Time.UnmarshalJSON` does, which
/// is with `time.Parse` and Go's RFC 3339 layout: `YYYY-MM-DD`, `T`, a one or
/// two digit hour, `:MM:SS` with valid ranges and no leap second, an optional
/// fraction introduced by `.` or `,` (digits past nanoseconds are ignored),
/// and `Z` or a `±hh:mm` offset of up to 24 hours and 60 minutes.
fn parse_rfc3339(text: &str) -> Option<DateTime<Utc>> {
    let mut parser = TimeParser(text.as_bytes());
    let year = parser.digits(4)?;
    parser.expect(b"-")?;
    let month = parser.digits(2)?;
    parser.expect(b"-")?;
    let day = parser.digits(2)?;
    parser.expect(b"T")?;
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

    /// Consumes a `Z` or `±hh:mm` UTC offset, returning it in seconds east of
    /// UTC.
    fn offset(&mut self) -> Option<i64> {
        let sign = match self.0.first()? {
            b'Z' => {
                self.0 = &self.0[1..];
                return Some(0);
            }
            b'+' => 1,
            b'-' => -1,
            _ => return None,
        };
        self.0 = &self.0[1..];
        let hours = self.digits(2)?;
        self.expect(b":")?;
        let minutes = self.digits(2)?;
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

    fn attempt_error(at: DateTime<Utc>, attempt: i32, error: &str, trace: &str) -> AttemptError {
        AttemptError::new(at, attempt, error).with_trace(trace)
    }

    fn assert_lenient<const N: usize>(cases: [(&str, &str, AttemptError); N]) {
        for (name, json, expected) in cases {
            assert_eq!(
                AttemptError::from_json_lenient(json).unwrap(),
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

    #[test]
    fn invalid_json_is_an_error() {
        assert!(AttemptError::from_json_lenient(r#"{"at":"#).is_err());
        assert!(AttemptError::from_json_array_lenient(r#"[{"at":"#).is_err());
        assert!(serde_json::from_str::<AttemptError>(r#"{"at":"#).is_err());
    }

    // The cases of River Go's `TestUnmarshalAttemptError`.
    #[test]
    fn lenient_like_go() {
        let zero = go_zero_time();
        assert_lenient([
            (
                "AtInvalid",
                r#"{"at":"not a time","attempt":2,"error":"err"}"#,
                attempt_error(zero, 2, "err", ""),
            ),
            (
                "AtNoOffset",
                r#"{"at":"2024-01-02T03:04:05.123456","attempt":2}"#,
                attempt_error(zero, 2, "", ""),
            ),
            (
                "AtNumber",
                r#"{"at":1704164645,"attempt":2}"#,
                attempt_error(zero, 2, "", ""),
            ),
            (
                "AtPostgresText",
                r#"{"at":"2024-01-02 03:04:05.123456+00","attempt":2}"#,
                attempt_error(zero, 2, "", ""),
            ),
            (
                "AtRFC3339WithOtherInvalidField",
                r#"{"at":"2024-01-02T03:04:05.123456Z","attempt":"2"}"#,
                attempt_error(attempt_at(), 2, "", ""),
            ),
            (
                "AtSpaceNoOffset",
                r#"{"at":"2024-01-02 03:04:05.123456","attempt":2}"#,
                attempt_error(zero, 2, "", ""),
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
            (
                "StrictShapeUnchanged",
                r#"{"attempt":2,"error":null}"#,
                attempt_error(zero, 2, "", ""),
            ),
        ]);
    }

    // Behavior of Go's `encoding/json` that the Go test cases don't reach.
    #[test]
    fn lenient_fields_edges_like_go() {
        let zero = go_zero_time();
        let whole = whole_second();
        assert_lenient([
            (
                "CaseInsensitiveLastWins",
                r#"{"error":"first","ERROR":"second","Attempt":2}"#,
                attempt_error(zero, 2, "second", ""),
            ),
            (
                "EscapedKey",
                concat!(r#"{""#, "\\", r#"u0061t":"2024-01-02T03:04:05Z"}"#),
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
                r#"{"attempt":4000000000}"#,
                attempt_error(zero, i32::MAX, "", ""),
            ),
            (
                "AttemptBool",
                r#"{"attempt":true}"#,
                attempt_error(zero, 0, "", ""),
            ),
        ]);
    }

    // Behavior of Go's `time.Time.UnmarshalJSON` that the Go test cases don't
    // reach: it accepts what `time.Parse` does with Go's RFC 3339 layout, and
    // anything else leaves zero.
    #[test]
    #[allow(clippy::too_many_lines)]
    fn lenient_time_edges_like_go() {
        let zero = go_zero_time();
        let whole = whole_second();
        assert_lenient([
            (
                "AtCommaFraction",
                r#"{"at":"2024-01-02T03:04:05,123456Z"}"#,
                attempt_error(attempt_at(), 0, "", ""),
            ),
            (
                "AtFractionBeyondNanoseconds",
                r#"{"at":"2024-01-02T03:04:05.1234560009Z"}"#,
                attempt_error(attempt_at(), 0, "", ""),
            ),
            (
                "AtOffset",
                r#"{"at":"2024-01-02T00:04:05-03:00"}"#,
                attempt_error(whole, 0, "", ""),
            ),
            (
                "AtOffsetLargestGoAccepts",
                r#"{"at":"2024-01-03T04:04:05+24:60"}"#,
                attempt_error(whole, 0, "", ""),
            ),
            (
                "AtOneDigitHour",
                r#"{"at":"2024-01-02T3:04:05Z"}"#,
                attempt_error(whole, 0, "", ""),
            ),
            (
                "AtYearZero",
                r#"{"at":"0000-01-01T00:00:00Z"}"#,
                attempt_error(
                    NaiveDate::from_ymd_opt(0, 1, 1)
                        .and_then(|date| date.and_hms_opt(0, 0, 0))
                        .unwrap()
                        .and_utc(),
                    0,
                    "",
                    "",
                ),
            ),
            (
                "AtEmptyFraction",
                r#"{"at":"2024-01-02T03:04:05.Z"}"#,
                attempt_error(zero, 0, "", ""),
            ),
            (
                "AtEscaped",
                concat!(r#"{"at":"2024-01-02T03:04:05"#, "\\", r#"u005a"}"#),
                attempt_error(zero, 0, "", ""),
            ),
            (
                "AtHourOutOfRange",
                r#"{"at":"2024-01-02T24:04:05Z"}"#,
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
                "AtLowercaseSeparator",
                r#"{"at":"2024-01-02t03:04:05Z"}"#,
                attempt_error(zero, 0, "", ""),
            ),
            (
                "AtOffsetHourOutOfRange",
                r#"{"at":"2024-01-02T03:04:05+25:00"}"#,
                attempt_error(zero, 0, "", ""),
            ),
            (
                "AtOffsetHoursOnly",
                r#"{"at":"2024-01-02T08:04:05+05"}"#,
                attempt_error(zero, 0, "", ""),
            ),
            (
                "AtOffsetMinuteOutOfRange",
                r#"{"at":"2024-01-02T03:04:05+05:61"}"#,
                attempt_error(zero, 0, "", ""),
            ),
            (
                "AtOffsetWithoutColon",
                r#"{"at":"2024-01-02T05:34:05+0230"}"#,
                attempt_error(zero, 0, "", ""),
            ),
            (
                "AtSurroundingSpace",
                r#"{"at":" 2024-01-02T03:04:05Z "}"#,
                attempt_error(zero, 0, "", ""),
            ),
            (
                "AtTrailingText",
                r#"{"at":"2024-01-02T03:04:05Zjunk"}"#,
                attempt_error(zero, 0, "", ""),
            ),
        ]);
    }

    // The cases of River Go's `TestUnmarshalAttemptErrors`.
    #[test]
    fn lenient_array_like_go() {
        assert_eq!(
            AttemptError::from_json_array_lenient("[]").unwrap(),
            Vec::new()
        );
        assert_eq!(
            AttemptError::from_json_array_lenient("null").unwrap(),
            Vec::new()
        );
        assert!(AttemptError::from_json_array_lenient(r#"{"error":"not an array"}"#).is_err());
        // One unexpected element doesn't prevent decoding the others.
        assert_eq!(
            AttemptError::from_json_array_lenient(
                r#"[{"at":"2024-01-02T03:04:05.123456Z","attempt":1,"error":"err1","trace":""},"err2"]"#
            )
            .unwrap(),
            vec![
                attempt_error(attempt_at(), 1, "err1", ""),
                attempt_error(go_zero_time(), 0, "err2", ""),
            ]
        );
        assert_eq!(
            AttemptError::from_json_array_lenient(
                r#"[{"at":"invalid","attempt":"2","error":"err"},{"error":"next"}]"#
            )
            .unwrap(),
            vec![
                attempt_error(go_zero_time(), 2, "err", ""),
                attempt_error(go_zero_time(), 0, "next", ""),
            ]
        );
    }

    // Deserializing an attempt error is strict like Go's `encoding/json`.
    #[test]
    fn deserializes_like_go_json() {
        let zero = go_zero_time();
        for (name, json, expected) in [
            (
                "Full",
                r#"{"at":"2024-01-02T03:04:05.123456Z","attempt":3,"error":"err","trace":"t","extra":[1]}"#,
                attempt_error(attempt_at(), 3, "err", "t"),
            ),
            (
                "CaseInsensitiveLastWins",
                r#"{"error":"first","ERROR":"second","Attempt":2}"#,
                attempt_error(zero, 2, "second", ""),
            ),
            (
                "MissingAndNull",
                r#"{"at":null,"attempt":null,"error":"err","trace":null}"#,
                attempt_error(zero, 0, "err", ""),
            ),
            ("Null", "null", attempt_error(zero, 0, "", "")),
            (
                "AttemptSaturates",
                r#"{"attempt":-4000000000}"#,
                attempt_error(zero, i32::MIN, "", ""),
            ),
        ] {
            assert_eq!(
                serde_json::from_str::<AttemptError>(json).unwrap(),
                expected,
                "{name}"
            );
        }

        for (name, json) in [
            ("AtNotRFC3339", r#"{"at":"2024-01-02 03:04:05+00"}"#),
            ("AtNumber", r#"{"at":1704164645}"#),
            ("AttemptFloat", r#"{"attempt":3.0}"#),
            ("AttemptString", r#"{"attempt":"3"}"#),
            ("AttemptOverflow", r#"{"attempt":9223372036854775808}"#),
            ("ErrorObject", r#"{"error":{"message":"boom"}}"#),
            ("TraceArray", r#"{"trace":["frame"]}"#),
            ("RepeatedInvalidField", r#"{"error":1,"error":"err"}"#),
            ("ElementString", r#""job failed""#),
            ("ElementArray", "[]"),
        ] {
            assert!(
                serde_json::from_str::<AttemptError>(json).is_err(),
                "{name}"
            );
        }
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
        assert_eq!(
            AttemptError::from_json_lenient(&encoded).unwrap(),
            attempt_error
        );
        // Values decoded from `serde_json::Value` work the same way.
        assert_eq!(
            serde_json::from_value::<AttemptError>(serde_json::to_value(&attempt_error).unwrap())
                .unwrap(),
            attempt_error
        );
    }
}
