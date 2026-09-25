//! Go-compatible JSON encoding for job arguments.
//!
//! River stores job arguments as the exact JSON bytes produced when they were
//! inserted, and River Go hashes those bytes to build unique keys. For a Rust
//! job and an equivalent Go job to share a unique key, they must encode to the
//! same bytes. [`encode_args`] serializes with [`serde_json`] using the output
//! rules of Go's `encoding/json`:
//!
//! * Floats use Go's shortest round-trip digits, in plain decimal notation
//!   when `1e-6 <= |x| < 1e21` and exponent notation otherwise (`1`, `0.1`,
//!   `100000000000000000000`, `1e+21`, `1e-7`). Negative zero encodes as `-0`.
//! * Strings escape `<`, `>`, `&`, U+2028, and U+2029 as `\u003c`, `\u003e`,
//!   `\u0026`, `\u2028`, and `\u2029`. Control characters use `\b`, `\f`,
//!   `\n`, `\r`, and `\t` where available and lowercase `\u00XX` otherwise.
//! * Struct fields keep their declaration order, as in Go.
//!
//! Some differences come from how types serialize rather than from the JSON
//! encoder and must be handled in the argument type:
//!
//! * Go sorts map keys. Use an ordered map such as
//!   [`BTreeMap`](std::collections::BTreeMap) with string keys to match; a
//!   `HashMap` serializes in an unspecified order.
//! * Go encodes `[]byte` as a base64 string; a Rust `Vec<u8>` encodes as an
//!   array of numbers.
//! * Go encodes `time.Time` with RFC 3339 and the shortest fractional
//!   seconds, while `chrono` pads fractional seconds to 3, 6, or 9 digits. Use
//!   [`go_time`] for `DateTime<Utc>` fields that participate in unique keys.
//! * Non-finite floats encode as `null`, where Go reports an error.
//!
//! These rules match Go 1.22 and later, which escape backspace and form feed
//! as `\b` and `\f`.

use std::{fmt, fmt::Write as _, io};

use chrono::{DateTime, SecondsFormat, Timelike, Utc};
use serde::Serialize;
use serde_json::{
    ser::{CharEscape, Formatter},
    value::RawValue,
};

/// Encodes job arguments to JSON bytes identical to those Go's
/// `encoding/json` produces for an equivalent Go value.
///
/// River uses this encoding for every inserted job, so it is only needed
/// directly when constructing [`JobRow`](crate::JobRow) values by hand, such
/// as in tests.
///
/// # Errors
///
/// Returns an error when the value's [`Serialize`] implementation fails, for
/// example because a map has non-string keys.
pub fn encode_args<T: Serialize + ?Sized>(args: &T) -> Result<Box<RawValue>, serde_json::Error> {
    RawValue::from_string(to_go_string(args)?)
}

/// Serde helpers that encode a `DateTime<Utc>` the way Go's `encoding/json`
/// encodes a UTC `time.Time`: RFC 3339 with the shortest fractional seconds
/// (`2026-01-02T03:04:05.5Z`).
///
/// Use it with `#[serde(with = "riverqueue::encoding::go_time")]` on argument
/// fields that participate in Go-compatible unique keys. Deserialization
/// accepts any RFC 3339 timestamp.
pub mod go_time {
    use chrono::{DateTime, Utc};
    use serde::{Deserialize, Deserializer, Serializer};

    /// Serializes a timestamp in Go's `time.RFC3339Nano` form.
    ///
    /// # Errors
    ///
    /// Returns the serializer's error.
    pub fn serialize<S: Serializer>(
        timestamp: &DateTime<Utc>,
        serializer: S,
    ) -> Result<S::Ok, S::Error> {
        serializer.serialize_str(&super::format_rfc3339_nano(*timestamp))
    }

    /// Deserializes an RFC 3339 timestamp.
    ///
    /// # Errors
    ///
    /// Returns an error when the input is not an RFC 3339 timestamp.
    pub fn deserialize<'de, D: Deserializer<'de>>(
        deserializer: D,
    ) -> Result<DateTime<Utc>, D::Error> {
        DateTime::<Utc>::deserialize(deserializer)
    }
}

/// Formats a UTC timestamp like Go's `time.RFC3339Nano`.
pub(crate) fn format_rfc3339_nano(timestamp: DateTime<Utc>) -> String {
    let mut formatted = timestamp.to_rfc3339_opts(SecondsFormat::Secs, true);
    let nanos = timestamp.nanosecond() % 1_000_000_000;
    if nanos > 0 {
        let fraction = format!("{nanos:09}");
        formatted.insert(formatted.len() - 1, '.');
        formatted.insert_str(formatted.len() - 1, fraction.trim_end_matches('0'));
    }
    formatted
}

/// A [`serde_json`] formatter that writes compact JSON with Go's
/// `encoding/json` number formatting and string escaping.
#[derive(Clone, Copy, Debug, Default)]
pub(crate) struct GoFormatter;

impl Formatter for GoFormatter {
    fn write_f32<W>(&mut self, writer: &mut W, value: f32) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        let absolute = value.abs();
        let exponent = absolute != 0.0 && !(1e-6..1e21).contains(&absolute);
        writer.write_all(format_go_float(value, exponent).as_bytes())
    }

    fn write_f64<W>(&mut self, writer: &mut W, value: f64) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        let absolute = value.abs();
        let exponent = absolute != 0.0 && !(1e-6..1e21).contains(&absolute);
        writer.write_all(format_go_float(value, exponent).as_bytes())
    }

    fn write_string_fragment<W>(&mut self, writer: &mut W, fragment: &str) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        let mut start = 0;
        for (index, character) in fragment.char_indices() {
            let escaped = match character {
                '<' => "\\u003c",
                '>' => "\\u003e",
                '&' => "\\u0026",
                '\u{2028}' => "\\u2028",
                '\u{2029}' => "\\u2029",
                _ => continue,
            };
            writer.write_all(&fragment.as_bytes()[start..index])?;
            writer.write_all(escaped.as_bytes())?;
            start = index + character.len_utf8();
        }
        writer.write_all(&fragment.as_bytes()[start..])
    }

    /// Embeds raw JSON (a [`RawValue`], such as job metadata) the way Go
    /// embeds a `json.RawMessage`: compacted, with the same HTML-safe string
    /// escaping, and every other token byte for byte.
    fn write_raw_fragment<W>(&mut self, writer: &mut W, fragment: &str) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        writer.write_all(go_compact(fragment).as_bytes())
    }

    fn write_char_escape<W>(&mut self, writer: &mut W, char_escape: CharEscape) -> io::Result<()>
    where
        W: ?Sized + io::Write,
    {
        let escaped: &[u8] = match char_escape {
            CharEscape::Quote => b"\\\"",
            CharEscape::ReverseSolidus => b"\\\\",
            CharEscape::Solidus => b"/",
            CharEscape::Backspace => b"\\b",
            CharEscape::FormFeed => b"\\f",
            CharEscape::LineFeed => b"\\n",
            CharEscape::CarriageReturn => b"\\r",
            CharEscape::Tab => b"\\t",
            CharEscape::AsciiControl(byte) => {
                return writer.write_all(&control_escape(byte));
            }
        };
        writer.write_all(escaped)
    }
}

/// Formats a float with Go's `strconv.FormatFloat(value, 'f' or 'e', -1)`
/// followed by `encoding/json`'s exponent cleanup (`e-07` becomes `e-7`).
fn format_go_float<F: GoFloat>(value: F, exponent_notation: bool) -> String {
    let wide = value.to_f64();
    if wide == 0.0 {
        return if wide.is_sign_negative() { "-0" } else { "0" }.to_owned();
    }

    // Rust's `LowerExp` writes the shortest round-trip digits, as Go does,
    // but breaks exact ties differently; see `round_tie_to_even`.
    let scientific = format!("{value:e}");
    let (negative, scientific) = match scientific.strip_prefix('-') {
        Some(rest) => (true, rest),
        None => (false, scientific.as_str()),
    };
    let Some((mantissa, power)) = scientific.split_once('e') else {
        return scientific.to_owned();
    };
    let mut digits = mantissa.replace('.', "");
    let mut power = power.parse::<i32>().unwrap_or_default();
    round_tie_to_even(value, &mut digits, &mut power);

    let mut output = String::with_capacity(digits.len() + 8);
    if negative {
        output.push('-');
    }
    if exponent_notation {
        output.push_str(&digits[..1]);
        if digits.len() > 1 {
            output.push('.');
            output.push_str(&digits[1..]);
        }
        if power < 0 {
            let _ = write!(output, "e-{}", -power);
        } else {
            let _ = write!(output, "e+{power:02}");
        }
        return output;
    }
    // `'f'` notation: place the decimal point `power + 1` digits into the
    // significant digits, padding with zeros on either side.
    let integer_digits = power + 1;
    if integer_digits <= 0 {
        output.push_str("0.");
        output.extend(std::iter::repeat_n(
            '0',
            usize::try_from(-integer_digits).unwrap_or(0),
        ));
        output.push_str(&digits);
    } else {
        let integer_digits = usize::try_from(integer_digits).unwrap_or(0);
        if integer_digits >= digits.len() {
            output.push_str(&digits);
            output.extend(std::iter::repeat_n('0', integer_digits - digits.len()));
        } else {
            output.push_str(&digits[..integer_digits]);
            output.push('.');
            output.push_str(&digits[integer_digits..]);
        }
    }
    output
}

/// Adjusts shortest digits for an exact tie. When a float lies exactly
/// halfway between the two nearest decimals with the shortest round-trip
/// digit count, Go chooses the one with an even last digit while Rust rounds
/// away from zero (`472476.125_f32` is `472476.12` in Go and `472476.13` in
/// Rust).
fn round_tie_to_even<F: GoFloat>(value: F, digits: &mut String, power: &mut i32) {
    // Decompose |value| exactly as `mantissa * 2^exponent` with an odd
    // mantissa. Only negative exponents have a fractional decimal expansion
    // that can end in the 5 of an exact tie.
    let bits = value.to_f64().abs().to_bits();
    let biased = i32::try_from((bits >> 52) & 0x7ff).unwrap_or(0);
    let fraction = bits & ((1_u64 << 52) - 1);
    let (mut mantissa, mut exponent) = if biased == 0 {
        (fraction, -1074)
    } else {
        (fraction | (1_u64 << 52), biased - 1075)
    };
    let trailing = mantissa.trailing_zeros();
    mantissa >>= trailing;
    exponent += i32::try_from(trailing).unwrap_or(0);
    let Ok(scale) = u32::try_from(-exponent) else {
        return;
    };
    // The exact value is `mantissa * 5^scale * 10^-scale`. A tie at the
    // shortest length means it has exactly one more significant digit.
    let Some(exact) = 5_u128
        .checked_pow(scale)
        .and_then(|factor| factor.checked_mul(u128::from(mantissa)))
    else {
        return;
    };
    let exact_digits = exact.to_string();
    if exact_digits.len() != digits.len() + 1 {
        return;
    }
    let below = exact / 10;
    let even = if below % 2 == 0 { below } else { below + 1 };
    let candidate = even.to_string();
    if candidate == *digits {
        return;
    }
    let scale = i32::try_from(scale).unwrap_or(i32::MAX);
    let candidate_power = i32::try_from(candidate.len()).unwrap_or(0) - 1 - scale + 1;
    if F::parses_to(&format!("{candidate}e{}", 1 - scale), value) {
        candidate.trim_end_matches('0').clone_into(digits);
        *power = candidate_power;
    }
}

/// Float widths formatted with Go's rules.
trait GoFloat: Copy + fmt::LowerExp {
    fn to_f64(self) -> f64;
    fn parses_to(text: &str, value: Self) -> bool;
}

impl GoFloat for f32 {
    fn to_f64(self) -> f64 {
        f64::from(self)
    }

    fn parses_to(text: &str, value: Self) -> bool {
        text.parse::<Self>()
            .is_ok_and(|parsed| parsed.abs().to_bits() == value.abs().to_bits())
    }
}

impl GoFloat for f64 {
    fn to_f64(self) -> f64 {
        self
    }

    fn parses_to(text: &str, value: Self) -> bool {
        text.parse::<Self>()
            .is_ok_and(|parsed| parsed.abs().to_bits() == value.abs().to_bits())
    }
}

fn control_escape(byte: u8) -> [u8; 6] {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    [
        b'\\',
        b'u',
        b'0',
        b'0',
        HEX[usize::from(byte >> 4)],
        HEX[usize::from(byte & 0xf)],
    ]
}

/// Compacts valid JSON like Go's `json.Compact` after `json.HTMLEscape`:
/// whitespace between tokens is removed, and inside strings `<`, `>`, `&`,
/// U+2028, and U+2029 are escaped. Numbers, key order, and existing escapes
/// are kept byte for byte.
pub(crate) fn go_compact(json: &str) -> String {
    let mut output = String::with_capacity(json.len());
    let mut in_string = false;
    let mut escaped = false;
    for character in json.chars() {
        if !in_string {
            match character {
                ' ' | '\t' | '\n' | '\r' => {}
                '"' => {
                    in_string = true;
                    output.push(character);
                }
                _ => output.push(character),
            }
            continue;
        }
        if escaped {
            escaped = false;
            output.push(character);
            continue;
        }
        match character {
            '\\' => {
                escaped = true;
                output.push(character);
            }
            '"' => {
                in_string = false;
                output.push(character);
            }
            '<' => output.push_str("\\u003c"),
            '>' => output.push_str("\\u003e"),
            '&' => output.push_str("\\u0026"),
            '\u{2028}' => output.push_str("\\u2028"),
            '\u{2029}' => output.push_str("\\u2029"),
            _ => output.push(character),
        }
    }
    output
}

/// Serializes `value` to JSON text with Go's `encoding/json` output rules,
/// as [`encode_args`] does.
pub(crate) fn to_go_string<T: Serialize + ?Sized>(value: &T) -> Result<String, serde_json::Error> {
    let mut buffer = Vec::with_capacity(128);
    value.serialize(&mut serde_json::Serializer::with_formatter(
        &mut buffer,
        GoFormatter,
    ))?;
    String::from_utf8(buffer).map_err(<serde_json::Error as serde::ser::Error>::custom)
}

/// Appends `value` as a JSON string with Go's `encoding/json` escaping.
pub(crate) fn write_go_string(value: &str, output: &mut String) {
    output.push('"');
    for character in value.chars() {
        match character {
            '"' => output.push_str("\\\""),
            '\\' => output.push_str("\\\\"),
            '\u{8}' => output.push_str("\\b"),
            '\u{c}' => output.push_str("\\f"),
            '\n' => output.push_str("\\n"),
            '\r' => output.push_str("\\r"),
            '\t' => output.push_str("\\t"),
            '<' => output.push_str("\\u003c"),
            '>' => output.push_str("\\u003e"),
            '&' => output.push_str("\\u0026"),
            '\u{2028}' => output.push_str("\\u2028"),
            '\u{2029}' => output.push_str("\\u2029"),
            character if u32::from(character) < 0x20 => {
                let escaped = control_escape(u8::try_from(u32::from(character)).unwrap_or(0));
                output.extend(escaped.iter().map(|&byte| char::from(byte)));
            }
            character => output.push(character),
        }
    }
    output.push('"');
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use chrono::TimeZone;

    use super::*;

    fn encoded<T: Serialize + ?Sized>(value: &T) -> String {
        encode_args(value).unwrap().get().to_owned()
    }

    #[test]
    fn encodes_floats_like_go() {
        // Expected values produced by Go's `encoding/json`.
        let cases: [(f64, &str); 19] = [
            (0.0, "0"),
            (-0.0, "-0"),
            (1.0, "1"),
            (0.1, "0.1"),
            (100.0, "100"),
            (1e6, "1000000"),
            (12_345.678, "12345.678"),
            (1e-6, "0.000001"),
            (0.000_001_234, "0.000001234"),
            (1e-7, "1e-7"),
            (-1.5e-9, "-1.5e-9"),
            (1e20, "100000000000000000000"),
            (123_456_789_012_345_680_000.0, "123456789012345680000"),
            (1e21, "1e+21"),
            (1.5e300, "1.5e+300"),
            (5e-324, "5e-324"),
            (f64::MAX, "1.7976931348623157e+308"),
            // Exact ties between two shortest candidates round to even.
            (1_357_346_946_266_522.2, "1357346946266522.2"),
            (3_371_836_896_475.031_2, "3371836896475.0312"),
        ];
        for (value, expected) in cases {
            assert_eq!(encoded(&value), expected, "{value:e}");
        }

        let cases: [(f32, &str); 10] = [
            (1.0, "1"),
            (0.1, "0.1"),
            (1.1, "1.1"),
            (1e-7, "1e-7"),
            (1e20, "100000000000000000000"),
            (1e21, "1e+21"),
            (16_777_216.0, "16777216"),
            (472_476.12, "472476.12"),
            (-368.140_62, "-368.14062"),
            (2_569_406.2, "2569406.2"),
        ];
        for (value, expected) in cases {
            assert_eq!(encoded(&value), expected, "{value:e}");
        }
        assert_eq!(encoded(&f32::MAX), "3.4028235e+38");
    }

    #[test]
    fn encodes_non_finite_floats_as_null() {
        assert_eq!(encoded(&f64::NAN), "null");
        assert_eq!(encoded(&f64::INFINITY), "null");
    }

    #[test]
    fn escapes_strings_like_go() {
        let value = "<>&\u{2028}\u{2029}\u{8}\u{c}\n\r\t\u{1}\u{1f}\u{7f}\"\\/é😀";
        let expected = r#""\u003c\u003e\u0026\u2028\u2029\b\f\n\r\t\u0001\u001f"#.to_owned()
            + "\u{7f}\\\"\\\\/é😀\"";
        assert_eq!(encoded(value), expected);

        let mut direct = String::new();
        write_go_string(value, &mut direct);
        assert_eq!(direct, expected);
    }

    #[test]
    fn embeds_raw_json_like_go_raw_messages() {
        let raw = RawValue::from_string(
            "{ \"b\" : \"a<b>&\u{2028}\\u003c\\\"<\" ,\n \"n\": 1.50e0, \"z\":[ 1 , 2 ] }"
                .to_owned(),
        )
        .unwrap();
        assert_eq!(
            encoded(&raw),
            r#"{"b":"a\u003cb\u003e\u0026\u2028\u003c\"\u003c","n":1.50e0,"z":[1,2]}"#
        );
    }

    #[test]
    fn escapes_keys_and_preserves_struct_order() {
        #[derive(Serialize)]
        struct Args {
            zulu: u8,
            #[serde(rename = "a<b>")]
            angle: u8,
            map: BTreeMap<&'static str, u8>,
        }

        let args = Args {
            zulu: 1,
            angle: 2,
            map: BTreeMap::from([("é&", 3), ("<k>", 4)]),
        };
        assert_eq!(
            encoded(&args),
            r#"{"zulu":1,"a\u003cb\u003e":2,"map":{"\u003ck\u003e":4,"é\u0026":3}}"#
        );
    }

    #[test]
    fn go_time_trims_fractional_seconds() {
        #[derive(Serialize)]
        struct Times {
            #[serde(with = "go_time")]
            at: DateTime<Utc>,
        }

        let whole = Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();
        for (nanos, expected) in [
            (0, r#"{"at":"2026-01-02T03:04:05Z"}"#),
            (500_000_000, r#"{"at":"2026-01-02T03:04:05.5Z"}"#),
            (120_000_000, r#"{"at":"2026-01-02T03:04:05.12Z"}"#),
            (123_456_000, r#"{"at":"2026-01-02T03:04:05.123456Z"}"#),
            (1, r#"{"at":"2026-01-02T03:04:05.000000001Z"}"#),
        ] {
            let at = whole + chrono::Duration::nanoseconds(nanos);
            assert_eq!(encoded(&Times { at }), expected);
        }
    }
}
