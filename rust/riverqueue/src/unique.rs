//! Go-compatible unique job hashing.
//!
//! River Go builds a job's unique key from its exact encoded argument bytes
//! with `gjson` and `sjson`. This module reproduces those semantics over raw
//! JSON text rather than a parsed [`serde_json::Value`], so number tokens,
//! string escapes, and nested member order are hashed exactly as encoded:
//!
//! * With no selected unique fields, every top-level member is hashed with
//!   keys sorted bytewise and duplicate keys collapsed to their first value.
//!   An empty array hashes as `{}`; other non-object arguments are rejected.
//! * With selected fields, each selected dotted path is looked up and written
//!   into a new object in sorted path order. Missing values are omitted, and
//!   explicit `null` values are retained. When every selected path is
//!   missing, no argument bytes are hashed.
//! * Nested values are hashed as their original raw text.
//! * Keys are rewritten the way `sjson` writes them: a key made only of
//!   printable ASCII without `"` or `\` is written verbatim, even when the
//!   encoded arguments escaped it; any other key is re-encoded with Go's
//!   `encoding/json` string escaping.
//!
//! Top-level argument names are literal in all-arguments mode, including
//! empty names and names that would be JSON path syntax in selected mode.

use std::{borrow::Cow, time::Duration};

use chrono::{DateTime, SecondsFormat, Utc};
use serde_json::value::RawValue;
use sha2::{Digest, Sha256};

use crate::{Error, UniqueOpts};

const SECONDS_FROM_YEAR_ONE_TO_UNIX_EPOCH: i128 = 62_135_596_800;

/// Builds the SHA-256 unique key River Go would build for the same inputs.
/// Returns `None` when `opts` enables no uniqueness dimension.
pub(crate) fn build_unique_key_parts(
    kind: &str,
    unique_fields: &[&str],
    encoded_args: &RawValue,
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
        if unique_fields.is_empty() {
            write_all_args(encoded_args.get(), &mut key)?;
        } else {
            write_selected_args(encoded_args.get(), unique_fields, &mut key)?;
        }
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

/// Writes every top-level member sorted by key, like River Go's walk over the
/// encoded object's members. Like Go, an empty array hashes as `{}` and any
/// other non-object arguments are rejected.
fn write_all_args(encoded_args: &str, output: &mut String) -> Result<(), Error> {
    if is_empty_array(encoded_args) {
        output.push_str("{}");
        return Ok(());
    }
    let mut members = object_members(encoded_args)?;
    // A stable sort keeps the first of any duplicate keys first; `gjson`
    // resolves a duplicated key to its first value.
    members.sort_by(|left, right| left.key.cmp(&right.key));
    members.dedup_by(|later, earlier| later.key == earlier.key);

    output.push('{');
    for (index, member) in members.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_sjson_key(&member.key, output);
        output.push(':');
        output.push_str(member.value);
    }
    output.push('}');
    Ok(())
}

/// Writes selected paths into a new object in sorted path order, like River
/// Go's `sjson` assembly of struct fields tagged `river:"unique"`.
fn write_selected_args(
    encoded_args: &str,
    unique_fields: &[&str],
    output: &mut String,
) -> Result<(), Error> {
    // Reject non-object arguments consistently with the all-arguments mode.
    object_members(encoded_args)?;

    let mut paths = unique_fields
        .iter()
        .map(|path| Ok((*path, parse_unique_path(path)?)))
        .collect::<Result<Vec<_>, Error>>()?;
    paths.sort_by(|(left_path, left), (right_path, right)| {
        left.join(".")
            .cmp(&right.join("."))
            .then_with(|| left_path.cmp(right_path))
    });
    paths.dedup_by(|left, right| left.1 == right.1);
    for (index, (path, parts)) in paths.iter().enumerate() {
        if paths[index + 1..]
            .iter()
            .any(|(_, other)| other.len() > parts.len() && other.starts_with(parts))
        {
            return Err(unique_args_error(format!(
                "unique path {path:?} contains another selected unique path"
            )));
        }
    }

    let mut selected = Vec::new();
    for (_, parts) in paths {
        if let Some(value) = lookup_path(encoded_args, &parts)? {
            insert_selected(&mut selected, &parts, value);
        }
    }
    if !selected.is_empty() {
        write_selected_object(&selected, output);
    }
    Ok(())
}

/// Splits a Go `gjson` path, where an escaped dot is part of a literal name.
fn parse_unique_path(path: &str) -> Result<Vec<String>, Error> {
    let mut parts = Vec::new();
    let mut part = String::new();
    let mut escaped = false;
    for character in path.chars() {
        if escaped {
            part.push(character);
            escaped = false;
        } else if character == '\\' {
            escaped = true;
        } else if character == '.' {
            validate_path_segment(&part, "unique path segment")?;
            parts.push(std::mem::take(&mut part));
        } else {
            part.push(character);
        }
    }
    if escaped {
        part.push('\\');
    }
    validate_path_segment(&part, "unique path segment")?;
    parts.push(part);
    Ok(parts)
}

struct SelectedMember<'a> {
    key: String,
    value: SelectedValue<'a>,
}

enum SelectedValue<'a> {
    Object(Vec<SelectedMember<'a>>),
    Raw(&'a str),
}

fn insert_selected<'a>(members: &mut Vec<SelectedMember<'a>>, segments: &[String], value: &'a str) {
    let Some((segment, rest)) = segments.split_first() else {
        return;
    };
    let position = members.iter().position(|member| member.key == *segment);
    if rest.is_empty() {
        let value = SelectedValue::Raw(value);
        match position {
            Some(position) => members[position].value = value,
            None => members.push(SelectedMember {
                key: segment.clone(),
                value,
            }),
        }
        return;
    }
    let position = position.unwrap_or_else(|| {
        members.push(SelectedMember {
            key: segment.clone(),
            value: SelectedValue::Object(Vec::new()),
        });
        members.len() - 1
    });
    // Prefix conflicts are rejected before insertion, so an intermediate
    // segment always names an object.
    if let SelectedValue::Object(children) = &mut members[position].value {
        insert_selected(children, rest, value);
    }
}

fn write_selected_object(members: &[SelectedMember<'_>], output: &mut String) {
    output.push('{');
    for (index, member) in members.iter().enumerate() {
        if index > 0 {
            output.push(',');
        }
        write_sjson_key(&member.key, output);
        output.push(':');
        match &member.value {
            SelectedValue::Object(children) => write_selected_object(children, output),
            SelectedValue::Raw(raw) => output.push_str(raw),
        }
    }
    output.push('}');
}

/// Resolves a dotted path to the raw text of its value, following `gjson`:
/// each segment selects the first object member with that (unescaped) key.
fn lookup_path<'a>(encoded_args: &'a str, path: &[String]) -> Result<Option<&'a str>, Error> {
    let mut current = encoded_args;
    for segment in path {
        if !current.trim_start().starts_with('{') {
            return Ok(None);
        }
        match object_members(current)?
            .into_iter()
            .find(|member| member.key == segment.as_str())
        {
            Some(member) => current = member.value,
            None => return Ok(None),
        }
    }
    Ok(Some(current))
}

fn validate_path_segment(segment: &str, description: &str) -> Result<(), Error> {
    if segment.is_empty() {
        return Err(unique_args_error(format!(
            "{description} is empty, which River Go cannot hash"
        )));
    }
    Ok(())
}

fn unique_args_error(message: String) -> Error {
    Error::invalid_job_context("job uniqueness", message)
}

/// Writes an object key the way `sjson` does: verbatim when it is printable
/// ASCII without `"` or `\`, otherwise with Go's `encoding/json` escaping.
fn write_sjson_key(key: &str, output: &mut String) {
    let verbatim = key
        .bytes()
        .all(|byte| (b' '..=0x7f).contains(&byte) && byte != b'"' && byte != b'\\');
    if verbatim {
        output.push('"');
        output.push_str(key);
        output.push('"');
    } else {
        crate::encoding::write_go_string(key, output);
    }
}

/// One member of a JSON object with its unescaped key and raw value text.
struct Member<'a> {
    key: Cow<'a, str>,
    value: &'a str,
}

/// Reports whether `source` is a JSON array with no elements.
fn is_empty_array(source: &str) -> bool {
    let mut scanner = Scanner::new(source);
    scanner.skip_whitespace();
    if !scanner.eat(b'[') {
        return false;
    }
    scanner.skip_whitespace();
    if !scanner.eat(b']') {
        return false;
    }
    scanner.skip_whitespace();
    scanner.position == source.len()
}

/// Splits a JSON object into its members without reinterpreting values.
fn object_members(source: &str) -> Result<Vec<Member<'_>>, Error> {
    let not_object = || unique_args_error("unique args must encode a JSON object".to_owned());
    let mut scanner = Scanner::new(source);
    scanner.skip_whitespace();
    if !scanner.eat(b'{') {
        return Err(not_object());
    }
    let mut members = Vec::new();
    scanner.skip_whitespace();
    if scanner.eat(b'}') {
        return Ok(members);
    }
    loop {
        scanner.skip_whitespace();
        let key = scanner.string().ok_or_else(not_object)?;
        scanner.skip_whitespace();
        if !scanner.eat(b':') {
            return Err(not_object());
        }
        scanner.skip_whitespace();
        let value = scanner.value().ok_or_else(not_object)?;
        members.push(Member {
            key: unescape_key(key)?,
            value,
        });
        scanner.skip_whitespace();
        if scanner.eat(b',') {
            continue;
        }
        if scanner.eat(b'}') {
            return Ok(members);
        }
        return Err(not_object());
    }
}

fn unescape_key(token: &str) -> Result<Cow<'_, str>, Error> {
    if token.contains('\\') {
        return serde_json::from_str::<String>(token)
            .map(Cow::Owned)
            .map_err(Error::from);
    }
    Ok(Cow::Borrowed(&token[1..token.len() - 1]))
}

/// Minimal scanner over JSON text already validated by `serde_json`.
struct Scanner<'a> {
    bytes: &'a [u8],
    position: usize,
    source: &'a str,
}

impl<'a> Scanner<'a> {
    const fn new(source: &'a str) -> Self {
        Self {
            bytes: source.as_bytes(),
            position: 0,
            source,
        }
    }

    fn eat(&mut self, expected: u8) -> bool {
        if self.bytes.get(self.position) == Some(&expected) {
            self.position += 1;
            return true;
        }
        false
    }

    fn skip_whitespace(&mut self) {
        while matches!(
            self.bytes.get(self.position),
            Some(b' ' | b'\t' | b'\n' | b'\r')
        ) {
            self.position += 1;
        }
    }

    /// Consumes a string token and returns it including its quotes.
    fn string(&mut self) -> Option<&'a str> {
        let start = self.position;
        if !self.eat(b'"') {
            return None;
        }
        while let Some(&byte) = self.bytes.get(self.position) {
            self.position += 1;
            match byte {
                b'\\' => self.position += 1,
                b'"' => return self.source.get(start..self.position),
                _ => {}
            }
        }
        None
    }

    /// Consumes any JSON value and returns its raw text.
    fn value(&mut self) -> Option<&'a str> {
        let start = self.position;
        match self.bytes.get(self.position)? {
            b'"' => {
                self.string()?;
            }
            b'{' | b'[' => {
                let mut depth = 0_usize;
                loop {
                    match self.bytes.get(self.position)? {
                        b'"' => {
                            self.string()?;
                            continue;
                        }
                        b'{' | b'[' => depth += 1,
                        b'}' | b']' => {
                            depth -= 1;
                            if depth == 0 {
                                self.position += 1;
                                break;
                            }
                        }
                        _ => {}
                    }
                    self.position += 1;
                }
            }
            _ => {
                while !matches!(
                    self.bytes.get(self.position),
                    None | Some(b',' | b'}' | b']' | b' ' | b'\t' | b'\n' | b'\r')
                ) {
                    self.position += 1;
                }
            }
        }
        self.source.get(start..self.position)
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
    use std::collections::BTreeMap;

    use chrono::TimeZone;
    use serde::{Deserialize, Serialize};

    use super::*;
    use crate::{JobArgs, JobArgs as JobArgsDerive, JobState, encoding::encode_args};

    #[derive(Deserialize)]
    struct Fixture {
        cases: Vec<FixtureCase>,
        protocol_revision: u32,
        typed_only_cases: Vec<FixtureCase>,
    }

    #[derive(Deserialize)]
    struct FixtureCase {
        args: Box<RawValue>,
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

    impl FixtureCase {
        /// The fixture file is indented; Go hashed the compact encoding.
        fn compact_args(&self) -> Box<RawValue> {
            RawValue::from_string(compact_json(self.args.get())).unwrap()
        }

        fn unique_opts(&self) -> UniqueOpts {
            UniqueOpts {
                by_args: self.options.by_args,
                by_period: (self.options.by_period_nanos > 0)
                    .then(|| Duration::from_nanos(self.options.by_period_nanos)),
                by_queue: self.options.by_queue,
                by_state: self.options.by_state.clone(),
                exclude_kind: self.options.exclude_kind,
            }
        }

        fn expected_key(&self) -> [u8; 32] {
            let mut decoded = [0_u8; 32];
            for (index, byte) in decoded.iter_mut().enumerate() {
                *byte = u8::from_str_radix(&self.expected_sha256[index * 2..index * 2 + 2], 16)
                    .unwrap();
            }
            decoded
        }
    }

    #[derive(Deserialize)]
    struct FixtureOptions {
        by_args: bool,
        by_period_nanos: u64,
        by_queue: bool,
        by_state: Option<Vec<JobState>>,
        exclude_kind: bool,
    }

    fn fixture() -> Fixture {
        let fixture: Fixture = serde_json::from_str(include_str!(
            "../../../conformance/fixtures/unique_keys.json"
        ))
        .unwrap();
        assert_eq!(fixture.protocol_revision, 1);
        fixture
    }

    fn golden(name: &str) -> FixtureCase {
        let fixture = fixture();
        fixture
            .cases
            .into_iter()
            .chain(fixture.typed_only_cases)
            .find(|case| case.name == name)
            .unwrap_or_else(|| panic!("missing golden {name}"))
    }

    /// Removes insignificant whitespace, as Go's `json.Compact` does.
    fn compact_json(source: &str) -> String {
        let mut output = String::with_capacity(source.len());
        let mut in_string = false;
        let mut escaped = false;
        for character in source.chars() {
            if in_string {
                output.push(character);
                if escaped {
                    escaped = false;
                } else if character == '\\' {
                    escaped = true;
                } else if character == '"' {
                    in_string = false;
                }
            } else if character == '"' {
                in_string = true;
                output.push(character);
            } else if !character.is_ascii_whitespace() {
                output.push(character);
            }
        }
        output
    }

    fn key_for(
        kind: &str,
        unique_fields: &[&str],
        args: &RawValue,
        opts: &UniqueOpts,
    ) -> Result<[u8; 32], Error> {
        build_unique_key_parts(
            kind,
            unique_fields,
            args,
            Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap(),
            opts,
            "default",
            None,
        )
        .map(Option::unwrap)
    }

    /// Asserts that typed Rust arguments encode to the exact bytes Go encoded
    /// for the golden case and hash to Go's key.
    fn assert_typed_golden<A: JobArgs>(name: &str, args: &A) {
        let case = golden(name);
        let encoded = encode_args(args).unwrap();
        assert_eq!(encoded.get(), case.compact_args().get(), "golden {name}");
        assert_eq!(case.kind, A::KIND, "golden {name}");
        assert_eq!(
            key_for(A::KIND, A::unique_fields(), &encoded, &case.unique_opts()).unwrap(),
            case.expected_key(),
            "golden {name}"
        );
    }

    #[test]
    fn matches_go_generated_golden_keys() {
        for case in fixture().cases {
            let unique_paths = case.selected_unique_paths.clone().unwrap_or_default();
            let unique_path_refs = unique_paths.iter().map(String::as_str).collect::<Vec<_>>();
            let opts = case.unique_opts();
            let actual = build_unique_key_parts(
                &case.kind,
                &unique_path_refs,
                &case.compact_args(),
                case.now,
                &opts,
                &case.queue,
                case.scheduled_at,
            )
            .unwrap()
            .unwrap();
            assert_eq!(case.expected_key(), actual, "fixture {}", case.name);
            assert_eq!(
                case.expected_state_mask,
                opts.state_bitmask(),
                "fixture {}",
                case.name
            );
        }
    }

    #[derive(Deserialize, JobArgsDerive, Serialize)]
    #[river(kind = "conformance_all_args")]
    struct AllArgs {
        zeta: String,
        alpha: String,
        maximum: i64,
    }

    #[test]
    fn typed_args_match_go_sorted_and_escaped_golden() {
        assert_typed_golden(
            "all_args_sorted_and_escaped",
            &AllArgs {
                alpha: "<alpha>&\u{2028}line".to_owned(),
                maximum: 9_007_199_254_740_991,
                zeta: r#"quoted \"value\" and \\ slash"#.to_owned(),
            },
        );
    }

    #[derive(Deserialize, JobArgsDerive, Serialize)]
    #[river(kind = "conformance_all_args")]
    struct CollectionsArgs {
        empty: Vec<String>,
        labels: BTreeMap<String, String>,
        matrix: Vec<Vec<i64>>,
        missing: Option<Vec<String>>,
        objects: Vec<CollectionsItem>,
        pointer: Option<String>,
    }

    #[derive(Deserialize, Serialize)]
    struct CollectionsItem {
        zulu: String,
        alpha: Option<i64>,
    }

    #[test]
    fn typed_args_match_go_collections_golden() {
        assert_typed_golden(
            "typed_collections_and_nulls",
            &CollectionsArgs {
                empty: Vec::new(),
                labels: BTreeMap::from(
                    [
                        ("zulu", "last"),
                        ("alpha", "first"),
                        ("k10", "ten"),
                        ("k2", "two"),
                    ]
                    .map(|(key, value)| (key.to_owned(), value.to_owned())),
                ),
                matrix: vec![vec![3, 1], vec![], vec![2]],
                missing: None,
                objects: vec![
                    CollectionsItem {
                        zulu: "z".to_owned(),
                        alpha: Some(1),
                    },
                    CollectionsItem {
                        zulu: "y".to_owned(),
                        alpha: None,
                    },
                ],
                pointer: None,
            },
        );
    }

    /// Go writes map keys in sorted byte order, so `"10"` precedes `"2"`.
    /// Rust's `BTreeMap` matches; the shared adapter goldens avoid this case
    /// because JavaScript objects enumerate integer-like keys numerically.
    #[test]
    fn typed_args_match_go_integer_like_map_keys_golden() {
        assert_typed_golden(
            "typed_integer_like_map_keys",
            &CollectionsArgs {
                empty: Vec::new(),
                labels: BTreeMap::from(
                    [
                        ("zulu", "last"),
                        ("alpha", "first"),
                        ("10", "ten"),
                        ("2", "two"),
                    ]
                    .map(|(key, value)| (key.to_owned(), value.to_owned())),
                ),
                matrix: Vec::new(),
                missing: None,
                objects: Vec::new(),
                pointer: None,
            },
        );
    }

    #[derive(Deserialize, JobArgsDerive, Serialize)]
    #[river(kind = "conformance_all_args")]
    struct EmptyArgs {}

    #[test]
    fn typed_args_match_go_empty_golden() {
        assert_typed_golden("typed_empty_args", &EmptyArgs {});
    }

    #[derive(Deserialize, JobArgsDerive, Serialize)]
    #[river(kind = "conformance_all_args")]
    struct EscapingArgs {
        #[serde(rename = "a<b>")]
        angle: String,
        controls: String,
        html: String,
        keys: BTreeMap<String, i64>,
        separators: String,
        unicode: String,
        #[serde(rename = "é&")]
        unicode_amp: String,
    }

    #[test]
    fn typed_args_match_go_escaping_golden() {
        assert_typed_golden(
            "typed_escaping",
            &EscapingArgs {
                angle: "<angle>".to_owned(),
                controls: "\u{8}\u{c}\n\r\t\u{0}\u{1}\u{1f}\u{7f}".to_owned(),
                html: r#"<a href="x">&amp;</a>"#.to_owned(),
                keys: BTreeMap::from(
                    [("<k>", 1), ("a&b", 2), ("é", 3), ("é<", 4)]
                        .map(|(key, value)| (key.to_owned(), value)),
                ),
                separators: "line\u{2028}paragraph\u{2029}end".to_owned(),
                unicode: "é😀/\\".to_owned(),
                unicode_amp: "unicode key".to_owned(),
            },
        );
    }

    #[derive(Deserialize, JobArgsDerive, Serialize)]
    #[river(kind = "conformance_all_args")]
    struct NestedOrderArgs {
        nested: NestedOrder,
    }

    #[derive(Deserialize, Serialize)]
    struct NestedOrder {
        z: i64,
        a: i64,
    }

    #[test]
    fn typed_args_match_go_nested_order_golden() {
        assert_typed_golden(
            "nested_struct_wire_order",
            &NestedOrderArgs {
                nested: NestedOrder { z: 1, a: 2 },
            },
        );
    }

    #[derive(Deserialize, JobArgsDerive, Serialize)]
    #[river(kind = "conformance_numeric_boundaries")]
    struct NumericBoundaryArgs {
        exponent: f64,
        fraction: f64,
        maximum: i64,
        minimum: i64,
        unsigned_maximum: u64,
    }

    #[test]
    fn typed_args_match_go_numeric_boundaries_golden() {
        assert_typed_golden(
            "numeric_boundaries",
            &NumericBoundaryArgs {
                exponent: 1e100,
                fraction: 1.25,
                maximum: i64::MAX,
                minimum: i64::MIN,
                unsigned_maximum: u64::MAX,
            },
        );
    }

    #[derive(Default, Deserialize, Serialize)]
    struct SelectedAccount {
        #[serde(skip_serializing_if = "String::is_empty")]
        id: String,
        #[serde(skip_serializing_if = "String::is_empty")]
        ignored: String,
        #[serde(skip_serializing_if = "String::is_empty")]
        region: String,
    }

    impl SelectedAccount {
        fn is_zero(&self) -> bool {
            self.id.is_empty() && self.ignored.is_empty() && self.region.is_empty()
        }
    }

    #[derive(Deserialize, JobArgsDerive, Serialize)]
    #[river(
        kind = "conformance_selected_args",
        unique(by_args("account.id", "account.region", "label", "path/key"))
    )]
    struct SelectedArgs {
        #[serde(skip_serializing_if = "SelectedAccount::is_zero")]
        account: SelectedAccount,
        #[serde(skip_serializing_if = "std::ops::Not::not")]
        ignored: bool,
        #[serde(skip_serializing_if = "String::is_empty")]
        label: String,
        #[serde(rename = "path/key", skip_serializing_if = "String::is_empty")]
        path_key: String,
    }

    #[test]
    fn typed_args_match_go_selected_goldens() {
        assert_typed_golden(
            "all_selected_fields_omitted",
            &SelectedArgs {
                account: SelectedAccount::default(),
                ignored: false,
                label: String::new(),
                path_key: String::new(),
            },
        );
        assert_typed_golden(
            "selected_siblings_and_slash_key",
            &SelectedArgs {
                account: SelectedAccount {
                    id: "acct".to_owned(),
                    ignored: "irrelevant".to_owned(),
                    region: "west".to_owned(),
                },
                ignored: false,
                label: String::new(),
                path_key: "slash".to_owned(),
            },
        );
        assert_typed_golden(
            "selected_nested_args",
            &SelectedArgs {
                account: SelectedAccount {
                    id: "acct-123".to_owned(),
                    ignored: "not selected".to_owned(),
                    region: String::new(),
                },
                ignored: true,
                label: "selected".to_owned(),
                path_key: String::new(),
            },
        );
    }

    #[derive(Deserialize, JobArgsDerive, Serialize)]
    #[river(
        kind = "conformance_selected_args",
        unique(by_args("account.id", "account.region", "label", "path/key"))
    )]
    struct SelectedNullArgs {
        #[serde(skip_serializing_if = "SelectedAccount::is_zero")]
        account: SelectedAccount,
        label: Option<String>,
        #[serde(rename = "path/key", skip_serializing_if = "String::is_empty")]
        path_key: String,
    }

    #[test]
    fn typed_args_match_go_selected_explicit_null_golden() {
        assert_typed_golden(
            "selected_explicit_null",
            &SelectedNullArgs {
                account: SelectedAccount::default(),
                label: None,
                path_key: String::new(),
            },
        );
    }

    #[derive(Deserialize, JobArgsDerive, Serialize)]
    #[river(kind = "conformance_all_args")]
    struct TimeArgs {
        #[serde(with = "crate::encoding::go_time")]
        fraction: DateTime<Utc>,
        #[serde(with = "crate::encoding::go_time")]
        micros: DateTime<Utc>,
        #[serde(with = "crate::encoding::go_time")]
        millis: DateTime<Utc>,
        whole: DateTime<Utc>,
    }

    #[test]
    fn typed_args_match_go_time_golden() {
        let whole = Utc.with_ymd_and_hms(2026, 1, 2, 3, 4, 5).unwrap();
        assert_typed_golden(
            "typed_time_values",
            &TimeArgs {
                fraction: whole + chrono::Duration::milliseconds(500),
                micros: whole + chrono::Duration::microseconds(123_456),
                millis: whole + chrono::Duration::milliseconds(120),
                // Whole seconds need no helper: chrono and Go agree.
                whole,
            },
        );
    }

    #[derive(Deserialize, JobArgsDerive, Serialize)]
    #[river(kind = "conformance_all_args")]
    struct TypedFloatArgs {
        below_large: f64,
        large: f64,
        large_boundary: f64,
        largest: f64,
        negative: f64,
        negative_zero: f64,
        one: f64,
        single: f32,
        single_large: f32,
        single_small: f32,
        small: f64,
        small_boundary: f64,
        smallest: f64,
        tenth: f64,
    }

    #[test]
    fn typed_args_match_go_float_golden() {
        assert_typed_golden(
            "typed_float_formatting",
            &TypedFloatArgs {
                below_large: f64::from_bits(1e21_f64.to_bits() - 1),
                large: 1e20,
                large_boundary: 1e21,
                largest: f64::MAX,
                negative: -1.5e-9,
                negative_zero: -0.0,
                one: 1.0,
                single: 1.1,
                single_large: 1e21,
                single_small: 1e-7,
                small: 1e-7,
                small_boundary: 1e-6,
                smallest: 5e-324,
                tenth: 0.1,
            },
        );
    }

    #[derive(Deserialize, Serialize)]
    #[serde(transparent)]
    struct MapArgs(BTreeMap<String, f64>);

    impl JobArgs for MapArgs {
        const KIND: &'static str = "conformance_all_args";
    }

    #[test]
    fn typed_map_args_hash_like_go_despite_member_order() {
        // Go encoded these members in a custom order; the all-arguments hash
        // sorts top-level keys, so a sorted Rust map hashes identically.
        let case = golden("map_order_and_negative_zero");
        let args = MapArgs(BTreeMap::from(
            [
                ("2", 2.0),
                ("10", 10.0),
                ("zero", -0.0),
                ("😀", 1.0),
                ("\u{e000}", 2.0),
            ]
            .map(|(key, value)| (key.to_owned(), value)),
        ));
        let encoded = encode_args(&args).unwrap();
        assert_ne!(encoded.get(), case.compact_args().get());
        assert_eq!(
            key_for(MapArgs::KIND, &[], &encoded, &case.unique_opts()).unwrap(),
            case.expected_key()
        );
    }

    #[test]
    fn hashes_raw_bytes_without_reinterpreting_values() {
        let opts = UniqueOpts::new().by_args();
        let raw = |json: &str| RawValue::from_string(json.to_owned()).unwrap();
        let key = |json: &str| key_for("raw", &[], &raw(json), &opts).unwrap();
        let expected =
            |text: &str| -> [u8; 32] { Sha256::digest(format!("&kind=raw&args={text}")).into() };

        // Number tokens, nested whitespace, and nested order are hashed as
        // written; top-level whitespace is not.
        assert_eq!(
            key(r#"{"b":1.0,"a":1e2}"#),
            expected(r#"{"a":1e2,"b":1.0}"#)
        );
        assert_eq!(
            key(r#" { "a" : [1, {"z":1, "y":2}] , "b":null } "#),
            expected(r#"{"a":[1, {"z":1, "y":2}],"b":null}"#)
        );
        // Duplicate keys resolve to their first value.
        assert_eq!(key(r#"{"a":1,"a":2}"#), expected(r#"{"a":1}"#));
        // Escaped printable ASCII keys are written verbatim, like sjson; other
        // keys are re-encoded with Go's escaping.
        assert_eq!(
            key(r#"{"\u003ck\u003e":1,"a\"b":2,"é\u0026":3,"line\n":4}"#),
            expected(r#"{"<k>":1,"a\"b":2,"line\n":4,"é\u0026":3}"#)
        );
    }

    #[test]
    fn selected_paths_follow_sjson_assembly() {
        let raw = RawValue::from_string(
            r#"{"b":{"y":2,"x":1},"a-b":3,"a":{"c":null},"ignored":true}"#.to_owned(),
        )
        .unwrap();
        let mut output = String::new();
        write_selected_args(
            raw.get(),
            &["b.x", "a.c", "a-b", "b.y", "missing.path"],
            &mut output,
        )
        .unwrap();
        // Paths are applied in sorted order ("a-b" < "a.c" < "b.x" < "b.y"),
        // explicit nulls are kept, and missing paths are omitted.
        assert_eq!(output, r#"{"a-b":3,"a":{"c":null},"b":{"x":1,"y":2}}"#);

        let mut output = String::new();
        write_selected_args(raw.get(), &["missing"], &mut output).unwrap();
        assert_eq!(output, "");
    }

    #[test]
    fn hashes_literal_top_level_names() {
        let opts = UniqueOpts::new().by_args();
        for json in [r#"{"a.b":1}"#, r#"{"@this":1}"#, r#"{"":1}"#] {
            let raw = RawValue::from_string(json.to_owned()).unwrap();
            assert!(key_for("raw", &[], &raw, &opts).is_ok(), "{json}");
        }

        let raw = RawValue::from_string(r#"{"a":{"b":1}}"#.to_owned()).unwrap();
        for paths in [&["a", "a.b"][..], &["a."]] {
            assert!(key_for("raw", paths, &raw, &opts).is_err(), "{paths:?}");
        }
    }

    #[test]
    fn empty_array_all_args_hash_an_empty_object() {
        let opts = UniqueOpts::new().by_args();
        let expected: [u8; 32] = Sha256::digest(b"&kind=raw&args={}").into();
        for json in ["[]", " [ \n] "] {
            let raw = RawValue::from_string(json.to_owned()).unwrap();
            assert_eq!(
                key_for("raw", &[], &raw, &opts).unwrap(),
                expected,
                "{json}"
            );
        }
    }

    #[test]
    fn non_object_all_args_are_rejected() {
        let opts = UniqueOpts::new().by_args();
        for json in ["[1]", "[[]]", "[{}]", "1", "true", "null", r#""text""#] {
            let raw = RawValue::from_string(json.to_owned()).unwrap();
            let error = key_for("raw", &[], &raw, &opts).unwrap_err();
            assert!(
                error
                    .to_string()
                    .contains("unique args must encode a JSON object"),
                "{json}: {error}"
            );
            assert!(key_for("raw", &["a"], &raw, &opts).is_err(), "{json}");
        }
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
