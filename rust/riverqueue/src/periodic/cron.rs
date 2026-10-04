//! Standard cron schedules with River Go's semantics.
//!
//! River Go documents periodic cron schedules parsed by robfig/cron's
//! `ParseStandard`. This module ports that parser and its `Next` algorithm so
//! that one expression string fires at the same times from either language:
//! five fields (minute, hour, day of month, month, day of week), weekdays
//! numbered 0-6 from Sunday, case-insensitive month and weekday names,
//! ranges, steps, lists, `*` and `?`, Vixie cron's day-of-month *or*
//! day-of-week rule, the `@yearly`, `@annually`, `@monthly`, `@weekly`,
//! `@daily`, `@midnight`, `@hourly`, and `@every <duration>` descriptors, and
//! `CRON_TZ=`/`TZ=` prefixes.

use std::{fmt, str::FromStr, time::Duration};

use chrono::{
    DateTime, Datelike, FixedOffset, Local, NaiveDate, NaiveDateTime, Offset as _, TimeZone,
    Timelike, Utc,
};
use thiserror::Error as ThisError;

use super::PeriodicSchedule;

/// Set when a field was written as `*` or `?` (robfig's `starBit`).
const STAR_BIT: u64 = 1 << 63;

struct Bounds {
    maximum: u32,
    minimum: u32,
    names: &'static [(&'static str, u32)],
}

const MINUTES: Bounds = Bounds {
    maximum: 59,
    minimum: 0,
    names: &[],
};
const HOURS: Bounds = Bounds {
    maximum: 23,
    minimum: 0,
    names: &[],
};
const DAYS_OF_MONTH: Bounds = Bounds {
    maximum: 31,
    minimum: 1,
    names: &[],
};
const MONTHS: Bounds = Bounds {
    maximum: 12,
    minimum: 1,
    names: &[
        ("jan", 1),
        ("feb", 2),
        ("mar", 3),
        ("apr", 4),
        ("may", 5),
        ("jun", 6),
        ("jul", 7),
        ("aug", 8),
        ("sep", 9),
        ("oct", 10),
        ("nov", 11),
        ("dec", 12),
    ],
};
const DAYS_OF_WEEK: Bounds = Bounds {
    maximum: 6,
    minimum: 0,
    names: &[
        ("sun", 0),
        ("mon", 1),
        ("tue", 2),
        ("wed", 3),
        ("thu", 4),
        ("fri", 5),
        ("sat", 6),
    ],
};

/// Time zone in which a [`CronSchedule`] is evaluated.
#[derive(Clone, Copy, Debug, Default, Eq, PartialEq)]
#[non_exhaustive]
pub enum CronTimeZone {
    /// The process's local time zone, the default. Containers usually run in
    /// UTC; set an explicit zone when clients in different zones share a
    /// schedule.
    #[default]
    Local,
    /// Coordinated Universal Time.
    Utc,
    /// A fixed offset from UTC, without daylight saving time.
    Fixed(FixedOffset),
    /// A zone from the IANA time zone database, with its daylight saving
    /// rules. `CRON_TZ=` and `TZ=` prefixes naming a zone such as
    /// `America/New_York` parse to this variant.
    #[cfg(feature = "chrono-tz")]
    Named(chrono_tz::Tz),
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Spec {
    Every(Duration),
    Fields {
        dom: u64,
        dow: u64,
        hour: u64,
        minute: u64,
        month: u64,
    },
}

/// A periodic schedule written in standard five-field cron syntax.
///
/// Parsing accepts exactly the expressions every River client accepts, and
/// [`CronSchedule::next_after`] returns the same occurrences, so a schedule
/// fires at the same times whichever client leads.
/// Without the `chrono-tz` feature, which bundles the IANA time zone
/// database, named `CRON_TZ=`/`TZ=` zones are limited to `UTC`, `Local`, and
/// `Etc/GMT±N`, and other names are rejected; use
/// [`CronSchedule::with_time_zone`] for other fixed offsets.
///
/// Across daylight saving transitions, a daily schedule at a skipped
/// wall-clock time doesn't fire that day, and one at a repeated time can
/// fire twice.
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct CronSchedule {
    expression: String,
    spec: Spec,
    zone: Option<CronTimeZone>,
}

impl CronSchedule {
    /// Parses a standard five-field cron expression or descriptor.
    ///
    /// # Errors
    ///
    /// Returns an error for an expression robfig/cron's `ParseStandard`
    /// rejects, and for a `CRON_TZ=` zone this build can't resolve.
    pub fn parse(expression: &str) -> Result<Self, CronScheduleParseError> {
        expression.parse()
    }

    /// Evaluates the schedule in `zone`, overriding any `CRON_TZ=` prefix.
    #[must_use]
    pub fn with_time_zone(mut self, zone: CronTimeZone) -> Self {
        self.zone = Some(zone);
        self
    }

    /// Returns the zone in which periodic occurrences are computed.
    #[must_use]
    pub fn time_zone(&self) -> CronTimeZone {
        self.zone.unwrap_or_default()
    }

    /// Returns the first occurrence strictly after `after`, or `None` when
    /// the schedule never matches within five years (robfig's zero time).
    ///
    /// Without an explicit zone, the occurrence is computed in `after`'s own
    /// time zone, exactly like robfig's `Next` for a schedule without
    /// `CRON_TZ`.
    #[must_use]
    pub fn next_after<Tz: TimeZone>(&self, after: &DateTime<Tz>) -> Option<DateTime<Tz>> {
        let zone = after.timezone();
        match self.zone {
            None => next_in(self.spec, after.clone()),
            Some(CronTimeZone::Local) => next_in(self.spec, after.with_timezone(&Local))
                .map(|next| next.with_timezone(&zone)),
            Some(CronTimeZone::Utc) => {
                next_in(self.spec, after.with_timezone(&Utc)).map(|next| next.with_timezone(&zone))
            }
            Some(CronTimeZone::Fixed(offset)) => next_in(self.spec, after.with_timezone(&offset))
                .map(|next| next.with_timezone(&zone)),
            #[cfg(feature = "chrono-tz")]
            Some(CronTimeZone::Named(named)) => next_in(self.spec, after.with_timezone(&named))
                .map(|next| next.with_timezone(&zone)),
        }
    }
}

impl FromStr for CronSchedule {
    type Err = CronScheduleParseError;

    fn from_str(expression: &str) -> Result<Self, Self::Err> {
        let (zone, spec) = parse(expression).map_err(|message| CronScheduleParseError {
            expression: expression.to_owned(),
            message,
        })?;
        Ok(Self {
            expression: expression.to_owned(),
            spec,
            zone,
        })
    }
}

impl fmt::Display for CronSchedule {
    fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
        formatter.write_str(&self.expression)
    }
}

impl PeriodicSchedule for CronSchedule {
    fn next(&self, current: DateTime<Utc>) -> Option<DateTime<Utc>> {
        match self.time_zone() {
            CronTimeZone::Local => next_in(self.spec, current.with_timezone(&Local))
                .map(|next| next.with_timezone(&Utc)),
            CronTimeZone::Utc => next_in(self.spec, current),
            CronTimeZone::Fixed(offset) => next_in(self.spec, current.with_timezone(&offset))
                .map(|next| next.with_timezone(&Utc)),
            #[cfg(feature = "chrono-tz")]
            CronTimeZone::Named(named) => next_in(self.spec, current.with_timezone(&named))
                .map(|next| next.with_timezone(&Utc)),
        }
    }
}

/// Error returned when parsing a [`CronSchedule`].
#[derive(Clone, Debug, Eq, PartialEq, ThisError)]
#[error("invalid periodic cron expression {expression:?}: {message}")]
pub struct CronScheduleParseError {
    expression: String,
    message: String,
}

fn parse(expression: &str) -> Result<(Option<CronTimeZone>, Spec), String> {
    if expression.is_empty() {
        return Err("empty spec string".to_owned());
    }
    let mut spec = expression;
    let mut zone = None;
    if spec.starts_with("TZ=") || spec.starts_with("CRON_TZ=") {
        let space = spec
            .find(' ')
            .ok_or_else(|| "time zone prefix must be followed by a schedule".to_owned())?;
        let equals = spec.find('=').expect("prefix contains '='");
        zone = Some(parse_zone(&spec[equals + 1..space])?);
        spec = spec[space..].trim();
    }
    if spec.starts_with('@') {
        return parse_descriptor(spec).map(|spec| (zone, spec));
    }
    let fields = spec.split_whitespace().collect::<Vec<_>>();
    if fields.len() != 5 {
        return Err(format!(
            "expected exactly 5 fields, found {}: {fields:?}",
            fields.len()
        ));
    }
    Ok((
        zone,
        Spec::Fields {
            minute: field(fields[0], &MINUTES)?,
            hour: field(fields[1], &HOURS)?,
            dom: field(fields[2], &DAYS_OF_MONTH)?,
            month: field(fields[3], &MONTHS)?,
            dow: field(fields[4], &DAYS_OF_WEEK)?,
        },
    ))
}

fn parse_zone(name: &str) -> Result<CronTimeZone, String> {
    match name {
        // Go's `time.LoadLocation` maps "" to UTC.
        "" | "UTC" | "Etc/UTC" => return Ok(CronTimeZone::Utc),
        "Local" => return Ok(CronTimeZone::Local),
        _ => {}
    }
    // POSIX-style `Etc/GMT+5` means five hours *behind* UTC.
    if let Some(offset) = name.strip_prefix("Etc/GMT")
        && let Some((sign, hours)) = offset
            .strip_prefix('+')
            .map(|hours| (-1, hours))
            .or_else(|| offset.strip_prefix('-').map(|hours| (1, hours)))
        && let Ok(hours) = hours.parse::<i32>()
        && (0..=14).contains(&hours)
        && let Some(offset) = FixedOffset::east_opt(sign * hours * 3_600)
    {
        return Ok(CronTimeZone::Fixed(offset));
    }
    #[cfg(feature = "chrono-tz")]
    if let Ok(named) = name.parse::<chrono_tz::Tz>() {
        return Ok(CronTimeZone::Named(named));
    }
    #[cfg(feature = "chrono-tz")]
    return Err(format!("provided bad location {name}: unknown time zone"));
    #[cfg(not(feature = "chrono-tz"))]
    Err(format!(
        "provided bad location {name}: only UTC, Local, and Etc/GMT offsets are supported \
         without River's chrono-tz feature; use CronSchedule::with_time_zone for other zones"
    ))
}

fn parse_descriptor(descriptor: &str) -> Result<Spec, String> {
    let all = |bounds: &Bounds| bits(bounds.minimum, bounds.maximum, 1) | STAR_BIT;
    let spec = match descriptor {
        "@yearly" | "@annually" => Spec::Fields {
            dom: 1 << 1,
            dow: all(&DAYS_OF_WEEK),
            hour: 1,
            minute: 1,
            month: 1 << 1,
        },
        "@monthly" => Spec::Fields {
            dom: 1 << 1,
            dow: all(&DAYS_OF_WEEK),
            hour: 1,
            minute: 1,
            month: all(&MONTHS),
        },
        "@weekly" => Spec::Fields {
            dom: all(&DAYS_OF_MONTH),
            dow: 1,
            hour: 1,
            minute: 1,
            month: all(&MONTHS),
        },
        "@daily" | "@midnight" => Spec::Fields {
            dom: all(&DAYS_OF_MONTH),
            dow: all(&DAYS_OF_WEEK),
            hour: 1,
            minute: 1,
            month: all(&MONTHS),
        },
        "@hourly" => Spec::Fields {
            dom: all(&DAYS_OF_MONTH),
            dow: all(&DAYS_OF_WEEK),
            hour: all(&HOURS),
            minute: 1,
            month: all(&MONTHS),
        },
        _ => {
            let Some(duration) = descriptor.strip_prefix("@every ") else {
                return Err(format!("unrecognized descriptor: {descriptor}"));
            };
            let nanos = parse_go_duration(duration)
                .map_err(|message| format!("failed to parse duration {descriptor}: {message}"))?;
            // robfig's `Every` rounds up to one second and drops subseconds.
            let nanos = nanos.max(1_000_000_000);
            let nanos = nanos - nanos % 1_000_000_000;
            Spec::Every(Duration::from_nanos(
                u64::try_from(nanos).expect("positive duration"),
            ))
        }
    };
    Ok(spec)
}

/// Parses a comma-separated list of ranges, skipping empty items like Go's
/// `strings.FieldsFunc`.
fn field(field: &str, bounds: &Bounds) -> Result<u64, String> {
    let mut result = 0;
    for expression in field.split(',').filter(|expression| !expression.is_empty()) {
        result |= range(expression, bounds)?;
    }
    Ok(result)
}

fn range(expression: &str, bounds: &Bounds) -> Result<u64, String> {
    let range_and_step = expression.split('/').collect::<Vec<_>>();
    let low_and_high = range_and_step[0].split('-').collect::<Vec<_>>();
    let single = low_and_high.len() == 1;
    let (start, mut end, mut extra) = if low_and_high[0] == "*" || low_and_high[0] == "?" {
        (bounds.minimum, bounds.maximum, STAR_BIT)
    } else {
        let start = int_or_name(low_and_high[0], bounds)?;
        let end = match low_and_high.len() {
            1 => start,
            2 => int_or_name(low_and_high[1], bounds)?,
            _ => return Err(format!("too many hyphens: {expression}")),
        };
        (start, end, 0)
    };
    let step = match range_and_step.len() {
        1 => 1,
        2 => {
            let step = go_atoi(range_and_step[1])?;
            // "N/step" means "N-max/step".
            if single {
                end = bounds.maximum;
            }
            if step > 1 {
                extra = 0;
            }
            step
        }
        _ => return Err(format!("too many slashes: {expression}")),
    };
    if start < bounds.minimum {
        return Err(format!(
            "beginning of range ({start}) below minimum ({}): {expression}",
            bounds.minimum
        ));
    }
    if end > bounds.maximum {
        return Err(format!(
            "end of range ({end}) above maximum ({}): {expression}",
            bounds.maximum
        ));
    }
    if start > end {
        return Err(format!(
            "beginning of range ({start}) beyond end of range ({end}): {expression}"
        ));
    }
    if step == 0 {
        return Err(format!(
            "step of range should be a positive number: {expression}"
        ));
    }
    Ok(bits(start, end, step) | extra)
}

fn int_or_name(expression: &str, bounds: &Bounds) -> Result<u32, String> {
    let lower = expression.to_ascii_lowercase();
    if let Some((_, value)) = bounds.names.iter().find(|(name, _)| *name == lower) {
        return Ok(*value);
    }
    go_atoi(expression)
}

/// Go's `strconv.Atoi` followed by robfig's non-negative check.
fn go_atoi(expression: &str) -> Result<u32, String> {
    let digits = expression.strip_prefix(['+', '-']).unwrap_or(expression);
    if digits.is_empty() || !digits.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(format!(
            "failed to parse int from {expression}: invalid syntax"
        ));
    }
    let number = expression
        .parse::<i64>()
        .map_err(|_| format!("failed to parse int from {expression}: value out of range"))?;
    if number < 0 {
        return Err(format!(
            "negative number ({number}) not allowed: {expression}"
        ));
    }
    // Anything above 63 is out of every field's range and rejected later.
    Ok(u32::try_from(number.min(i64::from(u32::MAX))).expect("clamped"))
}

fn bits(minimum: u32, maximum: u32, step: u32) -> u64 {
    let mut bits = 0;
    let mut value = minimum;
    while value <= maximum {
        bits |= 1 << value;
        value += step;
    }
    bits
}

/// Parses Go's `time.ParseDuration` syntax into signed nanoseconds.
fn parse_go_duration(text: &str) -> Result<i128, String> {
    let invalid = || format!("time: invalid duration {text:?}");
    let (negative, mut rest) = match text.as_bytes().first() {
        Some(b'-') => (true, &text[1..]),
        Some(b'+') => (false, &text[1..]),
        _ => (false, text),
    };
    if rest == "0" {
        return Ok(0);
    }
    if rest.is_empty() {
        return Err(invalid());
    }
    let mut total: i128 = 0;
    while !rest.is_empty() {
        let integer_length = rest.bytes().take_while(u8::is_ascii_digit).count();
        let integer = &rest[..integer_length];
        rest = &rest[integer_length..];
        let mut fraction = "";
        if let Some(after_dot) = rest.strip_prefix('.') {
            let fraction_length = after_dot.bytes().take_while(u8::is_ascii_digit).count();
            fraction = &after_dot[..fraction_length];
            rest = &after_dot[fraction_length..];
            if integer.is_empty() && fraction.is_empty() {
                return Err(invalid());
            }
        } else if integer.is_empty() {
            return Err(invalid());
        }
        let unit_length = rest
            .char_indices()
            .find(|(_, character)| *character == '.' || character.is_ascii_digit())
            .map_or(rest.len(), |(index, _)| index);
        let unit = &rest[..unit_length];
        rest = &rest[unit_length..];
        let unit_nanos: i128 = match unit {
            "ns" => 1,
            "us" | "\u{b5}s" | "\u{3bc}s" => 1_000,
            "ms" => 1_000_000,
            "s" => 1_000_000_000,
            "m" => 60_000_000_000,
            "h" => 3_600_000_000_000,
            "" => return Err(format!("time: missing unit in duration {text:?}")),
            _ => return Err(format!("time: unknown unit {unit:?} in duration {text:?}")),
        };
        let integer = if integer.is_empty() {
            0
        } else {
            integer.parse::<i128>().map_err(|_| invalid())?
        };
        let mut value = integer * unit_nanos;
        let mut scale = unit_nanos;
        for digit in fraction.bytes() {
            scale /= 10;
            if scale == 0 {
                break;
            }
            value += i128::from(digit - b'0') * scale;
        }
        total += value;
        if total > i128::from(i64::MAX) {
            return Err(invalid());
        }
    }
    Ok(if negative { -total } else { total })
}

/// robfig/cron's `SpecSchedule.Next` or `ConstantDelaySchedule.Next`.
#[expect(
    clippy::too_many_lines,
    reason = "a line-for-line port keeps robfig's field loops auditable"
)]
fn next_in<Z: TimeZone>(spec: Spec, after: DateTime<Z>) -> Option<DateTime<Z>> {
    let subsecond = chrono::Duration::nanoseconds(i64::from(after.nanosecond()));
    let (minute, hour, dom, month, dow) = match spec {
        Spec::Every(delay) => {
            return Some(after + chrono::Duration::from_std(delay).ok()? - subsecond);
        }
        Spec::Fields {
            dom,
            dow,
            hour,
            minute,
            month,
        } => (minute, hour, dom, month, dow),
    };
    let zone = after.timezone();
    let one_hour = chrono::Duration::hours(1);
    let one_minute = chrono::Duration::minutes(1);

    // Start at the earliest possible time (the upcoming second).
    let mut time = after + chrono::Duration::seconds(1) - subsecond;
    let mut added = false;
    let year_limit = time.year() + 5;

    'wrap: loop {
        if time.year() > year_limit {
            return None;
        }

        while (1 << time.month()) & month == 0 {
            if !added {
                added = true;
                time = go_date(&zone, time.year(), time.month(), 1, 0, 0, 0)?;
            }
            // Go's `AddDate(0, 1, 0)`, normalizing overflowing days.
            time = go_date(
                &zone,
                time.year(),
                time.month() + 1,
                time.day(),
                time.hour(),
                time.minute(),
                time.second(),
            )?;
            if time.month() == 1 {
                continue 'wrap;
            }
        }

        while !day_matches(dom, dow, &time) {
            if !added {
                added = true;
                time = go_date(&zone, time.year(), time.month(), time.day(), 0, 0, 0)?;
            }
            time = go_date(
                &zone,
                time.year(),
                time.month(),
                time.day() + 1,
                time.hour(),
                time.minute(),
                time.second(),
            )?;
            // Midnight may not exist on a daylight saving transition.
            if time.hour() != 0 {
                let hour = i64::from(time.hour());
                time = if hour > 12 {
                    time + chrono::Duration::hours(24 - hour)
                } else {
                    time - chrono::Duration::hours(hour)
                };
            }
            if time.day() == 1 {
                continue 'wrap;
            }
        }

        while (1 << time.hour()) & hour == 0 {
            if !added {
                added = true;
                time = go_date(
                    &zone,
                    time.year(),
                    time.month(),
                    time.day(),
                    time.hour(),
                    0,
                    0,
                )?;
            }
            time += one_hour;
            if time.hour() == 0 {
                continue 'wrap;
            }
        }

        while (1 << time.minute()) & minute == 0 {
            if !added {
                added = true;
                time = truncate_to_minute(time);
            }
            time += one_minute;
            if time.minute() == 0 {
                continue 'wrap;
            }
        }

        // Standard specs always fire at second zero. robfig's seconds loop
        // steps one second at a time until the minute rolls over and then
        // re-validates every field, which is a single jump here.
        let second = i64::from(time.second());
        if second != 0 {
            added = true;
            time += chrono::Duration::seconds(60 - second);
            continue 'wrap;
        }
        return Some(time);
    }
}

fn day_matches<Z: TimeZone>(dom: u64, dow: u64, time: &DateTime<Z>) -> bool {
    let day_of_month = (1 << time.day()) & dom != 0;
    let weekday = (1 << time.weekday().num_days_from_sunday()) & dow != 0;
    if dom & STAR_BIT != 0 || dow & STAR_BIT != 0 {
        day_of_month && weekday
    } else {
        day_of_month || weekday
    }
}

/// Go's `time.Truncate(time.Minute)`, which rounds absolute time.
fn truncate_to_minute<Z: TimeZone>(time: DateTime<Z>) -> DateTime<Z> {
    let seconds = time.timestamp().rem_euclid(60);
    let nanos = i64::from(time.nanosecond());
    time - chrono::Duration::seconds(seconds) - chrono::Duration::nanoseconds(nanos)
}

/// Go's `time.Date` in `zone`: overflowing months and days roll forward, and
/// a wall-clock time that a transition skips or repeats resolves as Go
/// resolves it (see [`resolve_local`]).
fn go_date<Z: TimeZone>(
    zone: &Z,
    year: i32,
    month: u32,
    day: u32,
    hour: u32,
    minute: u32,
    second: u32,
) -> Option<DateTime<Z>> {
    let months = i64::from(year) * 12 + i64::from(month) - 1;
    let year = i32::try_from(months.div_euclid(12)).ok()?;
    let month = u32::try_from(months.rem_euclid(12)).ok()? + 1;
    let date = NaiveDate::from_ymd_opt(year, month, 1)?
        .checked_add_days(chrono::Days::new(u64::from(day.checked_sub(1)?)))?;
    let naive = date.and_hms_opt(hour, minute, second)?;
    resolve_local(zone, naive)
}

/// Converts a wall-clock time in `zone` to an instant exactly like Go's
/// `time.Date`, which robfig's schedule arithmetic relies on.
///
/// Go takes the offset in effect at the instant whose UTC reading equals the
/// wall-clock time, and keeps it if subtracting it lands in the same offset
/// period; otherwise it uses the offset in effect at that result. A repeated
/// wall-clock time therefore usually resolves to its earlier instant, and a
/// skipped one to the instant an hour before the transition's end, which
/// reads as the hour before it: `02:30` on a New York spring-forward day is
/// `01:30` EST, so a daily `30 2 * * *` schedule skips that day. Like Go, a
/// zero first offset is used as is.
fn resolve_local<Z: TimeZone>(zone: &Z, naive: NaiveDateTime) -> Option<DateTime<Z>> {
    let offset_at = |instant: NaiveDateTime| {
        i64::from(
            zone.offset_from_utc_datetime(&instant)
                .fix()
                .local_minus_utc(),
        )
    };
    let mut offset = offset_at(naive);
    if offset != 0 {
        let utc = naive.checked_sub_signed(chrono::Duration::seconds(offset))?;
        let corrected = offset_at(utc);
        if corrected != offset {
            offset = corrected;
        }
    }
    let utc = naive.checked_sub_signed(chrono::Duration::seconds(offset))?;
    Some(zone.from_utc_datetime(&utc))
}

#[cfg(test)]
mod tests {
    use chrono::{DateTime, FixedOffset};
    use serde::Deserialize;

    use super::{CronSchedule, CronTimeZone, PeriodicSchedule};

    #[derive(Deserialize)]
    #[expect(
        clippy::struct_field_names,
        reason = "the fields mirror the fixture's keys"
    )]
    struct Fixture {
        cron_cases: Vec<CronCase>,
        cron_invalid: Vec<String>,
        cron_named_zone_cases: Vec<CronCase>,
    }

    #[derive(Deserialize)]
    struct CronCase {
        expression: String,
        from: DateTime<FixedOffset>,
        name: String,
        next: Vec<DateTime<FixedOffset>>,
    }

    fn fixture() -> Fixture {
        serde_json::from_str(include_str!("../../tests/fixtures/maintenance_values.json")).unwrap()
    }

    fn assert_matches_go(case: &CronCase) {
        let schedule = CronSchedule::parse(&case.expression)
            .unwrap_or_else(|error| panic!("{}: {error}", case.name));
        // The generator records five occurrences, stopping early at Go's
        // zero time.
        let mut current = case.from;
        let mut observed = Vec::new();
        while observed.len() < 5 {
            let Some(next) = schedule.next_after(&current) else {
                break;
            };
            observed.push(next);
            current = next;
        }
        assert_eq!(observed, case.next, "{}", case.name);
        for (observed, expected) in observed.iter().zip(&case.next) {
            assert_eq!(observed.offset(), expected.offset(), "{}", case.name);
        }
    }

    #[test]
    fn cron_schedules_match_go_fixture() {
        let fixture = fixture();
        assert!(!fixture.cron_cases.is_empty());
        for case in &fixture.cron_cases {
            assert_matches_go(case);
        }
    }

    // Go's `time.Date` resolves wall-clock times that a daylight saving
    // transition skips or repeats in a particular way, which decides whether
    // a schedule fires that day. Fixed offsets never exercise it, so check it
    // against the local zones Go would use, rebuilt from the named zone
    // cases' own offsets.
    #[test]
    fn transitions_resolve_like_go_time_date() {
        use chrono::{NaiveDate, TimeZone as _};

        // A zone at UTC-5 that springs forward to UTC-4 at 07:00 UTC on
        // 2026-03-08 and falls back at 06:00 UTC on 2026-11-01, like New
        // York.
        #[derive(Clone, Copy, Debug)]
        struct NewYork2026;

        impl chrono::TimeZone for NewYork2026 {
            type Offset = FixedOffset;

            fn from_offset(_: &FixedOffset) -> Self {
                Self
            }

            fn offset_from_local_date(&self, _: &NaiveDate) -> chrono::LocalResult<FixedOffset> {
                unimplemented!("River resolves wall-clock times itself")
            }

            fn offset_from_local_datetime(
                &self,
                _: &chrono::NaiveDateTime,
            ) -> chrono::LocalResult<FixedOffset> {
                unimplemented!("River resolves wall-clock times itself")
            }

            fn offset_from_utc_date(&self, _: &NaiveDate) -> FixedOffset {
                unimplemented!("River only looks up instants")
            }

            fn offset_from_utc_datetime(&self, utc: &chrono::NaiveDateTime) -> FixedOffset {
                let spring = NaiveDate::from_ymd_opt(2026, 3, 8)
                    .unwrap()
                    .and_hms_opt(7, 0, 0)
                    .unwrap();
                let fall = NaiveDate::from_ymd_opt(2026, 11, 1)
                    .unwrap()
                    .and_hms_opt(6, 0, 0)
                    .unwrap();
                let hours = if (spring..fall).contains(utc) { 4 } else { 5 };
                FixedOffset::west_opt(hours * 3_600).unwrap()
            }
        }

        let wall = |month, day, hour, minute| {
            NaiveDate::from_ymd_opt(2026, month, day)
                .unwrap()
                .and_hms_opt(hour, minute, 0)
                .unwrap()
        };
        let resolved = |naive| {
            super::resolve_local(&NewYork2026, naive)
                .unwrap()
                .naive_utc()
        };
        // A skipped time resolves to the hour before the transition.
        assert_eq!(resolved(wall(3, 8, 2, 30)), wall(3, 8, 6, 30));
        // A repeated time resolves to its earlier instant.
        assert_eq!(resolved(wall(11, 1, 1, 30)), wall(11, 1, 5, 30));
        assert_eq!(resolved(wall(7, 1, 9, 0)), wall(7, 1, 13, 0));
        assert_eq!(resolved(wall(12, 1, 9, 0)), wall(12, 1, 14, 0));

        // So, like Go, a daily schedule at a skipped time misses that day,
        // and one at a repeated time fires twice.
        let from = NewYork2026.from_utc_datetime(&wall(3, 7, 12, 0));
        let skipped = CronSchedule::parse("30 2 * * *").unwrap();
        let first = skipped.next_after(&from).unwrap();
        assert_eq!(first.naive_utc(), wall(3, 9, 6, 30));
        let from = NewYork2026.from_utc_datetime(&wall(10, 31, 12, 0));
        let repeated = CronSchedule::parse("30 1 * * *").unwrap();
        let first = repeated.next_after(&from).unwrap();
        let second = repeated.next_after(&first).unwrap();
        assert_eq!(
            (first.naive_utc(), second.naive_utc()),
            (wall(11, 1, 5, 30), wall(11, 1, 6, 30))
        );
    }

    #[cfg(feature = "chrono-tz")]
    #[test]
    fn named_zone_schedules_match_go_fixture() {
        let fixture = fixture();
        assert!(!fixture.cron_named_zone_cases.is_empty());
        for case in &fixture.cron_named_zone_cases {
            assert_matches_go(case);
        }
    }

    #[cfg(not(feature = "chrono-tz"))]
    #[test]
    fn named_zones_need_the_time_zone_database() {
        for case in fixture().cron_named_zone_cases {
            let error = CronSchedule::parse(&case.expression).unwrap_err();
            assert!(error.to_string().contains("chrono-tz"), "{error}");
        }
    }

    #[test]
    fn cron_rejects_what_go_rejects() {
        for expression in fixture().cron_invalid {
            assert!(
                CronSchedule::parse(&expression).is_err(),
                "{expression:?} should be rejected"
            );
        }
    }

    #[test]
    fn explicit_time_zones_override_the_reference_zone() {
        let schedule = CronSchedule::parse("0 9 * * *").unwrap();
        assert_eq!(schedule.time_zone(), CronTimeZone::Local);
        let eastern = FixedOffset::west_opt(5 * 3_600).unwrap();
        let utc = schedule.clone().with_time_zone(CronTimeZone::Utc);
        let from = DateTime::parse_from_rfc3339("2026-03-07T08:00:00-05:00").unwrap();
        assert_eq!(
            utc.next_after(&from).unwrap().to_rfc3339(),
            "2026-03-08T04:00:00-05:00"
        );
        let fixed = schedule.with_time_zone(CronTimeZone::Fixed(eastern));
        assert_eq!(
            fixed
                .next(
                    DateTime::parse_from_rfc3339("2026-03-07T13:00:00Z")
                        .unwrap()
                        .to_utc()
                )
                .unwrap()
                .to_rfc3339(),
            "2026-03-07T14:00:00+00:00"
        );
        assert_eq!(
            CronSchedule::parse("CRON_TZ=Etc/GMT+5 0 9 * * *")
                .unwrap()
                .time_zone(),
            CronTimeZone::Fixed(eastern)
        );
        #[cfg(feature = "chrono-tz")]
        assert_eq!(
            CronSchedule::parse("CRON_TZ=America/New_York 0 9 * * *")
                .unwrap()
                .time_zone(),
            CronTimeZone::Named(chrono_tz::America::New_York)
        );
        #[cfg(not(feature = "chrono-tz"))]
        assert!(CronSchedule::parse("CRON_TZ=America/New_York 0 9 * * *").is_err());
    }
}
