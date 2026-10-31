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

use jiff::{
    SignedDuration, Timestamp, Zoned,
    civil::{Date, DateTime},
    tz::{Offset, TimeZone},
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
#[derive(Clone, Debug, Default, Eq, PartialEq)]
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
    Fixed(Offset),
    /// A zone from the IANA time zone database, with its daylight saving
    /// rules. `CRON_TZ=` and `TZ=` prefixes naming a zone such as
    /// `America/New_York` parse to this variant.
    #[cfg(feature = "tzdb")]
    Named(TimeZone),
}

impl CronTimeZone {
    fn resolve(&self) -> TimeZone {
        match self {
            Self::Local => TimeZone::system(),
            Self::Utc => TimeZone::UTC,
            Self::Fixed(offset) => TimeZone::fixed(*offset),
            #[cfg(feature = "tzdb")]
            Self::Named(zone) => zone.clone(),
        }
    }
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
/// Without the `tzdb` feature, which bundles the IANA time zone
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
    /// Returns an error for an expression that isn't a valid standard cron
    /// expression or descriptor, and for a `CRON_TZ=` zone this build can't
    /// resolve.
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
        self.zone.clone().unwrap_or_default()
    }

    /// Returns the first occurrence strictly after `after`, or `None` when
    /// the schedule never matches within five years.
    ///
    /// Without an explicit zone or `CRON_TZ=` prefix, the occurrence is
    /// computed in `after`'s own time zone.
    #[must_use]
    pub fn next_after(&self, after: &Zoned) -> Option<Zoned> {
        let zone = after.time_zone().clone();
        let evaluation_zone = self
            .zone
            .as_ref()
            .map_or_else(|| zone.clone(), CronTimeZone::resolve);
        next_in(self.spec, &after.with_time_zone(evaluation_zone))
            .map(|next| next.with_time_zone(zone))
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
    fn next(&self, current: Timestamp) -> Option<Timestamp> {
        next_in(self.spec, &current.to_zoned(self.time_zone().resolve()))
            .map(|next| next.timestamp())
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
        && let Ok(offset) = Offset::from_seconds(sign * hours * 3_600)
    {
        return Ok(CronTimeZone::Fixed(offset));
    }
    #[cfg(feature = "tzdb")]
    if let Ok(named) = TimeZone::get(name) {
        return Ok(CronTimeZone::Named(named));
    }
    #[cfg(feature = "tzdb")]
    return Err(format!("provided bad location {name}: unknown time zone"));
    #[cfg(not(feature = "tzdb"))]
    Err(format!(
        "provided bad location {name}: only UTC, Local, and Etc/GMT offsets are supported \
         without River's tzdb feature; use CronSchedule::with_time_zone for other zones"
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
fn next_in(spec: Spec, after: &Zoned) -> Option<Zoned> {
    let subsecond = SignedDuration::from_nanos(i64::from(after.subsec_nanosecond()));
    let (minute, hour, dom, month, dow) = match spec {
        Spec::Every(delay) => {
            return after.checked_add(delay).ok()?.checked_sub(subsecond).ok();
        }
        Spec::Fields {
            dom,
            dow,
            hour,
            minute,
            month,
        } => (minute, hour, dom, month, dow),
    };
    let zone = after.time_zone().clone();
    let one_hour = SignedDuration::from_hours(1);
    let one_minute = SignedDuration::from_mins(1);

    // Start at the earliest possible time (the upcoming second).
    let mut time = after
        .checked_add(SignedDuration::from_secs(1))
        .ok()?
        .checked_sub(subsecond)
        .ok()?;
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
                    time.checked_add(SignedDuration::from_hours(24 - hour))
                        .ok()?
                } else {
                    time.checked_sub(SignedDuration::from_hours(hour)).ok()?
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
            time = time.checked_add(one_hour).ok()?;
            if time.hour() == 0 {
                continue 'wrap;
            }
        }

        while (1 << time.minute()) & minute == 0 {
            if !added {
                added = true;
                time = truncate_to_minute(&time)?;
            }
            time = time.checked_add(one_minute).ok()?;
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
            time = time
                .checked_add(SignedDuration::from_secs(60 - second))
                .ok()?;
            continue 'wrap;
        }
        return Some(time);
    }
}

fn day_matches(dom: u64, dow: u64, time: &Zoned) -> bool {
    let day_of_month = (1 << time.day()) & dom != 0;
    let weekday = (1 << time.weekday().to_sunday_zero_offset()) & dow != 0;
    if dom & STAR_BIT != 0 || dow & STAR_BIT != 0 {
        day_of_month && weekday
    } else {
        day_of_month || weekday
    }
}

/// Go's `time.Truncate(time.Minute)`, which rounds absolute time.
fn truncate_to_minute(time: &Zoned) -> Option<Zoned> {
    let truncated = time.timestamp().as_nanosecond().div_euclid(60_000_000_000) * 60_000_000_000;
    Some(
        Timestamp::from_nanosecond(truncated)
            .ok()?
            .to_zoned(time.time_zone().clone()),
    )
}

/// Go's `time.Date` in `zone`: overflowing months and days roll forward, and
/// a wall-clock time that a transition skips or repeats resolves as Go
/// resolves it (see [`resolve_local`]).
fn go_date(
    zone: &TimeZone,
    year: i16,
    month: i8,
    day: i8,
    hour: i8,
    minute: i8,
    second: i8,
) -> Option<Zoned> {
    let months = i32::from(year) * 12 + i32::from(month) - 1;
    let year = i16::try_from(months.div_euclid(12)).ok()?;
    let month = i8::try_from(months.rem_euclid(12)).ok()? + 1;
    let date = Date::new(year, month, 1)
        .ok()?
        .checked_add(jiff::Span::new().days(i64::from(day.checked_sub(1)?)))
        .ok()?;
    resolve_local(zone, date.at(hour, minute, second, 0))
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
fn resolve_local(zone: &TimeZone, naive: DateTime) -> Option<Zoned> {
    let local = naive.to_zoned(TimeZone::UTC).ok()?.timestamp();
    let mut offset = zone.to_offset(local);
    if offset != Offset::UTC {
        let utc = local
            .checked_sub(SignedDuration::from_secs(i64::from(offset.seconds())))
            .ok()?;
        let corrected = zone.to_offset(utc);
        if corrected != offset {
            offset = corrected;
        }
    }
    let utc = local
        .checked_sub(SignedDuration::from_secs(i64::from(offset.seconds())))
        .ok()?;
    Some(utc.to_zoned(zone.clone()))
}

#[cfg(test)]
mod tests {
    use jiff::{
        Zoned,
        tz::{Offset, TimeZone},
    };
    use serde::Deserialize;

    use super::{CronSchedule, CronTimeZone, PeriodicSchedule};
    use crate::conformance::read_fixture;

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
        #[serde(deserialize_with = "deserialize_zoned")]
        from: Zoned,
        name: String,
        #[serde(deserialize_with = "deserialize_zoned_vec")]
        next: Vec<Zoned>,
    }

    fn parse_zoned(text: &str) -> Result<Zoned, jiff::Error> {
        let offset = jiff::fmt::temporal::Pieces::parse(text)?
            .to_numeric_offset()
            .unwrap_or(Offset::UTC);
        Ok(text
            .parse::<jiff::Timestamp>()?
            .to_zoned(TimeZone::fixed(offset)))
    }

    fn deserialize_zoned<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Zoned, D::Error> {
        parse_zoned(&String::deserialize(deserializer)?).map_err(serde::de::Error::custom)
    }

    fn deserialize_zoned_vec<'de, D: serde::Deserializer<'de>>(
        deserializer: D,
    ) -> Result<Vec<Zoned>, D::Error> {
        Vec::<String>::deserialize(deserializer)?
            .iter()
            .map(|text| parse_zoned(text).map_err(serde::de::Error::custom))
            .collect()
    }

    fn fixture() -> Fixture {
        serde_json::from_str(&read_fixture("cron_schedules.json")).unwrap()
    }

    fn assert_matches_go(case: &CronCase) {
        let schedule = CronSchedule::parse(&case.expression)
            .unwrap_or_else(|error| panic!("{}: {error}", case.name));
        // The fixture records five occurrences, stopping early at Go's zero time.
        let mut current = case.from.clone();
        let mut observed = Vec::new();
        while observed.len() < 5 {
            let Some(next) = schedule.next_after(&current) else {
                break;
            };
            observed.push(next.clone());
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
        let zone = TimeZone::posix("EST5EDT,M3.2.0/2,M11.1.0/2").unwrap();
        let wall =
            |month, day, hour, minute| jiff::civil::date(2026, month, day).at(hour, minute, 0, 0);
        let resolved = |naive| {
            super::resolve_local(&zone, naive)
                .unwrap()
                .with_time_zone(TimeZone::UTC)
                .datetime()
        };
        // A skipped time resolves to the hour before the transition.
        assert_eq!(resolved(wall(3, 8, 2, 30)), wall(3, 8, 6, 30));
        // A repeated time resolves to its earlier instant.
        assert_eq!(resolved(wall(11, 1, 1, 30)), wall(11, 1, 5, 30));
        assert_eq!(resolved(wall(7, 1, 9, 0)), wall(7, 1, 13, 0));
        assert_eq!(resolved(wall(12, 1, 9, 0)), wall(12, 1, 14, 0));

        // So, like Go, a daily schedule at a skipped time misses that day,
        // and one at a repeated time fires twice.
        let from = wall(3, 7, 12, 0)
            .to_zoned(TimeZone::UTC)
            .unwrap()
            .with_time_zone(zone.clone());
        let skipped = CronSchedule::parse("30 2 * * *").unwrap();
        let first = skipped.next_after(&from).unwrap();
        assert_eq!(
            first.with_time_zone(TimeZone::UTC).datetime(),
            wall(3, 9, 6, 30)
        );
        let from = wall(10, 31, 12, 0)
            .to_zoned(TimeZone::UTC)
            .unwrap()
            .with_time_zone(zone);
        let repeated = CronSchedule::parse("30 1 * * *").unwrap();
        let first = repeated.next_after(&from).unwrap();
        let second = repeated.next_after(&first).unwrap();
        assert_eq!(
            (
                first.with_time_zone(TimeZone::UTC).datetime(),
                second.with_time_zone(TimeZone::UTC).datetime()
            ),
            (wall(11, 1, 5, 30), wall(11, 1, 6, 30))
        );
    }

    #[cfg(feature = "tzdb")]
    #[test]
    fn named_zone_schedules_match_go_fixture() {
        let fixture = fixture();
        assert!(!fixture.cron_named_zone_cases.is_empty());
        for case in &fixture.cron_named_zone_cases {
            assert_matches_go(case);
        }
    }

    #[cfg(not(feature = "tzdb"))]
    #[test]
    fn named_zones_need_the_time_zone_database() {
        for case in fixture().cron_named_zone_cases {
            let error = CronSchedule::parse(&case.expression).unwrap_err();
            assert!(error.to_string().contains("tzdb"), "{error}");
        }
    }

    #[test]
    fn schedules_stop_at_the_timestamp_limit() {
        let after = jiff::Timestamp::MAX.to_zoned(TimeZone::UTC);
        for expression in ["* * * * *", "@every 1h"] {
            assert!(
                CronSchedule::parse(expression)
                    .unwrap()
                    .next_after(&after)
                    .is_none()
            );
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
        let eastern = Offset::from_hours(-5).unwrap();
        let utc = schedule.clone().with_time_zone(CronTimeZone::Utc);
        let from = parse_zoned("2026-03-07T08:00:00-05:00").unwrap();
        assert_eq!(
            utc.next_after(&from)
                .unwrap()
                .strftime("%Y-%m-%dT%H:%M:%S%:z")
                .to_string(),
            "2026-03-08T04:00:00-05:00"
        );
        let fixed = schedule.with_time_zone(CronTimeZone::Fixed(eastern));
        assert_eq!(
            fixed
                .next(parse_zoned("2026-03-07T13:00:00Z").unwrap().timestamp())
                .unwrap()
                .to_string(),
            "2026-03-07T14:00:00Z"
        );
        assert_eq!(
            CronSchedule::parse("CRON_TZ=Etc/GMT+5 0 9 * * *")
                .unwrap()
                .time_zone(),
            CronTimeZone::Fixed(eastern)
        );
        #[cfg(feature = "tzdb")]
        assert_eq!(
            CronSchedule::parse("CRON_TZ=America/New_York 0 9 * * *")
                .unwrap()
                .time_zone(),
            CronTimeZone::Named(TimeZone::get("America/New_York").unwrap())
        );
        #[cfg(not(feature = "tzdb"))]
        assert!(CronSchedule::parse("CRON_TZ=America/New_York 0 9 * * *").is_err());
    }
}
