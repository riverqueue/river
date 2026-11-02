# Migrating from Chrono

Use `riverqueue::jiff::Timestamp` wherever River previously accepted or returned
`chrono::DateTime<Utc>`. For example, `Timestamp::now()` replaces `Utc::now()`,
and `"2026-01-02T03:04:05Z".parse::<Timestamp>()?` parses an absolute timestamp.
Use `std::time::Duration` for delays and intervals, and `jiff::SignedDuration`
when a duration can be negative. To measure elapsed time between timestamps,
use `later.duration_since(earlier)`; subtracting Jiff timestamps produces a
calendar `Span`.

Cron's `next_after` accepts a `jiff::Zoned` and preserves the reference time
zone. Use `timestamp.to_zoned(jiff::tz::TimeZone::UTC)` for a UTC reference.
Fixed cron offsets use `jiff::tz::Offset`, named zones use `jiff::tz::TimeZone`,
and the `tzdb` feature replaces `chrono-tz`. Daily UTC reindex schedules use
`jiff::civil::Time`. River's persisted timestamps and unique-key encoding are
unchanged; existing databases require no migration.
