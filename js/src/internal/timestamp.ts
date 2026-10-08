/**
 * Encode an instant as a Postgres `timestamptz` parameter the way Go's pgx
 * does: truncated to whole microseconds, Postgres's precision, rather than
 * leaving Postgres to round the sub-microsecond digits of its text input.
 * Truncation is toward the past, like pgx, so every engine stores the same
 * instant for the same value.
 */
export function postgresTimestamp(value: Temporal.Instant): string {
  return value
    .round({ roundingMode: "floor", smallestUnit: "microsecond" })
    .toString();
}
