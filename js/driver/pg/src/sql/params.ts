/** Parameter encoding and validation shared by the SQL modules. */
import { postgresTimestamp } from "riverqueue/unstable-driver";

/**
 * Encode an optional instant as a `timestamptz` parameter, truncated to
 * microseconds like Go's pgx so every engine stores the same instant.
 */
export function instantParameter(
  value: Temporal.Instant | null | undefined
): string | null {
  return value === null || value === undefined
    ? null
    : postgresTimestamp(value);
}

/** Require a row limit that fits Postgres's `int`. */
export function validateLimit(
  value: number,
  label = "queue list maximum"
): void {
  if (!Number.isInteger(value) || value < 0 || value > 2_147_483_647) {
    throw new RangeError(`${label} must be an integer from 0 to 2147483647`);
  }
}
