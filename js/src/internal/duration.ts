import { ConfigurationError } from "../errors.js";

/** A duration River accepts: `Temporal.Duration` or a like such as `{ seconds: 5 }`. */
export type DurationInput = Temporal.Duration | Temporal.DurationLike;

/** Go's maximum `time.Duration`, River's protocol limit for durations. */
const MAX_DURATION_NANOSECONDS = 9_223_372_036_854_775_807n;

/** How {@link toDuration} and its variants validate a duration. */
export interface DurationRules {
  /** Whether zero is allowed. Defaults to false. */
  readonly allowZero?: boolean;
  /** Error class to throw; defaults to `ConfigurationError`. */
  readonly error?: new (message: string, options?: ErrorOptions) => Error;
}

/**
 * Convert a duration option to a `Temporal.Duration`, rejecting bare numbers,
 * calendar units, and negative values with an error naming the option.
 */
export function toDuration(
  name: string,
  value: DurationInput,
  rules: DurationRules = {}
): Temporal.Duration {
  const ErrorClass = rules.error ?? ConfigurationError;
  if (typeof value === "number" || typeof value === "bigint") {
    throw new ErrorClass(
      `${name} must be a Temporal duration such as { seconds: 5 }, not a bare number`
    );
  }
  let duration: Temporal.Duration;
  try {
    duration = Temporal.Duration.from(value);
  } catch (cause: unknown) {
    throw new ErrorClass(
      `${name} must be a Temporal duration such as { seconds: 5 }`,
      { cause }
    );
  }
  if (duration.years !== 0 || duration.months !== 0 || duration.weeks !== 0) {
    throw new ErrorClass(
      `${name} must not contain calendar units (years, months, or weeks)`
    );
  }
  const nanoseconds = durationNanoseconds(duration);
  if (nanoseconds < 0n) throw new ErrorClass(`${name} must not be negative`);
  if (nanoseconds === 0n && rules.allowZero !== true) {
    throw new ErrorClass(`${name} must be positive`);
  }
  if (nanoseconds > MAX_DURATION_NANOSECONDS) {
    throw new ErrorClass(`${name} exceeds River's maximum duration`);
  }
  return duration;
}

/** Exact nanoseconds in a duration without calendar units; a day is 24 h. */
export function durationNanoseconds(duration: Temporal.Duration): bigint {
  return (
    BigInt(duration.days) * 86_400_000_000_000n +
    BigInt(duration.hours) * 3_600_000_000_000n +
    BigInt(duration.minutes) * 60_000_000_000n +
    BigInt(duration.seconds) * 1_000_000_000n +
    BigInt(duration.milliseconds) * 1_000_000n +
    BigInt(duration.microseconds) * 1_000n +
    BigInt(duration.nanoseconds)
  );
}

/**
 * Convert a duration option to whole milliseconds for timers, rounding a
 * sub-millisecond remainder up so a positive duration never becomes zero.
 */
export function toMilliseconds(
  name: string,
  value: DurationInput,
  rules: DurationRules = {}
): number {
  const nanoseconds = durationNanoseconds(toDuration(name, value, rules));
  const milliseconds = (nanoseconds + 999_999n) / 1_000_000n;
  if (milliseconds > BigInt(Number.MAX_SAFE_INTEGER)) {
    throw new (rules.error ?? ConfigurationError)(
      `${name} exceeds the maximum timer duration`
    );
  }
  return Number(milliseconds);
}

/** Like {@link toMilliseconds}, passing `null` through as "disabled". */
export function toNullableMilliseconds(
  name: string,
  value: DurationInput | null,
  rules: DurationRules = {}
): number | null {
  return value === null ? null : toMilliseconds(name, value, rules);
}

/**
 * Express a measured time in milliseconds, which may be fractional, as a
 * balanced `Temporal.Duration` to the nanosecond. Negative or non-finite
 * measurements become zero.
 */
export function measuredDuration(milliseconds: number): Temporal.Duration {
  const nanoseconds =
    Number.isFinite(milliseconds) && milliseconds > 0
      ? Math.round(milliseconds * 1_000_000)
      : 0;
  return Temporal.Duration.from({ nanoseconds }).round({
    largestUnit: "hours",
  });
}

/** Express whole milliseconds as a balanced `Temporal.Duration`. */
export function millisecondsToDuration(
  milliseconds: number
): Temporal.Duration {
  return Temporal.Duration.from({ milliseconds }).round({
    largestUnit: "hours",
  });
}
