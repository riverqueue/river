/**
 * Translation of attempt results into attempt-fenced completion commands,
 * and River's default retry schedule.
 */
import type { JobCompletionCommand } from "../driver.js";
import { ValidationError } from "../errors.js";
import type { JobEventKind } from "../events.js";
import type { WorkAttemptResult } from "../extensions.js";
import { toMilliseconds } from "../internal/duration.js";
import type { JobRow } from "../job.js";
import {
  exactJsonNumber,
  isExactJsonNumber,
  type ExactJsonNumber,
  type JsonValue,
} from "../json.js";
import { canonicalError, truncate } from "./failures.js";

/**
 * The completion command persisting `result` for `job`'s current attempt.
 * Retries and snoozes due within the scheduler interval are made available
 * immediately, like River's near-future fast path.
 */
export function completionCommand(
  job: JobRow,
  attemptedBy: string,
  result: WorkAttemptResult,
  now: Temporal.Instant,
  startedAt: Temporal.Instant,
  nextRetryAt: Temporal.Instant,
  schedulerIntervalMs: number
): JobCompletionCommand {
  const withinSchedulerInterval = (at: Temporal.Instant) =>
    at.epochNanoseconds - now.epochNanoseconds <=
    BigInt(schedulerIntervalMs) * 1_000_000n;
  const explicitOutput =
    result.outcome?.type === "complete" && result.outcome.output !== undefined;
  const hasOutput = explicitOutput || "output" in result;
  const output = explicitOutput
    ? (result.outcome.output ?? null)
    : (result.output ?? null);
  if (result.cancel === true) {
    return {
      attempt: job.attempt,
      attemptedBy,
      error: canonicalError(result.error, startedAt),
      finalizedAt: now,
      id: job.id,
      kind: "cancel",
      metadata: result.metadata ?? {},
      output,
      outputSet: hasOutput,
      scheduledAt: null,
    };
  }
  if (result.status === "failed") {
    const error = canonicalError(result.error, startedAt);
    return {
      attempt: job.attempt,
      attemptedBy,
      error,
      finalizedAt: job.attempt >= job.maxAttempts ? now : null,
      id: job.id,
      kind: job.attempt >= job.maxAttempts ? "discard" : "retry",
      ...(job.attempt < job.maxAttempts && withinSchedulerInterval(nextRetryAt)
        ? { available: true }
        : {}),
      metadata: result.metadata ?? {},
      output,
      outputSet: hasOutput,
      scheduledAt: job.attempt >= job.maxAttempts ? null : nextRetryAt,
    };
  }
  switch (result.outcome?.type) {
    case "cancel":
      // Go records `JobCancelError.Error()`, which reads `<nil>` without a
      // wrapped error, and never a trace.
      return {
        attempt: job.attempt,
        attemptedBy,
        error: {
          at: startedAt,
          error: truncate(
            `JobCancelError: ${result.outcome.reason ?? "<nil>"}`,
            32_768
          ),
          trace: "",
        },
        finalizedAt: now,
        id: job.id,
        kind: "cancel",
        metadata: result.metadata ?? {},
        output,
        outputSet: hasOutput,
        scheduledAt: null,
      };
    case "discard":
      return {
        attempt: job.attempt,
        attemptedBy,
        error:
          result.outcome.reason === undefined
            ? null
            : canonicalError(result.outcome.reason, startedAt),
        finalizedAt: now,
        id: job.id,
        kind: "discard",
        metadata: result.metadata ?? {},
        output,
        outputSet: hasOutput,
        scheduledAt: null,
      };
    case "snooze": {
      const scheduledAt = now.add({
        milliseconds: toMilliseconds(
          "snooze duration",
          result.outcome.duration,
          { allowZero: true, error: ValidationError }
        ),
      });
      return {
        attempt: job.attempt,
        attemptedBy,
        ...(withinSchedulerInterval(scheduledAt) ? { available: true } : {}),
        error: null,
        finalizedAt: null,
        id: job.id,
        kind: "snooze",
        metadata: {
          ...(result.metadata ?? {}),
          snoozes: nextSnoozeCount(job.metadata.snoozes),
        },
        output,
        outputSet: hasOutput,
        scheduledAt,
      };
    }
    case "complete":
      return {
        attempt: job.attempt,
        attemptedBy,
        error: null,
        finalizedAt: now,
        id: job.id,
        kind: "complete",
        metadata: result.metadata ?? {},
        output,
        outputSet: hasOutput,
        scheduledAt: null,
      };
    case undefined:
      return {
        attempt: job.attempt,
        attemptedBy,
        error: null,
        finalizedAt: now,
        id: job.id,
        kind: "complete",
        metadata: result.metadata ?? {},
        output,
        outputSet: hasOutput,
        scheduledAt: null,
      };
  }
}

/**
 * The snooze count after one more snooze, like River for Go's executor,
 * which writes `int(gjson.GetBytes(metadata, "snoozes").Int()) + 1`: `true`
 * counts as 1, a decimal integer string as its value, a number truncated
 * toward zero, and anything else as 0, with int64 wraparound.
 */
function nextSnoozeCount(
  value: JsonValue | undefined
): ExactJsonNumber | number {
  const next = BigInt.asIntN(64, gjsonInt(value) + 1n);
  return next >= BigInt(Number.MIN_SAFE_INTEGER) &&
    next <= BigInt(Number.MAX_SAFE_INTEGER)
    ? Number(next)
    : exactJsonNumber(next.toString(10));
}

/** gjson's `Result.Int()` for a JSON value. */
function gjsonInt(value: JsonValue | undefined): bigint {
  if (value === true) return 1n;
  if (typeof value === "string") return gjsonParseInt(value) ?? 0n;
  if (typeof value !== "number" && !isExactJsonNumber(value)) return 0n;
  const raw = typeof value === "number" ? String(value) : value.rawJSON;
  const float = typeof value === "number" ? value : Number(value.rawJSON);
  // gjson's safeInt, then its parse of the raw integer text, then Go's
  // float conversion, which saturates out of range on arm64.
  if (Math.abs(float) <= Number.MAX_SAFE_INTEGER) {
    return BigInt(Math.trunc(float));
  }
  const parsed = gjsonParseInt(raw);
  if (parsed !== undefined) return parsed;
  if (Number.isNaN(float)) return 0n;
  if (float >= 2 ** 63) return BigInt.asIntN(64, (1n << 63n) - 1n);
  if (float <= -(2 ** 63)) return -(1n << 63n);
  return BigInt(Math.trunc(float));
}

/**
 * gjson's `parseInt`: an optional `-` then decimal digits only, wrapping
 * like int64 arithmetic; undefined for anything else.
 */
function gjsonParseInt(text: string): bigint | undefined {
  const negative = text.startsWith("-");
  const digits = negative ? text.slice(1) : text;
  if (!/^[0-9]+$/.test(digits)) return undefined;
  let result = 0n;
  for (const digit of digits) {
    result = BigInt.asIntN(64, result * 10n + BigInt(digit));
  }
  return negative ? BigInt.asIntN(64, -result) : result;
}

/** The event announcing a committed completion, by the job's new state. */
export function completionEventKind(
  requested: JobCompletionCommand["kind"],
  job: JobRow
): JobEventKind {
  switch (job.state) {
    case "cancelled":
      return "job_cancelled";
    case "completed":
      return "job_completed";
    case "discarded":
    case "retryable":
      return "job_failed";
    case "scheduled":
      return "job_snoozed";
    case "available":
      if (requested === "interrupt") return "job_interrupted";
      if (requested === "snooze") return "job_snoozed";
      return "job_failed";
    case "pending":
    case "running":
      return "job_race";
  }
}

/** Go's maximum `time.Duration`, about 292 years. */
const MAX_RETRY_DURATION_NANOSECONDS = (1n << 63n) - 1n;

/** Go's maximum `time.Duration`, in seconds. */
const MAX_RETRY_DURATION_SECONDS =
  Number(MAX_RETRY_DURATION_NANOSECONDS) / 1_000_000_000;

/** @internal Exact default scheduling shared with deterministic tests. */
export function defaultNextRetry(
  job: Readonly<JobRow>,
  now: Temporal.Instant,
  random: () => number = Math.random
): Temporal.Instant {
  const errorCount = job.errors.length + 1;
  const baseSeconds = Math.min(errorCount ** 4, MAX_RETRY_DURATION_SECONDS);
  const randomValue = random();
  const boundedRandom =
    Number.isFinite(randomValue) && randomValue >= 0 && randomValue < 1
      ? randomValue
      : 0.5;
  const seconds =
    baseSeconds === MAX_RETRY_DURATION_SECONDS
      ? baseSeconds
      : Math.min(
          baseSeconds + baseSeconds * (boundedRandom * 0.2 - 0.1),
          MAX_RETRY_DURATION_SECONDS
        );
  // Like Go, a capped delay is exactly the maximum duration: the capped
  // seconds are a float of one nanosecond more.
  if (seconds >= MAX_RETRY_DURATION_SECONDS) {
    return Temporal.Instant.fromEpochNanoseconds(
      now.epochNanoseconds + MAX_RETRY_DURATION_NANOSECONDS
    );
  }
  const durationNanoseconds = BigInt(Math.trunc(seconds * 1_000_000_000));
  return Temporal.Instant.fromEpochNanoseconds(
    now.epochNanoseconds + durationNanoseconds
  );
}
