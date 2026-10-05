/**
 * What the runtime's collaborators share: the client and driver they work
 * for, its clock and timers, event and metric delivery, and the supervision
 * hooks through which a background task fails the runtime.
 */
import type { Client } from "../client.js";
import type { RuntimeDriver } from "../driver.js";
import { isRetryableError } from "../errors.js";
import type { RiverEvent } from "../events.js";
import { abortableDelay } from "../internal/abort.js";
import {
  BACKGROUND_BACKOFF,
  exponentialBackoffMs,
  type RuntimeTimer,
} from "../internal/backoff.js";
import type { JsonValue } from "../json.js";
import type { InternalLogger } from "../logger.js";
import type { RiverMetric } from "../metrics.js";
import type { PilotOperations } from "./pilot-operations.js";
import { describeError, isPermanentRuntimeError } from "./failures.js";

/** Services `RuntimeController` provides to each of its collaborators. */
export interface RuntimeContext {
  /** The backend's name, for errors. */
  readonly backend: string;
  /** Aborts once the runtime stops claiming work, on stop or failure. */
  readonly claimSignal: AbortSignal;
  readonly client: Client;
  readonly clientId: string;
  readonly driver: RuntimeDriver;
  readonly logger: InternalLogger;
  /** Operations the client's pilot may intercept. */
  readonly operations: PilotOperations;
  readonly random: () => number;
  /** Aborts when the runtime abandons its work: a cancelling stop or failure. */
  readonly runSignal: AbortSignal;
  readonly timer: RuntimeTimer;
  /** Deliver an event to subscribers and `onEvent` hooks. */
  emit(event: RiverEvent): Promise<void>;
  /** Publish a metric to diagnostics channels and `onMetric` hooks. */
  emitMetric(metric: RiverMetric): void;
  /** Fail the runtime: abort its work and reject `completed`. */
  fail(error: unknown): void;
  /** Fail the runtime if `task` rejects, rethrowing the rejection. */
  guard(task: Promise<void>): Promise<void>;
  now(): Temporal.Instant;
  /** Add `task` to the background work a stop waits for. */
  trackTask(task: Promise<void>): void;
}

/**
 * Log a failed background database operation, then wait with capped,
 * jittered exponential backoff before the caller retries. Configuration and
 * capability errors are rethrown because retrying cannot fix them.
 */
export async function backOffAfterFailure(
  context: RuntimeContext,
  task: string,
  error: unknown,
  failures: number,
  signal: AbortSignal,
  attributes: Readonly<Record<string, JsonValue>> = {}
): Promise<void> {
  if (isPermanentRuntimeError(error)) throw error;
  const retryable = isRetryableError(error);
  const delayMs = exponentialBackoffMs(
    failures,
    BACKGROUND_BACKOFF,
    context.random
  );
  context.logger[retryable ? "warn" : "error"](
    `River ${task} failed; retrying after backoff`,
    {
      ...attributes,
      attempt: failures,
      delayMs,
      error: describeError(error),
      retryable,
    }
  );
  await context.timer.delay(delayMs, signal);
}

/**
 * Run a foreground database operation, retrying retryable failures after
 * 10 ms doubling to 1 s until it succeeds or `signal` aborts.
 */
export async function retryDatabaseOperation<T>(
  operation: () => PromiseLike<T> | T,
  signal: AbortSignal
): Promise<T> {
  let failures = 0;
  while (true) {
    if (signal.aborted) throw signal.reason;
    try {
      return await operation();
    } catch (error: unknown) {
      if (!isRetryableError(error)) throw error;
      const delayMs = Math.min(1_000, 10 * 2 ** Math.min(failures, 7));
      failures += 1;
      await abortableDelay(delayMs, signal);
    }
  }
}
