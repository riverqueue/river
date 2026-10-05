import { abortableDelay } from "./abort.js";

/**
 * Timers and the monotonic clock River's runtime waits on. By default they
 * are unreferenced `setTimeout` handles and `performance.now()`; tests
 * replace them, for example with `overrideRuntimeTiming`, so backoff,
 * intervals, and deadlines run deterministically instead of by wall-clock
 * sleeps.
 */
export interface RuntimeTimer {
  /**
   * Resolve after `milliseconds`, or reject with `signal.reason` as soon as
   * the signal aborts. Timers never keep the process alive on their own.
   */
  delay(milliseconds: number, signal: AbortSignal): Promise<void>;
  /**
   * Monotonic milliseconds from an arbitrary origin, the clock `delay` and
   * `timeout` count against, like `performance.now()`.
   */
  now(): number;
  /**
   * Return a signal that aborts with `reason()` after `milliseconds`. Call
   * `dispose` once the guarded operation settles to clear the timer.
   */
  timeout(milliseconds: number, reason: () => unknown): OperationTimeout;
}

/** A disposable timeout signal returned by {@link RuntimeTimer.timeout}. */
export interface OperationTimeout {
  readonly signal: AbortSignal;
  dispose(): void;
}

/** Bounds for {@link exponentialBackoffMs}. */
export interface BackoffPolicy {
  /** Delay before the first retry. */
  readonly baseMs: number;
  /** Largest delay, reached after repeated failures. */
  readonly maxMs: number;
}

/**
 * Background retry policy: 250 ms, 500 ms, 1 s, ... capped at 30 s. This has
 * the shape of River's Go `ExponentialBackoff`, starting lower so brief
 * contention such as a busy SQLite database clears quickly and capping lower
 * so a producer recovers promptly after a database outage ends.
 */
export const BACKGROUND_BACKOFF: BackoffPolicy = Object.freeze({
  baseMs: 250,
  maxMs: 30_000,
});

/** Timer backed by unreferenced `setTimeout` handles. */
export const SYSTEM_TIMER: RuntimeTimer = Object.freeze({
  delay: abortableDelay,
  now: () => performance.now(),
  timeout(milliseconds: number, reason: () => unknown): OperationTimeout {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(reason()), milliseconds);
    timer.unref();
    return {
      dispose: () => clearTimeout(timer),
      signal: controller.signal,
    };
  },
});

/**
 * Exponential delay for the given 1-based consecutive failure count, with
 * +/-10% jitter so many processes recovering from one outage spread out.
 */
export function exponentialBackoffMs(
  failures: number,
  policy: BackoffPolicy,
  random: () => number = Math.random
): number {
  const exponent = Math.min(Math.max(failures, 1) - 1, 30);
  const base = Math.min(policy.maxMs, policy.baseMs * 2 ** exponent);
  const sample = random();
  const jitter =
    Number.isFinite(sample) && sample >= 0 && sample < 1
      ? sample * 0.2 - 0.1
      : 0;
  return Math.max(1, Math.round(base + base * jitter));
}
