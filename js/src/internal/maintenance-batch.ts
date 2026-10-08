/**
 * Batch handling shared by the leader's maintenance services, like Go
 * River's `riversharedmaintenance` package: each service works in bounded
 * batches with a per-batch timeout, shrinks its batches after repeated
 * timeouts, and pauses for a random 50 ms to 1 s between batches to give the
 * database room.
 */

import type { RuntimeMaintenanceBatch } from "../driver.js";
import { interruptibleDelay } from "./abort.js";

/** Rows most maintenance services handle per batch. */
export const BATCH_SIZE_DEFAULT = 10_000;

/** The batch size after {@link ReducedBatchSizeBreaker} opens. */
export const BATCH_SIZE_REDUCED = 1_000;

/** Shortest pause between two batches of one pass. */
export const BATCH_BACKOFF_MIN_MS = 50;

/** Longest pause between two batches of one pass. */
export const BATCH_BACKOFF_MAX_MS = 1_000;

/** Timeout of one batch for services without their own setting. */
export const MAINTENANCE_TIMEOUT_DEFAULT_MS = 30_000;

/** Options for {@link CircuitBreaker}. */
export interface CircuitBreakerOptions {
  /** Trips within `windowMs` that open the breaker. */
  readonly limit: number;
  /** Sliding window, in milliseconds, in which trips count. */
  readonly windowMs: number;
}

/**
 * A circuit breaker that opens once `limit` trips happen within a sliding
 * window, then stays open for its lifetime, like Go River's
 * `circuitbreaker.CircuitBreaker`.
 */
export class CircuitBreaker {
  readonly #now: () => number;
  readonly #options: CircuitBreakerOptions;
  #open = false;
  #trips: number[] = [];

  constructor(options: CircuitBreakerOptions, now: () => number = Date.now) {
    if (!Number.isSafeInteger(options.limit) || options.limit < 1) {
      throw new RangeError("CircuitBreaker limit must be above zero");
    }
    if (!Number.isFinite(options.windowMs) || options.windowMs < 1) {
      throw new RangeError("CircuitBreaker windowMs must be above zero");
    }
    this.#now = now;
    this.#options = options;
  }

  /** Trips within the window that open the breaker. */
  get limit(): number {
    return this.#options.limit;
  }

  /** Whether the breaker has opened. */
  get open(): boolean {
    return this.#open;
  }

  /**
   * Forget earlier trips unless the breaker is open, so only consecutive
   * failures open it. Returns whether the breaker was reset.
   */
  resetIfNotOpen(): boolean {
    if (!this.#open) this.#trips = [];
    return !this.#open;
  }

  /**
   * Count one trip, dropping trips older than the window. Returns whether
   * the breaker is open afterward.
   */
  trip(): boolean {
    if (this.#open) return true;
    const now = this.#now();
    const horizon = now - this.#options.windowMs;
    this.#trips = this.#trips.filter((trippedAt) => trippedAt >= horizon);
    this.#trips.push(now);
    if (this.#trips.length >= this.#options.limit) this.#open = true;
    return this.#open;
  }
}

/**
 * The breaker most maintenance services use: three timed-out batches in a row
 * within ten minutes switch the service to {@link BATCH_SIZE_REDUCED} for the
 * rest of its life.
 */
function reducedBatchSizeBreaker(now: () => number = Date.now): CircuitBreaker {
  return new CircuitBreaker({ limit: 3, windowMs: 10 * 60_000 }, now);
}

/** Options for {@link MaintenanceBatcher}. */
export interface MaintenanceBatcherOptions {
  /** Monotonic clock in milliseconds. Defaults to `performance.now`. */
  readonly now?: () => number;
  /** Uniform random source in `[0, 1)`. Defaults to `Math.random`. */
  readonly random?: () => number;
  /** Per-batch timeout, or `null` for none. */
  readonly timeoutMs: number | null;
}

/**
 * Runs one maintenance service's batches: picks the batch size, bounds each
 * batch by its timeout, and trips the service's reduced batch size breaker
 * when a batch times out.
 *
 * A batch times out when it's still running at its deadline. Postgres
 * enforces the timeout on the server, so the batch fails and rolls back like
 * Go's. SQLite statements can't be interrupted, so a SQLite batch that
 * overruns keeps its work; it still counts as a timeout for the breaker.
 */
export class MaintenanceBatcher {
  readonly #breaker: CircuitBreaker;
  readonly #now: () => number;
  readonly #random: () => number;
  readonly #timeoutMs: number | null;

  constructor(options: MaintenanceBatcherOptions) {
    this.#now = options.now ?? (() => performance.now());
    this.#random = options.random ?? Math.random;
    this.#timeoutMs = options.timeoutMs;
    this.#breaker = reducedBatchSizeBreaker(this.#now);
  }

  /** The breaker that selects the batch size. */
  get breaker(): CircuitBreaker {
    return this.#breaker;
  }

  /** Rows the next batch should handle. */
  get batchSize(): number {
    return this.#breaker.open ? BATCH_SIZE_REDUCED : BATCH_SIZE_DEFAULT;
  }

  /**
   * Pause for a random 50 ms to 1 s before the next batch of a pass. Resolves
   * early when `signal` aborts.
   */
  backoff(signal: AbortSignal): Promise<void> {
    return interruptibleDelay(this.backoffMs(), signal);
  }

  /** Draw the next pause between batches, in milliseconds. */
  backoffMs(): number {
    return (
      BATCH_BACKOFF_MIN_MS +
      Math.floor(this.#random() * (BATCH_BACKOFF_MAX_MS - BATCH_BACKOFF_MIN_MS))
    );
  }

  /**
   * Run one batch within its timeout. `signal` ends the batch early, such as
   * when the leadership term ends; only a timeout trips the breaker.
   */
  async run<T>(
    signal: AbortSignal,
    batch: (bounds: RuntimeMaintenanceBatch) => Promise<T> | T
  ): Promise<T> {
    signal.throwIfAborted();
    const timeoutMs = this.#timeoutMs;
    const timeout = new AbortController();
    const timer =
      timeoutMs === null
        ? undefined
        : setTimeout(() => {
            timeout.abort(new MaintenanceBatchTimeoutError(timeoutMs));
          }, timeoutMs);
    timer?.unref();
    const startedAt = this.#now();
    const timedOut = () =>
      timeoutMs !== null &&
      (timeout.signal.aborted || this.#now() - startedAt >= timeoutMs);
    try {
      const result = await batch({
        // One per maintenance batch, so `AbortSignal.any` stays cheap, and
        // the signal still aborts with the term after the batch.
        // eslint-disable-next-line no-restricted-properties
        signal: AbortSignal.any([signal, timeout.signal]),
        timeoutMs,
      });
      if (timedOut()) this.#breaker.trip();
      else this.#breaker.resetIfNotOpen();
      return result;
    } catch (error: unknown) {
      if (timedOut()) this.#breaker.trip();
      throw error;
    } finally {
      clearTimeout(timer);
    }
  }
}

/** The reason a maintenance batch's signal aborts when it times out. */
export class MaintenanceBatchTimeoutError extends Error {
  constructor(timeoutMs: number) {
    super(`maintenance batch timed out after ${timeoutMs.toString(10)} ms`);
    this.name = "MaintenanceBatchTimeoutError";
  }
}
