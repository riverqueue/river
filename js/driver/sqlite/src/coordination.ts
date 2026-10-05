import { isRetryableSqliteError } from "./errors.js";

/**
 * First-in, first-out asynchronous mutex.
 *
 * `node:sqlite` exposes one connection per `DatabaseSync`, so every River
 * operation on a handle must wait for any transaction that is open on it.
 * Waiters are resumed in arrival order so a steady stream of short operations
 * cannot starve a queued transaction.
 */
export class FifoLock {
  #held = false;
  readonly #waiters: (() => void)[] = [];

  /**
   * Wait for the lock and return a function that releases it exactly once.
   * When `signal` aborts while waiting, stop waiting and reject with its
   * reason.
   */
  async acquire(signal?: AbortSignal): Promise<() => void> {
    signal?.throwIfAborted();
    if (this.#held) {
      await new Promise<void>((resolve, reject) => {
        const waiter = (): void => {
          signal?.removeEventListener("abort", abort);
          resolve();
        };
        const abort = (): void => {
          const index = this.#waiters.indexOf(waiter);
          if (index !== -1) this.#waiters.splice(index, 1);
          reject(signal?.reason);
        };
        signal?.addEventListener("abort", abort, { once: true });
        this.#waiters.push(waiter);
      });
    } else {
      this.#held = true;
    }
    let released = false;
    return () => {
      if (released) return;
      released = true;
      const next = this.#waiters.shift();
      if (next === undefined) this.#held = false;
      else next();
    };
  }
}

/** Bounds for retrying an operation while another connection holds a lock. */
export interface BusyRetryPolicy {
  /** Monotonic clock in milliseconds. */
  readonly now: () => number;
  /** Resolve after roughly the given number of milliseconds. */
  readonly sleep: (milliseconds: number) => Promise<void>;
  /** Total time to keep retrying before surfacing the busy error. */
  readonly timeoutMs: number;
}

const FIRST_BACKOFF_MS = 2;
const MAX_BACKOFF_MS = 50;

/**
 * Run a synchronous SQLite attempt, retrying `SQLITE_BUSY`/`SQLITE_LOCKED`
 * with exponential backoff until the policy's deadline.
 *
 * Each attempt runs synchronously, and the event loop runs between attempts.
 * With a zero `busy_timeout`, as River's connection has, an attempt fails at
 * once instead of blocking the event loop while it waits. The attempt must leave no transaction
 * open when it throws, so it is safe to run again. The last busy error is
 * rethrown unchanged once the deadline passes; callers classify it as
 * retryable.
 */
export async function retryBusy<T>(
  policy: BusyRetryPolicy,
  attempt: () => T
): Promise<T> {
  const deadline = policy.now() + policy.timeoutMs;
  let backoff = FIRST_BACKOFF_MS;
  for (;;) {
    try {
      return attempt();
    } catch (cause: unknown) {
      if (!isRetryableSqliteError(cause)) throw cause;
      const remaining = deadline - policy.now();
      if (remaining <= 0) throw cause;
      await policy.sleep(Math.min(backoff, remaining));
      backoff = Math.min(backoff * 2, MAX_BACKOFF_MS);
    }
  }
}
