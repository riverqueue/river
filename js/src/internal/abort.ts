/**
 * Abort-aware timing primitives shared by the runtime and its services.
 *
 * Every timer here is unreferenced: a pending delay never keeps the process
 * alive on its own, so a stopped client can always exit.
 */

/**
 * The longest delay Node's timers support. Node fires a timer with a longer
 * delay after 1 ms, so {@link unrefTimeout} waits out longer delays in
 * chunks of at most this.
 */
const MAX_TIMER_DELAY_MS = 2_147_483_647;

/**
 * Run `callback` once after `milliseconds`, however long, like Go's timers,
 * on an unreferenced timer. Returns a function that cancels it.
 */
export function unrefTimeout(
  callback: () => void,
  milliseconds: number
): () => void {
  let timer: NodeJS.Timeout;
  const arm = (remaining: number): void => {
    timer = setTimeout(
      () => {
        if (remaining > MAX_TIMER_DELAY_MS) {
          arm(remaining - MAX_TIMER_DELAY_MS);
        } else {
          callback();
        }
      },
      Math.min(remaining, MAX_TIMER_DELAY_MS)
    );
    timer.unref();
  };
  arm(milliseconds);
  return () => {
    clearTimeout(timer);
  };
}

/**
 * Resolve after `milliseconds`, or reject with `signal.reason` as soon as the
 * signal aborts.
 */
export function abortableDelay(
  milliseconds: number,
  signal: AbortSignal
): Promise<void> {
  if (signal.aborted) return Promise.reject(signal.reason);
  return new Promise<void>((resolve, reject) => {
    const onAbort = () => {
      cancel();
      reject(signal.reason);
    };
    const cancel = unrefTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, milliseconds);
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

/**
 * Resolve after `milliseconds`, or early as soon as the signal aborts. Unlike
 * {@link abortableDelay} this never rejects, which suits loops that check
 * `signal.aborted` themselves after each pause.
 */
export function interruptibleDelay(
  milliseconds: number,
  signal: AbortSignal
): Promise<void> {
  if (signal.aborted) return Promise.resolve();
  return new Promise<void>((resolve) => {
    const finish = () => {
      cancel();
      signal.removeEventListener("abort", finish);
      resolve();
    };
    const cancel = unrefTimeout(finish, milliseconds);
    signal.addEventListener("abort", finish, { once: true });
  });
}

/**
 * Settle with `operation`, or reject with `signal.reason` as soon as the
 * signal aborts. The operation itself keeps running; only the wait ends.
 */
export function raceWithAbort<T>(
  operation: PromiseLike<T> | T,
  signal: AbortSignal
): Promise<T> {
  if (signal.aborted) return Promise.reject(signal.reason);
  return new Promise<T>((resolve, reject) => {
    const onAbort = () => reject(signal.reason);
    signal.addEventListener("abort", onAbort, { once: true });
    void Promise.resolve(operation).then(
      (value) => {
        signal.removeEventListener("abort", onAbort);
        resolve(value);
      },
      (error: unknown) => {
        signal.removeEventListener("abort", onAbort);
        reject(error);
      }
    );
  });
}

/**
 * A signal that aborts when the first of its parents aborts, with that
 * parent's reason, as `AbortSignal.any(parents)` does, until it is disposed.
 *
 * Node cleans up each signal `AbortSignal.any` creates by scanning every
 * other dependent of the same parent, so dependents of a long-lived parent,
 * such as a runtime's run signal, cost time quadratic in their number. This
 * signal instead holds a `{ once: true }` listener on each parent, which
 * disposing it removes. Dispose it when its operation settles; the parents
 * no longer abort it after that.
 *
 * A parent that is already aborted aborts it at once, the first such parent
 * in order. Otherwise it aborts inside its parent's abort event, after the
 * parent's listeners added before it.
 */
export class LinkedAbortSignal implements Disposable {
  readonly signal: AbortSignal;
  readonly #controller = new AbortController();
  #parents: readonly AbortSignal[] = [];

  constructor(parents: readonly AbortSignal[]) {
    this.signal = this.#controller.signal;
    const aborted = parents.find((parent) => parent.aborted);
    if (aborted !== undefined) {
      this.#controller.abort(aborted.reason);
      return;
    }
    this.#parents = parents;
    for (const parent of parents) {
      parent.addEventListener("abort", this.#onAbort, { once: true });
    }
  }

  /** Stop listening to the parents. */
  [Symbol.dispose](): void {
    for (const parent of this.#parents) {
      parent.removeEventListener("abort", this.#onAbort);
    }
    this.#parents = [];
  }

  readonly #onAbort = (event: Event): void => {
    const parent = event.target as AbortSignal;
    this[Symbol.dispose]();
    this.#controller.abort(parent.reason);
  };
}
