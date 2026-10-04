/**
 * A `RuntimeTimer` on a virtual clock for tests of River's runtime and of
 * extensions built on `riverqueue/unstable-driver`.
 */
import { AssertionError } from "node:assert";

/** A pending delay or timeout of a {@link ManualTimer}. */
export interface ManualTimerEntry {
  /** Virtual time, in milliseconds, at which it fires. */
  readonly dueAt: number;
  readonly kind: "delay" | "timeout";
  /** The duration it was created with. */
  readonly ms: number;
}

/** A timeout signal from {@link ManualTimer.timeout}. */
export interface ManualTimeout {
  readonly signal: AbortSignal;
  /** Cancel the timeout once the guarded work settled. */
  dispose(): void;
}

/**
 * A timer on a virtual clock that only moves when a test advances it, so
 * code that waits on delays and deadlines runs deterministically. Pass it
 * as the `timer` of `overrideRuntimeTiming` to drive a client's
 * runtime, including a pilot's producer reports and services:
 *
 * ```ts
 * const timer = new ManualTimer();
 * overrideRuntimeTiming(client, { timer });
 * await client.start();
 * await timer.waitFor((entry) => entry.ms === 30_000);
 * await timer.advance(30_000);
 * ```
 */
export class ManualTimer {
  readonly #entries = new Set<
    ManualTimerEntry & { readonly fire: () => void; readonly order: number }
  >();
  #now = 0;
  #order = 0;

  /**
   * Move the clock forward by `ms`, firing each delay and timeout that
   * comes due in order, and letting the code they wake run before the next
   * fires, so timers it creates within the window fire too.
   */
  async advance(ms: number): Promise<void> {
    if (!Number.isFinite(ms) || ms < 0) {
      throw new RangeError("ManualTimer.advance requires a non-negative ms");
    }
    const target = this.#now + ms;
    for (;;) {
      await settle();
      const next = this.#next();
      if (next === undefined || next.dueAt > target) break;
      this.#now = Math.max(this.#now, next.dueAt);
      this.#entries.delete(next);
      next.fire();
    }
    this.#now = target;
    await settle();
  }

  /**
   * Resolve after `ms` on the virtual clock, or reject with `signal.reason`
   * as soon as `signal` aborts.
   */
  delay(ms: number, signal: AbortSignal): Promise<void> {
    if (signal.aborted) return Promise.reject(signal.reason as unknown);
    return new Promise((resolve, reject) => {
      const onAbort = () => {
        this.#entries.delete(entry);
        reject(signal.reason as unknown);
      };
      const entry = this.#add("delay", ms, () => {
        signal.removeEventListener("abort", onAbort);
        resolve();
      });
      signal.addEventListener("abort", onAbort, { once: true });
    });
  }

  /** The virtual clock's time, in milliseconds. */
  now(): number {
    return this.#now;
  }

  /** Pending delays and timeouts, earliest first. */
  pending(): readonly ManualTimerEntry[] {
    return [...this.#entries]
      .sort(
        (left, right) => left.dueAt - right.dueAt || left.order - right.order
      )
      .map(({ dueAt, kind, ms }) => Object.freeze({ dueAt, kind, ms }));
  }

  /** A signal that aborts with `reason()` once `ms` elapsed. */
  timeout(ms: number, reason: () => unknown): ManualTimeout {
    const controller = new AbortController();
    const entry = this.#add("timeout", ms, () => {
      controller.abort(reason());
    });
    return {
      dispose: () => {
        this.#entries.delete(entry);
      },
      signal: controller.signal,
    };
  }

  /**
   * Wait, in real time, until a pending entry matches `predicate`, and
   * return it. Rejects after `timeoutMs` (default 5 s) of real time.
   */
  async waitFor(
    predicate: (entry: ManualTimerEntry) => boolean,
    options: { readonly timeoutMs?: number } = {}
  ): Promise<ManualTimerEntry> {
    const deadline = performance.now() + (options.timeoutMs ?? 5_000);
    for (;;) {
      const entry = this.pending().find(predicate);
      if (entry !== undefined) return entry;
      if (performance.now() > deadline) {
        throw new AssertionError({
          message: `no matching timer; pending: ${JSON.stringify(this.pending())}`,
        });
      }
      await settle();
    }
  }

  #add(
    kind: ManualTimerEntry["kind"],
    ms: number,
    fire: () => void
  ): ManualTimerEntry & { readonly fire: () => void; readonly order: number } {
    const entry = {
      dueAt: this.#now + Math.max(0, ms),
      fire,
      kind,
      ms,
      order: this.#order++,
    };
    this.#entries.add(entry);
    return entry;
  }

  #next():
    | (ManualTimerEntry & { readonly fire: () => void; readonly order: number })
    | undefined {
    let next:
      | (ManualTimerEntry & {
          readonly fire: () => void;
          readonly order: number;
        })
      | undefined;
    for (const entry of this.#entries) {
      if (
        next === undefined ||
        entry.dueAt < next.dueAt ||
        (entry.dueAt === next.dueAt && entry.order < next.order)
      ) {
        next = entry;
      }
    }
    return next;
  }
}

/** Let pending promise reactions and one macrotask turn run. */
function settle(): Promise<void> {
  return new Promise((resolve) => setImmediate(resolve));
}
