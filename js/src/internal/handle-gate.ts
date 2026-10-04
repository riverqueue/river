/**
 * One-at-a-time access to a caller's transaction handle for a client with
 * a pilot. An intercepted operation runs statements on the handle across
 * its interceptor's awaits, so a second operation on the same handle must
 * not run in the meantime and interleave its statements with the first's.
 * Operations on one handle therefore run in arrival order, like
 * node-postgres's own queue of a client's queries.
 *
 * An operation started from inside one that holds the handle, such as a
 * nested insertion from an interceptor, runs inside it rather than waiting
 * for it, and operations nested in the same holder again run one at a time.
 */
import { AsyncLocalStorage } from "node:async_hooks";

/** One holder of a handle, and the queue of operations nested inside it. */
class Level {
  /** Set once the holder's operation finished. */
  ended = false;
  readonly parent: Level | undefined;
  #held = false;
  readonly #waiters: (() => void)[] = [];

  constructor(parent: Level | undefined) {
    this.parent = parent;
  }

  /** Wait for this level's turn; the result releases it exactly once. */
  async acquire(): Promise<() => void> {
    if (this.#held) {
      await new Promise<void>((resolve) => {
        this.#waiters.push(resolve);
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

/** Each handle's outermost queue. */
const roots = new WeakMap<object, Level>();
/** The levels the current async context holds, by handle. */
const holding = new AsyncLocalStorage<ReadonlyMap<object, Level>>();

/**
 * Run `operation` once no other operation holds `handle`, holding it
 * meanwhile. A handle that isn't an object runs at once.
 */
export async function withHandle<T>(
  handle: unknown,
  operation: () => PromiseLike<T> | T
): Promise<T> {
  if (
    (typeof handle !== "object" && typeof handle !== "function") ||
    handle === null
  ) {
    return operation();
  }
  const held = holding.getStore();
  // A callback created inside an operation can outlive it; it then queues
  // where that operation did.
  let owner = held?.get(handle);
  while (owner?.ended === true) owner = owner.parent;
  let parent = owner;
  if (parent === undefined) {
    parent = roots.get(handle);
    if (parent === undefined) {
      parent = new Level(undefined);
      roots.set(handle, parent);
    }
  }
  const release = await parent.acquire();
  const level = new Level(parent);
  const nested = new Map(held);
  nested.set(handle, level);
  try {
    return await holding.run(nested, operation);
  } finally {
    level.ended = true;
    release();
  }
}
