import {
  createHook,
  executionAsyncResource,
  type AsyncHook,
} from "node:async_hooks";

/**
 * An internal, test-only strict check that River's own SQLite transaction
 * stays within one turn of the event loop.
 *
 * The `setImmediate` probe misses fast I/O that completes before the check
 * phase. This counts every macrotask callback instead: an `async_hooks`
 * hook tags each non-promise, non-microtask, non-tick resource when it is
 * created and counts `before` callbacks of tagged resources, so any
 * callback of a timer, immediate, or I/O request between two reads of the
 * counter means the event loop turned. Promise continuations are never
 * counted, so a scope that awaits only promises sees no change.
 *
 * Hooking every async resource slows promise-heavy code several times over,
 * and resources created before the hook is enabled are never counted, so it
 * is enabled only for River's own tests and conformance runs, for as long as
 * any strict driver is open. It is not part of the public API.
 */

/** Async resource types that never run macrotask callbacks. */
const UNCOUNTED_TYPES: ReadonlySet<string> = new Set([
  "Microtask",
  "PROMISE",
  "TickObject",
]);

let hook: AsyncHook | null = null;
let turns = 0;
let users = 0;
const counted = new WeakSet<object>();

/** Start counting, and return a function that stops once, when unused. */
export function retainTurnCounter(): () => void {
  if (users === 0) {
    hook ??= createHook({
      before() {
        const resource: unknown = executionAsyncResource();
        if (typeof resource === "object" && resource !== null) {
          if (counted.has(resource)) turns++;
        }
      },
      init(_asyncId, type, _triggerAsyncId, resource: unknown) {
        if (UNCOUNTED_TYPES.has(type)) return;
        if (typeof resource === "object" && resource !== null) {
          counted.add(resource);
        }
      },
    });
    hook.enable();
  }
  users++;
  let released = false;
  return () => {
    if (released) return;
    released = true;
    users--;
    if (users === 0) hook?.disable();
  };
}

/** Macrotask callbacks run since the counter started. */
export function eventLoopTurns(): number {
  return turns;
}
