// Shared Vitest setup for every unit and integration test file.
//
// It turns asynchronous failures that escape a test into failures of the test
// that produced them, fails a file that leaves new event-loop handles open,
// and pins fast-check's seed so property tests replay identically.
import { performance } from "node:perf_hooks";
import process from "node:process";
import {
  setImmediate as nextMacrotask,
  setTimeout as sleep,
} from "node:timers/promises";

import fc from "fast-check";
import { afterAll, afterEach, beforeAll } from "vitest";

// Property tests are deterministic by default. Set FAST_CHECK_SEED to an
// integer to replay a reported failure or explore another region of the
// input space; FAST_CHECK_SEED=random picks a fresh seed per run. fast-check
// prints the seed and shrink path of every failure.
const DEFAULT_FAST_CHECK_SEED = 0x5eed;
const seedSetting = process.env.FAST_CHECK_SEED;
if (seedSetting !== "random") {
  const seed =
    seedSetting === undefined || seedSetting === ""
      ? DEFAULT_FAST_CHECK_SEED
      : Number(seedSetting);
  if (!Number.isSafeInteger(seed)) {
    throw new Error(`FAST_CHECK_SEED must be an integer or "random"`);
  }
  fc.configureGlobal({ ...fc.readConfigureGlobal(), seed });
}

// Vitest reports stray errors for the whole run without failing the test
// that caused them, and loses errors raised after a file's last test.
// Attribute them to the running test (or the file) instead.
const escapedErrors = [];
const recordEscapedError = (kind) => (reason) => {
  escapedErrors.push({ kind, reason });
};
const onUncaughtException = recordEscapedError("uncaughtException");
const onUnhandledRejection = recordEscapedError("unhandledRejection");

// Resource types that keep the event loop alive, counted when the file starts.
let initialResources = new Map();

// Some clients resolve their close promise before the socket's own close
// callback runs (node-postgres reports `end` first). Give handles that are
// already closing a bounded grace period before calling them leaked.
const HANDLE_SETTLE_TIMEOUT_MS = 2_000;
const HANDLE_SETTLE_POLL_MS = 10;

beforeAll(() => {
  process.on("uncaughtException", onUncaughtException);
  process.on("unhandledRejection", onUnhandledRejection);
  initialResources = countResources();
});

afterEach(async () => {
  // Rejections are reported after the microtask queue drains.
  await nextMacrotask();
  throwEscapedErrors("during this test");
});

afterAll(async () => {
  await nextMacrotask();
  try {
    throwEscapedErrors("after this file's last test");
    let leaked = leakedResources();
    const deadline = performance.now() + HANDLE_SETTLE_TIMEOUT_MS;
    while (leaked.length > 0 && performance.now() < deadline) {
      await sleep(HANDLE_SETTLE_POLL_MS);
      leaked = leakedResources();
    }
    throwEscapedErrors("after this file's last test");
    if (leaked.length > 0) {
      throw new Error(
        `test file left event-loop handles open: ${leaked.join(", ")}; ` +
          "close pools, servers, listeners, and timers (or unref timers " +
          "that must outlive a test) before the file finishes"
      );
    }
  } finally {
    process.off("uncaughtException", onUncaughtException);
    process.off("unhandledRejection", onUnhandledRejection);
  }
});

function countResources() {
  const counts = new Map();
  for (const type of process.getActiveResourcesInfo()) {
    counts.set(type, (counts.get(type) ?? 0) + 1);
  }
  return counts;
}

function leakedResources() {
  const leaked = [];
  for (const [type, count] of countResources()) {
    const initial = initialResources.get(type) ?? 0;
    if (count > initial) leaked.push(`${type} x${count - initial}`);
  }
  return leaked;
}

function throwEscapedErrors(when) {
  if (escapedErrors.length === 0) return;
  const errors = escapedErrors.splice(0);
  throw new AggregateError(
    errors.map(({ reason }) => reason),
    `${errors.length} error(s) escaped ${when}: ${errors
      .map(({ kind, reason }) => `${kind}: ${describe(reason)}`)
      .join("; ")}`
  );
}

function describe(reason) {
  if (reason instanceof Error) return `${reason.name}: ${reason.message}`;
  try {
    return String(reason);
  } catch {
    return Object.prototype.toString.call(reason);
  }
}
