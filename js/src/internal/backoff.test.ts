import { describe, expect, it } from "vitest";

import {
  BACKGROUND_BACKOFF,
  exponentialBackoffMs,
  SYSTEM_TIMER,
} from "./backoff.js";

describe("exponentialBackoffMs", () => {
  it("doubles from the base delay up to the cap", () => {
    const delays = Array.from({ length: 10 }, (_, index) =>
      exponentialBackoffMs(index + 1, BACKGROUND_BACKOFF, () => 0.5)
    );

    expect(delays).toEqual([
      250, 500, 1_000, 2_000, 4_000, 8_000, 16_000, 30_000, 30_000, 30_000,
    ]);
  });

  it("applies bounded jitter and tolerates invalid random values", () => {
    const policy = { baseMs: 1_000, maxMs: 1_000 };

    expect(exponentialBackoffMs(1, policy, () => 0)).toBe(900);
    expect(exponentialBackoffMs(1, policy, () => 0.999_999)).toBe(1_100);
    expect(exponentialBackoffMs(1, policy, () => Number.NaN)).toBe(1_000);
    expect(exponentialBackoffMs(1, policy, () => 2)).toBe(1_000);
    expect(exponentialBackoffMs(0, policy, () => 0.5)).toBe(1_000);
    expect(exponentialBackoffMs(10_000, policy, () => 0.5)).toBe(1_000);
  });
});

describe("SYSTEM_TIMER", () => {
  it("rejects a delay with the abort reason", async () => {
    const controller = new AbortController();
    const reason = new Error("stopping");
    const delayed = SYSTEM_TIMER.delay(60_000, controller.signal);

    controller.abort(reason);

    await expect(delayed).rejects.toBe(reason);
    await expect(SYSTEM_TIMER.delay(1, controller.signal)).rejects.toBe(reason);
  });

  it("aborts a timeout signal with the supplied reason until disposed", async () => {
    const reason = new Error("timed out");
    const expired = SYSTEM_TIMER.timeout(1, () => reason);
    await new Promise((resolve) =>
      expired.signal.addEventListener("abort", resolve, { once: true })
    );
    expect(expired.signal.reason).toBe(reason);

    const disposed = SYSTEM_TIMER.timeout(1, () => reason);
    disposed.dispose();
    await SYSTEM_TIMER.delay(5, new AbortController().signal);
    expect(disposed.signal.aborted).toBe(false);
  });
});
