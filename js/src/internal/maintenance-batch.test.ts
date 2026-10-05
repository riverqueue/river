import { describe, expect, it } from "vitest";

import {
  BATCH_BACKOFF_MAX_MS,
  BATCH_BACKOFF_MIN_MS,
  BATCH_SIZE_DEFAULT,
  BATCH_SIZE_REDUCED,
  CircuitBreaker,
  MaintenanceBatcher,
  MaintenanceBatchTimeoutError,
} from "./maintenance-batch.js";

// Ported from Go River's `rivershared/circuitbreaker` tests.
describe("CircuitBreaker", () => {
  const limit = 5;
  const windowMs = 60_000;

  function setup() {
    const clock = { now: 1_000_000 };
    const breaker = new CircuitBreaker({ limit, windowMs }, () => clock.now);
    return { breaker, clock };
  }

  it("is configured", () => {
    expect(setup().breaker.limit).toBe(limit);
  });

  it("opens at its limit", () => {
    const { breaker } = setup();

    for (let index = 0; index < limit - 1; index++) {
      expect(breaker.trip()).toBe(false);
      expect(breaker.open).toBe(false);
    }
    expect(breaker.trip()).toBe(true);
    expect(breaker.open).toBe(true);
    expect(breaker.trip()).toBe(true);
    expect(breaker.open).toBe(true);
  });

  it("counts a trip exactly at the window's edge", () => {
    const { breaker, clock } = setup();
    const start = clock.now;

    for (let index = 0; index < limit - 2; index++) {
      expect(breaker.trip()).toBe(false);
    }
    clock.now = start + windowMs - 1_000;
    expect(breaker.trip()).toBe(false);
    clock.now = start + windowMs;
    expect(breaker.trip()).toBe(true);
  });

  it("drops trips that fall out of the window", () => {
    const { breaker, clock } = setup();
    const start = clock.now;

    expect(breaker.trip()).toBe(false);
    clock.now = start + windowMs - 1_000;
    for (let index = 0; index < limit - 2; index++) {
      expect(breaker.trip()).toBe(false);
    }
    // The first trip has fallen out of the window.
    clock.now = start + windowMs + 1_000;
    expect(breaker.trip()).toBe(false);
  });

  it("drops several trips that fall out of the window at once", () => {
    const { breaker, clock } = setup();
    const start = clock.now;

    for (let index = 0; index < limit - 1; index++) {
      expect(breaker.trip()).toBe(false);
    }
    clock.now = start + windowMs + 1_000;
    expect(breaker.trip()).toBe(false);
  });

  it("resets only while closed", () => {
    const { breaker } = setup();

    for (let index = 0; index < limit - 1; index++) {
      expect(breaker.trip()).toBe(false);
    }
    expect(breaker.resetIfNotOpen()).toBe(true);
    for (let index = 0; index < limit - 1; index++) {
      expect(breaker.trip()).toBe(false);
    }
    expect(breaker.trip()).toBe(true);
    expect(breaker.resetIfNotOpen()).toBe(false);
    expect(breaker.trip()).toBe(true);
  });

  it("rejects an invalid configuration", () => {
    expect(() => new CircuitBreaker({ limit: 0, windowMs: 1 })).toThrow(
      RangeError
    );
    expect(() => new CircuitBreaker({ limit: 1, windowMs: 0 })).toThrow(
      RangeError
    );
  });
});

describe("MaintenanceBatcher", () => {
  const signal = new AbortController().signal;

  function setup(timeoutMs: number | null = 1_000) {
    const clock = { now: 0 };
    const batcher = new MaintenanceBatcher({
      now: () => clock.now,
      random: () => 0,
      timeoutMs,
    });
    return { batcher, clock };
  }

  /** A batch that runs until its bounds' signal aborts, then fails. */
  function hangingBatch(bounds: { signal: AbortSignal }): Promise<never> {
    return new Promise((_resolve, reject) => {
      bounds.signal.addEventListener(
        "abort",
        () => {
          reject(new Error("statement timeout"));
        },
        { once: true }
      );
    });
  }

  it("trips to the reduced batch size after consecutive timeouts", async () => {
    const batcher = new MaintenanceBatcher({ random: () => 0, timeoutMs: 1 });
    const limit = batcher.breaker.limit;

    expect(batcher.batchSize).toBe(BATCH_SIZE_DEFAULT);
    for (let index = 0; index < limit - 1; index++) {
      await expect(batcher.run(signal, hangingBatch)).rejects.toThrow(
        "statement timeout"
      );
      expect(batcher.batchSize).toBe(BATCH_SIZE_DEFAULT);
    }
    await expect(batcher.run(signal, hangingBatch)).rejects.toThrow(
      "statement timeout"
    );
    expect(batcher.batchSize).toBe(BATCH_SIZE_REDUCED);

    // Once tripped, successful batches keep the reduced size.
    for (let index = 0; index < 2; index++) {
      await expect(batcher.run(signal, () => 0)).resolves.toBe(0);
      expect(batcher.batchSize).toBe(BATCH_SIZE_REDUCED);
    }
  });

  it("resets the breaker when a batch succeeds", async () => {
    const batcher = new MaintenanceBatcher({ random: () => 0, timeoutMs: 1 });
    const limit = batcher.breaker.limit;

    for (let round = 0; round < 2; round++) {
      for (let index = 0; index < limit - 1; index++) {
        await expect(batcher.run(signal, hangingBatch)).rejects.toThrow();
        expect(batcher.batchSize).toBe(BATCH_SIZE_DEFAULT);
      }
      await expect(batcher.run(signal, () => 1)).resolves.toBe(1);
      expect(batcher.batchSize).toBe(BATCH_SIZE_DEFAULT);
    }
  });

  it("ignores failures that aren't timeouts", async () => {
    const { batcher } = setup();
    const cancelled = new AbortController();
    cancelled.abort(new Error("term ended"));

    for (let index = 0; index < batcher.breaker.limit; index++) {
      await expect(
        batcher.run(signal, () => {
          throw new Error("delete failed");
        })
      ).rejects.toThrow("delete failed");
      await expect(batcher.run(cancelled.signal, () => 0)).rejects.toThrow(
        "term ended"
      );
    }
    expect(batcher.batchSize).toBe(BATCH_SIZE_DEFAULT);
  });

  it("counts a batch that overruns its timeout, keeping its result", async () => {
    // A SQLite statement can't be interrupted, so it finishes late.
    const { batcher, clock } = setup(1_000);
    const overrun = () => {
      clock.now += 1_000;
      return 7;
    };

    for (let index = 0; index < batcher.breaker.limit - 1; index++) {
      await expect(batcher.run(signal, overrun)).resolves.toBe(7);
      expect(batcher.batchSize).toBe(BATCH_SIZE_DEFAULT);
    }
    await expect(batcher.run(signal, overrun)).resolves.toBe(7);
    expect(batcher.batchSize).toBe(BATCH_SIZE_REDUCED);
  });

  it("passes the batch its timeout and a signal that aborts at it", async () => {
    const batcher = new MaintenanceBatcher({ timeoutMs: 5 });
    let reason: unknown;

    await expect(
      batcher.run(signal, async (bounds) => {
        expect(bounds.timeoutMs).toBe(5);
        await hangingBatch(bounds).catch(() => undefined);
        reason = bounds.signal.reason;
        return 0;
      })
    ).resolves.toBe(0);
    expect(reason).toBeInstanceOf(MaintenanceBatchTimeoutError);
  });

  it("runs without a timeout when none is configured", async () => {
    const { batcher, clock } = setup(null);

    for (let index = 0; index < batcher.breaker.limit; index++) {
      await batcher.run(signal, (bounds) => {
        expect(bounds.timeoutMs).toBeNull();
        clock.now += 3_600_000;
        return 0;
      });
    }
    expect(batcher.batchSize).toBe(BATCH_SIZE_DEFAULT);
  });

  it("backs off a random 50 ms to 1 s between batches", async () => {
    const pause = (random: number) =>
      new MaintenanceBatcher({
        random: () => random,
        timeoutMs: null,
      }).backoffMs();

    expect(pause(0)).toBe(BATCH_BACKOFF_MIN_MS);
    expect(pause(0.5)).toBe(525);
    expect(pause(0.999_999)).toBe(BATCH_BACKOFF_MAX_MS - 1);

    // A pause ends as soon as the pass is cancelled.
    const controller = new AbortController();
    const batcher = new MaintenanceBatcher({
      random: () => 0.999_999,
      timeoutMs: null,
    });
    const startedAt = performance.now();
    const backoff = batcher.backoff(controller.signal);
    controller.abort();
    await backoff;
    expect(performance.now() - startedAt).toBeLessThan(BATCH_BACKOFF_MIN_MS);
  });
});
