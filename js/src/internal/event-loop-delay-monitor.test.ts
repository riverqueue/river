import { describe, expect, it, vi } from "vitest";

import { EventLoopDelayMonitor } from "./event-loop-delay-monitor.js";

describe("EventLoopDelayMonitor", () => {
  it("reports deterministic millisecond observations and resets its window", () => {
    const histogram = {
      disable: vi.fn(() => true),
      enable: vi.fn(() => true),
      max: 125_000_000,
      mean: 40_000_000,
      percentile: vi.fn(() => 80_000_000),
      reset: vi.fn(),
    };
    const observed: unknown[] = [];
    const monitor = new EventLoopDelayMonitor(
      {
        reportIntervalMs: 1_000,
        resolutionMs: 20,
        warningThresholdMs: 100,
      },
      (observation) => observed.push(observation),
      { histogram }
    );

    const sample = monitor.sample();
    expect(sample.exceededThreshold).toBe(true);
    expect([sample.max, sample.mean, sample.p99].map(String)).toEqual([
      "PT0.125S",
      "PT0.04S",
      "PT0.08S",
    ]);
    expect(monitor.last).toEqual(observed[0]);
    expect(histogram.percentile).toHaveBeenCalledWith(99);
    expect(histogram.reset).toHaveBeenCalledOnce();
  });

  it("owns one unreferenced sampling timer and shuts it down idempotently", () => {
    const histogram = {
      disable: vi.fn(() => true),
      enable: vi.fn(() => true),
      max: 0,
      mean: 0,
      percentile: vi.fn(() => 0),
      reset: vi.fn(),
    };
    const timer = { unref: vi.fn() };
    const setInterval = vi.fn(
      () => timer
    ) as unknown as typeof globalThis.setInterval;
    const monitor = new EventLoopDelayMonitor(
      {
        reportIntervalMs: 5_000,
        resolutionMs: 20,
        warningThresholdMs: 100,
      },
      () => undefined,
      { histogram, setInterval }
    );

    monitor.start();
    monitor.start();
    expect(histogram.enable).toHaveBeenCalledOnce();
    expect(setInterval).toHaveBeenCalledOnce();
    expect(timer.unref).toHaveBeenCalledOnce();

    monitor.stop();
    monitor.stop();
    expect(histogram.disable).toHaveBeenCalledTimes(2);
  });
});
