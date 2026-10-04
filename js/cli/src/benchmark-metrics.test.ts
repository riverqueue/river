import { describe, expect, it } from "vitest";

import { BenchmarkResourceMonitor } from "./benchmark-metrics.js";
import type { RunDiagnostics } from "riverqueue";

describe("BenchmarkResourceMonitor", () => {
  it("retains high-water marks and accepts River's resource bounds", () => {
    const monitor = new BenchmarkResourceMonitor({
      maxConnections: 4,
      maxPendingCompletions: 20,
      maxWorkers: 10,
    });
    monitor.sample(
      diagnostics({
        activeAttempts: 6,
        completionQueries: 2,
        pendingCompletions: 5,
      }),
      { idleCount: 1, totalCount: 4, waitingCount: 2 },
      memory(100, 200)
    );
    monitor.sample(
      diagnostics({
        activeAttempts: 2,
        completionQueries: 1,
        pendingCompletions: 3,
      }),
      { idleCount: 2, totalCount: 3, waitingCount: 0 },
      memory(90, 180)
    );

    expect(monitor.peaks).toMatchObject({
      activeAttempts: 6,
      completionQueries: 2,
      heapUsedBytes: 100,
      pendingCompletions: 5,
      poolActiveConnections: 3,
      poolTotalConnections: 4,
      poolWaitingRequests: 2,
      rssBytes: 200,
      samples: 2,
    });
    expect(() => monitor.assertBounded()).not.toThrow();
  });

  it.each([
    [{ activeAttempts: 11 }, "active attempts exceeded worker bound"],
    [
      { pendingCompletions: 21 },
      "completion backlog exceeded configured capacity",
    ],
    [{ completionQueries: 3 }, "completion query concurrency exceeded"],
  ] as const)(
    "rejects an architectural bound violation: %s",
    (values, message) => {
      const monitor = new BenchmarkResourceMonitor({
        maxConnections: 4,
        maxPendingCompletions: 20,
        maxWorkers: 10,
      });
      monitor.sample(
        diagnostics(values),
        {
          idleCount: 0,
          totalCount: 4,
          waitingCount: 0,
        },
        memory(1, 1)
      );
      expect(() => monitor.assertBounded()).toThrow(message);
    }
  );

  it("rejects pool growth beyond its configured maximum", () => {
    const monitor = new BenchmarkResourceMonitor({
      maxConnections: 4,
      maxPendingCompletions: 20,
      maxWorkers: 10,
    });
    monitor.sample(
      diagnostics({}),
      {
        idleCount: 0,
        totalCount: 5,
        waitingCount: 0,
      },
      memory(1, 1)
    );
    expect(() => monitor.assertBounded()).toThrow(
      "pool exceeded configured connection bound"
    );
  });
});

function diagnostics(
  values: Partial<
    Pick<
      RunDiagnostics,
      "activeAttempts" | "completionQueries" | "pendingCompletions"
    >
  >
): RunDiagnostics {
  return {
    activeAttempts: values.activeAttempts ?? 0,
    clientId: "benchmark",
    completionCapacity: 20,
    completionQueries: values.completionQueries ?? 0,
    eventLoopDelay: {
      exceededThreshold: false,
      max: Temporal.Duration.from({ milliseconds: 12 }),
      mean: Temporal.Duration.from({ milliseconds: 3 }),
      p99: Temporal.Duration.from({ milliseconds: 8 }),
    },
    executors: {},
    maintenance: null,
    pendingCompletions: values.pendingCompletions ?? 0,
    queues: {},
    state: "running",
  };
}

function memory(heapUsed: number, rss: number): NodeJS.MemoryUsage {
  return {
    arrayBuffers: 0,
    external: 0,
    heapTotal: heapUsed,
    heapUsed,
    rss,
  };
}
