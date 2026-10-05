import type { Pool } from "pg";
import type { RunDiagnostics } from "riverqueue";

export interface BenchmarkResourceBounds {
  readonly maxConnections: number;
  readonly maxPendingCompletions: number;
  readonly maxWorkers: number;
}

export interface BenchmarkResourcePeaks {
  readonly activeAttempts: number;
  readonly completionQueries: number;
  readonly eventLoopDelayMaxMs: number;
  readonly eventLoopDelayP99Ms: number;
  readonly heapUsedBytes: number;
  readonly pendingCompletions: number;
  readonly poolActiveConnections: number;
  readonly poolTotalConnections: number;
  readonly poolWaitingRequests: number;
  readonly rssBytes: number;
  readonly samples: number;
}

/** Collect high-water marks without retaining per-sample benchmark data. */
export class BenchmarkResourceMonitor {
  readonly #bounds: BenchmarkResourceBounds;
  #peaks: BenchmarkResourcePeaks = emptyPeaks();

  constructor(bounds: BenchmarkResourceBounds) {
    requirePositiveInteger(bounds.maxConnections, "maxConnections");
    requirePositiveInteger(
      bounds.maxPendingCompletions,
      "maxPendingCompletions"
    );
    requirePositiveInteger(bounds.maxWorkers, "maxWorkers");
    this.#bounds = bounds;
  }

  get peaks(): BenchmarkResourcePeaks {
    return this.#peaks;
  }

  assertBounded(): void {
    if (this.#peaks.activeAttempts > this.#bounds.maxWorkers) {
      throw new Error(
        `benchmark active attempts exceeded worker bound: ${this.#peaks.activeAttempts} > ${this.#bounds.maxWorkers}`
      );
    }
    if (this.#peaks.pendingCompletions > this.#bounds.maxPendingCompletions) {
      throw new Error(
        `benchmark completion backlog exceeded configured capacity: ${this.#peaks.pendingCompletions} > ${this.#bounds.maxPendingCompletions}`
      );
    }
    if (this.#peaks.completionQueries > 2) {
      throw new Error(
        `benchmark completion query concurrency exceeded River's two-way bound: ${this.#peaks.completionQueries}`
      );
    }
    if (this.#peaks.poolTotalConnections > this.#bounds.maxConnections) {
      throw new Error(
        `benchmark pool exceeded configured connection bound: ${this.#peaks.poolTotalConnections} > ${this.#bounds.maxConnections}`
      );
    }
  }

  sample(
    diagnostics: RunDiagnostics,
    pool: Pick<Pool, "idleCount" | "totalCount" | "waitingCount">,
    memory = process.memoryUsage()
  ): void {
    const delay = diagnostics.eventLoopDelay;
    this.#peaks = {
      activeAttempts: Math.max(
        this.#peaks.activeAttempts,
        diagnostics.activeAttempts
      ),
      completionQueries: Math.max(
        this.#peaks.completionQueries,
        diagnostics.completionQueries
      ),
      eventLoopDelayMaxMs: Math.max(
        this.#peaks.eventLoopDelayMaxMs,
        delay?.max.total("milliseconds") ?? 0
      ),
      eventLoopDelayP99Ms: Math.max(
        this.#peaks.eventLoopDelayP99Ms,
        delay?.p99.total("milliseconds") ?? 0
      ),
      heapUsedBytes: Math.max(this.#peaks.heapUsedBytes, memory.heapUsed),
      pendingCompletions: Math.max(
        this.#peaks.pendingCompletions,
        diagnostics.pendingCompletions
      ),
      poolActiveConnections: Math.max(
        this.#peaks.poolActiveConnections,
        pool.totalCount - pool.idleCount
      ),
      poolTotalConnections: Math.max(
        this.#peaks.poolTotalConnections,
        pool.totalCount
      ),
      poolWaitingRequests: Math.max(
        this.#peaks.poolWaitingRequests,
        pool.waitingCount
      ),
      rssBytes: Math.max(this.#peaks.rssBytes, memory.rss),
      samples: this.#peaks.samples + 1,
    };
  }
}

function emptyPeaks(): BenchmarkResourcePeaks {
  return {
    activeAttempts: 0,
    completionQueries: 0,
    eventLoopDelayMaxMs: 0,
    eventLoopDelayP99Ms: 0,
    heapUsedBytes: 0,
    pendingCompletions: 0,
    poolActiveConnections: 0,
    poolTotalConnections: 0,
    poolWaitingRequests: 0,
    rssBytes: 0,
    samples: 0,
  };
}

function requirePositiveInteger(value: number, name: string): void {
  if (!Number.isSafeInteger(value) || value < 1) {
    throw new RangeError(`${name} must be a positive safe integer`);
  }
}
