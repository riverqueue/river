import { monitorEventLoopDelay } from "node:perf_hooks";

import { measuredDuration } from "./duration.js";

/** Event-loop delay measured over one reporting interval. */
export interface EventLoopDelayObservation {
  /** Whether the longest delay reached the warning threshold. */
  readonly exceededThreshold: boolean;
  /** Longest delay. */
  readonly max: Temporal.Duration;
  readonly mean: Temporal.Duration;
  /** 99th-percentile delay. */
  readonly p99: Temporal.Duration;
}

export interface EventLoopDelayMonitorOptions {
  readonly reportIntervalMs: number;
  readonly resolutionMs: number;
  readonly warningThresholdMs: number;
}

interface DelayHistogram {
  readonly max: number;
  readonly mean: number;
  disable(): boolean;
  enable(): boolean;
  percentile(percentile: number): number;
  reset(): void;
}

interface EventLoopDelayMonitorDependencies {
  readonly histogram?: DelayHistogram;
  readonly setInterval?: typeof globalThis.setInterval;
}

/** River-owned event-loop delay sampling with an unreferenced report timer. */
export class EventLoopDelayMonitor {
  // Created on first use: Node keeps a histogram that was never enabled
  // open as an event-loop handle, so a runtime that failed to start would
  // otherwise leak it.
  #histogram: DelayHistogram | undefined;
  readonly #onObservation: (observation: EventLoopDelayObservation) => void;
  readonly #options: EventLoopDelayMonitorOptions;
  readonly #setInterval: typeof globalThis.setInterval;
  #last: EventLoopDelayObservation | null = null;
  #timer: ReturnType<typeof globalThis.setInterval> | undefined;

  constructor(
    options: EventLoopDelayMonitorOptions,
    onObservation: (observation: EventLoopDelayObservation) => void,
    dependencies: EventLoopDelayMonitorDependencies = {}
  ) {
    this.#options = options;
    this.#onObservation = onObservation;
    this.#histogram = dependencies.histogram;
    this.#setInterval = dependencies.setInterval ?? globalThis.setInterval;
  }

  get last(): EventLoopDelayObservation | null {
    return this.#last;
  }

  sample(): EventLoopDelayObservation {
    const histogram = this.#ensureHistogram();
    const observation = Object.freeze({
      exceededThreshold:
        nanosecondsToMilliseconds(histogram.max) >=
        this.#options.warningThresholdMs,
      max: measuredDuration(nanosecondsToMilliseconds(histogram.max)),
      mean: measuredDuration(nanosecondsToMilliseconds(histogram.mean)),
      p99: measuredDuration(
        nanosecondsToMilliseconds(histogram.percentile(99))
      ),
    });
    histogram.reset();
    this.#last = observation;
    this.#onObservation(observation);
    return observation;
  }

  start(): void {
    if (this.#timer !== undefined) return;
    this.#ensureHistogram().enable();
    this.#timer = this.#setInterval(
      () => this.sample(),
      this.#options.reportIntervalMs
    );
    this.#timer.unref();
  }

  stop(): void {
    if (this.#timer !== undefined) {
      clearInterval(this.#timer);
      this.#timer = undefined;
    }
    this.#histogram?.disable();
  }

  #ensureHistogram(): DelayHistogram {
    this.#histogram ??= monitorEventLoopDelay({
      resolution: this.#options.resolutionMs,
    });
    return this.#histogram;
  }
}

function nanosecondsToMilliseconds(value: number): number {
  return Number.isFinite(value) ? value / 1_000_000 : 0;
}
