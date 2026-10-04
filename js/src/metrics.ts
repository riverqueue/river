import { channel } from "node:diagnostics_channel";

/**
 * Strongly typed runtime observations.
 *
 * Fetch metrics are emitted after every successful claim. Completion metrics
 * report persistence failures: a requeued batch is retried, while dropped
 * completions leave their jobs `running` until the rescuer recovers them.
 */
export type RiverMetric =
  | {
      readonly count: number;
      readonly name: "job_completion_dropped" | "job_completion_requeued";
    }
  | {
      readonly duration: Temporal.Duration;
      readonly name: "job_get_available_duration";
      readonly queue: string;
    }
  | {
      readonly count: number;
      readonly name: "job_get_available_count";
      readonly queue: string;
    };

const riverMetricChannel = channel("riverqueue:metric");

/** @internal Publish to Node's zero-subscriber-cost diagnostics channel. */
export function publishRiverMetric(metric: RiverMetric): void {
  riverMetricChannel.publish(metric);
}
