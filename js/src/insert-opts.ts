import type { JobState } from "./job.js";

/**
 * Options for job insertion. Can be provided via `insertOpts` on job args
 * (as defaults for all jobs of that kind) or passed directly to `insert` /
 * `insertMany` (which take precedence over args-level defaults).
 */
export interface InsertOpts {
  /** Maximum total attempts (including retries) before discarding. */
  maxAttempts?: number;

  /** Priority 1 (highest) to 4 (lowest). Defaults to PRIORITY_DEFAULT. */
  priority?: number;

  /** Queue name. Defaults to QUEUE_DEFAULT. */
  queue?: string;

  /** Schedule the job for a future time instead of running immediately. */
  scheduledAt?: Date;

  /** Arbitrary tags for grouping and categorizing jobs. */
  tags?: string[];

  /** Options for unique job constraints. */
  uniqueOpts?: UniqueOpts;
}

/**
 * Parameters for unique job constraints. Each enabled property adds a
 * dimension to the uniqueness check. With no properties set, no uniqueness
 * is enforced.
 */
export interface UniqueOpts {
  /**
   * Enforce uniqueness by encoded args. Set `true` for all args, or an
   * array of specific field names to consider.
   */
  byArgs?: boolean | string[];

  /**
   * Enforce uniqueness within a time period (in seconds). Time is rounded
   * down to the nearest multiple of the period.
   */
  byPeriod?: number;

  /** Enforce uniqueness per queue. */
  byQueue?: boolean;

  /**
   * Job states to consider for uniqueness. Defaults to available, completed,
   * pending, retryable, running, and scheduled. The states available,
   * pending, running, and scheduled are always required.
   */
  byState?: JobState[];

  /** Exclude job kind from the uniqueness check. */
  excludeKind?: boolean;
}
