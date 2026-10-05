import type { DurablePeriodicJob } from "./periodic.js";

/** A durable next-run time persisted with a periodic insertion batch. */
export interface DurablePeriodicJobUpsert {
  readonly id: string;
  readonly nextRunAt: Temporal.Instant;
  readonly updatedAt: Temporal.Instant;
}

/**
 * A pilot's durable storage for periodic job schedules, so the next run of
 * a periodic job with an `id` survives leader changes and restarts. It
 * mirrors River for Go's periodic-job pilot operations.
 *
 * The leader calls `getAll` when it starts enqueuing periodic jobs and seeds
 * each job's next run from the matching record; calls `upsertMany` inside the
 * same transaction that inserts each batch of periodic jobs; and calls
 * `keepAliveAndReap` with the registered IDs every ten minutes so the store
 * can delete records for jobs no client registers anymore.
 */
export interface PeriodicJobStore<Transaction = unknown> {
  /** Return every durable periodic job record. */
  getAll(options: {
    readonly signal: AbortSignal;
  }): PromiseLike<readonly DurablePeriodicJob[]>;
  /** Refresh records for `ids` and delete records not refreshed recently. */
  keepAliveAndReap(
    ids: readonly string[],
    options: { readonly signal: AbortSignal }
  ): PromiseLike<void>;
  /** Persist next-run times in the transaction inserting their jobs. */
  upsertMany(
    tx: Transaction,
    jobs: readonly DurablePeriodicJobUpsert[]
  ): PromiseLike<void>;
}
