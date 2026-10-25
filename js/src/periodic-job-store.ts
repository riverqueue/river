import type { DurablePeriodicJob } from "./periodic.js";

/** A durable next-run time persisted with a periodic insertion batch. */
export interface DurablePeriodicJobUpsert {
  readonly id: string;
  readonly nextRunAt: Temporal.Instant;
  readonly updatedAt: Temporal.Instant;
}

/**
 * A pilot's durable storage for periodic schedules. A job with an `id` keeps
 * its next run time across leader changes and restarts.
 *
 * At startup, the leader loads saved run times with `getAll`. It writes new
 * times through `upsertMany` in the same transaction as periodic insertions.
 * Every ten minutes, `keepAliveAndReap` refreshes registered jobs whose run
 * times were loaded at startup or committed afterward. It can receive an
 * empty ID list when only cleanup is needed.
 */
export interface PeriodicJobStore<Transaction = unknown> {
  /** Return saved schedules that have not expired. */
  getAll(options: {
    readonly signal: AbortSignal;
  }): PromiseLike<readonly DurablePeriodicJob[]>;
  /** Refresh records for `ids` and reap expired records, even when `ids` is empty. */
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
