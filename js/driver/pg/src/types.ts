import type { PostgresTransactionAdapter } from "riverqueue/unstable-driver";
import type { AttemptError, JobRow, JsonObject } from "riverqueue";

/** Construction options owned by the Postgres backend. */
export interface PgDriverOptions<Transaction = never> {
  /** Postgres schema containing River's tables and functions. */
  schema?: string;
  /**
   * Enable native ORM transactions for `completeTx(tx)` in workers and
   * `{ tx }` on client operations. Import the adapter from its driver package
   * and configure it on each `PgDriver` that needs it. The ORM owns commit,
   * rollback, and savepoints; River uses the supplied transaction only.
   *
   * Knex and the pg pool must connect to the same database. Configure the
   * same River schema on the worker and insertion clients. Knex remains an
   * optional dependency, installed with `@riverqueue/driver-knex`.
   *
   * @example
   * ```ts
   * import knex, { type Knex } from "knex";
   * import { Pool } from "pg";
   * import { Client, defineJob, Workers } from "riverqueue";
   * import { knexTransactionAdapter } from "@riverqueue/driver-knex";
   * import { PgDriver } from "@riverqueue/driver-pg";
   *
   * const connectionString = "postgres://localhost/river";
   * const db = knex({ client: "pg", connection: connectionString });
   * const pool = new Pool({ connectionString });
   * const syncAccount = defineJob<{ accountId: string }>()({
   *   kind: "sync_account",
   * });
   * const workers = new Workers<Knex.Transaction>().add(
   *   syncAccount,
   *   async ({ completeTx, job }) => {
   *     await db.transaction(async (tx) => {
   *       await tx("account")
   *         .where({ id: job.args.accountId })
   *         .update({ synced: true });
   *       await completeTx(tx);
   *     });
   *   }
   * );
   * const client = new Client(
   *   new PgDriver(pool, { transactionAdapters: [knexTransactionAdapter] }),
   *   { queues: { default: { maxWorkers: 10 } }, workers }
   * );
   * ```
   */
  transactionAdapters?: readonly PostgresTransactionAdapter<Transaction>[];
}

/** Options common to Postgres semantic operations. */
export interface PgOperationOptions {
  /** Caller-owned transaction connection used for the entire operation. */
  tx?: unknown;
}

/** Exact semantic inputs for cancelling a job. */
export interface PgJobCancelParams {
  cancelAttemptedAt: Temporal.Instant;
  controlTopic: string;
  id: bigint;
  now?: Temporal.Instant;
}

/** Exact semantic inputs for retrying a job. */
export interface PgJobRetryParams {
  id: bigint;
  now?: Temporal.Instant;
}

/** Exact properties of a persisted River queue. */
export interface PgQueueRow {
  createdAt: Temporal.Instant;
  metadata: JsonObject;
  name: string;
  pausedAt: Temporal.Instant | null;
  updatedAt: Temporal.Instant;
}

/** Exact semantic inputs for pausing or resuming queues. */
export interface PgQueueControlParams {
  /** A queue name, or `"*"` to target all known queues. */
  name: string;
  now?: Temporal.Instant;
}

/** One job selected for rescue after a stale running attempt. */
export interface PgJobRescue {
  error: AttemptError;
  finalizedAt?: Temporal.Instant;
  id: bigint;
  scheduledAt: Temporal.Instant;
  state: "cancelled" | "discarded" | "retryable";
}

/** Runtime inputs for safely rescuing stuck jobs. */
export interface PgJobRescueManyParams {
  items: readonly PgJobRescue[];
  stuckHorizon: Temporal.Instant;
}

/** Scheduler output, including uniqueness conflicts discarded by River. */
export interface PgJobScheduleResult {
  conflictDiscarded: boolean;
  job: JobRow;
}

/** Cleaner retention horizons and optional queue selection. */
export interface PgJobDeleteBeforeParams {
  cancelledFinalizedAt?: Temporal.Instant;
  completedFinalizedAt?: Temporal.Instant;
  discardedFinalizedAt?: Temporal.Instant;
  max: number;
  queuesExcluded?: readonly string[];
  queuesIncluded?: readonly string[];
}

/** One River leadership lease. */
export interface PgLeader {
  electedAt: Temporal.Instant;
  expiresAt: Temporal.Instant;
  leaderId: string;
}

/** Inputs shared by election and lease renewal. */
export interface PgLeaderElectParams {
  leaderId: string;
  now?: Temporal.Instant;
  ttlSeconds: number;
}

/** Inputs that bind renewal/resignation to an exact election term. */
export interface PgLeaderTermParams extends PgLeaderElectParams {
  electedAt: Temporal.Instant;
}

/** A Postgres notification emitted through River's namespaced channels. */
export interface PgNotification {
  payload: string;
  topic: string;
}

/** Inputs for creating or refreshing a persisted queue. */
export interface PgQueueUpsertParams {
  metadata?: JsonObject;
  name: string;
  now?: Temporal.Instant;
  pausedAt?: Temporal.Instant;
  updatedAt?: Temporal.Instant;
}
