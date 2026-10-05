import type { JobRow, JobState } from "./job.js";

/**
 * Internal insert parameters sent to drivers. This interface is meant for
 * driver implementations and is subject to change.
 */
export interface JobInsertParams {
  encodedArgs: string;
  kind: string;
  maxAttempts: number;
  priority: number;
  queue: string;
  scheduledAt: Date;
  state: JobState;
  tags: string[];
  uniqueKey: Uint8Array | null;
  /** Bitmask string like "10110001" representing states for uniqueness. */
  uniqueStates: string | null;
}

/**
 * Interface that database drivers must implement. River drivers translate
 * the generic insert params into database-specific operations.
 */
/**
 * Interface that database drivers must implement. The TTx type parameter
 * represents the driver-specific transaction type (e.g. PoolClient for pg,
 * PrismaClientLike for Prisma).
 */
export interface Driver<TTx = unknown> {
  /** Insert a single job. */
  jobInsert(
    params: JobInsertParams,
    options?: DriverOptions<TTx>
  ): Promise<[JobRow, boolean]>;

  /** Insert multiple jobs in a single batch operation. */
  jobInsertMany(
    params: JobInsertParams[],
    options?: DriverOptions<TTx>
  ): Promise<[JobRow, boolean][]>;
}

/** Options passed from the Client to drivers on each operation. */
export interface DriverOptions<TTx = unknown> {
  /** Schema-qualified table prefix (e.g. `"my_schema".`), or empty string for default. */
  schemaPrefix: string;
  /** Optional transaction to run the operation within. */
  tx?: TTx;
}
