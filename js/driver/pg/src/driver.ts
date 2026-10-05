import type { Client as PgClient, Pool, PoolClient, QueryResultRow } from "pg";
import type {
  AttemptError,
  Driver,
  DriverOptions,
  JobInsertParams,
  JobRow,
  JobState,
} from "riverqueue";
import { uniqueBitmaskToStates } from "riverqueue";

/**
 * A River driver for node-postgres (`pg`).
 *
 *     import { Pool } from "pg";
 *     import { Client } from "riverqueue";
 *     import { PgDriver } from "@riverqueue/driver-pg";
 *
 *     const pool = new Pool({ connectionString: "postgres://..." });
 *     const client = new Client(new PgDriver(pool));
 *
 * For transactions, pass a `PoolClient` as the `tx` option:
 *
 *     const poolClient = await pool.connect();
 *     await poolClient.query("BEGIN");
 *     await client.insert(args, { tx: poolClient });
 *     await poolClient.query("COMMIT");
 *     poolClient.release();
 */
export class PgDriver implements Driver<PoolClient> {
  private client: Pool | PoolClient | PgClient;

  constructor(client: Pool | PoolClient | PgClient) {
    this.client = client;
  }

  async jobInsert(
    params: JobInsertParams,
    options?: DriverOptions<PoolClient>
  ): Promise<[JobRow, boolean]> {
    const results = await this.jobInsertMany([params], options);
    return results[0] as [JobRow, boolean];
  }

  async jobInsertMany(
    params: JobInsertParams[],
    options?: DriverOptions<PoolClient>
  ): Promise<[JobRow, boolean][]> {
    if (params.length === 0) return [];

    const COLUMNS_PER_ROW = 10;
    const values: unknown[] = [];
    const valueClauses: string[] = [];

    for (let i = 0; i < params.length; i++) {
      const p = params[i] as JobInsertParams;
      const offset = i * COLUMNS_PER_ROW;
      valueClauses.push(
        `($${offset + 1}::jsonb, $${offset + 2}, $${offset + 3}, $${offset + 4}, ` +
          `$${offset + 5}, $${offset + 6}::timestamptz, $${offset + 7}, ` +
          `$${offset + 8}::text[], $${offset + 9}::bytea, $${offset + 10}::bit(8))`
      );
      values.push(
        p.encodedArgs,
        p.kind,
        p.maxAttempts,
        p.priority,
        p.queue,
        p.scheduledAt,
        p.state,
        p.tags,
        p.uniqueKey ? Buffer.from(p.uniqueKey) : null,
        p.uniqueStates
      );
    }

    const schemaPrefix = options?.schemaPrefix ?? "";
    const sql = `
      INSERT INTO ${schemaPrefix}river_job (
        args, kind, max_attempts, priority,
        queue, scheduled_at, state,
        tags, unique_key, unique_states
      )
      VALUES ${valueClauses.join(", ")}
      ON CONFLICT (unique_key)
        WHERE unique_key IS NOT NULL
          AND unique_states IS NOT NULL
          AND ${schemaPrefix}river_job_state_in_bitmask(unique_states, state)
      DO UPDATE SET kind = EXCLUDED.kind
      RETURNING *, (xmax != 0) AS unique_skipped_as_duplicate
    `;

    const queryable = options?.tx ?? this.client;
    const result = await queryable.query(sql, values);
    return result.rows.map((row: QueryResultRow) => this.toInsertResult(row));
  }

  private toInsertResult(row: QueryResultRow): [JobRow, boolean] {
    return [this.toJobRow(row), row.unique_skipped_as_duplicate as boolean];
  }

  private toJobRow(row: QueryResultRow): JobRow {
    return {
      id: Number(row.id),
      args: row.args as Record<string, unknown>,
      attempt: row.attempt as number,
      attemptedAt: (row.attempted_at as Date) ?? null,
      attemptedBy: (row.attempted_by as string[]) ?? null,
      createdAt: row.created_at as Date,
      errors: row.errors
        ? (row.errors as Record<string, unknown>[]).map((e): AttemptError => ({
            at: new Date(e.at as string),
            attempt: e.attempt as number,
            error: e.error as string,
            trace: e.trace as string,
          }))
        : null,
      finalizedAt: (row.finalized_at as Date) ?? null,
      kind: row.kind as string,
      maxAttempts: row.max_attempts as number,
      metadata: row.metadata as Record<string, unknown>,
      priority: row.priority as number,
      queue: row.queue as string,
      scheduledAt: row.scheduled_at as Date,
      state: row.state as JobState,
      tags: (row.tags as string[]) ?? [],
      uniqueKey: row.unique_key
        ? new Uint8Array(row.unique_key as Buffer)
        : null,
      uniqueStates: row.unique_states
        ? uniqueBitmaskToStates(parseInt(row.unique_states as string, 2))
        : null,
    };
  }
}
