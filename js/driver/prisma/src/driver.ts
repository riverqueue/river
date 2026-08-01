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
 * Minimal interface matching PrismaClient's raw query method. Using an
 * interface avoids a direct import dependency on @prisma/client.
 */
export interface PrismaClientLike {
  $queryRawUnsafe<T = unknown>(query: string, ...values: unknown[]): Promise<T>;
}

/**
 * A River driver for Prisma.
 *
 *     import { PrismaClient } from "@prisma/client";
 *     import { Client } from "riverqueue";
 *     import { PrismaDriver } from "@riverqueue/driver-prisma";
 *
 *     const prisma = new PrismaClient();
 *     const client = new Client(new PrismaDriver(prisma));
 *
 * For transactions, pass the transaction client as the `tx` option:
 *
 *     await prisma.$transaction(async (tx) => {
 *       await client.insert(args, { tx });
 *     });
 */
export class PrismaDriver implements Driver<PrismaClientLike> {
  private prisma: PrismaClientLike;

  constructor(prisma: PrismaClientLike) {
    this.prisma = prisma;
  }

  async jobInsert(
    params: JobInsertParams,
    options?: DriverOptions<PrismaClientLike>
  ): Promise<[JobRow, boolean]> {
    const results = await this.jobInsertMany([params], options);
    return results[0] as [JobRow, boolean];
  }

  async jobInsertMany(
    params: JobInsertParams[],
    options?: DriverOptions<PrismaClientLike>
  ): Promise<[JobRow, boolean][]> {
    if (params.length === 0) return [];

    const schemaPrefix = options?.schemaPrefix ?? "";
    const COLUMNS_PER_ROW = 10;
    const values: unknown[] = [];
    const valueClauses: string[] = [];

    for (let i = 0; i < params.length; i++) {
      const p = params[i] as JobInsertParams;
      const offset = i * COLUMNS_PER_ROW;
      valueClauses.push(
        `($${offset + 1}::jsonb, $${offset + 2}, $${offset + 3}, $${offset + 4}, ` +
          `$${offset + 5}, $${offset + 6}::timestamptz, $${offset + 7}::${schemaPrefix}river_job_state, ` +
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

    const queryable = options?.tx ?? this.prisma;
    const rows = await queryable.$queryRawUnsafe<Record<string, unknown>[]>(
      sql,
      ...values
    );
    return rows.map((row) => this.toInsertResult(row));
  }

  private toInsertResult(row: Record<string, unknown>): [JobRow, boolean] {
    return [this.toJobRow(row), row.unique_skipped_as_duplicate as boolean];
  }

  private toJobRow(row: Record<string, unknown>): JobRow {
    return {
      // Prisma returns BigInt for bigint columns.
      id: Number(row.id),
      args: row.args as Record<string, unknown>,
      attempt: Number(row.attempt),
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
      maxAttempts: Number(row.max_attempts),
      metadata: row.metadata as Record<string, unknown>,
      priority: Number(row.priority),
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
