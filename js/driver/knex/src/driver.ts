import type { Knex } from "knex";
import { ConfigurationError } from "riverqueue";
import { PostgresInserter, registerDriver } from "riverqueue/unstable-driver";
import {
  queryKnex,
  validateKnexClient,
  validateKnexTransaction,
} from "./transaction.js";

/**
 * River's insertion adapter for Knex on Postgres (`client: "pg"`).
 *
 * The Knex instance is caller-owned. Pass an active Knex transaction as
 * `{ tx }` to insert jobs atomically with application writes. Otherwise River
 * opens a Knex transaction for the insertion, its hooks, and notifications.
 * Run workers with `@riverqueue/driver-pg`.
 */
export class KnexDriver {
  /** Type-only marker for insertion with Knex transactions. */
  declare readonly "~river"?: {
    readonly capability: "insert";
    readonly transaction: Knex.Transaction;
  };

  constructor(knex: Knex, options: KnexDriverOptions = {}) {
    validateKnexClient(knex);
    if (knex.isTransaction) {
      throw new ConfigurationError(
        "KnexDriver requires a root Knex instance; pass transactions as { tx }"
      );
    }
    const inserter = new PostgresInserter<Knex.Transaction>({
      backend: "postgres-knex-insert",
      name: "Knex",
      operationScope: (tx, callback) => {
        if (tx === undefined) return knex.transaction(callback);
        validateKnexTransaction(tx);
        return callback(tx);
      },
      query: async <T>(
        tx: Knex.Transaction | undefined,
        sql: string,
        ...values: unknown[]
      ): Promise<T> => {
        const result = await queryKnex<unknown>(tx ?? knex, {
          text: sql,
          values,
        });
        return result.rows as T;
      },
      ...(options.schema === undefined ? {} : { schema: options.schema }),
    });
    registerDriver<Knex.Transaction>(this, {
      backend: inserter.backend,
      capability: "insert",
      operations: inserter,
    });
  }
}

/** Postgres configuration owned by the Knex adapter. */
export interface KnexDriverOptions {
  /** Postgres schema containing River's tables and functions. */
  schema?: string;
}
