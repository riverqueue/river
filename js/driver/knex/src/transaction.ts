import type { Knex } from "knex";
import { ConfigurationError } from "riverqueue";
import type {
  PostgresTransactionAdapter,
  PostgresTransactionQuery,
  PostgresTransactionResult,
} from "riverqueue/unstable-driver";

/**
 * Let a PgDriver use native Knex transactions, including `completeTx(tx)`
 * in worker handlers. Configure `new PgDriver(pool, {
 * transactionAdapters: [knexTransactionAdapter] })`. Knex owns transaction
 * commit, rollback, and savepoints; River uses the supplied transaction only.
 */
export const knexTransactionAdapter: PostgresTransactionAdapter<Knex.Transaction> =
  Object.freeze({
    matches: (value: unknown): value is Knex.Transaction =>
      typeof value === "function" &&
      "isTransaction" in value &&
      value.isTransaction === true &&
      "raw" in value &&
      typeof value.raw === "function",
    query: <Row>(transaction: unknown, query: PostgresTransactionQuery) => {
      validateKnexTransaction(transaction);
      return queryKnex<Row>(transaction, query);
    },
    validate: validateKnexTransaction,
  });

/** Run River's fixed SQL through Knex without changing its bound values. */
export async function queryKnex<Row>(
  knex: Knex,
  query: PostgresTransactionQuery
): Promise<PostgresTransactionResult<Row>> {
  // River's schema names cannot contain '$' or '?'. Its fixed SQL uses $n
  // only for parameters. Escape literal '?' (including Postgres JSON
  // operators) before introducing Knex's bindings, preserving repeated and
  // out-of-order numbered parameters.
  const bindings: Knex.RawBinding[] = [];
  const statement = query.text
    .replaceAll("?", "\\?")
    .replace(/\$(\d+)/g, (_match, index: string) => {
      bindings.push(query.values[Number(index) - 1] as Knex.RawBinding);
      return "?";
    });
  const raw = knex
    .raw<PostgresTransactionResult<Row>>(statement, bindings)
    .queryContext({ river: true });
  if (query.types !== undefined) raw.options({ types: query.types });
  return raw;
}

/** Validate a Knex client configured for Postgres. */
export function validateKnexClient(value: unknown): asserts value is Knex {
  if (
    typeof value !== "function" ||
    !("raw" in value) ||
    typeof value.raw !== "function" ||
    !("client" in value) ||
    typeof value.client !== "object" ||
    value.client === null ||
    !("dialect" in value.client) ||
    value.client.dialect !== "postgresql"
  ) {
    throw new ConfigurationError(
      'Knex requires a Postgres client (client: "pg") for River'
    );
  }
}

/** Validate a live Knex transaction, including a nested savepoint. */
export function validateKnexTransaction(
  value: unknown
): asserts value is Knex.Transaction {
  validateKnexClient(value);
  if (
    !value.isTransaction ||
    !("isCompleted" in value) ||
    typeof value.isCompleted !== "function" ||
    (value as Knex.Transaction).isCompleted()
  ) {
    throw new ConfigurationError(
      "River requires an active Knex transaction for { tx }"
    );
  }
}
