import {
  isExactJsonNumber,
  jsonNumberToBigInt,
  parseJsonObject,
} from "riverqueue";
import type { PostgresTransactionTextQuery } from "riverqueue/unstable-driver";
import { PG_EXACT_TYPES } from "./exact-types.js";

/**
 * A query and, when needed, its top-level WITH clause. Data-modifying CTEs
 * must stay at the top level when an ORM wraps the result in a text projection.
 * Keep that boundary explicit instead of parsing or rewriting SQL strings.
 */
export type PgStatement =
  | string
  | {
      readonly text: string;
      /** The entire WITH clause, including any leading statement comment. */
      readonly with: string;
    };

/** Render the original statement for native Postgres transports. */
export function postgresQueryText(statement: PgStatement): string {
  return typeof statement === "string"
    ? statement
    : `${statement.with}\n${statement.text}`;
}

/**
 * Return each row as JSON text so an ORM cannot round JSON numbers or
 * truncate timestamps. The same CTEs and final query execute once, with the
 * original bindings, ordering, row locks, and transaction ownership.
 */
export function postgresTextQuery(
  statement: PgStatement
): PostgresTransactionTextQuery {
  const prefix = typeof statement === "string" ? "WITH" : `${statement.with},`;
  const text = typeof statement === "string" ? statement : statement.text;
  return {
    decode: decodeTextRow,
    text: `${prefix} river_transaction_result AS (${text})
      SELECT row_to_json(river_transaction_result)::text AS river_row
      FROM river_transaction_result`,
  };
}

/** Restore the Postgres types used by River's fixed query projections. */
function decodeTextRow(text: string): Record<string, unknown> {
  const json = parseJsonObject(text);
  const row: Record<string, unknown> = { ...json };
  // An explicitly selected id::text stays text; ordinary bigint IDs are
  // JSON numbers, read exactly before conversion to bigint.
  if (typeof json.id === "number" || isExactJsonNumber(json.id)) {
    row.id = jsonNumberToBigInt(json.id);
  }
  for (const field of [
    "attempted_at",
    "created_at",
    "elected_at",
    "expires_at",
    "finalized_at",
    "paused_at",
    "scheduled_at",
    "updated_at",
  ]) {
    const value = json[field];
    if (typeof value === "string") row[field] = Temporal.Instant.from(value);
  }
  if (Array.isArray(json.errors)) {
    // The normal pg decoder leaves jsonb[] elements as text for River's
    // tolerant attempt-error decoder, including NULL elements.
    row.errors = json.errors.map((value) =>
      value === null ? null : JSON.stringify(value)
    );
  }
  if (typeof json.unique_key === "string") {
    // Honor both Postgres bytea_output formats with River's existing parser.
    const parseBytea = PG_EXACT_TYPES.getTypeParser(17, "text") as (
      value: string
    ) => unknown;
    row.unique_key = parseBytea(json.unique_key);
  }
  return row;
}
