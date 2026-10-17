/**
 * Query transport for a caller-owned Postgres transaction. First-party ORM
 * packages provide these adapters; applications configure them on PgDriver.
 * An adapter never begins, commits, rolls back, or releases the transaction.
 */
export interface PostgresTransactionAdapter<Transaction = unknown> {
  /** Recognize the ORM's transaction handle, including a completed handle. */
  matches(transaction: unknown): transaction is Transaction;
  /** Run River's SQL on that exact transaction with the supplied parsers. */
  query<Row>(
    transaction: unknown,
    query: PostgresTransactionQuery
  ): Promise<PostgresTransactionResult<Row>>;
  /** Reject completed transactions and unsupported database dialects. */
  validate(transaction: unknown): void;
}

/** River's numbered-parameter SQL and query-scoped Postgres type parsers. */
export interface PostgresTransactionQuery {
  readonly text: string;
  /** Driver-owned parsers; the adapter must pass these through unchanged. */
  readonly types?: unknown;
  readonly values: readonly unknown[];
}

/** Query results used by River, independent of an ORM or pg package. */
export interface PostgresTransactionResult<Row> {
  readonly rowCount: number | null;
  readonly rows: Row[];
}
