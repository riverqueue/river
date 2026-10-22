/**
 * Query transport for a caller-owned Postgres transaction. First-party ORM
 * packages provide these adapters; applications configure them on PgDriver.
 * An adapter never begins, commits, rolls back, or releases the transaction.
 */
export interface PostgresTransactionAdapter<Transaction = unknown> {
  /** Recognize the ORM's transaction handle, including a completed handle. */
  matches(transaction: unknown): transaction is Transaction;
  /** Run River's SQL on that exact transaction, preserving its exact values. */
  query<Row>(
    transaction: unknown,
    query: PostgresTransactionQuery
  ): Promise<PostgresTransactionResult<Row>>;
  /** Validate the handle; query execution must also reject closed transactions. */
  validate(transaction: unknown): void;
}

/** River's numbered-parameter SQL and query-scoped Postgres type parsers. */
export interface PostgresTransactionQuery {
  readonly text: string;
  /**
   * An equivalent query returning each row as JSON text, with a decoder back
   * to the driver's row shape. Used by ORMs that cannot pass through `types`.
   * Constructed lazily so native transports keep their existing SQL.
   */
  readonly textResult?: () => PostgresTransactionTextQuery;
  /** Driver-owned parsers; use these or the exact `textResult` transport. */
  readonly types?: unknown;
  readonly values: readonly unknown[];
}

/** Query results used by River, independent of an ORM or pg package. */
export interface PostgresTransactionResult<Row> {
  readonly rowCount: number | null;
  readonly rows: Row[];
}

/** Exact row transport for ORMs that would otherwise coerce JSON and dates. */
export interface PostgresTransactionTextQuery {
  /** Decode one `river_row` string into the driver's ordinary row shape. */
  decode(row: string): unknown;
  /** Returns one text column named `river_row` for each result row. */
  readonly text: string;
}
