import type { DurationInput } from "riverqueue";
import { ConfigurationError } from "riverqueue";
import {
  durationToMilliseconds,
  PostgresInserter,
  registerDriver,
} from "riverqueue/unstable-driver";

/** The raw-query surface shared by Prisma clients and transaction clients. */
export interface PrismaClientLike {
  $queryRawUnsafe<T = unknown>(query: string, ...values: unknown[]): Promise<T>;
  /**
   * Prisma's interactive transaction, present on a root client and on
   * transaction clients in versions supporting nested transactions. River
   * uses it to run an insertion without `{ tx }` in a transaction of its own.
   */
  $transaction?<R>(
    callback: (tx: PrismaClientLike) => Promise<R>,
    options?: PrismaTransactionOptions
  ): Promise<R>;
}

/** Options River passes to Prisma's interactive `$transaction`. */
export interface PrismaTransactionOptions {
  /** Longest wait, in milliseconds, for a connection from Prisma's pool. */
  maxWait?: number;
  /** Longest time, in milliseconds, the transaction may stay open. */
  timeout?: number;
}

/** Postgres configuration owned by the adapter. */
export interface PrismaDriverOptions {
  /** Postgres schema containing River's tables and functions. */
  schema?: string;
  /**
   * Limits for the interactive transaction River opens for an insertion
   * without `{ tx }`, such as `{ timeout: { seconds: 10 } }`. Prisma's
   * defaults apply otherwise: a 2 second wait for a connection and a 5
   * second transaction timeout, which also bounds any I/O insert middleware
   * awaits before calling `next()`.
   */
  transactionOptions?: {
    maxWait?: DurationInput;
    timeout?: DurationInput;
  };
}

/**
 * River's insertion adapter for Prisma on Postgres.
 *
 * The Prisma client is caller-owned. A caller-owned transaction client may be
 * supplied as `{ tx }` and is used for the exact operation. Schema selection
 * belongs to the adapter constructor so it cannot accidentally vary within a
 * transaction.
 */
export class PrismaDriver {
  /**
   * Type-only marker: an insert-only driver whose transactions are Prisma
   * interactive-transaction clients. `new Client(new PrismaDriver(prisma))`
   * is therefore typed as an `InsertClient`.
   */
  declare readonly "~river"?: {
    readonly capability: "insert";
    readonly transaction: PrismaClientLike;
  };

  constructor(prisma: PrismaClientLike, options: PrismaDriverOptions = {}) {
    const inserter = new PrismaInserter(prisma, options);
    registerDriver<PrismaClientLike>(this, {
      backend: inserter.backend,
      capability: "insert",
      operations: inserter,
    });
  }
}

/**
 * @internal A registered driver whose operations are callable, for this
 * package's tests.
 */
export function testPrismaDriver(
  prisma: PrismaClientLike,
  options: PrismaDriverOptions = {}
): PrismaInserter {
  const inserter = new PrismaInserter(prisma, options);
  registerDriver<PrismaClientLike>(inserter, {
    backend: inserter.backend,
    capability: "insert",
    operations: inserter,
  });
  return inserter;
}

/** @internal Prisma transport for the shared Postgres insertion engine. */
export class PrismaInserter extends PostgresInserter<PrismaClientLike> {
  constructor(prisma: PrismaClientLike, options: PrismaDriverOptions = {}) {
    if (!isPrismaClientLike(prisma)) {
      throw new ConfigurationError(
        "PrismaDriver requires a Prisma client with $queryRawUnsafe"
      );
    }
    const txOptions = transactionOptions(options.transactionOptions);
    super({
      backend: "postgres-prisma-insert",
      name: "Prisma",
      operationScope: (tx, callback) => {
        if (tx !== undefined) return callback(tx);
        if (typeof prisma.$transaction !== "function") {
          throw new ConfigurationError(
            "the Prisma client given to PrismaDriver has no $transaction, so " +
              "River can't open a transaction of its own for this operation; " +
              "pass { tx } or construct PrismaDriver with a root PrismaClient"
          );
        }
        return txOptions === undefined
          ? prisma.$transaction(callback)
          : prisma.$transaction(callback, txOptions);
      },
      query: (tx, sql, ...values) =>
        (tx ?? prisma).$queryRawUnsafe(sql, ...values),
      ...(options.schema === undefined ? {} : { schema: options.schema }),
    });
  }
}

function transactionOptions(
  options: PrismaDriverOptions["transactionOptions"]
): PrismaTransactionOptions | undefined {
  if (options === undefined) return undefined;
  const result: PrismaTransactionOptions = {};
  if (options.maxWait !== undefined) {
    result.maxWait = durationToMilliseconds(
      "transactionOptions.maxWait",
      options.maxWait
    );
  }
  if (options.timeout !== undefined) {
    result.timeout = durationToMilliseconds(
      "transactionOptions.timeout",
      options.timeout
    );
  }
  return result;
}

function isPrismaClientLike(value: unknown): value is PrismaClientLike {
  return (
    (typeof value === "object" || typeof value === "function") &&
    value !== null &&
    "$queryRawUnsafe" in value &&
    typeof value.$queryRawUnsafe === "function"
  );
}
