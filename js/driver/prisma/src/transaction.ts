import { ConfigurationError } from "riverqueue";
import type {
  PostgresTransactionAdapter,
  PostgresTransactionQuery,
  PostgresTransactionResult,
} from "riverqueue/unstable-driver";
import type { PrismaClientLike } from "./driver.js";

/**
 * The raw-query surface of Prisma's interactive-transaction client. It omits
 * the root client's connection lifecycle methods. A generated
 * `Prisma.TransactionClient` is assignable to this interface, including
 * versions that expose `$transaction` for nested savepoints.
 */
export interface PrismaTransactionClientLike extends Pick<
  PrismaClientLike,
  "$queryRawUnsafe"
> {
  readonly $connect?: never;
  readonly $disconnect?: never;
}

/**
 * Let a PgDriver use native Prisma interactive transactions, including
 * `completeTx(tx)` in worker handlers. Configure `new PgDriver(pool, {
 * transactionAdapters: [prismaTransactionAdapter] })`. Prisma owns commit,
 * rollback, and transaction timeouts; River uses the supplied transaction only.
 */
export const prismaTransactionAdapter: PostgresTransactionAdapter<PrismaTransactionClientLike> =
  Object.freeze({
    matches: isPrismaTransaction,
    query: queryPrisma,
    validate: validatePrismaTransaction,
  });

function isPrismaTransaction(
  value: unknown
): value is PrismaTransactionClientLike {
  return (
    typeof value === "object" &&
    value !== null &&
    "$queryRawUnsafe" in value &&
    typeof value.$queryRawUnsafe === "function" &&
    (!("$connect" in value) || value.$connect === undefined) &&
    (!("$disconnect" in value) || value.$disconnect === undefined)
  );
}

function postgresErrorCode(meta: unknown): string | undefined {
  if (typeof meta !== "object" || meta === null) return undefined;
  if (
    "code" in meta &&
    typeof meta.code === "string" &&
    /^[0-9A-Z]{5}$/.test(meta.code)
  ) {
    return meta.code;
  }
  if (
    "driverAdapterError" in meta &&
    typeof meta.driverAdapterError === "object" &&
    meta.driverAdapterError !== null &&
    "cause" in meta.driverAdapterError
  ) {
    return postgresErrorCode(meta.driverAdapterError.cause);
  }
  return undefined;
}

async function queryPrisma<Row>(
  transaction: unknown,
  query: PostgresTransactionQuery
): Promise<PostgresTransactionResult<Row>> {
  validatePrismaTransaction(transaction);
  if (query.textResult === undefined) {
    throw new ConfigurationError(
      "Prisma transactions require River's exact text-result query transport"
    );
  }
  const textQuery = query.textResult();
  // Prisma has no query-scoped type parsers: selecting rows as text prevents
  // it from rounding JSON numbers or turning microsecond timestamps into Date.
  let result: { river_row: string }[];
  try {
    result = await transaction.$queryRawUnsafe<{ river_row: string }[]>(
      textQuery.text,
      ...query.values
    );
  } catch (cause: unknown) {
    // Prisma puts SQLSTATE in meta.code or, with newer driver adapters,
    // meta.driverAdapterError.cause.code. Preserve it for PgDriver's
    // retryability classification, retaining the original Prisma error.
    if (cause instanceof Error && "meta" in cause) {
      const code = postgresErrorCode(cause.meta);
      if (code !== undefined) {
        throw Object.assign(new Error(cause.message, { cause }), { code });
      }
    }
    throw cause;
  }
  return {
    rowCount: result.length,
    rows: result.map(({ river_row }) => textQuery.decode(river_row) as Row),
  };
}

function validatePrismaTransaction(
  value: unknown
): asserts value is PrismaTransactionClientLike {
  if (!isPrismaTransaction(value)) {
    throw new ConfigurationError(
      "River requires a Prisma interactive transaction for { tx }"
    );
  }
  // Prisma itself rejects closed/expired interactive transactions before it
  // sends a query. Do not inspect private Prisma engine or connection state.
}
