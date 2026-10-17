import { Pool, type ClientBase } from "pg";
import { Client } from "riverqueue";
import { describe, expect, expectTypeOf, it, vi } from "vitest";
import { PgDriver } from "../../pg/src/index.js";
import {
  prismaTransactionAdapter,
  type PrismaTransactionClientLike,
} from "./index.js";

describe("prismaTransactionAdapter", () => {
  it("adds Prisma transactions to the configured Postgres client's types", async () => {
    const pool = new Pool();
    try {
      const client = new Client(
        new PgDriver(pool, { transactionAdapters: [prismaTransactionAdapter] })
      );
      expectTypeOf(client).toEqualTypeOf<
        Client<ClientBase | PrismaTransactionClientLike>
      >();
      const typed = (
        tx: PrismaTransactionClientLike,
        root: Pick<PrismaTransactionClientLike, "$queryRawUnsafe"> & {
          $connect(): Promise<void>;
        }
      ) => {
        void client.jobs.get(1n, { tx });
        // @ts-expect-error A root PrismaClient is not a transaction.
        void client.jobs.get(1n, { tx: root });
      };
      expect(typed).toBeTypeOf("function");
    } finally {
      await pool.end();
    }
  });

  it("preserves Postgres error codes across Prisma error formats", async () => {
    for (const meta of [
      { code: "55P03" },
      { driverAdapterError: { cause: { code: "55P03", kind: "postgres" } } },
    ]) {
      const cause = Object.assign(new Error("lock timeout"), { meta });
      const $queryRawUnsafe = vi.fn().mockRejectedValue(cause);
      await expect(
        prismaTransactionAdapter.query(
          { $queryRawUnsafe },
          {
            text: "native SQL",
            textResult: () => ({ decode: JSON.parse, text: "exact SQL" }),
            values: [],
          }
        )
      ).rejects.toMatchObject({
        cause,
        code: "55P03",
        message: "lock timeout",
      });
    }
  });

  it("preserves unrelated Prisma failures unchanged", async () => {
    const cause = Object.assign(new Error("transaction expired"), {
      code: "P2028",
      meta: { error: "Transaction already closed" },
    });
    await expect(
      prismaTransactionAdapter.query(
        { $queryRawUnsafe: vi.fn().mockRejectedValue(cause) },
        {
          text: "native SQL",
          textResult: () => ({ decode: JSON.parse, text: "exact SQL" }),
          values: [],
        }
      )
    ).rejects.toBe(cause);
  });

  it("recognizes transactions with and without native savepoint support", () => {
    const transaction = { $queryRawUnsafe: vi.fn() };
    expect(prismaTransactionAdapter.matches(transaction)).toBe(true);
    expect(
      prismaTransactionAdapter.matches({
        ...transaction,
        $transaction: vi.fn(),
      })
    ).toBe(true);
    // Prisma proxies can expose a masked property to `in` while returning
    // undefined when the method is read.
    expect(
      prismaTransactionAdapter.matches({
        ...transaction,
        $connect: undefined,
        $disconnect: undefined,
      })
    ).toBe(true);
    for (const value of [
      null,
      {},
      { ...transaction, $connect: vi.fn() },
      { ...transaction, $disconnect: vi.fn() },
    ]) {
      expect(prismaTransactionAdapter.matches(value)).toBe(false);
      expect(() => prismaTransactionAdapter.validate(value)).toThrow(
        "interactive transaction"
      );
    }
  });

  it("requires the exact text transport before issuing a query", async () => {
    const $queryRawUnsafe = vi.fn();
    await expect(
      prismaTransactionAdapter.query(
        { $queryRawUnsafe },
        { text: "SELECT 1", values: [] }
      )
    ).rejects.toThrow("exact text-result");
    expect($queryRawUnsafe).not.toHaveBeenCalled();
  });

  it("uses the exact transport with bound values on the supplied transaction", async () => {
    const $queryRawUnsafe = vi.fn(async () => [
      { river_row: "opaque exact row" },
    ]);
    const decode = vi.fn((text: string) => ({ text }));
    const values = ["? $1 ' ☃", [null, "9223372036854775807"]];
    const result = await prismaTransactionAdapter.query(
      { $queryRawUnsafe },
      {
        text: "native SQL",
        textResult: () => ({ decode, text: "exact SQL $1 $2" }),
        values,
      }
    );
    expect($queryRawUnsafe).toHaveBeenCalledExactlyOnceWith(
      "exact SQL $1 $2",
      ...values
    );
    expect(decode).toHaveBeenCalledExactlyOnceWith("opaque exact row");
    expect(result).toEqual({
      rowCount: 1,
      rows: [{ text: "opaque exact row" }],
    });
  });
});
