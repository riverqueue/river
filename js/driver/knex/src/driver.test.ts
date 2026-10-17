import knex, { type Knex } from "knex";
import { Pool, type ClientBase } from "pg";
import { Client, defineJob, type InsertClient } from "riverqueue";
import { describe, expect, expectTypeOf, it } from "vitest";
import { KnexDriver, knexTransactionAdapter } from "./index.js";
import { PgDriver } from "../../pg/src/index.js";

describe("KnexDriver", () => {
  it("adds Knex transactions to the configured Postgres client's types", async () => {
    const pool = new Pool();
    try {
      const client = new Client(
        new PgDriver(pool, { transactionAdapters: [knexTransactionAdapter] })
      );
      expectTypeOf(client).toEqualTypeOf<
        import("riverqueue").Client<ClientBase | Knex.Transaction>
      >();
      const typed = (tx: Knex.Transaction, root: Knex) => {
        void client.jobs.get(1n, { tx });
        // @ts-expect-error A root Knex instance is not a transaction.
        void client.jobs.get(1n, { tx: root });
      };
      expect(typed).toBeTypeOf("function");
    } finally {
      await pool.end();
    }
  });

  it("exposes an insertion-only client with Knex transactions", async () => {
    const db = knex({ client: "pg" });
    try {
      const client = new Client(new KnexDriver(db));
      expectTypeOf(client).toEqualTypeOf<InsertClient<Knex.Transaction>>();
      // @ts-expect-error Knex does not support the worker runtime API.
      await expect(client.start()).rejects.toThrow();
    } finally {
      await db.destroy();
    }
  });

  it("rejects a root client passed as a transaction", async () => {
    const db = knex({ client: "pg" });
    try {
      const client = new Client(new KnexDriver(db));
      await expect(
        // @ts-expect-error A root Knex instance cannot be used as a transaction.
        client.insert(defineJob({ kind: "test" }), {}, { tx: db })
      ).rejects.toThrow("active Knex transaction");
    } finally {
      await db.destroy();
    }
  });

  it("rejects invalid schemas before issuing queries", async () => {
    const db = knex({ client: "pg" });
    try {
      for (const schema of ["", "a.b", "bad$1", "bad?", "a".repeat(47)]) {
        expect(() => new KnexDriver(db, { schema })).toThrow();
      }
      expect(
        () => new KnexDriver(db, { schema: "a".repeat(46) })
      ).not.toThrow();
    } finally {
      await db.destroy();
    }
  });

  it("rejects unsupported clients", () => {
    for (const value of [null, {}, () => {}, { raw: () => {} }]) {
      expect(() => new KnexDriver(value as unknown as Knex)).toThrow(
        "Knex requires a Postgres client"
      );
    }
    const wrongDialect = Object.assign(() => {}, {
      client: { dialect: "sqlite3" },
      raw: () => {},
    });
    expect(() => new KnexDriver(wrongDialect as unknown as Knex)).toThrow(
      "Knex requires a Postgres client"
    );
  });
});
