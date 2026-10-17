import { randomUUID } from "node:crypto";
import knex, { type Knex } from "knex";
import { Pool } from "pg";
import { Client, defineJob, type InsertClient } from "riverqueue";
import { createMigrator } from "../../../migrate/src/index.js";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { KnexDriver } from "./index.js";

const TEST_DATABASE_URL =
  process.env.TEST_DATABASE_URL ??
  "postgres://localhost:5432/river_test?sslmode=disable";

describe("KnexDriver transactions", () => {
  const schema = `knex_${randomUUID().replaceAll("-", "")}`;
  const job = defineJob<{ value?: string }>()({ kind: "knex_test" });
  let client: InsertClient<Knex.Transaction>;
  let db: Knex;
  let pool: Pool;

  beforeAll(async () => {
    pool = new Pool({ connectionString: TEST_DATABASE_URL });
    await pool.query(`CREATE SCHEMA "${schema}"`);
    await createMigrator({ pool, schema }).migrateUp();
    db = knex({
      client: "pg",
      connection: TEST_DATABASE_URL,
      pool: { min: 0, max: 2 },
    });
    await db.schema
      .withSchema(schema)
      .createTable("application_row", (table) => {
        table.text("value").notNullable();
      });
    client = new Client(new KnexDriver(db, { schema }));
  });

  afterAll(async () => {
    await db.destroy();
    await pool.query(`DROP SCHEMA "${schema}" CASCADE`);
    await pool.end();
  });

  afterEach(async () => {
    await db.withSchema(schema).table("application_row").delete();
    await db.withSchema(schema).table("river_job").delete();
  });

  it("commits application writes and jobs together", async () => {
    const value = "quotes ' ? $1 \\ and Unicode ☃";
    await db.transaction(async (tx) => {
      await tx.withSchema(schema).table("application_row").insert({ value });
      const inserted = await client.insert(job, { value }, { tx });
      expect(inserted.job.args).toEqual({ value });
      expect(await db.withSchema(schema).table("river_job")).toHaveLength(0);
      expect(await tx.withSchema(schema).table("river_job")).toHaveLength(1);
    });
    expect(await db.withSchema(schema).table("application_row")).toEqual([
      { value },
    ]);
    expect(await db.withSchema(schema).table("river_job")).toHaveLength(1);
  });

  it("honors caller-owned savepoints", async () => {
    const failure = new Error("rollback savepoint");
    await db.transaction(async (tx) => {
      await client.insert(job, { value: "outer" }, { tx });
      await expect(
        tx.transaction(async (nested) => {
          await client.insert(job, { value: "inner" }, { tx: nested });
          throw failure;
        })
      ).rejects.toBe(failure);
      expect(
        await tx.withSchema(schema).table("river_job").select("args")
      ).toEqual([{ args: { value: "outer" } }]);
    });
    expect(await db.withSchema(schema).table("river_job")).toHaveLength(1);
  });

  it("preserves raw responses when application postprocessing opts out", async () => {
    const processed = db.withUserParams({});
    processed.client.config.postProcessResponse = (
      result: unknown,
      context: unknown
    ) => {
      if (
        typeof context === "object" &&
        context !== null &&
        "river" in context &&
        context.river === true
      )
        return result;
      throw new Error("application postprocessing must not change River rows");
    };
    const result = await new Client(
      new KnexDriver(processed, { schema })
    ).insert(job, {});
    expect(result.status).toBe("inserted");
  });

  it("rejects completed transactions and transaction constructors", async () => {
    const tx = await db.transaction();
    expect(() => new KnexDriver(tx)).toThrow("root Knex instance");
    await tx.rollback();
    await expect(client.insert(job, {}, { tx })).rejects.toThrow(
      "active Knex transaction"
    );
  });

  it("rolls back application writes and jobs together", async () => {
    const failure = new Error("rollback transaction");
    await expect(
      db.transaction(async (tx) => {
        await tx
          .withSchema(schema)
          .table("application_row")
          .insert({ value: "rolled back" });
        await client.insertMany(
          [
            { args: {}, job },
            { args: { value: "second" }, job },
          ],
          { tx }
        );
        throw failure;
      })
    ).rejects.toBe(failure);
    expect(await db.withSchema(schema).table("application_row")).toHaveLength(
      0
    );
    expect(await db.withSchema(schema).table("river_job")).toHaveLength(0);
  });

  it("rolls back its own transaction when insert middleware fails", async () => {
    const failure = new Error("after insert");
    const failingClient = new Client(new KnexDriver(db, { schema }), {
      insertMiddleware: [
        async (_context, next) => {
          await next();
          throw failure;
        },
      ],
    });
    await expect(failingClient.insert(job, {})).rejects.toBe(failure);
    expect(await db.withSchema(schema).table("river_job")).toHaveLength(0);
    expect((await client.insert(job, {})).status).toBe("inserted");
  });
});
