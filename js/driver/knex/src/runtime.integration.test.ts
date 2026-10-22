import { randomUUID } from "node:crypto";
import knex, { type Knex } from "knex";
import { Pool, type ClientBase } from "pg";
import {
  Client,
  defineJob,
  exactJsonNumber,
  type JobRow,
  type WorkHandler,
  Workers,
} from "riverqueue";
import { PilotClient, type PilotFactory } from "riverqueue/unstable-driver";
import { afterAll, afterEach, beforeAll, describe, expect, it } from "vitest";
import { createMigrator } from "../../../migrate/src/index.js";
import { PgDriver } from "../../pg/src/index.js";
import { knexTransactionAdapter } from "./index.js";

const TEST_DATABASE_URL =
  process.env.TEST_DATABASE_URL ??
  "postgres://localhost:5432/river_test?sslmode=disable";

class CompanionClient extends PilotClient<ClientBase | Knex.Transaction> {}

describe("Knex transactional completion", () => {
  const schema = `knex_work_${randomUUID().replaceAll("-", "")}`;
  const definition = defineJob({ kind: "knex_work" });
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

  const runWorker = async (
    handler: WorkHandler<typeof definition>,
    createPilot?: PilotFactory<ClientBase | Knex.Transaction>
  ): Promise<JobRow> => {
    const driver = new PgDriver(pool, {
      schema,
      transactionAdapters: [knexTransactionAdapter],
    });
    const options = {
      completionBatchSize: 1,
      leaderElectionDisabled: true,
      queues: {
        default: {
          fetchCooldown: { milliseconds: 1 },
          maxWorkers: 1,
          pollInterval: { milliseconds: 5 },
        },
      },
      workers: new Workers().add(definition, handler),
    };
    const client =
      createPilot === undefined
        ? new Client(driver, options)
        : new CompanionClient(driver, options, createPilot);
    const inserted = await client.insert(
      definition,
      { id: exactJsonNumber("9223372036854775807") },
      { maxAttempts: 1 }
    );
    const events = client.subscribe({
      kinds: ["job_completed", "job_failed"],
      signal: AbortSignal.timeout(10_000),
    });
    const run = await client.start();
    try {
      let completed = false;
      for await (const event of events) {
        if (event.kind === "subscription_lag") continue;
        if (event.job.id !== inserted.job.id) continue;
        completed = true;
        break;
      }
      expect(completed).toBe(true);
      const row = await client.jobs.get(inserted.job.id);
      expect(row).not.toBeNull();
      return row!;
    } finally {
      events.close();
      await run.stop({ timeout: { seconds: 5 } });
    }
  };

  it("commits application writes and exact completion values together", async () => {
    let completed: JobRow | undefined;
    const row = await runWorker(async ({ completeTx, job, setMetadata }) => {
      setMetadata("custom", "value");
      await db.transaction(async (tx) => {
        await tx
          .withSchema(schema)
          .table("application_row")
          .insert({ value: "committed ? $1 ' ☃" });
        completed = await completeTx(tx, {
          output: { id: exactJsonNumber("9223372036854775807") },
        });
        expect(completed.id).toBe(job.id);
        expect(completed.createdAt.toString()).toBe(job.createdAt.toString());
        expect(completed.finalizedAt).toBeInstanceOf(Temporal.Instant);
        expect(completed.state).toBe("completed");
        expect(JSON.stringify(completed.args)).toBe(
          '{"id":9223372036854775807}'
        );
        expect(
          (
            await pool.query(
              `SELECT state FROM "${schema}".river_job WHERE id = $1`,
              [job.id.toString()]
            )
          ).rows[0].state
        ).toBe("running");
        expect(
          await db.withSchema(schema).table("application_row")
        ).toHaveLength(0);
      });
      // A later handler failure cannot undo a completion already committed.
      throw new Error("after commit");
    });
    expect(row.state).toBe("completed");
    expect(row.metadata.custom).toBe("value");
    expect(JSON.stringify(row.metadata.output)).toBe(
      '{"id":9223372036854775807}'
    );
    expect(await db.withSchema(schema).table("application_row")).toEqual([
      { value: "committed ? $1 ' ☃" },
    ]);
    expect(completed?.finalizedAt?.toString()).toBe(
      row.finalizedAt?.toString()
    );
  });

  it("inserts and reads through the configured runtime transaction adapter", async () => {
    const client = new Client(
      new PgDriver(pool, {
        schema,
        transactionAdapters: [knexTransactionAdapter],
      })
    );
    const tx = await db.transaction();
    let id: bigint;
    try {
      const { job } = await client.insert(
        definition,
        {
          id: exactJsonNumber("9223372036854775807"),
        },
        { tx }
      );
      id = job.id;
      expect((await client.jobs.get(id, { tx }))?.args).toEqual(job.args);
      expect(await client.jobs.get(id)).toBeNull();
    } finally {
      await tx.rollback();
    }
    expect(await client.jobs.get(id)).toBeNull();
  });

  it("keeps completion interceptors inside the Knex transaction", async () => {
    let expectedTx: Knex.Transaction | undefined;
    let intercepted = false;
    const row = await runWorker(
      async ({ completeTx }) => {
        await db.transaction(async (tx) => {
          expectedTx = tx;
          await completeTx(tx);
          expect(intercepted).toBe(true);
          expect(await tx.withSchema(schema).table("application_row")).toEqual([
            { value: "intercepted" },
          ]);
          throw new Error("rollback interceptor and completion");
        });
      },
      () => ({
        intercept: {
          complete: async (context, next) => {
            if (context.tx !== expectedTx) return next();
            intercepted = true;
            await expectedTx
              .withSchema(schema)
              .table("application_row")
              .insert({ value: "intercepted" });
            const result = await next();
            expect(
              await db.withSchema(schema).table("application_row")
            ).toHaveLength(0);
            return result;
          },
        },
      })
    );
    expect(intercepted).toBe(true);
    expect(row.state).toBe("discarded");
    expect(await db.withSchema(schema).table("application_row")).toHaveLength(
      0
    );
  });

  it("rejects completed and unconfigured transactions before writing", async () => {
    const tx = await db.transaction();
    const unconfigured = new Client(new PgDriver(pool, { schema }));
    try {
      // @ts-expect-error Knex transactions require a configured adapter.
      await expect(unconfigured.jobs.get(1n, { tx })).rejects.toMatchObject({
        code: "backend_mismatch",
      });
      await tx.rollback();
      const row = await runWorker(async ({ completeTx }) => {
        await expect(completeTx(tx)).rejects.toThrow("active Knex transaction");
        throw new Error("transaction closed");
      });
      expect(row.state).toBe("discarded");
    } finally {
      if (!tx.isCompleted()) await tx.rollback();
    }
  });

  it("rejects completion from an attempt whose identity changed", async () => {
    let rejected = false;
    const row = await runWorker(async ({ completeTx, job }) => {
      await db.transaction(async (tx) => {
        await tx
          .withSchema(schema)
          .table("river_job")
          .where({ id: job.id.toString() })
          .update({ attempt: job.attempt + 1 });
        await expect(completeTx(tx)).rejects.toThrow("no longer running");
        rejected = true;
        // Roll the simulated rescue back, allowing this attempt to finish.
        throw new Error("rollback attempt change");
      });
    });
    expect(rejected).toBe(true);
    expect(row.state).toBe("discarded");
    expect(row.attempt).toBe(1);
  });

  it("rolls back application writes and completion together", async () => {
    const row = await runWorker(async ({ completeTx }) => {
      await db.transaction(async (tx) => {
        await tx
          .withSchema(schema)
          .table("application_row")
          .insert({ value: "rolled back" });
        await completeTx(tx, { output: { rolledBack: true } });
        throw new Error("rollback both writes");
      });
    });
    expect(row.state).toBe("discarded");
    expect(row.metadata).not.toHaveProperty("output");
    expect(await db.withSchema(schema).table("application_row")).toHaveLength(
      0
    );
  });

  it("uses native savepoints without ending the outer transaction", async () => {
    let nestedTx: Knex.Transaction | undefined;
    const row = await runWorker(async ({ completeTx, job }) => {
      await db.transaction(async (tx) => {
        await tx
          .withSchema(schema)
          .table("application_row")
          .insert({ value: "outer" });
        await expect(
          tx.transaction(async (nested) => {
            nestedTx = nested;
            await nested
              .withSchema(schema)
              .table("application_row")
              .insert({ value: "inner" });
            await completeTx(nested, { output: { inner: true } });
            throw new Error("rollback savepoint");
          })
        ).rejects.toThrow("rollback savepoint");
        expect(
          (
            await tx
              .withSchema(schema)
              .table("river_job")
              .where({ id: job.id.toString() })
              .first()
          ).state
        ).toBe("running");
        expect(tx.isCompleted()).toBe(false);
        await completeTx(tx, { output: { outer: true } });
        // The original transaction remains usable after completeTx.
        await tx
          .withSchema(schema)
          .table("application_row")
          .insert({ value: "after completion" });
      });
    });
    expect(nestedTx).toBeDefined();
    expect(row.state).toBe("completed");
    expect(row.metadata.output).toEqual({ outer: true });
    expect(
      await db.withSchema(schema).table("application_row").orderBy("value")
    ).toEqual([{ value: "after completion" }, { value: "outer" }]);
  });
});
