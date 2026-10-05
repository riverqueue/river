import assert from "node:assert/strict";
import { randomBytes } from "node:crypto";
import process from "node:process";
import { after, before, describe, it } from "node:test";

import { PgDriver } from "@riverqueue/driver-pg";
import { createMigrator } from "@riverqueue/migrate";
import pg from "pg";
import {
  Client,
  defineJob,
  exactJsonNumber,
  jsonNumberToBigInt,
  Workers,
} from "riverqueue";

import { workUntilFinalized } from "./helpers.mjs";

const DATABASE_URL = process.env.DATABASE_URL ?? "";
if (DATABASE_URL === "" && process.env.RIVER_REQUIRE_POSTGRES === "1") {
  throw new Error("RIVER_REQUIRE_POSTGRES is set but DATABASE_URL is not");
}
const INT8_MAX = 9_223_372_036_854_775_807n;

const accountJob = defineJob({
  kind: "packed_pg_account",
  decode: (value) => value,
});

describe(
  "packed PostgreSQL driver",
  { skip: DATABASE_URL === "" && "set DATABASE_URL to run" },
  () => {
    // Everything happens in a throwaway schema, so a shared database is safe.
    const schema = `river_packed_${randomBytes(6).toString("hex")}`;
    let pool;

    before(async () => {
      pool = new pg.Pool({ connectionString: DATABASE_URL, max: 4 });
      await pool.query(`CREATE SCHEMA ${schema}`);
      const result = await createMigrator({ pool, schema }).migrateUp();
      assert.ok(result.versions.length > 0);
    });

    after(async () => {
      await pool.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
      await pool.end();
    });

    it("works exact int64 args end to end", async () => {
      const workers = new Workers().add(accountJob, ({ job, recordOutput }) => {
        recordOutput({ accountId: job.args.accountId });
      });
      const client = new Client(new PgDriver(pool, { schema }), {
        queues: { default: { maxWorkers: 2 } },
        workers,
      });
      const { job } = await client.insert(accountJob, {
        accountId: exactJsonNumber(INT8_MAX.toString()),
      });
      const events = await workUntilFinalized(client, [job.id]);
      assert.equal(events.get(job.id)?.kind, "job_completed");
      const row = await client.jobs.get(job.id);
      assert.equal(row?.state, "completed");
      assert.equal(jsonNumberToBigInt(row.metadata.output.accountId), INT8_MAX);
      const { rows } = await pool.query(
        `SELECT args->>'accountId' AS account_id FROM ${schema}.river_job WHERE id = $1`,
        [job.id.toString()]
      );
      assert.equal(rows[0]?.account_id, INT8_MAX.toString());
    });

    it("keeps transactional inserts inside the caller's transaction", async () => {
      const client = new Client(new PgDriver(pool, { schema }));
      const connection = await pool.connect();
      let id;
      try {
        await connection.query("BEGIN");
        ({
          job: { id },
        } = await client.insert(
          accountJob,
          { accountId: 1 },
          { tx: connection }
        ));
        assert.equal(
          await client.jobs.get(id),
          null,
          "invisible before commit"
        );
        await connection.query("ROLLBACK");
      } finally {
        connection.release();
      }
      assert.equal(await client.jobs.get(id), null, "rolled back");
    });
  }
);
