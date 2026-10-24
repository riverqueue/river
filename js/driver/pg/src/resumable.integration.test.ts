import { readdir, readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import {
  Client,
  defineJob,
  snooze,
  Workers,
  type JsonObject,
} from "riverqueue";

import { PgDriver } from "./driver.js";

const fixtures = JSON.parse(
  await readFile(
    new URL("../../../../conformance/testdata/resumable.json", import.meta.url),
    "utf8"
  )
) as {
  cases: { name: string; expected_metadata: JsonObject }[];
};

describe("Go resumable conformance", () => {
  const schema = `js_resumable_${Math.random().toString(36).slice(2, 10)}`;
  const pool = new pg.Pool({
    connectionString:
      process.env.TEST_DATABASE_URL ??
      "postgres://localhost:5432/river_test?sslmode=disable",
  });

  beforeAll(async () => {
    await pool.query(`CREATE SCHEMA "${schema}"`);
    const directory = fileURLToPath(
      new URL("../../../migrate/migrations/postgres/main/", import.meta.url)
    );
    for (const file of (await readdir(directory))
      .filter((name) => name.endsWith(".up.sql"))
      .sort()) {
      const sql = await readFile(`${directory}/${file}`, "utf8");
      await pool.query(
        sql.replaceAll("/* TEMPLATE: schema */", `"${schema}".`)
      );
    }
  });

  afterAll(async () => {
    await pool.query(`DROP SCHEMA "${schema}" CASCADE`);
    await pool.end();
  });

  it.each(fixtures.cases)("$name", async (fixture) => {
    const definition = defineJob({ kind: fixture.name });
    const queue = fixture.name;
    const client = new Client(new PgDriver(pool, { schema }), {
      queues: { [queue]: { maxWorkers: 1 } },
      workers: new Workers().add(definition, async ({ resumable }) => {
        if (!fixture.name.startsWith("first_cursor")) {
          await resumable.step("first", () => undefined);
        }
        if (fixture.name === "step_snooze") return snooze({ seconds: 60 });
        await resumable.stepWithCursor("loop", async () => {
          if (fixture.name.startsWith("first_cursor")) {
            resumable.setCursor(7);
          } else {
            const tx = await pool.connect();
            try {
              await tx.query("BEGIN");
              if (fixture.name === "checkpoint_write_failure") {
                await tx.query("SELECT 1 / 0").catch(() => undefined);
              }
              await resumable.checkpoint({ cursor: 1, tx });
              if (
                fixture.name === "checkpoint_commit_error" ||
                fixture.name === "checkpoint_then_cursor_error"
              )
                await tx.query("COMMIT");
            } finally {
              await tx.query("ROLLBACK");
              tx.release();
            }
          }
          if (fixture.name === "checkpoint_then_cursor_error")
            resumable.setCursor(2);
          throw new Error("retry");
        });
        return snooze({ seconds: 60 });
      }),
    });
    const inserted = await client.insert(
      definition,
      {},
      { maxAttempts: 1, queue }
    );
    const runtime = await client.start();
    try {
      let row = await client.jobs.get(inserted.job.id);
      await expect
        .poll(async () => {
          row = await client.jobs.get(inserted.job.id);
          return row?.state;
        })
        .toBe(fixture.name.endsWith("snooze") ? "scheduled" : "discarded");
      const metadata = Object.fromEntries(
        Object.entries(row?.metadata ?? {}).filter(([key]) =>
          key.startsWith("river:resumable_")
        )
      );
      expect(metadata).toEqual(fixture.expected_metadata);
    } finally {
      await runtime.stop();
    }
  });
});
