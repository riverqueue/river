import { readdir, readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";
import { Client, defineJob, Workers, type JsonObject } from "riverqueue";

import { PgDriver } from "./driver.js";

describe("nested transactional cursor checkpoints", () => {
  const schema = `js_nested_${Math.random().toString(36).slice(2, 10)}`;
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

  it.each([
    { commit: true, name: "committed" },
    { commit: false, name: "rolled_back" },
  ])(
    "preserves only durable parent progress after the parent transaction is $name",
    async ({ commit, name }) => {
      const definition = defineJob({ kind: name });
      let afterTransaction: JsonObject | undefined;
      const client = new Client(new PgDriver(pool, { schema }), {
        queues: { [name]: { maxWorkers: 1 } },
        workers: new Workers().add(definition, async ({ job, resumable }) => {
          await resumable.stepWithCursor("parent", async () => {
            const tx = await pool.connect();
            try {
              await tx.query("BEGIN");
              await resumable.checkpoint({ cursor: 1, tx });
              await resumable.stepWithCursor("child", () => {
                resumable.setCursor(7);
                throw new Error("child interrupted");
              });
            } finally {
              await tx.query(commit ? "COMMIT" : "ROLLBACK");
              tx.release();
              afterTransaction = (await client.jobs.get(job.id))?.metadata;
            }
          });
        }),
      });
      const inserted = await client.insert(
        definition,
        {},
        { maxAttempts: 1, queue: name }
      );
      const runtime = await client.start();
      try {
        let row = await client.jobs.get(inserted.job.id);
        await expect
          .poll(async () => {
            row = await client.jobs.get(inserted.job.id);
            return row?.state;
          })
          .toBe("discarded");
        if (!commit)
          expect(afterTransaction?.["river:resumable_cursor"]).toBeUndefined();
        const metadata = Object.fromEntries(
          Object.entries(row?.metadata ?? {}).filter(([key]) =>
            key.startsWith("river:resumable_")
          )
        );
        expect(metadata).toEqual(
          commit
            ? {
                "river:resumable_step": "parent",
                "river:resumable_cursor": { parent: 1, child: 7 },
              }
            : {
                "river:resumable_cursor": { child: 7 },
              }
        );
      } finally {
        await runtime.stop();
      }
    }
  );
});
