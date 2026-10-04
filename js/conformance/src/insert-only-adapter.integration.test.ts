import pg from "pg";
import { afterAll, beforeAll, describe, expect, it } from "vitest";

import { ADAPTER_ERROR_CODE } from "./errors.js";
import { InsertOnlyConformanceAdapter } from "./insert-only-adapter.js";
import { POSTGRES_CONFORMANCE_APPLICATION_NAME } from "./pg-adapter.js";
import { loadInsertOnlyProfile } from "./profile.js";

const DATABASE_URL =
  process.env.TEST_DATABASE_URL ?? process.env.RIVER_CONFORMANCE_DATABASE_URL;

describe.skipIf(DATABASE_URL === undefined)(
  "InsertOnlyConformanceAdapter integration",
  () => {
    let adapter: InsertOnlyConformanceAdapter;
    let pool: pg.Pool;

    beforeAll(async () => {
      pool = new pg.Pool({
        application_name: POSTGRES_CONFORMANCE_APPLICATION_NAME,
        connectionString: DATABASE_URL,
        max: 4,
      });
      adapter = new InsertOnlyConformanceAdapter(
        pool,
        await loadInsertOnlyProfile()
      );
    });

    afterAll(async () => {
      await adapter.close();
      await pool.query(
        "DELETE FROM river_job WHERE 'conformance_insert_only' = ANY(tags)"
      );
      await pool.end();
    });

    it("advertises only the insert-only profile", async () => {
      await expect(adapter.dispatch("handshake", {})).resolves.toMatchObject({
        backend: "postgres",
        profile: "insert-only-v1",
      });
      await expect(adapter.dispatch("get", { id: 1 })).rejects.toMatchObject({
        code: ADAPTER_ERROR_CODE.methodNotFound,
      });
      await expect(
        adapter.dispatch("insert_many", { jobs: [] })
      ).rejects.toMatchObject({ code: ADAPTER_ERROR_CODE.rejected });
    });

    it("inserts through the Prisma driver inside and outside transactions", async () => {
      const tags = ["conformance_insert_only"];
      const inserted = (await adapter.dispatch("insert", {
        message: "insert-only",
        opts: { priority: 2, tags },
      })) as Record<string, unknown>;
      expect(inserted).toMatchObject({ priority: 2, state: "available", tags });

      await adapter.dispatch("tx_begin", { handle: "rolled-back" });
      const rolledBack = (await adapter.dispatch("tx_insert", {
        handle: "rolled-back",
        job: { message: "rolled back", opts: { tags } },
      })) as Record<string, unknown>;
      await adapter.dispatch("tx_rollback", { handle: "rolled-back" });

      await adapter.dispatch("tx_begin", { handle: "committed" });
      const batch = (await adapter.dispatch("tx_insert_many", {
        handle: "committed",
        jobs: [{ message: "committed", opts: { tags } }],
      })) as { results: readonly { job: Record<string, unknown> }[] };
      await adapter.dispatch("tx_commit", { handle: "committed" });

      const ids = await pool.query<{ id: string }>(
        "SELECT id::text FROM river_job WHERE 'conformance_insert_only' = ANY(tags) ORDER BY id"
      );
      expect(ids.rows.map(({ id }) => BigInt(id))).toEqual([
        inserted.id,
        batch.results[0]?.job.id,
      ]);
      expect(ids.rows.map(({ id }) => BigInt(id))).not.toContain(rolledBack.id);
      await expect(
        adapter.dispatch("tx_commit", { handle: "committed" })
      ).rejects.toMatchObject({ code: ADAPTER_ERROR_CODE.notFound });
    });
  }
);
