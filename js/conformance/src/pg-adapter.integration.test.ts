import { Buffer } from "node:buffer";

import pg from "pg";
import { afterAll, beforeAll, beforeEach, describe, expect, it } from "vitest";

import { ADAPTER_ERROR_CODE, adapterErrorCode } from "./errors.js";
import {
  POSTGRES_CONFORMANCE_APPLICATION_NAME,
  PostgresConformanceAdapter,
} from "./pg-adapter.js";
import { loadPostgresFullProfile } from "./profile.js";

const DATABASE_URL =
  process.env.TEST_DATABASE_URL ?? process.env.RIVER_CONFORMANCE_DATABASE_URL;

describe.skipIf(DATABASE_URL === undefined)(
  "PostgresConformanceAdapter integration",
  () => {
    let adapter: PostgresConformanceAdapter;
    let pool: pg.Pool;

    beforeAll(async () => {
      pool = new pg.Pool({
        application_name: POSTGRES_CONFORMANCE_APPLICATION_NAME,
        connectionString: DATABASE_URL,
        max: 12,
      });
      adapter = new PostgresConformanceAdapter(
        pool,
        await loadPostgresFullProfile()
      );
      await adapter.dispatch("migrate", {});
    });

    beforeEach(async () => {
      await adapter.close();
      await adapter.dispatch("reset", {});
    });

    afterAll(async () => {
      await adapter.close();
      await pool.end();
    });

    it("uses public job operations with exact cursors and transaction ownership", async () => {
      const inserted = asRecord(
        await adapter.dispatch("insert", {
          message: "storage",
          opts: {
            metadata: { account: { id: 42 }, keep: true },
            priority: 3,
            scheduled_at: "2099-01-02T03:04:05.123456Z",
            tags: ["conformance_storage"],
          },
        })
      );
      const id = inserted.id as bigint;
      expect(typeof id).toBe("bigint");

      const page = asRecord(
        await adapter.dispatch("list", {
          limit: 1,
          metadata: { account: { id: 42 } },
          order_by: "scheduled_at",
          states: ["scheduled"],
        })
      );
      expect(asRecords(page.jobs)).toEqual([inserted]);
      expect(typeof page.cursor).toBe("string");

      const updated = asRecord(
        await adapter.dispatch("update", {
          id,
          output: { result: "recorded" },
        })
      );
      expect(updated.metadata).toEqual({
        account: { id: 42 },
        keep: true,
        output: { result: "recorded" },
      });

      await adapter.dispatch("tx_begin", { handle: "owned" });
      const transactional = asRecord(
        await adapter.dispatch("tx_insert", {
          handle: "owned",
          job: { message: "transactional" },
        })
      );
      await expect(
        adapter.dispatch("get", { id: transactional.id })
      ).rejects.toThrow("not found");
      expect(
        await adapter.dispatch("tx_get", {
          handle: "owned",
          id: transactional.id,
        })
      ).toEqual(transactional);
      await adapter.dispatch("tx_commit", { handle: "owned" });
      expect(await adapter.dispatch("get", { id: transactional.id })).toEqual(
        transactional
      );

      const cancelled = asRecord(await adapter.dispatch("cancel", { id }));
      expect(cancelled.state).toBe("cancelled");
      const retried = asRecord(await adapter.dispatch("retry", { id }));
      expect(retried.state).toBe("available");
      expect(await adapter.dispatch("delete", { id })).toEqual(retried);
    });

    it("stores raw kinds as given and lists them with Go cursor text", async () => {
      const kind = "conformance_cursor<>&~~~";
      const inserted = asRecord(
        await adapter.dispatch("raw_insert_no_notify", {
          kind,
          message: "raw",
        })
      );
      expect(inserted.kind).toBe(kind);

      const page = asRecord(
        await adapter.dispatch("list", { kinds: [kind], limit: 1 })
      );
      expect(asRecords(page.jobs)).toEqual([inserted]);
      const cursor = String(page.cursor);
      expect(cursor).toMatch(/-/);
      expect(Buffer.from(cursor, "base64url").toString("utf8")).toContain(
        '"kind":"conformance_cursor\\u003c\\u003e\\u0026~~~"'
      );
    });

    it("normalizes full rows and custom-schema storage", async () => {
      const full = asRecord(await adapter.dispatch("raw_insert_full_row", {}));
      expect(full.id).toBe(1n);
      expect(full.errors).toEqual([
        {
          at: "2026-01-02T03:04:06.123456Z",
          attempt: 3,
          error: 'worker failed: escaped "detail"',
          trace: "frame one\nframe two",
        },
      ]);
      expect(full.attempted_by).toEqual(["go-client", "candidate-client"]);
      expect(full.unique_key).toBe("ab".repeat(32));

      const exact = asRecord(
        await adapter.dispatch("raw_insert_exact_json", {})
      );
      await expect(
        adapter.dispatch("raw_job_exact_json", { id: exact.id })
      ).resolves.toEqual({
        decimal: "0.12345678901234567890123456789",
        integer: "9223372036854775807",
        negative: "-9223372036854775808",
      });
      const raw = asRecord(
        await adapter.dispatch("raw_job_row", { id: exact.id })
      );
      // PostgreSQL renders jsonb in its own canonical text, and both times
      // come from the same `now()` default.
      expect(raw).toEqual({
        args: '{"decimal": 0.12345678901234567890123456789, "integer": 9223372036854775807}',
        attempted_at: null,
        attempted_by: null,
        created_at: expect.stringMatching(
          /^\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}(\.\d{1,6})?[+-]\d{2}(:\d{2})?$/
        ),
        errors: null,
        finalized_at: null,
        jsonb: null,
        metadata: '{"negative": -9223372036854775808}',
        scheduled_at: raw.created_at,
        tags: "{}",
        unique_key: null,
        unique_key_type: null,
        unique_states: null,
        unique_states_type: null,
      });

      const schema = "javascript_conformance_custom";
      // Drop the schema even when an assertion fails, so a failed run can't
      // break the next one.
      const dropSchema = () =>
        pool.query(`DROP SCHEMA IF EXISTS "${schema}" CASCADE`);
      await dropSchema();
      try {
        await adapter.dispatch("migrate", { direction: "up", schema });
        await adapter.dispatch("reset", { schema });
        const inserted = await adapter.dispatch("insert", {
          message: "custom schema",
          schema,
        });
        expect(
          await adapter.dispatch("get", {
            id: asRecord(inserted).id,
            schema,
          })
        ).toEqual(inserted);
        await adapter.dispatch("migrate", {
          direction: "down",
          schema,
          target_version: -1,
        });
      } finally {
        await dropSchema();
      }
      // Migrating a schema up and down takes several seconds under load.
    }, 30_000);

    it("retains huge metadata numbers through PostgreSQL read and update", async () => {
      const inserted = asRecord(
        await adapter.dispatch("raw_insert_exact_json", {
          metadata_json:
            '{"negative":-9223372036854775808,"big_integer":123456789012345678901234567890,"beyond_float":1e400,"long_decimal":0.1000000000000000055511151231257827}',
        })
      );
      const expected = {
        beyond_float: `1${"0".repeat(400)}`,
        big_integer: "123456789012345678901234567890",
        decimal: "0.12345678901234567890123456789",
        integer: "9223372036854775807",
        long_decimal: "0.1000000000000000055511151231257827",
        negative: "-9223372036854775808",
      };
      await expect(
        adapter.dispatch("raw_job_exact_json", { id: inserted.id })
      ).resolves.toEqual(expected);
      await adapter.dispatch("update", {
        id: inserted.id,
        output: { updated: true },
      });
      await expect(
        adapter.dispatch("raw_job_exact_json", { id: inserted.id })
      ).resolves.toEqual(expected);
    });

    it("works cancellation and hooks through the public runtime", async () => {
      await adapter.dispatch("start", {
        client_id: "javascript-conformance-runtime",
        instrumented: true,
        max_workers: 2,
        retry_delay_ms: 5,
      });
      try {
        const ordinary = asRecord(
          await adapter.dispatch("insert", { message: "ordinary" })
        );
        expect(
          asRecord(
            await adapter.dispatch("wait", {
              id: ordinary.id,
            })
          ).state
        ).toBe("completed");

        const remotelyCancelled = asRecord(
          await adapter.dispatch("insert", {
            behavior: "cooperative_cancel",
            message: "remote cancellation",
          })
        );
        await adapter.dispatch("wait", {
          id: remotelyCancelled.id,
          states: ["running"],
        });
        await adapter.dispatch("cancel", { id: remotelyCancelled.id });
        const cancelledRemotely = asRecord(
          await adapter.dispatch("wait", { id: remotelyCancelled.id })
        );
        expect(cancelledRemotely.state).toBe("cancelled");
        expect(cancelledRemotely.errors).toEqual([
          expect.objectContaining({
            attempt: 1,
            error: "JobCancelError: job cancelled remotely",
          }),
        ]);

        const cancelled = asRecord(
          await adapter.dispatch("insert", {
            behavior: "cancel",
            message: "cancel from worker",
          })
        );
        expect(
          asRecord(
            await adapter.dispatch("wait", {
              id: cancelled.id,
            })
          ).state
        ).toBe("cancelled");

        const transactional = asRecord(
          await adapter.dispatch("insert", {
            behavior: "transactional_complete",
            message: "transactional completion",
          })
        );
        const transactionallyCompleted = asRecord(
          await adapter.dispatch("wait", { id: transactional.id })
        );
        expect(transactionallyCompleted.state).toBe("completed");
        expect(asRecord(transactionallyCompleted.metadata)).toMatchObject({
          transactional_completion: true,
        });

        const resumable = asRecord(
          await adapter.dispatch("insert", {
            behavior: "resumable",
            message: "resume once",
            opts: { max_attempts: 2 },
          })
        );
        const worked = asRecord(
          await adapter.dispatch("wait", { id: resumable.id })
        );
        expect(worked.state).toBe("completed");
        expect(worked.errors).toHaveLength(1);
        expect(asRecord(worked.metadata)["river:resumable_step"]).toBe("first");

        await adapter.dispatch("queue_add", {
          max_workers: 1,
          name: "dynamic",
        });
        const dynamic = asRecord(
          await adapter.dispatch("insert", {
            message: "dynamic queue",
            opts: { queue: "dynamic" },
          })
        );
        expect(
          asRecord(await adapter.dispatch("wait", { id: dynamic.id })).state
        ).toBe("completed");
        await adapter.dispatch("queue_add", {
          max_workers: 2,
          name: "dynamic",
        });
        await adapter.dispatch("queue_remove", { name: "dynamic" });

        const stats = asRecord(await adapter.dispatch("runtime_stats", {}));
        expect(stats.resumable_first_runs).toBe(1);
        expect(stats.resumable_second_runs).toBe(2);
        expect(stats.events).toEqual(
          expect.arrayContaining([
            "job_cancelled",
            "job_completed",
            "job_failed",
          ])
        );
        expect(stats.trace).toEqual(
          expect.arrayContaining([
            "hook:insert_begin",
            "hook:work_begin",
            "middleware:insert_before",
            "middleware:insert_after",
            "middleware:work_before",
            "middleware:work_after",
            "hook:work_end",
          ])
        );
      } finally {
        await adapter.dispatch("stop", { cancel: true });
      }
    });

    it("runs instrumented periodic jobs through the public registry", async () => {
      await adapter.dispatch("start", {
        client_id: "javascript-conformance-periodic",
        instrumented: true,
        max_workers: 1,
        periodic_run_on_start: true,
      });
      try {
        const deadline = Date.now() + 5_000;
        let periodic: Record<string, unknown> | undefined;
        while (Date.now() < deadline) {
          const listed = asRecord(
            await adapter.dispatch("list", {
              metadata: { "river:periodic_job_id": "conformance-periodic" },
            })
          );
          periodic = asRecords(listed.jobs)[0];
          if (periodic !== undefined) break;
          await new Promise((resolve) => setTimeout(resolve, 10));
        }
        expect(periodic).toBeDefined();
        const worked = asRecord(
          await adapter.dispatch("wait", { id: periodic!.id })
        );
        expect(worked.state).toBe("completed");
        expect(asRecord(worked.metadata)).toMatchObject({
          periodic: true,
          "river:periodic_job_id": "conformance-periodic",
        });
        const stats = asRecord(await adapter.dispatch("runtime_stats", {}));
        expect(stats.periodic_starts).toBe(1);
        expect(stats.trace).toEqual(
          expect.arrayContaining(["hook:periodic_start"])
        );
      } finally {
        await adapter.dispatch("stop", { cancel: true });
      }
    });

    it("keeps a client started with leader election disabled from leading", async () => {
      const startError = (params: Record<string, unknown>) =>
        adapter.dispatch("start", params).then(
          () => expect.unreachable("start succeeded"),
          (error: unknown) => adapterErrorCode(error)
        );
      expect(
        await startError({
          client_id: "javascript-conformance-no-election",
          leader_election_disabled: true,
          periodic_run_on_start: true,
        })
      ).toBe(ADAPTER_ERROR_CODE.rejected);
      expect(await startError({ leader_election_disabled: "yes" })).toBe(
        ADAPTER_ERROR_CODE.invalidParams
      );

      // A short election interval would make a client that still took part
      // in elections lead almost at once.
      await adapter.dispatch("start", {
        client_id: "javascript-conformance-no-election",
        elect_interval_ms: 10,
        instrumented: true,
        leader_election_disabled: true,
        max_workers: 1,
      });
      try {
        const inserted = asRecord(
          await adapter.dispatch("insert", { message: "no election" })
        );
        const worked = asRecord(
          await adapter.dispatch("wait", { id: inserted.id })
        );
        expect(worked.state).toBe("completed");
        expect(worked.attempted_by).toEqual([
          "javascript-conformance-no-election",
        ]);
        await new Promise((resolve) => setTimeout(resolve, 100));
        expect(await adapter.dispatch("leader", {})).toEqual({
          elected_at: null,
          leader_id: null,
        });
        const stats = asRecord(await adapter.dispatch("runtime_stats", {}));
        expect(stats.periodic_starts).toBe(0);
      } finally {
        await adapter.dispatch("stop", { cancel: true });
      }
    });

    it("resigns from insert-only and transactional clients on commit", async () => {
      const requester = new PostgresConformanceAdapter(
        pool,
        await loadPostgresFullProfile()
      );
      // Like the harness, shorten River's default five-second election
      // interval so each new term is observed promptly.
      await adapter.dispatch("start", {
        client_id: "javascript-conformance-resign",
        elect_interval_ms: 10,
        max_workers: 1,
      });
      try {
        const first = await waitForLeaderTerm(adapter);
        await requester.dispatch("request_resign", {});
        const second = await waitForLeaderTerm(adapter, first.elected_at);
        expect(second.leader_id).toBe("javascript-conformance-resign");
        expect(second.elected_at).not.toBe(first.elected_at);

        await requester.dispatch("tx_begin", { handle: "rollback-resign" });
        await requester.dispatch("request_resign", {
          handle: "rollback-resign",
        });
        await requester.dispatch("tx_rollback", { handle: "rollback-resign" });
        await new Promise((resolve) => setTimeout(resolve, 100));
        expect((await adapter.dispatch("leader", {})) as object).toMatchObject({
          elected_at: second.elected_at,
        });

        await requester.dispatch("tx_begin", { handle: "commit-resign" });
        await requester.dispatch("request_resign", {
          handle: "commit-resign",
        });
        await requester.dispatch("tx_commit", { handle: "commit-resign" });
        const third = await waitForLeaderTerm(adapter, second.elected_at);
        expect(third.leader_id).toBe("javascript-conformance-resign");
      } finally {
        await requester.close();
        await adapter.dispatch("stop", { cancel: true });
      }
    });

    it("hard-aborts an ignored cancellation through worker threads", async () => {
      await adapter.dispatch("start", {
        client_id: "javascript-conformance-hard-abort",
        job_stuck_threshold_ms: 100,
        max_workers: 1,
        queue: "ignored",
      });
      const inserted = asRecord(
        await adapter.dispatch("insert", {
          behavior: "ignored_cancel",
          message: "ignored cancellation",
          opts: { queue: "ignored" },
        })
      );
      await adapter.dispatch("wait", {
        id: inserted.id,
        states: ["running"],
      });

      await adapter.dispatch("stop", { cancel: true });

      // The thread ignored its abort through the stuck threshold and was
      // terminated, so the attempt counts and failed.
      const job = asRecord(await adapter.dispatch("get", { id: inserted.id }));
      expect(job).toMatchObject({
        attempt: 1,
        errors: [
          { attempt: 1, error: "job aborted after ignoring cancellation" },
        ],
      });
      expect(["available", "retryable"]).toContain(job.state);
    });

    it("routes persisted queue operations through the public client", async () => {
      await pool.query(
        `INSERT INTO river_queue (name, metadata)
         VALUES ($1::text, $2::jsonb)`,
        ["adapter_queue", JSON.stringify({ owner: "test" })]
      );
      const queue = asRecord(
        await adapter.dispatch("queue_get", { name: "adapter_queue" })
      );
      expect(queue.metadata).toEqual({ owner: "test" });
      const updated = asRecord(
        await adapter.dispatch("queue_update", {
          metadata: { owner: "javascript" },
          name: "adapter_queue",
        })
      );
      expect(updated.metadata).toEqual({ owner: "javascript" });
      await adapter.dispatch("queue_pause", { name: "adapter_queue" });
      expect(
        asRecord(await adapter.dispatch("queue_get", { name: "adapter_queue" }))
          .paused_at
      ).not.toBeNull();
      await adapter.dispatch("queue_resume", { name: "adapter_queue" });
      expect(
        asRecord(await adapter.dispatch("queue_get", { name: "adapter_queue" }))
          .paused_at
      ).toBeNull();
      expect(
        asRecords(asRecord(await adapter.dispatch("queue_list", {})).queues)
      ).toHaveLength(1);
    });
  }
);

function asRecord(value: unknown): Record<string, unknown> {
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    throw new TypeError("expected an object");
  }
  return value as Record<string, unknown>;
}

function asRecords(value: unknown): readonly Record<string, unknown>[] {
  if (!Array.isArray(value)) throw new TypeError("expected an array");
  return value.map(asRecord);
}

async function waitForLeaderTerm(
  adapter: PostgresConformanceAdapter,
  prior: unknown = null
): Promise<Record<string, unknown>> {
  const deadline = Date.now() + 5_000;
  while (Date.now() < deadline) {
    const leader = asRecord(await adapter.dispatch("leader", {}));
    if (leader.elected_at !== null && leader.elected_at !== prior)
      return leader;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(`leadership term did not change from ${String(prior)}`);
}
