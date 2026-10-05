import { AssertionError } from "node:assert";

import {
  defineJob,
  PayloadValidationError,
  snooze,
  Workers,
  type InsertClient,
} from "riverqueue";
import { describe, expect, expectTypeOf, test } from "vitest";
import { z } from "zod";

import {
  createTestClient,
  requireInserted,
  requireInsertedInDatabase,
  requireManyInserted,
  requireNotInserted,
  requireNotInsertedInDatabase,
  testJob,
  workOnce,
} from "./index.js";
import type { JobListingClient } from "./index.js";

const emailJob = defineJob({
  defaults: { maxAttempts: 7, priority: 3, queue: "testing" },
  kind: "test_email",
  schema: z.object({
    message: z.string().transform((message) => message.trim()),
  }),
});

describe("testJob", () => {
  test("validates input like the runtime and applies definition defaults", async () => {
    const now = Temporal.Instant.from("2026-08-30T20:00:00.123456789Z");
    const job = await testJob(
      emailJob,
      { message: " hello " },
      {
        attempt: 4,
        id: 9_007_199_254_740_993n,
        metadata: { source: "test" },
        now,
        state: "retryable",
      }
    );

    expectTypeOf(job.args).toEqualTypeOf<{ message: string }>();
    expect(job).toMatchObject({
      args: { message: "hello" },
      attempt: 4,
      attemptedAt: now,
      attemptedBy: ["riverqueue-test"],
      createdAt: now,
      id: 9_007_199_254_740_993n,
      kind: "test_email",
      maxAttempts: 7,
      metadata: { source: "test" },
      priority: 3,
      queue: "testing",
      rawArgs: { message: " hello " },
      state: "retryable",
    });
    await expect(
      testJob(emailJob, { message: 1 } as unknown as { message: string })
    ).rejects.toSatisfy(
      (error: unknown) =>
        error instanceof PayloadValidationError && error.phase === "work"
    );
  });
});

describe("workOnce", () => {
  test("persists metadata and suppressed resumable errors like the runtime", async () => {
    const job = await testJob(emailJob, { message: "resume" });
    let firstRuns = 0;
    const first = await workOnce(job, async ({ resumable, setMetadata }) => {
      setMetadata("application", true);
      await resumable.step("first", () => {
        firstRuns++;
      });
      await resumable
        .step("second", () => {
          throw new Error("retry");
        })
        .catch(() => undefined);
    });
    expect(first.status).toBe("failed");
    expect(first.metadata).toEqual({
      application: true,
      "river:resumable_step": "first",
    });
    const second = await workOnce(
      { ...job, attempt: 2, metadata: first.metadata },
      async ({ resumable }) => {
        await resumable.step("first", () => {
          firstRuns++;
        });
        await resumable.step("second", () => undefined);
      }
    );
    expect(second.status).toBe("succeeded");
    expect(firstRuns).toBe(1);
  });

  test("captures output, logs, outcomes, and errors", async () => {
    const job = await testJob(emailJob, { message: "work once" });
    const worked = await workOnce(job, ({ job, logger, recordOutput }) => {
      logger.info({ id: job.id.toString(10) }, "working");
      recordOutput({ delivered: true });
      return snooze({ seconds: 30 });
    });

    expect(worked).toMatchObject({
      logs: [
        {
          attributes: { id: "1" },
          level: "info",
          message: "working",
        },
      ],
      outcome: {
        duration: Temporal.Duration.from({ seconds: 30 }),
        type: "snooze",
      },
      output: { delivered: true },
      status: "succeeded",
    });

    const error = new Error("worker failed");
    const failed = await workOnce(job, () => {
      throw error;
    });
    expect(failed).toMatchObject({ error, status: "failed" });
  });

  test("runs the handler registered in a Workers bundle with its timeout", async () => {
    const workers = new Workers().add(
      emailJob,
      async ({ job, signal }) => {
        await new Promise((resolve) => setTimeout(resolve, 50));
        signal.throwIfAborted();
        return job.args.message.length > 0 ? undefined : undefined;
      },
      { timeout: { milliseconds: 1 } }
    );
    const job = await testJob(emailJob, { message: "timeout" });

    const worked = await workOnce(job, workers);

    expect(worked.status).toBe("failed");
    await expect(
      workOnce({ ...job, kind: "unknown" }, workers)
    ).rejects.toBeInstanceOf(AssertionError);
  });

  test("records insertions from ctx.client and makes transactions explicit", async () => {
    const job = await testJob(emailJob, { message: "steps" });
    const followUp = defineJob({ kind: "follow_up" });
    const worked = await workOnce(
      job,
      async ({ client, completeTx, resumable }) => {
        await resumable.step("first", () => undefined);
        await client.insert(followUp, { from: job.id.toString(10) });
        await expect(completeTx({})).rejects.toThrow(
          'does not support "transactional completion"'
        );
      }
    );

    expect(worked.status).toBe("succeeded");
  });
});

describe("createTestClient", () => {
  test("records exact deterministic insertions and transactions", async () => {
    interface ApplicationTransaction {
      readonly name: string;
    }
    const now = Temporal.Instant.from("2026-08-30T21:00:00.123456789Z");
    const testClient = createTestClient<ApplicationTransaction>({
      now: () => now,
      startingId: 100n,
    });
    expectTypeOf(testClient.client).toEqualTypeOf<
      InsertClient<ApplicationTransaction>
    >();
    const tx = { name: "application transaction" };
    const result = await testClient.client.insert(
      emailJob,
      { message: "inserted" },
      { tx }
    );

    expect(result.job).toMatchObject({ createdAt: now, id: 100n });
    expect(testClient.insertions).toHaveLength(1);
    expect(testClient.insertions[0]).toMatchObject({
      job: { args: { message: "inserted" }, kind: "test_email" },
      transaction: tx,
    });
  });

  test("asserts on insertions like rivertest", async () => {
    const other = defineJob({ kind: "other_job" });
    const { client, insertions } = createTestClient();
    await client.insert(emailJob, { message: "one" }, { priority: 1 });
    await client.insertMany([
      { args: { n: 1 }, job: other },
      { args: { message: "two" }, job: emailJob },
    ]);

    const inserted = requireInserted(insertions, emailJob, {
      args: { message: "one" },
      priority: 1,
    });
    expectTypeOf(inserted.args).toEqualTypeOf<{ message: string }>();
    expect(inserted.queue).toBe("testing");
    expect(() => requireInserted(insertions, emailJob)).toThrow(AssertionError);
    requireNotInserted(insertions, emailJob, { args: { message: "three" } });
    expect(() => requireNotInserted(insertions, other)).toThrow(AssertionError);
    expect(
      requireManyInserted(insertions, [
        { job: emailJob, queue: "testing" },
        { args: { n: 1 }, job: other },
        { job: emailJob },
      ])
    ).toHaveLength(3);
    expect(() =>
      requireManyInserted(insertions, [{ job: emailJob }, { job: other }])
    ).toThrow(AssertionError);
  });

  test("runs insert options, hooks, middleware, and plugins", async () => {
    const seen: string[] = [];
    const { client, insertions } = createTestClient({
      defaultInsertOptions: { tags: ["default"] },
      hooks: {
        beforeInsert: ({ operation }) => {
          seen.push(`hook:${operation}`);
        },
      },
      insertMiddleware: [
        async ({ requests }, next) => {
          seen.push(`middleware:${requests.length}`);
          return next();
        },
      ],
      plugins: [{ hooks: {}, name: "noop" }],
    });

    await client.insert(emailJob, { message: "one" });

    expect(seen).toEqual(["middleware:1", "hook:insert"]);
    expect(requireInserted(insertions, emailJob).tags).toEqual(["default"]);
  });
});

describe("requireInsertedInDatabase", () => {
  test("pages through persisted jobs, optionally inside a transaction", async () => {
    const tx = { transaction: true };
    const listed: unknown[] = [];
    const rows = await Promise.all(
      Array.from({ length: 1_500 }, (_, index) =>
        testJob(
          emailJob,
          { message: index === 1_200 ? "wanted" : "other" },
          { id: BigInt(index + 1), state: "available" }
        )
      )
    );
    const client = {
      jobs: {
        list: (options: { after?: string; limit?: number; tx?: unknown }) => {
          listed.push(options);
          const start = options.after === undefined ? 0 : Number(options.after);
          const end = start + (options.limit ?? 100);
          return Promise.resolve({
            jobs: rows.slice(start, end),
            nextCursor: end < rows.length ? String(end) : null,
          });
        },
      },
    } as unknown as JobListingClient<typeof tx>;

    const found = await requireInsertedInDatabase(
      client,
      emailJob,
      { args: { message: "wanted" } },
      { tx }
    );

    expect(found.id).toBe(1_201n);
    expectTypeOf(found.args).toEqualTypeOf<{ message: string }>();
    expect(listed).toEqual([
      { kinds: ["test_email"], limit: 1_000, tx },
      { after: "1000", kinds: ["test_email"], limit: 1_000, tx },
    ]);
    await requireNotInsertedInDatabase(client, emailJob, {
      args: { message: "missing" },
    });
    await expect(requireInsertedInDatabase(client, emailJob)).rejects.toThrow(
      AssertionError
    );
    await expect(
      requireNotInsertedInDatabase(client, emailJob, {
        args: { message: "wanted" },
      })
    ).rejects.toThrow(AssertionError);
  });
});
