import { describe, expect, it, vi } from "vitest";

import type { Client } from "./client.js";
import type { JobRow } from "./job.js";
import type { JsonObject } from "./json.js";
import { Resumable } from "./resumable.js";

describe("Resumable", () => {
  it.each([{}, { "river:resumable_step": "later" }])(
    "rejects duplicate names even among skipped steps: %j",
    async (metadata) => {
      const resumable = new Resumable(client(), job(metadata));
      const callback = vi.fn();
      await resumable.step("first", callback);
      await expect(resumable.stepWithCursor("first", callback)).rejects.toThrow(
        'duplicate resumable step name "first"'
      );
      expect(callback).toHaveBeenCalledTimes(
        metadata["river:resumable_step"] === undefined ? 1 : 0
      );
      expect(resumable.finish(false).error).not.toBeNull();
    }
  );

  it("treats an empty persisted step as no checkpoint", async () => {
    const resumable = new Resumable(
      client(),
      job({ "river:resumable_step": "" })
    );
    const callback = vi.fn();
    await resumable.step("first", callback);
    expect(callback).toHaveBeenCalledOnce();
    expect(resumable.finish(false).error).toBeNull();
  });

  it("restores the enclosing step after a nested step", async () => {
    const resumable = new Resumable(client(), job({}));
    await resumable
      .stepWithCursor("outer", async () => {
        await resumable.step("inner", () => undefined);
        resumable.setCursor({ offset: 7 });
        throw new Error("retry outer");
      })
      .catch(() => undefined);
    expect(resumable.finish(false).metadata).toEqual({
      "river:resumable_step": "inner",
      "river:resumable_cursor": { outer: { offset: 7 } },
    });
  });

  it("retains progress and the step's own error when it is caught", async () => {
    const resumable = new Resumable(client(), job({}));
    const cause = new Error("service unavailable");
    await resumable.step("first", () => undefined);
    await resumable
      .step("second", () => {
        throw cause;
      })
      .catch(() => undefined);
    const finished = resumable.finish(false);
    expect(finished.error).toBe(cause);
    expect(finished.metadata).toEqual({ "river:resumable_step": "first" });
  });

  it("rejects the malformed cursor arrays rejected by Go", () => {
    expect(
      () => new Resumable(client(), job({ "river:resumable_cursor": [1, 2] }))
    ).toThrow("river:resumable_cursor must be an object");
  });

  it("fails a successful worker that never declares its resume step", () => {
    const resumable = new Resumable(
      client(),
      job({ "river:resumable_step": "missing" })
    );

    expect(resumable.finish(false).error?.message).toContain(
      'resumable step "missing" not found in worker'
    );
  });

  it("resumes cursor steps and clears consumed cursor metadata", async () => {
    const resumable = new Resumable(
      client(),
      job({
        "river:resumable_cursor": { process: { offset: 2 } },
        "river:resumable_step": "process",
      })
    );
    const visited: string[] = [];

    await resumable.step("before", () => {
      visited.push("before");
    });
    await resumable.stepWithCursor("process", (cursor) => {
      expect(cursor).toEqual({ offset: 2 });
      visited.push("process");
    });
    await expect(
      resumable.step("after", () => {
        throw new Error("retry");
      })
    ).rejects.toThrow(new Error("retry"));

    expect(visited).toEqual(["process"]);
    expect(resumable.finish(true).metadata).toEqual({
      "river:resumable_cursor": null,
      "river:resumable_step": "process",
    });
  });

  it("persists an explicit checkpoint through the caller transaction", async () => {
    const update = vi.fn().mockResolvedValue(job({}));
    const resumable = new Resumable(client(update), job({}));
    const tx = { id: "transaction" };

    await resumable
      .stepWithCursor("process", async () => {
        await resumable.checkpoint({ cursor: { offset: 3 }, tx });
        throw new Error("retry after checkpoint");
      })
      .catch(() => undefined);

    expect(update).toHaveBeenCalledWith(
      1n,
      {
        metadata: {
          "river:resumable_cursor": { process: { offset: 3 } },
          "river:resumable_step": "process",
        },
      },
      { tx }
    );
    expect(resumable.finish(true).metadata).toEqual({
      "river:resumable_cursor": { process: { offset: 3 } },
      "river:resumable_step": "process",
    });
  });
});

function client(
  update: (...args: readonly unknown[]) => unknown = () => job({})
): Client {
  return { jobs: { update } } as unknown as Client;
}

function job(metadata: JsonObject): JobRow {
  const now = Temporal.Instant.from("2026-09-01T12:00:00Z");
  return {
    args: {},
    attempt: 1,
    attemptedAt: now,
    attemptedBy: ["test"],
    createdAt: now,
    errors: [],
    finalizedAt: null,
    id: 1n,
    kind: "test",
    maxAttempts: 25,
    metadata,
    priority: 1,
    queue: "default",
    scheduledAt: now,
    state: "running",
    tags: [],
    uniqueKey: null,
    uniqueStates: [],
  };
}
