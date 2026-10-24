import { describe, expect, it } from "vitest";

import type { Client } from "./client.js";
import type { JsonObject } from "./json.js";
import type { JobRow } from "./job.js";
import { Resumable } from "./resumable.js";

// These draft assertions assume nesting remains supported. They fail until
// nested resume points are made reachable or the API rejects nesting upfront.
describe("nested resumable resume points", () => {
  it.each(["cursor", "plain"])(
    "reaches a completed %s child after its parent fails",
    async (kind) => {
      const calls: string[] = [];
      const run = (resumable: Resumable, fail: boolean) =>
        resumable.step("parent", async () => {
          calls.push("parent");
          const child = () => {
            calls.push("child");
          };
          if (kind === "cursor") await resumable.stepWithCursor("child", child);
          else await resumable.step("child", child);
          if (fail) throw new Error("parent interrupted after child completed");
        });
      const first = new Resumable({} as Client, job({}));
      await expect(run(first, true)).rejects.toThrow("parent interrupted");
      const retry = new Resumable(
        {} as Client,
        job(first.finish(true).metadata)
      );
      await run(retry, false);
      expect(retry.finish(false).error).toBeNull();
      expect(calls).toEqual(["parent", "child", "parent"]);
    }
  );
});

function job(metadata: JsonObject): JobRow {
  const now = Temporal.Instant.from("2026-10-24T12:00:00Z");
  return {
    args: {},
    attempt: 1,
    attemptedAt: now,
    attemptedBy: ["test"],
    createdAt: now,
    errors: [],
    finalizedAt: null,
    id: 1n,
    kind: "nested",
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
