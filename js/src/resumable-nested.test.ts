import { describe, expect, it, vi } from "vitest";

import type { Client } from "./client.js";
import type { JsonObject } from "./json.js";
import type { JobRow } from "./job.js";
import { Resumable } from "./resumable.js";

describe("nested resumable steps", () => {
  it.each([
    { parent: "step", child: "step" },
    { parent: "step", child: "stepWithCursor" },
    { parent: "stepWithCursor", child: "step" },
    { parent: "stepWithCursor", child: "stepWithCursor" },
  ] as const)(
    "rejects $child inside $parent before recording progress",
    async ({ parent, child }) => {
      const resumable = new Resumable({} as Client, job({}));
      const childCallback = vi.fn();
      await expect(
        resumable[parent]("parent", () =>
          resumable[child]("child", childCallback)
        )
      ).rejects.toThrow("resumable steps cannot be nested");
      expect(childCallback).not.toHaveBeenCalled();
      expect(resumable.finish(false).error?.message).toBe(
        "resumable steps cannot be nested"
      );
      expect(resumable.finish(false).metadata).toEqual({});
      expect(() => resumable.setCursor(1)).toThrow("inside stepWithCursor");
    }
  );

  it.each(["step", "stepWithCursor"] as const)(
    "does not complete a %s parent that catches the nesting error",
    async (parent) => {
      const resumable = new Resumable({} as Client, job({}));
      const later = vi.fn();
      await resumable.step("before", () => undefined);
      await resumable[parent]("parent", async () => {
        if (parent === "stepWithCursor") resumable.setCursor(1);
        await expect(
          resumable.step("child", () => {
            throw new Error("must not run");
          })
        ).rejects.toThrow("resumable steps cannot be nested");
      });
      await expect(resumable.step("after", later)).rejects.toThrow(
        "resumable steps cannot be nested"
      );
      expect(later).not.toHaveBeenCalled();
      const finished = resumable.finish(false);
      expect(finished.error?.message).toBe("resumable steps cannot be nested");
      expect(finished.metadata).toEqual(
        parent === "stepWithCursor"
          ? {
              "river:resumable_step": "before",
              "river:resumable_cursor": { parent: 1 },
            }
          : { "river:resumable_step": "before" }
      );
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
