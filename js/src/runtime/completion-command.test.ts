import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

import type { JobRow } from "../job.js";
import {
  isExactJsonNumber,
  parseJson,
  type JsonObject,
  type JsonValue,
} from "../json.js";
import { snooze } from "../worker.js";
import { completionCommand } from "./completion-command.js";

interface SnoozeCounterCase {
  readonly expected_snoozes: JsonValue;
  readonly metadata: JsonObject;
  readonly name: string;
}

/** River Go's snooze counter goldens, generated from its executor. */
const GOLDENS = new URL(
  "../../../conformance/testdata/snooze_counters.json",
  import.meta.url
);

/**
 * Reads a fixture that `make generate/fixtures` writes from River's Go
 * implementation. A missing fixture fails the test rather than skipping it.
 */
async function readFixture(url: URL): Promise<string> {
  try {
    return await readFile(url, "utf8");
  } catch (error) {
    if ((error as NodeJS.ErrnoException).code === "ENOENT") {
      throw new Error(
        `missing conformance fixture ${fileURLToPath(url)}; run \`make generate/fixtures\` from the repository root`,
        { cause: error }
      );
    }
    throw error;
  }
}

describe("completionCommand", () => {
  it("counts snoozes like River for Go's executor", async () => {
    // Parse with River's exact JSON so integers beyond 2^53 stay exact.
    const golden = parseJson(await readFixture(GOLDENS)) as unknown as {
      readonly snooze_counters: readonly SnoozeCounterCase[];
    };
    const now = Temporal.Instant.from("2026-09-01T00:00:00Z");

    const results = golden.snooze_counters.map(({ metadata, name }) => {
      const command = completionCommand(
        job(metadata),
        "client",
        { outcome: snooze({ seconds: 30 }), status: "succeeded" },
        now,
        now,
        now,
        5_000
      );
      return [name, numberText(command.metadata?.snoozes)];
    });

    expect(results).toEqual(
      golden.snooze_counters.map(({ expected_snoozes, name }) => [
        name,
        numberText(expected_snoozes),
      ])
    );
  });
});

function job(metadata: JsonObject): JobRow {
  const at = Temporal.Instant.from("2026-09-01T00:00:00Z");
  return {
    args: {},
    attempt: 1,
    attemptedAt: at,
    attemptedBy: ["client"],
    createdAt: at,
    errors: [],
    finalizedAt: null,
    id: 1n,
    kind: "snooze",
    maxAttempts: 25,
    metadata,
    priority: 1,
    queue: "default",
    scheduledAt: at,
    state: "running",
    tags: [],
    uniqueKey: null,
    uniqueStates: null,
  };
}

/** A JSON number's exact text, so ordinary and exact numbers compare. */
function numberText(value: JsonValue | undefined): string {
  if (typeof value === "number") return String(value);
  if (isExactJsonNumber(value)) return value.rawJSON;
  throw new Error(`not a JSON number: ${JSON.stringify(value)}`);
}
