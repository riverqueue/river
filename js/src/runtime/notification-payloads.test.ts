import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";

import { describe, expect, it } from "vitest";

import {
  notificationCancellation,
  notificationLeaderResigned,
  notificationQueue,
  notificationRequestsLeadershipResignation,
} from "./notification-payloads.js";

/**
 * A notification from River Go's protocol goldens: its topic, its payload
 * struct's JSON fields, and a payload Go sends.
 */
interface NotificationGolden {
  readonly fields: readonly { name: string; omitempty: boolean }[];
  readonly name: string;
  readonly payload: Record<string, unknown>;
  readonly topic: string;
}

/** River Go's protocol goldens, including its notification payloads. */
const PROTOCOL_GOLDENS = new URL(
  "../../../conformance/testdata/protocol_values.json",
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

describe("notification payloads", () => {
  // `notification-pump.conformance.test.ts` dispatches Go's compact
  // payloads. Postgres's `json_build_object` adds spaces, as in
  // `{"action" : "cancel", ...}`, so check every reader parses that form of
  // each payload alike.
  it("reads the spaced form of every payload River for Go sends", async () => {
    const golden = JSON.parse(await readFixture(PROTOCOL_GOLDENS)) as {
      readonly notifications: readonly NotificationGolden[];
    };
    expect(golden.notifications.length).toBeGreaterThan(0);

    const read = golden.notifications.map(({ name, payload }) => {
      const spaced = JSON.stringify(payload, null, 1)
        .replaceAll("\n", "")
        .replaceAll('":', '" :');
      return {
        cancellation: notificationCancellation(spaced),
        leaderResigned: notificationLeaderResigned(spaced),
        name,
        queue: notificationQueue(spaced),
        requestsResignation: notificationRequestsLeadershipResignation(spaced),
      };
    });

    expect(read).toEqual(
      golden.notifications.map(({ name, payload }) => ({
        cancellation:
          payload.action === "cancel" ? BigInt(payload.job_id as number) : null,
        leaderResigned:
          payload.action === "resigned" ? (payload.leader_id as string) : null,
        name,
        queue: typeof payload.queue === "string" ? payload.queue : null,
        requestsResignation: payload.action === "request_resign",
      }))
    );
  });
});
