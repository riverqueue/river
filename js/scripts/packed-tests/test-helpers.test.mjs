import assert from "node:assert/strict";
import { describe, it } from "node:test";

import {
  createTestClient,
  requireInserted,
  requireNotInserted,
  testJob,
  workOnce,
} from "@riverqueue/test";
import {
  defineJob,
  exactJsonNumber,
  isExactJsonNumber,
  snooze,
  Workers,
} from "riverqueue";

const sendEmail = defineJob({
  kind: "packed_send_email",
  decode(value) {
    if (typeof value.to !== "string") throw new TypeError("to");
    return { accountId: value.accountId ?? null, to: value.to };
  },
});

describe("packed @riverqueue/test helpers", () => {
  it("records deterministic insertions and matches them", async () => {
    const { client, insertions } = createTestClient({ startingId: 41n });
    await client.insert(
      sendEmail,
      {
        accountId: exactJsonNumber("9223372036854775807"),
        to: "a@example.com",
      },
      { queue: "email" }
    );
    assert.equal(insertions[0]?.job.id, 41n);
    const job = requireInserted(insertions, sendEmail, {
      args: { to: "a@example.com" },
      queue: "email",
    });
    assert.ok(isExactJsonNumber(job.args.accountId));
    assert.equal(job.args.accountId.rawJSON, "9223372036854775807");
    requireNotInserted(insertions, sendEmail, {
      args: { to: "b@example.com" },
    });
    assert.throws(() =>
      requireInserted(insertions, sendEmail, { args: { to: "b@example.com" } })
    );
  });

  it("works one job through a Workers bundle", async () => {
    const workers = new Workers().add(sendEmail, ({ job, recordOutput }) => {
      recordOutput({ recipient: job.args.to });
      return snooze({ seconds: 30 });
    });
    const running = await testJob(
      sendEmail,
      { to: "a@example.com" },
      { id: 7n }
    );
    assert.equal(running.id, 7n);
    const worked = await workOnce(running, workers);
    assert.equal(worked.status, "succeeded");
    assert.equal(worked.outcome?.type, "snooze");
    assert.deepEqual({ ...worked.output }, { recipient: "a@example.com" });
  });

  it("reports a handler failure without throwing", async () => {
    const running = await testJob(sendEmail, { to: "a@example.com" });
    const worked = await workOnce(running, () => {
      throw new Error("smtp down");
    });
    assert.equal(worked.status, "failed");
    assert.match(String(worked.error), /smtp down/);
  });
});
