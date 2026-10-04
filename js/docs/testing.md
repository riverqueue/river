# Testing

River code has two sides to test: the code that inserts jobs, and the
handlers that work them. `@riverqueue/test` covers both without a database,
and a real client on an in-memory SQLite database covers everything else.

## Producers

Type producer code against `InsertClient`, which both the real client and the
test client satisfy, and assert on what it inserted:

<!-- ts-setup
import { defineJob, type InsertClient } from "riverqueue";
import { z } from "zod";
import { describe, it } from "vitest";
const sendWelcomeEmail = defineJob({
  kind: "send_welcome_email",
  schema: z.object({ to: z.email() }),
});
-->

```ts
import { createTestClient, requireInserted } from "@riverqueue/test";

export async function signUp(client: InsertClient, email: string) {
  // ... create the account ...
  await client.insert(sendWelcomeEmail, { to: email }, { queue: "email" });
}

describe("signUp", () => {
  it("queues a welcome email", async () => {
    const { client, insertions } = createTestClient();
    await signUp(client, "person@example.com");

    const job = requireInserted(insertions, sendWelcomeEmail, {
      args: { to: "person@example.com" },
      queue: "email",
    });
    console.assert(job.state === "available");
  });
});
```

`requireInserted` fails unless exactly one recorded job matches, and returns it
with typed args; `requireNotInserted` and `requireManyInserted` (the full list,
in order) complete the set, like Go's `rivertest`. Insertions run the
definition's validation, so an invalid payload fails the test just as it
would fail in production. `createTestClient<PoolClient>()` records the
`{ tx }` each insertion received for code that inserts inside transactions.
Pass `hooks`, `insertMiddleware`, `plugins`, or `defaultInsertOptions` to
`createTestClient` to record the jobs they produce.

Against a real database, `requireInsertedInDatabase(client, definition,
match, { tx })` and `requireNotInsertedInDatabase` make the same assertions
on the jobs a client persisted, like Go's `rivertest.RequireInsertedTx`;
pass `tx` to look inside a transaction that hasn't committed.

## Workers

`testJob` builds the job a handler would receive, validating producer input
through the definition exactly as the runtime does, and `workOnce` runs a
handler (or the one a `Workers` bundle registered for the job's kind):

<!-- ts-setup
import { Workers, defineJob, snooze } from "riverqueue";
import { z } from "zod";
import { expect, it } from "vitest";
const chargeCard = defineJob({
  kind: "charge_card",
  schema: z.object({ amountCents: z.number().int().positive() }),
});
declare const payments: { charge(amount: number): Promise<"ok" | "busy"> };
const workers = new Workers().add(chargeCard, async ({ job }) =>
  (await payments.charge(job.args.amountCents)) === "busy"
    ? snooze({ seconds: 30 })
    : undefined
);
-->

```ts
import { testJob, workOnce } from "@riverqueue/test";

it("snoozes when the payment provider is busy", async () => {
  const job = await testJob(chargeCard, { amountCents: 500 }, { attempt: 2 });
  const result = await workOnce(job, workers);

  expect(result.status).toBe("succeeded");
  if (result.status === "succeeded") {
    expect(result.outcome).toMatchObject({ type: "snooze" });
  }
});
```

The result also carries recorded `output`, merged `metadata` (including
[resumable](./resumable-jobs.md) progress), and every message the handler
logged. `ctx.client` inside `workOnce` records insertions like
`createTestClient`; pass `client` to use a real one.

## The whole runtime

`workOnce` runs one handler; it doesn't run middleware, hooks, retries,
scheduling, or persistence. To test those, start a real client on an
in-memory SQLite database. It needs no external service and uses the same
runtime as production:

<!-- ts-setup
import { Workers, defineJob } from "riverqueue";
import { expect, it } from "vitest";
const chargeCard = defineJob<{ amountCents: number }>()({ kind: "charge_card" });
declare const workers: Workers;
-->

```ts
import { SqliteDriver } from "@riverqueue/driver-sqlite";
import { createMigrator } from "@riverqueue/migrate";
import { Client } from "riverqueue";

it("works a job end to end", async () => {
  using driver = SqliteDriver.memory();
  await createMigrator(driver).migrateUp();
  const client = new Client(driver, {
    leaderElectionDisabled: true,
    queues: {
      default: {
        fetchCooldown: { milliseconds: 20 },
        maxWorkers: 1,
        pollInterval: { milliseconds: 50 },
      },
    },
    workers,
  });
  const { job } = await client.insert(chargeCard, { amountCents: 500 });

  using events = client.subscribe({ kinds: ["job_completed"] });
  await using run = await client.start();
  const { value: event } = await events.next();
  expect(event?.kind === "job_completed" && event.job.id).toBe(job.id);
  await run.stop();
});
```

`leaderElectionDisabled: true` keeps leader election and maintenance services
out of short tests. A queue's `pollInterval` can't be shorter than its
`fetchCooldown` (the client's `fetchCooldown`, 100 ms by default), so a test
that polls faster lowers both. Use the PostgreSQL driver against a disposable
database when a test depends on PostgreSQL behavior (`LISTEN`/`NOTIFY`, custom
schemas, or concurrent clients).
