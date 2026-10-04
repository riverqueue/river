# `@riverqueue/test`

Typed, database-free helpers for testing River producers and workers. They use
the same exact `bigint`, `Temporal.Instant`, and JSON values as the production
API.

## Testing producers

`createTestClient` returns an insert-only client with deterministic IDs and a
log of every insertion. Type producer code against `InsertClient` so the same
function accepts the real client and the test client, then assert on the log
like Go's `rivertest`:

```ts
import { defineJob, type InsertClient } from "riverqueue";
import { z } from "zod";
import {
  createTestClient,
  requireInserted,
  requireNotInserted,
} from "@riverqueue/test";

const sendWelcomeEmail = defineJob({
  kind: "send_welcome_email",
  schema: z.object({ to: z.email() }),
});

async function signUp(client: InsertClient, email: string) {
  await client.insert(sendWelcomeEmail, { to: email }, { queue: "email" });
}

const { client, insertions } = createTestClient();
await signUp(client, "person@example.com");

const job = requireInserted(insertions, sendWelcomeEmail, {
  args: { to: "person@example.com" },
  queue: "email",
});
console.assert(job.args.to === "person@example.com");
requireNotInserted(insertions, sendWelcomeEmail, {
  args: { to: "someone-else@example.com" },
});
```

`requireManyInserted` asserts the complete ordered list. Each insertion also
records the `{ tx }` value it received, so `createTestClient<PoolClient>()`
fits code that inserts inside a node-postgres transaction.

## Testing workers

`testJob` builds a running job from producer input. It validates the input
through the job definition exactly as the runtime does before working, so
`job.args` has the worker's type and `job.rawArgs` holds the persisted JSON.
`workOnce` runs a handler, or the handler a `Workers` bundle registered for the
job's kind (with its timeout), and returns the outcome, output, metadata, and
logs:

```ts
import { Workers, defineJob, snooze } from "riverqueue";
import { z } from "zod";
import { testJob, workOnce } from "@riverqueue/test";

const sendEmail = defineJob({
  kind: "send_email",
  schema: z.object({ to: z.email() }),
});
const workers = new Workers().add(sendEmail, ({ job, recordOutput }) => {
  recordOutput({ recipient: job.args.to });
  return snooze({ seconds: 30 });
});

const running = await testJob(
  sendEmail,
  { to: "person@example.com" },
  { id: 42n }
);
const worked = await workOnce(running, workers);

if (worked.status !== "succeeded") throw worked.error;
console.assert(worked.outcome?.type === "snooze");
console.assert(worked.output !== undefined);
```

`workOnce` initializes resumable state from the job's metadata and returns the
updated `metadata`, including checkpoints and recorded output. Pass it back
through `testJob(..., { metadata })` to test a retry. Inside `workOnce`,
`ctx.client` records insertions like `createTestClient`; pass `client` to use a
real one.

`workOnce` does not run middleware, hooks, retries, scheduling, or
persistence. For those, run a real client against an in-memory SQLite database
(`SqliteDriver.memory()` from `@riverqueue/driver-sqlite`), which needs no
external service and exercises the complete runtime.

## Requirements

Node.js 26 or newer with native `Temporal`: `node -p "typeof Temporal"` must
print `object`. Official Node.js binaries include it; some builds compiled from
source, including some distribution and Homebrew packages, do not.

Install `riverqueue` at exactly this package's version. It is a peer dependency,
so npm rejects a mismatched pair instead of loading two copies.

TypeScript users need TypeScript 6.0 or newer and `@types/node`, with `"node"`
listed in `compilerOptions.types`. See [River's
requirements](https://github.com/riverqueue/river/tree/master/js#requirements) for
details.
