# River for JavaScript and TypeScript: guide

This guide covers defining, inserting, and working jobs. The
[README](../README.md) has requirements and a runnable quickstart; the topic
guides linked at the end go deeper.

## Define jobs

A job definition is an immutable value naming a job `kind` and describing its
arguments. Producers and workers both import it. It holds no client, pool, or
handler, so a web server can import it without worker dependencies.

Validate arguments with any [Standard Schema](https://standardschema.dev)
library (Zod, Valibot, ArkType, ...):

```ts
import { defineJob } from "riverqueue";
import { z } from "zod";

export const sendEmail = defineJob({
  kind: "send_email",
  schema: z.object({
    messageId: z.string(),
    to: z.email(),
  }),
  defaults: { maxAttempts: 5, queue: "email" },
});
```

River runs the schema when a job is inserted and again before it is worked,
because another producer (an older deploy, a Go or Rust service, or a SQL
script) may have inserted it. Producers pass the schema's input type; workers
receive its output type, so schema defaults and transforms apply to what the
worker sees. River persists the producer's input exactly as given, so
uniqueness hashes and other languages see the same JSON.

The schema's input must be JSON. A schema with a `Date` or `bigint` input is
rejected when you call `defineJob`; validate the JSON shape (for example an
ISO string) and convert it in the schema's output or in the worker.

Without a validation library, write a decoder. It receives the persisted JSON
object and returns the worker's arguments, or throws to reject them:

```ts
import { defineJob } from "riverqueue";

export const resizeImage = defineJob({
  kind: "resize_image",
  decode(value) {
    const { url, width } = value;
    if (typeof url !== "string" || typeof width !== "number") {
      throw new TypeError("expected { url: string, width: number }");
    }
    return { url, width };
  },
});
```

Producers insert the decoder's return type. When the decoder returns
something that is not JSON, declare the producer type separately with
`defineJob<Input>()`:

```ts
import { defineJob } from "riverqueue";

interface ReportInput {
  reportId: string;
  since: string; // ISO 8601
}

export const buildReport = defineJob<ReportInput>()({
  kind: "build_report",
  decode(value) {
    if (typeof value.reportId !== "string" || typeof value.since !== "string") {
      throw new TypeError("expected { reportId, since }");
    }
    return {
      reportId: value.reportId,
      since: Temporal.Instant.from(value.since),
    };
  },
});
```

`defineJob<Input>()({ kind })` without a decoder types producers only: its
workers receive an unvalidated `JsonObject`, because a type annotation cannot
check what another producer stored. `defineJob({ kind })` accepts any JSON
object on both sides.

Kinds are persisted and shared with other languages, so treat them as part of
your data model. A kind starts with a letter, digit, or underscore, and kinds
starting with `river_internal_` are reserved.

To rename a kind without orphaning jobs already stored under the old name,
make the new name the `kind` and list the old one in `kindAliases`, like River
for Go's `JobArgsWithKindAliases`. New jobs are inserted under the new kind,
and the definition's worker also works jobs stored under the alias. Remove the
alias once those jobs have finished, including their retries:

```ts
import { defineJob } from "riverqueue";

export const sendInvoice = defineJob({
  kind: "send_invoice",
  kindAliases: ["email_invoice"],
});
```

## Insert jobs

<!-- ts-setup
import { Client, defineJob } from "riverqueue";
import { PgDriver } from "@riverqueue/driver-pg";
import { Pool } from "pg";
import { z } from "zod";
const sendEmail = defineJob({
  kind: "send_email",
  schema: z.object({ messageId: z.string(), to: z.email() }),
});
const pool = new Pool();
const client = new Client(new PgDriver(pool));
-->

```ts
const result = await client.insert(
  sendEmail,
  { messageId: "msg_123", to: "person@example.com" },
  { priority: 1, queue: "email", tags: ["welcome"] }
);

if (result.status === "inserted") {
  console.log(result.job.id); // bigint
}
```

Insertion options resolve per option, most specific first: the call's options,
then the definition's `defaults`, then the client's `defaultInsertOptions`,
then River's defaults (queue `default`, priority 1, 25 attempts).

| Option        | Meaning                                                             |
| ------------- | ------------------------------------------------------------------- |
| `queue`       | Queue the job is worked from                                        |
| `priority`    | 1 (highest) through 4                                               |
| `maxAttempts` | Attempts before the job is discarded, including the first           |
| `scheduledAt` | When the job becomes available, as a `Temporal.Instant` or a `Date` |
| `delay`       | Or, how long from now, such as `{ minutes: 5 }`                     |
| `tags`        | Labels for querying                                                 |
| `metadata`    | Application JSON stored with the job                                |
| `pending`     | Insert as `pending`, for another process to make available          |
| `unique`      | Deduplicate against existing jobs; see below                        |

### Unique jobs

`unique` rejects a job that duplicates an existing one instead of inserting
it; the result has `status: "duplicate"` and the existing job:

<!-- ts-setup
import { Client, defineJob } from "riverqueue";
import { PgDriver } from "@riverqueue/driver-pg";
import { Pool } from "pg";
const syncAccount = defineJob<{ accountId: string }>()({ kind: "sync_account" });
const client = new Client(new PgDriver(new Pool()));
-->

```ts
const result = await client.insert(
  syncAccount,
  { accountId: "acct_1" },
  { unique: { byArgs: true, byPeriod: { hours: 1 } } }
);
if (result.status === "duplicate") {
  console.log(`already queued as ${result.job.id}`);
}
```

Uniqueness is always by kind (unless `excludeKind`), and optionally by all
args or selected `byArgs` paths (such as `["account.id"]`), by `byQueue`, and
within fixed `byPeriod` windows. Like River for Go, `excludeKind` requires
`byArgs`, `byQueue`, or `byPeriod`. `byState` chooses which job states count as
duplicates; the default is every state except `cancelled` and `discarded`.
Unique keys are computed exactly as River for Go computes them, so uniqueness
holds across languages.

By-args uniqueness hashes the JSON River writes. Top-level keys are sorted, but
nested objects are hashed in their own key order, so jobs from different
languages deduplicate only when they encode nested objects identically. Go
writes struct fields in declaration order and map keys sorted; JavaScript writes
object keys in insertion order, except that keys that look like array indices
(`"2"`, `"10"`) always come first in ascending numeric order. Keep nested keys
in the same order in every producer, and avoid integer-like keys in nested
objects that must deduplicate against another language.

With `byArgs: true`, every top-level key is hashed literally, including empty
keys and keys containing JSON path punctuation. A selected `byArgs` path uses
an unescaped dot to reach a nested field: `"account.id"` selects `id` within
`account`. Escape a dot to select a literal key containing one:
`"account\\.id"` selects the top-level key `account.id`. Use `"\\\\"` for
a literal backslash in a key. Empty path segments and a trailing backslash
are invalid. So is a segment that is an unsigned integer or `-1`, escaped or
not: River for Go reads it as an array index and builds a JSON array instead
of an object when assembling the selected fields, so the keys wouldn't
match.

### Batches

`insertMany` inserts a batch atomically and returns results in input order;
an empty batch returns immediately. Items may use different definitions, and
each item's `args` is checked against its own definition:

<!-- ts-setup
import { Client, defineJob } from "riverqueue";
import { PgDriver } from "@riverqueue/driver-pg";
import { Pool } from "pg";
const sendEmail = defineJob<{ to: string }>()({ kind: "send_email" });
const syncAccount = defineJob<{ accountId: string }>()({ kind: "sync_account" });
const client = new Client(new PgDriver(new Pool()));
-->

```ts
const results = await client.insertMany([
  { args: { to: "a@example.com" }, job: sendEmail },
  { args: { accountId: "acct_1" }, job: syncAccount, options: { priority: 2 } },
]);
console.log(results.map(({ status }) => status));
```

### Transactions

Insert jobs in the same transaction as the application writes that caused
them, so both commit or neither does. River never begins, commits, or rolls
back your transaction. With `node-postgres`, pass any client that ran `BEGIN`
(a `PoolClient` or a `pg.Client`):

<!-- ts-setup
import { Client, defineJob } from "riverqueue";
import { PgDriver } from "@riverqueue/driver-pg";
import { Pool } from "pg";
const syncAccount = defineJob<{ accountId: string }>()({ kind: "sync_account" });
const pool = new Pool();
const client = new Client(new PgDriver(pool));
-->

```ts
const tx = await pool.connect();
try {
  await tx.query("BEGIN");
  await tx.query("INSERT INTO accounts (id) VALUES ($1)", ["acct_1"]);
  await client.insert(syncAccount, { accountId: "acct_1" }, { tx });
  await tx.query("COMMIT");
} catch (error) {
  await tx.query("ROLLBACK");
  throw error;
} finally {
  tx.release();
}
```

Like River for Go, River runs its statements directly in your transaction and
opens no savepoint or nested transaction in it, which on PostgreSQL would cost
a subtransaction for every call. When a call fails after River wrote to the
database, for example because insert middleware or an `afterInsert` hook threw
after the job was inserted, the write stays in your transaction, so roll it
back as above. On PostgreSQL a database error also aborts the transaction. To
recover from a failed call and continue the transaction, wrap the call in a
savepoint of your own. Without `{ tx }`, River's own transaction rolls the
whole call back.

The job's insert notification is delivered when the transaction commits, so
workers never see a job whose transaction rolled back. Prisma and SQLite use
the same `{ tx }` option; see the [Prisma](../driver/prisma/README.md) and
[SQLite](../driver/sqlite/README.md) drivers.

## Work jobs

Register a handler per definition. Its context carries the validated `job`
(`job.args`, plus the persisted JSON in `job.rawArgs`), an `AbortSignal`, a
`logger` bound to the job, and the `client` for inserting follow-up jobs:

<!-- ts-setup
import { defineJob } from "riverqueue";
import { z } from "zod";
const sendEmail = defineJob({
  kind: "send_email",
  schema: z.object({ messageId: z.string(), to: z.email() }),
});
declare const mailer: {
  send(
    message: { messageId: string; to: string },
    options: { signal: AbortSignal }
  ): Promise<{ id: string; rateLimited: boolean }>;
};
-->

```ts
import { Workers, complete, snooze } from "riverqueue";

const workers = new Workers().add(
  sendEmail,
  async ({ job, logger, signal }) => {
    const response = await mailer.send(job.args, { signal });
    if (response.rateLimited) return snooze({ seconds: 30 });

    logger.info({ providerId: response.id }, "sent");
    return complete({ output: { providerId: response.id } });
  },
  { timeout: { minutes: 2 } }
);
```

Resolving completes the job; throwing or rejecting fails the attempt and
schedules a retry. Handlers may instead return `complete({ output })`,
`snooze(duration)` (run again later without using an attempt),
`discard({ reason })`, or `cancel({ reason })`. See
[errors, retries, and cancellation](./errors-and-retries.md).

Pass the `signal` to every cancel-aware operation. It aborts when the job's
timeout expires, when the job is cancelled from anywhere in the fleet, and
when the client stops with `mode: "cancel"`.

`ctx.recordOutput` stores JSON output even when the attempt fails, and
`ctx.setMetadata` merges into the job's metadata.

`ctx.completeTx(tx)` completes the job inside your own transaction, so the
job's completion commits atomically with the handler's writes:

<!-- ts-setup
import { defineJob } from "riverqueue";
import { Pool } from "pg";
const pool = new Pool();
const chargeCard = defineJob<{ amountCents: number }>()({ kind: "charge_card" });
-->

```ts
import type { PoolClient } from "pg";
import { Workers } from "riverqueue";

const workers = new Workers<PoolClient>().add(
  chargeCard,
  async ({ completeTx, job }) => {
    const tx = await pool.connect();
    try {
      await tx.query("BEGIN");
      await tx.query("INSERT INTO charges (amount_cents) VALUES ($1)", [
        job.args.amountCents,
      ]);
      await completeTx(tx);
      await tx.query("COMMIT");
    } catch (error) {
      await tx.query("ROLLBACK");
      throw error;
    } finally {
      tx.release();
    }
  }
);
```

If the transaction rolls back, so does the completion, and River records the
attempt from the handler's result as usual: a thrown error fails it and a
normal return completes it. `new Workers<PoolClient>()` types `completeTx`
(and `ctx.client`) for node-postgres. Without it, `Workers` accepts the
transaction of any installed River driver, so with both the PostgreSQL and
SQLite drivers installed, passing a SQLite transaction to a PostgreSQL client's
handler would compile and then fail at runtime.

## Run workers

Configure queues and start the client. `maxWorkers` bounds concurrent
handlers per queue in this process:

<!-- ts-setup
import { Client, Workers } from "riverqueue";
import { PgDriver } from "@riverqueue/driver-pg";
import { Pool } from "pg";
declare const workers: Workers;
const pool = new Pool();
-->

```ts
const client = new Client(new PgDriver(pool), {
  queues: {
    default: { maxWorkers: 100 },
    email: { maxWorkers: 20, pollInterval: { seconds: 2 } },
  },
  workers,
});

await using run = await client.start();
await run.addQueue("reports", { maxWorkers: 5 });
console.log(run.diagnostics.queues);
```

`client.start()` returns a `RunHandle`. `run.completed` settles when the
runtime stops and rejects if it fails; `run.stop()` stops gracefully (running
jobs finish; pass `timeout` to bound the wait and `mode: "cancel"` to abort
them), and `await using` stops it when the scope ends. A client starts at most
once. See [the runtime guide](./runtime.md) for shutdown, concurrency, and
the event loop.

## Query and control jobs

Job operations live on `client.jobs` and queue operations on `client.queues`.
IDs are `bigint`, missing rows are `null`, and list pagination uses an opaque
cursor. Each accepts `{ tx }`:

<!-- ts-setup
import { Client } from "riverqueue";
import { PgDriver } from "@riverqueue/driver-pg";
import { Pool } from "pg";
const client = new Client(new PgDriver(new Pool()));
-->

```ts
const page = await client.jobs.list({
  kinds: ["send_email"],
  limit: 100,
  states: ["available", "retryable"],
});
for (const job of page.jobs) console.log(job.id, job.state);

if (page.nextCursor !== null) {
  await client.jobs.list({ after: page.nextCursor, limit: 100 });
}

const job = await client.jobs.get(9_007_199_254_740_993n);
if (job !== null) {
  await client.jobs.retry(job.id);
  await client.jobs.cancel(job.id); // also aborts a running attempt
}

await client.queues.pause("email"); // or "*" for every queue
await client.queues.resume("email");
```

A job list cursor is the same text River for Go's `JobListCursor` and River
for Rust's `JobListCursor` produce, so a service in one language can hand a
page token to a service in another to continue the listing. River accepts
cursors in either base64 alphabet, with or without padding. Queue list
cursors are specific to JavaScript.

Like River for Go, `orderBy: "time"` sorts every listed job by the time
field of the first listed state (`scheduledAt` for `available`, `pending`,
`retryable`, and `scheduled`, `attemptedAt` for `running`, `finalizedAt`
for finalized states), and by `scheduledAt` when `states` is empty. Jobs
where that field is null, like `finalizedAt` for an unfinished job, sort
after all others ascending and before all others descending.

## Exact values and JSON

River never exposes a lossy database value:

- job IDs and other 64-bit integers are `bigint`;
- timestamps are `Temporal.Instant`, with PostgreSQL's microseconds; and
- job args and metadata are JSON, typed as `JsonValue`/`JsonObject`.

At every JSON boundary River rejects `bigint`, non-finite numbers, integers
beyond `Number.MAX_SAFE_INTEGER`, cycles, sparse arrays, accessors, class
instances, and invalid Unicode instead of silently coercing them. Like
`JSON.stringify`, it omits object properties whose value is `undefined`.

A number another producer stored that JavaScript cannot represent exactly
(such as a Go `int64` above 2^53) arrives as an `ExactJsonNumber`, so reading
and re-inserting it never changes it. Validated args only contain one if your
schema accepts it. For untyped `JsonObject` args, use `isJsonNumber` and
`jsonNumberToBigInt`:

```ts
import { isJsonNumber, jsonNumberToBigInt, parseJsonObject } from "riverqueue";

const args = parseJsonObject('{"userId":9223372036854775807}');
if (isJsonNumber(args.userId)) {
  console.log(jsonNumberToBigInt(args.userId)); // 9223372036854775807n
}
```

`JSON.stringify` cannot encode `bigint`. Use `jobToJsonValue(job)` for a
JSON-safe copy of a job (IDs as decimal strings, instants as ISO strings) and
`jobFromJsonValue` to restore it. River never patches
`BigInt.prototype.toJSON`.

## More guides

- [Errors, retries, timeouts, and cancellation](./errors-and-retries.md)
- [Periodic jobs](./periodic-jobs.md)
- [Resumable jobs](./resumable-jobs.md)
- [Testing](./testing.md)
- [Runtime, concurrency, and the event loop](./runtime.md)
- [Databases, pools, and migrations](./databases.md)
- [Logging, events, and metrics](./observability.md)
- [Running alongside Go and Rust](./deployment.md)
- [Migrating from `riverqueue` 0.1](./migrating-from-0.1.md)

Runnable examples:

- [PostgreSQL worker](../examples/pg-worker/README.md)
- [node-postgres insertion and transactions](../examples/node-postgres/README.md)
- [Prisma insertion and transactions](../examples/prisma/README.md)
- [SQLite worker](../examples/sqlite-worker/README.md)
- [Graceful shutdown](../examples/graceful-shutdown/README.md)
- [Hooks and metrics](../examples/hooks-metrics/README.md)
- [CPU work on worker threads](../examples/worker-thread-cpu/README.md)
- [A payload shared with Go](../examples/mixed-language/README.md)
