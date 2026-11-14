# Migrating from `riverqueue` 0.1

`riverqueue` 0.1 was an insert-only client. This release adds workers and the
full runtime, and corrects types that could lose database values before 1.0
locks them in. Most changes are mechanical. Use the compiler to find type
changes, then review the behavioral changes described below.

## Runtime requirements

The package needs Node.js 24 or newer and ships as ESM; see
[requirements](../README.md#requirements). On runtimes without native `Temporal`,
[preload an application-provided polyfill](../README.md#temporal-on-nodejs-24)
before importing River.

## Upgrade checklist

Use this guide alongside the complete [before](../fixtures/migration-0.1/before.ts.txt)
and [after](../fixtures/migration-0.1/after.ts) examples. CI compiles the original
example against 0.1 and the upgraded example against the current package.

1. Replace argument classes and `JobArgsObject` with job definitions and plain
   JSON arguments, preserving the serialized shape and insertion defaults.
2. Update batch items, insert options, result handling, and renamed exports.
3. Review job IDs, timestamps, JSON serialization, and transaction ownership.
4. Run your formatter, type checker, and tests. Type checking alone cannot
   verify that persisted arguments and transaction behavior are unchanged.

The CLI no longer includes an upgrade codemod. If you need the historical
`codemod-0.1` command, `@riverqueue/cli@0.3.2` is the last release containing
it and needs TypeScript 5 or 6 for its compiler API. Its output still requires
manual review using this guide.

## Job definitions replace argument classes

0.1 described a job with a class implementing `JobArgs`:

```ts ignore
class SortArgs implements JobArgs {
  kind = "sort";
  insertOpts = { queue: "sorting" };
  constructor(readonly strings: string[]) {}
}
await client.insert(new SortArgs(["b", "a"]));
```

Now a job is a definition value, and arguments are a plain JSON object:

<!-- ts-setup
import { Client, defineJob } from "riverqueue";
import { PgDriver } from "@riverqueue/driver-pg";
import { Pool } from "pg";
import { z } from "zod";
const client = new Client(new PgDriver(new Pool()));
-->

```ts
const sort = defineJob({
  defaults: { queue: "sorting" },
  kind: "sort",
  schema: z.object({ strings: z.array(z.string()) }),
});
await client.insert(sort, { strings: ["b", "a"] });
```

`JobArgsObject("kind", args)` becomes `defineJob({ kind: "kind" })`, which
accepts any JSON object. `JobArgsObject` did not inherit an argument class's
`insertOpts`, even when both used the same kind. Preserve that distinction with
a separate definition or explicit insert options if you combine them.

If an argument class had a constructor body or custom `toJSON`, preserve its
serialized arguments when replacing it with a plain object. Give a definition
a schema or a `decode` function before registering a worker for it: a type
annotation alone does not validate
jobs another producer inserted. See [defining jobs](./README.md#define-jobs).

## Batch items and results

`new InsertManyParams(args, options)` becomes a plain object, and a
definition identifies each item's kind:

<!-- ts-setup
import { Client, defineJob } from "riverqueue";
import { PgDriver } from "@riverqueue/driver-pg";
import { Pool } from "pg";
const client = new Client(new PgDriver(new Pool()));
const sort = defineJob<{ strings: string[] }>()({ kind: "sort" });
-->

```ts
const results = await client.insertMany([
  { args: { strings: ["c"] }, job: sort, options: { priority: 2 } },
  { args: { strings: ["d"] }, job: sort },
]);

if (results[0].status === "duplicate") {
  // results[0].job is the existing job that conflicted
}
```

`result.uniqueSkippedAsDuplicated` becomes `result.status === "duplicate"`.

A batch may name a unique key only once. `insertMany` rejects a batch that
repeats one with a `ValidationError` and writes none of it, where 0.1 inserted
the first job and reported the others as duplicates.

## Insert options

| 0.1                                      | Now                                        |
| ---------------------------------------- | ------------------------------------------ |
| `uniqueOpts: { byPeriod: 60 }` (seconds) | `unique: { byPeriod: { seconds: 60 } }`    |
| `scheduledAt: new Date(...)`             | unchanged; a `Temporal.Instant` also works |
| compute `Date.now() + 60_000` yourself   | `delay: { minutes: 1 }`                    |
| `schema` on the client                   | `new PgDriver(pool, { schema })`           |
| `insertOpts` on an argument class        | `defaults` on the definition               |
| `InsertOpts` and `UniqueOpts` types      | `InsertOptions` and `UniqueOptions`        |

Options now resolve by presence rather than truthiness, so an invalid `0` or
empty value is rejected instead of silently falling back to a default.

## Renamed exports

`ClientOpts` is now `ClientOptions`. The `JOB_STATE_AVAILABLE`,
`JOB_STATE_RUNNING`, and other state constants are properties of `JOB_STATE`,
such as `JOB_STATE.available` and `JOB_STATE.running`.

## Drivers

A driver is only passed to River: `PgDriver` no longer has `jobInsert` or
`jobInsertMany`, nor exposes its pool. Insert through the client, and keep
your own reference to the pool if you use it directly;
`createMigrator(driver)` still migrates the driver's connection and schema.
A hand-written driver or test double
passed to `new Client()` is rejected; use `createTestClient` from
`@riverqueue/test` in tests.

## Exact IDs and timestamps

`JobRow.id` is a `bigint`, not a `number`: IDs are 64-bit and larger values
would lose precision. Use `id.toString()` for logs, URLs, and JSON. Review any
ID arithmetic by hand rather than converting back to `number`.

Every timestamp on a job row is a `Temporal.Instant`. It keeps Postgres's
microseconds and carries no local time zone, so review code that relied on
`Date`'s local-time methods. Convert with `new Date(instant.epochMilliseconds)`
where an API needs a `Date`.

## JSON

`JSON.stringify(job)` throws on `bigint`. Use `jobToJsonValue(job)` for a
JSON-safe copy and `jobFromJsonValue` to restore it. Don't add a global
`BigInt.prototype.toJSON`.

Arguments must be JSON. River rejects class instances, `bigint`, and integers
beyond `Number.MAX_SAFE_INTEGER` (which have already lost precision). Store
large integers as strings, or use `exactJsonNumber("9223372036854775807")`
when the wire format needs a JSON number. Numbers that other languages wrote
and JavaScript cannot represent exactly arrive as `ExactJsonNumber`; see
[exact values](./README.md#exact-values-and-json).

## Inserting without a transaction

An insertion without `{ tx }` now runs in a transaction River begins itself, so
its insert middleware and hooks commit or roll back with the jobs. River needs a
connection of its own for that:

- A `PgDriver` constructed from a single `pg.Client` or a checked-out
  `PoolClient`, rather than a `Pool`, now rejects insertions without `{ tx }`
  with a `ConfigurationError`. In 0.1 they ran directly on that client.
  Construct the driver with a `Pool`, or begin a transaction on the client and
  pass it as `{ tx }`.
- A `PrismaDriver` runs insertions without `{ tx }` in an interactive
  transaction on the root client's `$transaction`, so construct it with the
  root `PrismaClient`, not a transaction client.

On Postgres, the transaction adds a `BEGIN` and a `COMMIT` round trip to
each call that 0.1 ran as a single statement. To insert many jobs, pass them
to one `insertMany` call.

Fast batch insertion is not part of this release: it is held back until River
decides how it relates to extensions that act on every insertion. Code written
against a development build's `insertManyFast` should insert large batches with
one `insertMany` call instead, which differs in three ways: it resolves with one
result per row rather than a count; a unique conflict is reported as that row's
`status: "duplicate"` instead of being skipped (SQLite) or failing the whole
batch (Postgres); and insert middleware and hooks run for every row.

## Prisma

`@riverqueue/driver-prisma` still inserts jobs inside Prisma transactions. It
does not run workers: `new Client(new PrismaDriver(prisma))` is typed as an
insert-only client, so worker and query methods don't compile. Use
`@riverqueue/driver-pg` in worker processes.
