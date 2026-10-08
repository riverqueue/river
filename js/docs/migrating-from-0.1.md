# Migrating from `riverqueue` 0.1

`riverqueue` 0.1 was an insert-only client. This release adds workers and the
full runtime, and corrects types that could lose database values before 1.0
locks them in. Most changes are mechanical, and the compiler flags every site
that still needs attention.

## Runtime requirements

The package needs Node.js 26 with native `Temporal` and ships as ESM; see
[requirements](../README.md#requirements). There is no second CommonJS build
and no `Temporal` polyfill.

## Codemod

`@riverqueue/cli` includes a codemod for the mechanical part of this
migration. It needs the `typescript` package, version 5 or 6, installed in
the project it migrates:

```sh
npx riverqueue codemod-0.1 src           # report what would change
npx riverqueue codemod-0.1 --write src   # rewrite files in place
npx riverqueue codemod-0.1 --check src   # exit 1 if anything would change
```

Arguments are files, directories, or glob patterns. Pass every file that
declares or constructs argument classes in one run, so that call sites in
other files are rewritten against their classes. The codemod edits only the
expressions it rewrites and never reformats other code, so run your formatter
afterwards. Running it again changes nothing.

It rewrites the patterns that have one meaning:

- A `JobArgs` class whose persisted args are exactly its constructor parameter
  properties (with no `toJSON`, or one that returns exactly those properties)
  becomes a `defineJob<Args>()({ kind, defaults })` definition, and its
  `insertOpts` become `defaults`. `SortArgs` becomes `sort`, and imports and
  re-exports of the class follow the new name. `new SortArgs(a)` in an
  `insert` or `insertMany` call becomes the definition and a plain args
  object.
- `new JobArgsObject("kind", args)` in those calls uses a module-level
  unchecked `defineJob({ kind: "kind" })`, one per kind. It stays separate from
  a class definition of the same kind because the class's `insertOpts` never
  applied to it.
- `new InsertManyParams(args, options)` becomes `{ job, args, options }`.
- The `uniqueOpts` insert option becomes `unique`, and a numeric `byPeriod` in
  seconds becomes a duration such as `{ seconds: 60 }`.
- The `ClientOpts`, `InsertOpts`, and `UniqueOpts` types, renamed
  `ClientOptions`, `InsertOptions`, and `UniqueOptions`, are imported under
  their 0.1 names.
- `result.uniqueSkippedAsDuplicated` becomes `result.status === "duplicate"`,
  and its negation `result.status === "inserted"`.
- `JOB_STATE_AVAILABLE` and the other state constants become
  `JOB_STATE.available` and so on.
- `new Client(new PgDriver(pool), { schema })` moves the schema into
  `new PgDriver(pool, { schema })`.
- `riverqueue` imports drop what the rewrites replaced and add `defineJob`.

Everything that needs judgment gets a `// TODO(riverqueue-0.1): ...` comment,
and the command lists each marked site. That includes classes with methods,
computed kinds, constructor bodies, or a custom `toJSON`; argument objects
constructed outside insertion calls; non-literal `byPeriod` values; exports
that no longer exist; and uses of `JobRow.id` and `JobRow` timestamps. The
codemod deliberately leaves ID arithmetic, `number` annotations, `Date`
handling, and string formatting alone: `JobRow.id` is now a `bigint` and
timestamps are `Temporal.Instant`, as described below, and the compiler
reports each site that depends on the old types. A `Date` passed as
`scheduledAt` remains valid.

Generated definitions declare only the producer type. Add a schema or
`decode` callback before registering a worker for them, as described in the
next section.

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
accepts any JSON object. Give a definition a schema or a `decode` function
before registering a worker for it: a type annotation alone does not validate
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

A batch may name a unique key only once. Like River for Go, `insertMany`
rejects a batch that repeats one with a `ValidationError` and writes none of
it, where 0.1 inserted the first job and reported the others as duplicates.

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

Like River for Go, an insertion without `{ tx }` now runs in a transaction
River begins itself, so its insert middleware and hooks commit or roll back
with the jobs. River needs a connection of its own for that:

- A `PgDriver` constructed from a single `pg.Client` or a checked-out
  `PoolClient`, rather than a `Pool`, now rejects insertions without `{ tx }`
  with a `ConfigurationError`. In 0.1 they ran directly on that client. The
  codemod can't detect this. Construct the driver with a `Pool`, or begin a
  transaction on the client and pass it as `{ tx }`.
- A `PrismaDriver` runs insertions without `{ tx }` in an interactive
  transaction on the root client's `$transaction`, so construct it with the
  root `PrismaClient`, not a transaction client.

On Postgres, the transaction adds a `BEGIN` and a `COMMIT` round trip to
each call that 0.1 ran as a single statement. To insert many jobs, pass them
to one `insertMany` call.

River for Go's fast batch insertion (`InsertManyFast`) has no JavaScript
equivalent: it is held back until River for Go decides how it relates to
extensions that act on every insertion. Code written against a development
build's `insertManyFast` should insert large batches with one `insertMany`
call instead, which differs in three ways: it resolves with one result per
row rather than a count; a unique conflict is reported as that row's
`status: "duplicate"` instead of being skipped (SQLite) or failing the whole
batch (Postgres); and insert middleware and hooks run for every row.

## Prisma

`@riverqueue/driver-prisma` still inserts jobs inside Prisma transactions. It
does not run workers: `new Client(new PrismaDriver(prisma))` is typed as an
insert-only client, so worker and query methods don't compile. Use
`@riverqueue/driver-pg` in worker processes.
