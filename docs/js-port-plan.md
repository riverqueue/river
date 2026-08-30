# River JavaScript/TypeScript port plan

Status: implementation plan, researched 2026-08-30

This document specifies a complete, idiomatic JavaScript/TypeScript River
implementation that shares River databases with Go and Rust. It is an
execution plan, not a proposal that leaves routine design choices to the
implementer. Where an implementation spike is still required, this document
defines the preferred choice, the evidence the spike must collect, and the
fallback so work can continue without waiting for product input.

The implementation must not be published as part of this work. It should be
left in a state from which a later, explicit release decision could publish it.

## Executive decision

Build the full port by extending the existing public `riverqueue-js`
repository and its already-published `riverqueue`,
`@riverqueue/driver-pg`, and `@riverqueue/driver-prisma` packages. Do not create
a second JavaScript implementation in the Go repository. The public River Go
repository remains the canonical owner of database migrations, protocol
fixtures, scenario inventories, and the candidate-neutral conformance harness.
The JavaScript repository owns the native JavaScript API and runtime. River Pro
JavaScript code lives only in the private River Pro repository.

Use these defaults:

- Require Node.js 26 or newer for the full runtime. Node 26 enables Temporal by
  default, supplies the modern cancellation and async-context APIs River needs,
  and is scheduled to become LTS before an initial release should be considered.
  Do not publish while Node 26 is still a Current release.
- Publish compiled ESM JavaScript and declarations, not raw TypeScript and not
  a parallel CommonJS build. Avoid top-level await so Node's `require(esm)` can
  consume the same artifact where supported.
- Preserve the current insert-only capabilities while adding the complete
  runtime. It is acceptable to make the data-correctness type changes described
  below: job IDs become `bigint`, and persisted instants become
  `Temporal.Instant`. Do not retain a lossy `number`/`Date` mode.
- Make `@riverqueue/driver-pg` the complete PostgreSQL backend. Keep
  `@riverqueue/driver-prisma` as an insert and transaction adapter; Prisma is
  not a separate database engine and is not expected to host River's listener,
  leadership, claiming, or maintenance runtime.
- Add a first-party SQLite backend based on Node's built-in `node:sqlite`. Mark
  the backend preview while that Node API remains a release candidate, but
  implement and validate the complete River SQLite storage and runtime profiles.
- Keep the common job, worker, client, and extension APIs independent of the
  backend. A future first-party MySQL implementation must be additive and must
  pass a MySQL semantic profile before it is claimed as supported.
- Run ordinary async handlers in the main Node event loop. Promise concurrency
  is the correct default for database and network-bound jobs. Offer a separate,
  opt-in worker-thread executor for CPU-heavy or forcibly interruptible jobs;
  do not put all handlers in threads.
- Use the existing language-neutral conformance system as the compatibility
  authority. Do not compare Go, Rust, and TypeScript public symbol names.
- Implement River Pro only after the public engine's applicable gates pass.
  Keep all Pro code, fixtures, migrations, package metadata, and capability
  names out of the public River and `riverqueue-js` repositories.
- Treat the River website at `/Users/bgentry/River/homepage` as a read-only
  conceptual reference. Do not modify it in this work.

## Goals and non-goals

### Goals

The completed port must:

1. Insert, query, claim, work, complete, cancel, retry, rescue, schedule, clean,
   and migrate the same jobs as matched Go and Rust versions.
2. Safely run JavaScript and TypeScript workers with native promise concurrency,
   `AbortSignal` cancellation, graceful shutdown, stuck-job reporting, and an
   explicit hard-isolation option.
3. Share one PostgreSQL database with Go and Rust producers, workers, leaders,
   listeners, migrators, and maintenance services during a rolling deployment.
4. Implement the existing SQLite portable-storage and runtime profiles without
   silently pretending PostgreSQL-only behavior is portable.
5. Expose an API that feels designed for TypeScript: inferred job definitions,
   runtime payload validation without a required schema-library dependency,
   plain option objects, discriminated results, structured errors, async
   iterables, and standard cancellation/lifecycle primitives.
6. Preserve and improve the current TypeScript producer use case, including
   node-postgres and Prisma transactions.
7. Give future language ports a larger, genuinely reusable conformance suite
   rather than growing another language-specific checklist.
8. Port River Pro in the private repository, including its current public
   packages and protocol behavior, without leaking private implementation.
9. Produce API documentation, conceptual guides, migration guidance, benchmarks,
   package archives, and release checks, but perform no npm publication.
10. Be implemented autonomously in one sustained effort. Phases below are
    internal verification gates, not points at which the implementer should ask
    the user to approve routine choices.

### Non-goals

- Browser, edge-worker, Deno, and Bun runtimes are not initial targets. Some
  producer-only APIs may happen to work elsewhere, but no compatibility claim
  should be made without their own package and database-driver tests.
- Worker threads are not a security sandbox. Hostile handlers require a process
  or container boundary with separately managed permissions.
- River does not promise exactly-once side effects. The port preserves River's
  durable at-least-once model and continues to require idempotent jobs.
- Users cannot implement arbitrary full database engines through a stable SPI in
  the initial release. Insert adapters remain possible; full engines are
  first-party semantic implementations.
- The plan does not redesign River's persisted protocol to imitate another Node
  queue, nor does it import Redis-specific or delete-on-success semantics.
- The plan does not make the River documentation site multi-language.

## Current baseline

### Existing TypeScript package

`riverqueue-js` is an official repository, not a hypothetical starting point.
Its `riverqueue@0.1.0` package currently provides:

- `Client.insert` and `Client.insertMany`;
- `JobArgs`, `JobArgsObject`, `InsertManyParams`, insert options, unique-job
  hashing, and persisted job-row types;
- a generic insertion-driver interface;
- `@riverqueue/driver-pg` for node-postgres pools/clients and transactions; and
- `@riverqueue/driver-prisma` for Prisma and its transaction clients.

It is producer-only. It has no claim loop, handler registry, completion
pipeline, listener, leader election, maintenance services, migrator, query
surface, lifecycle, or worker runtime.

The implementation must start from the latest `origin/master` of that
repository and preserve its Git history. Before changing it, audit every commit
since the checked-out branch's merge base with `origin/master`, then rebase.
The package tarball and current docs should be retained as consumer fixtures so
the migration from 0.1.0 stays tested.

Known correctness and API issues to fix deliberately include:

- PostgreSQL `bigint` job IDs are converted to JavaScript `number`, which is not
  an exact representation over the column's full range.
- PostgreSQL microsecond timestamps are converted to `Date`, which truncates to
  milliseconds.
- plain `JSON.stringify` can silently turn non-finite numbers into `null`, omit
  `undefined`, and reject `bigint`; job args need an explicit JSON-domain codec.
- insert-option resolution uses truthiness in places where presence and value
  must be distinguished.
- the current class-oriented `JobArgsObject` and `InsertManyParams` surface is a
  Go-shaped convenience rather than the best TypeScript authoring API.
- the main export exposes an insertion driver interface that is described as
  internal but looks stable. Full engine operations must not turn this into an
  accidental permanent third-party database SPI.
- schema quoting, unique hashing, tags, batch limits, transaction behavior, and
  row decoding must be tested against the canonical fixtures rather than trusted
  because the implementations look similar.

The exact-value changes are intentionally breaking. This package is pre-1.0,
and a visible compile error is preferable to silent data loss. Supply a
migration guide and a small codemod where a mechanical rewrite is useful; do
not keep lossy compatibility aliases.

### Existing cross-language foundation

The public River repository already has most of the language-neutral foundation
this port needs:

- a versioned protocol manifest and normalized JSON schemas;
- canonical PostgreSQL and SQLite migration hashes;
- unique-key and protocol-value golden fixtures;
- executable PostgreSQL, SQLite storage, and SQLite runtime scenario
  inventories;
- a candidate-neutral JSON-RPC process adapter;
- mixed storage/runtime/fault/performance/soak harnesses; and
- a candidate descriptor explicitly designed for a future JavaScript process.

The JavaScript adapter should therefore be executable as, conceptually:

```json
{
  "application_name": "river-conformance-javascript",
  "command": ["node", "dist/conformance-adapter.js"],
  "implementation": "javascript",
  "restart_command": ["node", "dist/conformance-adapter.js"],
  "version": "0.46.0-alpha.1"
}
```

Do not fork the Go test suite into TypeScript. Add language-neutral fixtures or
adapter methods only when a behavior cannot be expressed by the current
contract. Every declarative scenario must continue to have an executable owner.

### Repository responsibilities

| Repository | Owns | Must not own |
|---|---|---|
| Public River (`river`) | Go implementation; Rust implementation; canonical migrations; protocol manifest, fixtures, schemas, scenario registry, and shared harness | River Pro code or JavaScript-native API implementation |
| Public `riverqueue-js` | JavaScript runtime, native API, first-party JS backends/adapters, JS tests/docs/CLI/package archives, JS conformance adapter | Canonical migration history or River Pro code |
| Private River Pro (`riverpro`) | Go/Rust/JavaScript Pro implementations, Pro migrations/fixtures/harnesses, private package metadata | Any private artifact copied to either public repository |
| Homepage | Existing conceptual and Go documentation | Changes during this implementation |

The JavaScript repository may contain generated migration bundles because npm
packages must be self-contained. Those bundles are generated mirrors, never a
second authority. A generator consumes an explicitly pinned River checkout,
records source hashes, and a verification target rejects any byte or ordering
drift. Do not hand-edit mirrored SQL.

## Research conclusions and prior art

The implementation should adopt ideas selectively rather than copy another
queue's surface.

### Node's concurrency model

Node's event loop is not a reason to serialize River jobs. Network and database
jobs should run concurrently as promises in one process. Node's own
[`worker_threads` documentation](https://nodejs.org/api/worker_threads.html)
says threads help CPU-intensive JavaScript and do not help much with I/O, for
which built-in asynchronous operations are more efficient. It also recommends a
pool rather than one new thread per task.

The practical split is:

- in-process async handlers for the default, lowest-overhead path;
- multiple Node processes/containers to use multiple cores and provide failure
  isolation for ordinary services; and
- an opt-in pooled worker-thread handler for CPU-heavy work or jobs whose
  JavaScript execution must be terminable after a grace period.

The database/control plane remains in the main event loop even when handler
code is isolated. This prevents a CPU-bound handler from starving claim,
notification, lease-renewal, completion, and shutdown bookkeeping.

### Graphile Worker

[Graphile Worker](https://worker.graphile.org/docs) demonstrates several sound
PostgreSQL/Node patterns: simple async handlers; `LISTEN`/`NOTIFY` plus polling;
`SKIP LOCKED`; an explicit supervised runner; `AbortSignal`; and batching/local
prefetch as an important throughput tool. Its
[TypeScript guidance](https://worker.graphile.org/docs/typescript) correctly
types external job payloads as `unknown` unless they are validated, because old
or non-TypeScript producers can insert them. It also recommends compiling tasks
to JavaScript for production.

Adopt those principles. Do not adopt global declaration merging as the primary
payload type system, task-directory magic as the library default, deletion on
success, or unbounded local prefetch. River's persisted state machine,
attempt identity, and cross-language jobs are the authority.

### BullMQ

BullMQ's documentation on
[local concurrency](https://docs.bullmq.io/guide/workers/concurrency) and
[sandboxed processors](https://docs.bullmq.io/guide/workers/sandboxed-processors)
captures the key Node failure mode: a CPU-bound handler can block the event loop
and prevent lock/bookkeeping work, while an isolated processor avoids that
failure. Adopt the explicit in-process versus isolated distinction. Do not
adopt Redis-specific queue semantics or imply that racing an arbitrary promise
against a timeout stops the underlying work.

### pg-boss

[pg-boss](https://github.com/timgit/pg-boss) is useful evidence for first-class
transaction adapters for node-postgres, Prisma, Kysely, Knex, and Drizzle, and
for caller-owned database connections. Preserve River's existing Prisma
adapter and design an explicit insert-adapter seam for future ORM transaction
integration. Do not copy its stringly job definitions or its “exactly once”
marketing terminology.

### Runtime validation

Job args come from databases and other languages, so a TypeScript generic is
not validation. Accept the
[`Standard Schema`](https://github.com/standard-schema/standard-schema)
interface structurally. This lets users choose Zod, Valibot, ArkType, or another
compatible validator without making one of them a River dependency. Definitions
without a schema keep args `unknown` at the worker boundary unless the user
provides an explicit decoder.

### CPU isolation

Use Node's worker-thread primitives through a small River-owned abstraction.
Prefer [Piscina](https://github.com/piscinajs/piscina) as the initial internal
pool implementation after a focused audit: it supports Node 24+, cancellation,
resource limits, async tracking, bounded queues, and pool metrics. Put it in a
separate optional package so ordinary River users do not install it. Pin it and
test its cancellation behavior; River's public API must not expose Piscina
types so the implementation remains replaceable.

### PostgreSQL values and transactions

[`node-postgres` type documentation](https://node-postgres.com/features/types)
confirms that JSON/JSONB is parsed with `JSON.parse`, timestamps become `Date`,
and PostgreSQL microseconds are truncated unless a custom parser is used. Its
[transaction documentation](https://node-postgres.com/features/transactions)
requires every statement in a transaction to use the same checked-out client.
River must therefore install query-scoped or driver-scoped exact parsers without
mutating process-global `pg` parsers and must never acquire a new pool client
inside an operation that receives a transaction client.

## Compatibility contract

Compatibility means the same persisted and externally observable behavior, not
matching method or constant names. A matched JavaScript version must agree with
Go and Rust on:

- migration lines, ordering, hashes, custom schema behavior, and down/up paths;
- every job and queue field, state, error, timestamp, JSON value, byte value,
  and reserved metadata transition;
- default and override precedence;
- unique-key inputs, canonical JSON, time-period truncation, hashes, conflicts,
  and returned existing rows;
- transaction visibility and rollback, including notification timing;
- claim ordering, queue pause semantics, kind handling, attempt identity, and
  lock behavior;
- completion, retry, discard, snooze, cancellation override, and external
  terminal-state races;
- notifications with polling recovery, reconnects, leadership leases/terms,
  maintenance ownership, and scheduling;
- hook/middleware/plugin order, error policy, subscriptions and lag reporting;
- periodic jobs, resumable work, and every public query/update operation; and
- Pro hashes, workflows, encryption, batch behavior, migrations, and expression
  evaluation in the private suite.

The protocol manifest records matched Go, Rust, and JavaScript versions and
capabilities. Use the corresponding River feature minor for the JavaScript
package line (for example `0.46.0-alpha.1`), while allowing language-specific
patch releases. A matching number alone is not compatibility; the checked
manifest and passing gates are.

Language-local API changes use TypeScript API baselines and consumer fixtures,
not the protocol revision. Protocol-visible changes update neutral fixtures,
scenario IDs, and every applicable adapter before a capability is marked
complete.

## Runtime and toolchain baseline

### Node.js

Target Node 26+ and declare it in every runtime package's `engines.node`. At
the time of this plan, [Node 26 is Current and Node 24 is
LTS](https://nodejs.org/en/about/previous-releases); Node 26 is scheduled for
LTS in October 2026. Node 26 is nevertheless the implementation baseline
because it enables [Temporal by
default](https://nodejs.org/en/blog/release/v26.0.0), avoiding a permanent
date/time polyfill boundary. The no-publication rule prevents exposing users to
a Current-only production requirement.

If implementation completes before Node 26 enters LTS, run CI on Node 26 but
leave every package unpublished. Do not lower the baseline or reintroduce
`Date` merely to publish earlier.

### TypeScript and JavaScript output

- Author strict TypeScript and publish compiled JavaScript plus `.d.ts`, source
  maps, and declaration maps.
- Use `module`/`moduleResolution: "NodeNext"`, an ES2025-or-newer target proven
  against Node 26, `verbatimModuleSyntax`, `exactOptionalPropertyTypes`,
  `noUncheckedIndexedAccess`, `noImplicitOverride`,
  `noUncheckedSideEffectImports`, and `useUnknownInCatchVariables`.
- Avoid TypeScript enums, namespaces, parameter properties, decorators, and
  other emitted type-only conveniences in the public API. Prefer `as const`
  value objects, unions, and plain classes/functions.
- Build with the newest stable compiler compatible with the lint/declaration
  toolchain. At this plan's date, TypeScript 7 has a fast new compiler but no
  compiler API until 7.1; keep a TypeScript 6 compatibility lane for tooling and
  declarations and a TypeScript 7 consumer/typecheck lane. Re-evaluate this
  mechanically at implementation time rather than asking the user.
- Keep the existing pnpm workspace and lockfile unless a measured tool defect
  requires replacement.
- Publish one ESM distribution. Node's own
  [package-publishing guidance](https://nodejs.org/learn/modules/publishing-a-package)
  recommends one format and explains the dual-package hazard. Provide a
  `default` export condition where needed so Node 26 `require(esm)` resolves the
  same module instance; never ship a second stateful CommonJS build.
- Do not bundle library code. Test the exact packed files and export map.

### Package layout

Keep packages cohesive and dependencies optional:

| Package | Purpose |
|---|---|
| `riverqueue` | Job definitions/types, client/runtime, workers, queries, outcomes, hooks, subscriptions, and legacy producer migration surface |
| `@riverqueue/driver-pg` | Complete node-postgres PostgreSQL backend and transaction type |
| `@riverqueue/driver-prisma` | PostgreSQL insert/transaction adapter only |
| `@riverqueue/driver-sqlite` | Complete `node:sqlite` backend |
| `@riverqueue/migrate` | Generated main-line migration bundle and library API |
| `@riverqueue/test` | Typed worker/job/client test helpers and deterministic fakes |
| `@riverqueue/worker-threads` | Optional CPU/isolation executor with no Piscina types in its public API |
| `@riverqueue/cli` | Migration commands and the compatible `bench` command |
| private workspace packages | Internal build/conformance utilities; `"private": true`, never packed |

Do not create a general `utils` package or fragment the state machine across
packages. First-party packages in a matched line use exact internal versions so
an old driver cannot silently run against a new operation contract. The full
driver operation boundary is exported only from an explicitly unstable
subpath, is version-branded, and is omitted from the root API. Existing insert
adapters get a narrow, documented insertion protocol rather than the entire
runtime SPI.

Private River Pro packages should use the natural npm scope while retaining the
same responsibility split as Go/Rust:

- `@riverqueue/pro`
- `@riverqueue/batch`
- `@riverqueue/encrypt`
- `@riverqueue/secretbox`
- `@riverqueue/workflow`
- `@riverqueue/pro-migrate`
- `@riverqueue/pro-test`

These names are preparation only. Do not reserve or publish them in this work.

## Public API design

The following shapes are normative at the design level. Exact spelling may be
refined if compile-tested examples reveal an ambiguity, but the underlying
ownership, type-safety, and behavior must remain.

### Exact public values

Use:

- `bigint` for job IDs and every persisted 64-bit integer that can exceed the
  safe-number range;
- `number` for bounded attempts, priorities, worker counts, and millisecond
  durations after explicit range/integer validation;
- `Temporal.Instant` for persisted absolute timestamps;
- `Uint8Array` for byte values;
- string literal unions plus `as const` objects for persisted states;
- `null` for a persisted nullable value and omission for an optional input;
  with `exactOptionalPropertyTypes`, `undefined` is not an implicit clear; and
- a recursively defined `JsonValue`/`JsonObject` domain that rejects
  `undefined`, functions, symbols, `bigint`, cycles, non-finite numbers, class
  instances without an explicit encoder, and unsafe prototype-merging paths.

`Temporal.Instant` already serializes to an ISO string, but native `bigint`
does not serialize through `JSON.stringify`. Export explicit
`jobToJsonValue`/`jobFromJsonValue` helpers and corresponding JSON-safe types
that encode IDs as decimal strings and instants as ISO strings. Do not attach a
surprising global `BigInt.prototype.toJSON` mutation.

The conformance adapter always uses the JSON-safe form. Opaque pagination
cursors retain the exact database timestamp and ID rather than rebuilding a
cursor from a millisecond value.

### Job definitions

Replace class inheritance as the recommended authoring model with a stable
definition value:

```ts
import { defineJob } from "riverqueue";
import { z } from "zod";

export const sendEmail = defineJob({
  kind: "send_email",
  schema: z.object({
    messageId: z.string(),
    to: z.string().email(),
  }),
  defaults: {
    maxAttempts: 5,
    queue: "email",
  },
});
```

The `schema` property accepts Standard Schema and infers its input/output types.
River validates on insertion and again before work because a Go/Rust/old
producer can bypass the JavaScript insertion path. The insertion path first
requires the supplied input to be a valid JSON object, then validates it, but
persists that canonical input rather than an arbitrary schema transform.
Schema defaults/transforms may produce the typed value handed to a worker;
`job.rawArgs` always contains the exact persisted JSON used for uniqueness.
Definitions that intentionally transform what is persisted need a separate
explicit `encode` function whose result is validated as River JSON. This keeps
validation-library behavior from silently changing cross-language hashes.

For a definition without a schema, worker args are `unknown`. Permit an
explicit `decode(value): T | Promise<T>` callback for users who do not use a
Standard Schema library. A compile-time generic without a decoder may type
producer input, but it must not make externally sourced worker input appear
runtime-validated.

Definitions are cheap immutable values and contain no pool, client, or handler.
This lets web producers import a job definition without importing worker-only
dependencies. Reject empty/reserved kinds and duplicate registrations during
configuration.

### Workers and work results

Register handlers separately and infer their args from the definition:

```ts
import { Workers, complete, snooze } from "riverqueue";

const workers = new Workers();

workers.add(sendEmail, async ({ client, job, logger, signal }) => {
  const response = await mailer.send(job.args, { signal });

  if (response.rateLimited) {
    return snooze({ seconds: 30 });
  }

  return complete({ output: { providerId: response.id } });
});
```

Use one named context object so the API can grow without positional churn.
Include the typed `job`, `signal`, current `client`, job-scoped logger, and
documented execution metadata. Worker-local retry/timeout policy is configured
on registration with named options, not subclass overrides.

Returning `undefined` means ordinary successful completion. Special outcomes
are closed discriminated values constructed by helpers such as `complete`,
`snooze`, and `discard`; throwing/rejecting means failure and invokes the River
retry/discard policy. Add only the helpers needed to represent existing River
outcomes. Do not treat an arbitrary returned object as recorded output, and do
not use magic exception subclasses for successful control flow.

The runtime must catch synchronous throws, promise rejections, and non-`Error`
throws. Persist the canonical message and trace with bounded size while retaining
the original error and `cause` chain for local hooks/logging. Never include job
args or secrets in an error/log unless the application explicitly opts in.

### Client construction and lifecycle

Continue the recognizable driver-first construction model while expanding
configuration:

```ts
import { Client } from "riverqueue";
import { PgDriver } from "@riverqueue/driver-pg";

const driver = new PgDriver(pool, { schema: "river" });
const client = new Client(driver, {
  queues: {
    default: { maxWorkers: 100 },
    email: { maxWorkers: 50 },
  },
  workers,
});

await using run = await client.start();
```

The client is a concrete runtime concept; do not parameterize it by the job
registry. A narrowly inferred driver/transaction capability generic is
acceptable to keep `{ tx }` type-safe, but it must not appear in worker/job
types or require applications to maintain a global type registry.

Construction validates the entire static configuration before doing background
work. A client with no queues/workers is a valid insert-only client and starts
no leader or maintenance services until explicitly started. A driver that only
supports insertion, such as Prisma, exposes insertion at compile time and
returns a structured capability error if runtime startup is reached through
untyped JavaScript.

`start()` returns a supervised `RunHandle` with:

- `stop({ mode: "graceful" | "cancel", timeoutMs?, signal? })`;
- a `completed` promise that rejects on fatal background failure;
- state/diagnostic inspection; and
- `[Symbol.asyncDispose]` so `await using` performs graceful stop.

JavaScript has no reliable `must_use` or drop hook, and garbage collection is
not lifecycle control. Do not use a finalizer to stop a client. Do not install
global `SIGINT`, `SIGTERM`, `unhandledRejection`, or `uncaughtException`
handlers from the library. The CLI and an opt-in `installSignalHandlers` helper
may own signal wiring and must remove it on shutdown.

### Insertion and transactions

The new typed insertion surface is:

```ts
const result = await client.insert(sendEmail, {
  messageId: "msg_123",
  to: "person@example.com",
});

if (result.status === "inserted") {
  console.log(result.job.id);
}

const results = await client.insertMany([
  { job: sendEmail, args: { messageId: "a", to: "a@example.com" } },
  {
    job: sendEmail,
    args: { messageId: "b", to: "b@example.com" },
    options: { queue: "priority" },
  },
]);
```

Use a discriminated `status: "inserted" | "duplicate"` rather than the current
awkward boolean name. Preserve input order and atomicity. Plain batch item
objects replace `new InsertManyParams(...)` in recommended code.

Keep caller-owned transaction semantics and the established `{ tx }` pattern:

```ts
const pgClient = await pool.connect();
try {
  await pgClient.query("BEGIN");
  await writeApplicationRows(pgClient);
  await client.insert(sendEmail, args, { tx: pgClient });
  await pgClient.query("COMMIT");
} catch (error) {
  await pgClient.query("ROLLBACK");
  throw error;
} finally {
  pgClient.release();
}
```

Prisma retains the same shape in `$transaction`. The driver must validate the
transaction/backend pairing before hooks, queries, or notifications have side
effects. A transaction operation never checks out another connection. River
does not issue `BEGIN` or commit a caller-owned transaction. PostgreSQL
notifications naturally become visible only on commit; SQLite uses its durable
transactional outbox.

Options use explicit units for every duration (`timeoutMs`,
`pollIntervalMs`, `byPeriodSeconds`, or a named duration object). Do not expose
ambiguous bare `timeout: number` fields. Validate integer/range constraints and
apply precedence in one shared resolver:

1. call-site override;
2. job-definition defaults;
3. client defaults;
4. River protocol defaults.

### Queries, updates, and pagination

Expose the complete River query surface through typed option objects: get,
list, keyset pagination, update, delete, cancel, retry, queue operations, and
transaction variants. Return `null` for not-found where absence is ordinary and
throw structured errors for operational failure. Update patches distinguish
omitted from explicit `null` and reject `undefined` under strict TypeScript.

Pagination cursors are opaque versioned strings. Their decoded representation
is internal and exact. List results contain `nextCursor: string | null`; users
must not concatenate SQL fragments or construct cursor records.

### Cancellation and stuck jobs

Every work context gets one `AbortSignal` composed from client shutdown,
job timeout, remote cancellation, and an executor-specific abort. Use
`AbortSignal.any`, `reason`, and `throwIfAborted` rather than a River-specific
cancellation token. All River-owned waits and timers accept the signal and use
unreferenced timers where an idle timer should not keep the process alive.

An in-process handler cannot be forcibly stopped if it never yields. On the
stuck threshold:

1. abort its signal once with a typed reason;
2. report a structured stuck event and invoke the configured handler;
3. replace the capacity slot only according to River's matched stuck-job
   semantics;
4. retain attempt identity so a late result cannot overwrite a newer attempt;
5. never mark a `Promise.race` winner as proof that the underlying handler
   stopped; and
6. monitor event-loop delay so a synchronous loop is diagnosable.

For a worker-thread handler, send cooperative abort first, wait the configured
grace period, then terminate the thread/pool task. Persist the canonical
retry/cancellation result only after the executor reports termination, and
still guard it by attempt identity. Isolated handlers are referenced by an ESM
module URL and export name because closures, pools, sockets, and arbitrary
object graphs cannot be transferred safely. Messages contain only validated
structured-clone/JSON data and bounded error/result records.

### Hooks, middleware, plugins, events, and context

Use ordinary functions and objects rather than decorators. A Koa-like
`async (context, next) => ...` middleware function is idiomatic for around-work
composition, but River's documented begin/end ordering and transaction boundary
remain authoritative. Separate insertion, work, and maintenance extension
contexts so an impossible combination cannot be constructed.

Completion concurrency must not change extension behavior:

- work-end hooks and local error policy run exactly once for the matching
  attempt before its completion item is enqueued;
- database completion and after-commit events run exactly once for the row that
  actually transitioned;
- stale/external-terminal results produce the documented race observation,
  never a duplicate ordinary completion;
- per-job ordering is preserved while unrelated jobs have no global completion
  order; and
- plugin failures follow the same fatal/nonfatal policy as Go/Rust.

Use `AsyncLocalStorage.run` for optional job-scoped context such as job ID,
kind, attempt, logger, and trace correlation. Do not use `enterWith` for async
work. Publish stable, namespaced low-level observations through
`diagnostics_channel`; keep an OpenTelemetry bridge in an optional integration
package so the core does not require the OTel SDK.

Subscriptions are bounded async iterables, not unbounded `EventEmitter`
buffers. A slow subscriber receives an explicit lag record/error with the
dropped count. Events are emitted after the corresponding database transition
commits. Closing or aborting a subscription is deterministic and releases all
listeners.

### Error model

Export a small hierarchy rooted at `RiverError`, with discriminants for at
least configuration/validation, unsupported capability, backend mismatch,
database operation, migration, lifecycle/background failure, worker payload,
extension, and subscription lag. Preserve `cause`. Include operation, backend,
job/queue identity, and retryability where useful, but do not create a distinct
class for every validation sentence.

Unknown persisted states or protocol values are errors, not aliases for an
existing value. Expected misuse returns a typed error or is prevented by the
types; internal invariant failures remain visibly different.

### Migration from `riverqueue@0.1.0`

Provide a focused migration document and package consumer fixtures covering:

- `JobRow.id: number` to `bigint`;
- `Date` fields/options to `Temporal.Instant`;
- JSON serialization through the explicit JSON-safe helper;
- new `defineJob` and `client.insert(definition, args)` usage;
- class-based `JobArgs` to definition values;
- `InsertManyParams` to plain batch items;
- insert result boolean to discriminated status;
- database-specific schema configuration on the backend;
- Prisma's intentionally insert-only capability; and
- ESM/Node 26 requirements.

A codemod may rewrite property names and obvious constructor patterns. It must
not guess the semantics of ID arithmetic, string formatting, or timezone
conversion. Compiler errors plus concise recipes make those remaining sites
straightforward for humans or coding agents.

## Internal architecture

Keep the architecture recognizably aligned with Go/Rust while using native
Node primitives.

### Layers

1. **Protocol values and codecs** — states, exact timestamps/IDs, JSON domain,
   unique hashes, errors, metadata, cursors, and neutral fixtures.
2. **Semantic backend operations** — insert, claim, transition, query, queue,
   notification, leadership, maintenance, and migration operations. SQL stays
   in first-party backend packages; the core never branches on SQL strings.
3. **Client/runtime coordinator** — lifecycle, queues, notifications, leader
   services, fatal-error supervision, and configuration.
4. **Queue producers** — calculate available capacity, claim bounded batches,
   and dispatch attempts.
5. **Attempt executor** — validate/decode args, construct context, run
   middleware/handler/error policy, and emit one attempt outcome.
6. **Completion coordinator** — batch outcomes, persist transitions with bounded
   parallelism, resolve races, and emit after-commit observations.
7. **Leader-owned services** — scheduler, rescuer, cleaners, periodic enqueuer,
   and PostgreSQL reindexing where applicable.
8. **Public extensions/test support** — stable contexts and typed fakes over the
   core; no backend SQL leakage.

Each background task is owned by a supervisor. An unexpected task rejection is
observed immediately, aborts dependent tasks, and rejects `RunHandle.completed`;
it must never become an unhandled rejection or disappear in a detached promise.

### Async queues and backpressure

Implement small internal bounded async queues with explicit close, abort,
capacity, and waiter cleanup semantics. Do not add a general reactive-stream
dependency. Prove with tests that cancellation removes waiters and that shutdown
drains or rejects every pending item exactly once.

Claim only enough jobs for available slots plus a small, measured bounded
prefetch. A large local locked queue improves synthetic throughput but harms
crash recovery and latency. Make the default conservative and expose a bounded
backend/runtime option only after benchmarks and fault tests quantify it.

Completion persistence follows River's bounded two-way design: one full batch
may be in flight while a second batch accumulates, and at most two persistence
queries run concurrently. By default, launch the second query only when that
second batch is full. A measured implementation may lower the threshold, but
never below 75 percent and only when benchmarks show a material improvement and
the hook/race/stress suites remain clean. Preserve the exact attempt-to-result
mapping by job ID/attempt token, never by array position after filtering.

### Time and deterministic scheduling

Use database time for protocol decisions where Go/Rust do. Inject a monotonic
clock/timer abstraction into runtime services for deterministic local tests;
do not globally monkey-patch time. Persist `Temporal.Instant`, and convert
durations with checked integer arithmetic. Account for event-loop delay when
renewing leases and reporting stuck work, but do not silently extend protocol
leases based on local lag.

## Database backends

### PostgreSQL through node-postgres

`@riverqueue/driver-pg` becomes the complete baseline backend.

- Accept a caller-owned `pg.Pool` for a runtime client and never call
  `pool.end()`. A queryable client remains valid for producer-only use, but
  runtime construction rejects it because River needs independently leased
  connections.
- Use ordinary pooled queries for short operations and dedicated leased clients
  for `LISTEN`/`NOTIFY` and other session-scoped work. Release/recreate those
  clients on reconnect and shutdown without leaks.
- Parameterize all values. Validate and centrally quote the configured schema;
  never interpolate user-provided identifiers or job kinds directly.
- Use query-scoped/driver-owned type parsing. Decode `int8` to `bigint` and
  `timestamptz` text/binary to exact `Temporal.Instant`; do not mutate global
  `pg.types` because the pool belongs to the caller.
- Benchmark row-object versus `rowMode: "array"`, prepared statements,
  pipelining where supported, and text versus binary result formats. Choose the
  simplest exact path that meets the throughput gate. Do not assume binary is
  faster for every node-postgres type.
- Match Go/Rust batch insertion mechanisms in comparative benchmarks. Do not
  inflate JavaScript benchmark results with a JavaScript-only COPY producer
  unless the comparison explicitly measures equivalent fast-insert APIs.
- Chunk batches below PostgreSQL parameter/protocol limits without losing
  atomicity, result order, or unique duplicate mapping.
- Preserve `LISTEN`/`NOTIFY` recovery through periodic polling. Document that
  PgBouncer transaction pooling cannot host session listeners; support a
  separate direct listener pool or notifications-disabled polling mode.
- Size and expose connection usage: queue operations borrow from the pool;
  listeners and leader services use documented dedicated capacity. Detect
  obvious starvation and expose pool-wait metrics rather than deadlocking.

Keep Prisma's raw-query adapter narrow. Validate schema before any unavoidable
raw SQL construction and prefer Prisma's parameterized facilities. It supports
producer transactions but does not claim full runtime, migration, listener, or
maintenance capability.

### SQLite through `node:sqlite`

Use a caller-owned `DatabaseSync`, WAL, foreign-key settings, and the canonical
busy timeout. A single internal statement factory must call
`StatementSync.setReadBigInts(true)` on every result-producing statement so no
64-bit value passes through `number`. Node's
[`node:sqlite` API](https://nodejs.org/api/sqlite.html) is release-candidate and
synchronous; do not wrap synchronous statements in promises and claim they
became nonblocking.

River's SQLite operations are intentionally short and serialized. Document that
a heavily loaded SQLite worker service should run in its own Node process so it
does not block an HTTP server's event loop. Do not proxy the database through a
hidden worker thread in the initial backend: that would break caller-owned
transaction identity and complicate closure/lifecycle semantics.

Provide a driver-owned transaction helper that serializes `BEGIN`/commit/
rollback on the same `DatabaseSync` and yields a branded transaction token plus
access to that database for application SQL. River operations receiving the
token execute inline on that same handle and never nest or acquire another
connection. Test thrown/rejected callbacks, nested-transaction rejection, and
visibility/rollback. Document that awaiting unrelated I/O while holding the
SQLite transaction extends its lock.

Implement the existing SQLite durable notification outbox, polling, leadership,
scheduler/periodic work, queue runtime, transactions, exact millisecond storage,
and every applicable profile scenario. PostgreSQL-only custom schemas, COPY,
`SKIP LOCKED`, concurrent reindexing, and backend fault modes remain explicitly
unsupported rather than emulated poorly.

### Future MySQL

Do not include a placeholder MySQL package. Keep common APIs free of PostgreSQL
types and model the internal backend boundary in semantic operations so MySQL
can be added without changing workers or clients.

A MySQL port is supportable only after it has:

- canonical migrations and a manifest backend entry;
- exact row/JSON/time/ID codecs;
- atomic claim ordering and locking for the supported MySQL versions;
- transactional inserts/completions and notification/outbox polling;
- leadership leases/terms and all applicable maintenance services;
- migration, storage, runtime, mixed-language, fault, and soak scenarios; and
- a first-party async driver selected after a separate ecosystem/performance
  review.

Do not reduce the public API to a boolean capability bag. Backend-only options
belong to the backend constructor, and unsupported operations return structured
capability errors.

## River Pro plan

Start the private JavaScript workspace only after the public PostgreSQL engine
and its adapter pass the full shared suite. Rebase the private River Pro branch
on current `origin/master`, inspect every new Go/Rust Pro commit since the merge
base, and classify each change as protocol-visible, common API, or
language-local before porting it.

Use composition, not inheritance: a `ProClient` owns/exposes its core `Client`
explicitly and adds Pro behavior without duplicating common lifecycle or backend
configuration. Pin the exact compatible public JavaScript package version.

Port the current Pro packages and their complete behavior:

- Pro client/configuration and Pro database semantics;
- batch operations and attempt-safe keyed outcomes;
- workflows, waits, signals, timers, transitions, evaluators, and durable wire
  versions;
- encryption and secretbox formats, key selection, recorded metadata, and
  failure behavior;
- Pro migrator and historical migration paths; and
- typed private test support and private conformance adapter.

### CEL compatibility

River core must not depend on CEL. In River Pro, run a mandatory evaluator spike
before choosing an implementation:

1. Execute all existing Go and Rust Pro CEL goldens, every River-specific
   function/type case, error/null/unknown behavior, timestamp/duration case, and
   the relevant official `cel-spec` data against
   [`@bufbuild/cel`](https://github.com/bufbuild/cel-es).
2. Compare values, type checking, overload selection, error classification, and
   messages where the persisted protocol observes them.
3. If the beta pure-TypeScript implementation passes, pin its exact version
   behind a River-owned compatibility adapter and add the goldens permanently.
4. If it fails, use the existing Rust/Go implementation as the oracle and spike
   a CEL C++/Wasm or maintained native binding. Keep that dependency private to
   the workflow package. Do not weaken expressions or ask the user to choose
   between incomplete engines.

### Cryptography

Do not implement cryptographic primitives by hand. Select an audited Node,
Wasm, or maintained library that exactly reproduces the Go/Rust wire format and
passes cross-language known-answer, malformed-input, rotation, and tamper tests.
Keep keys in bounded byte buffers and overwrite owned buffers on disposal where
possible, while documenting that JavaScript garbage collection cannot guarantee
complete memory zeroization. Never log secret values or expose them through
`inspect`/JSON helpers.

### Private boundary

All Pro SQL, migrations, package names, scenario IDs, fixtures, adapter methods,
benchmarks, docs, and generated artifacts stay in the private repository. The
public extension seam may gain neutral capabilities required by Pro, but its
names and tests must describe generic semantics. Before every public commit,
search the staged diff for Pro identifiers and private paths.

## Testing and shared conformance

### Evidence layers

No single suite is sufficient. Require all of these:

| Layer | Evidence |
|---|---|
| Existing Go/Rust suites | Canonical current behavior and regression protection |
| JavaScript unit/type tests | Native API, exact codecs, validation, options, errors, lifecycle, async queues, and package types |
| JavaScript backend integration | PostgreSQL/SQLite operations, transactions, listeners/outbox, reconnection, claiming, leadership, and migrations |
| Neutral fixtures | IDs, timestamps, JSON edge cases, unique hashes, cursors, errors, metadata, and migration hashes |
| Go-reference process harness | Complete candidate-neutral PostgreSQL and SQLite scenario inventories |
| Direct multi-candidate smoke/soak | Go/Rust/JavaScript competition and failover against one database |
| Private Pro suite | Pro Go/Rust/JavaScript behavior without public leakage |
| Package consumer matrix | Packed ESM/import/require, JavaScript, TS6/TS7, node-postgres, Prisma, SQLite, and examples |

### JavaScript-local tests

Keep Vitest if it remains compatible and useful for source-level tests, but also
run packed built JavaScript with Node's built-in test runner so a transpiler
cannot hide package/export failures. Inject clocks and executors rather than
using timing sleeps. Use property-based tests for JSON codecs, unique hashing,
cursors, and state-machine command sequences where it finds edge cases beyond
tables.

Test at least:

- every exported value/type and compile-time inference/error fixture;
- invalid JSON including `undefined`, holes, non-finite numbers, cycles,
  accessors/prototypes, `bigint`, invalid Unicode, and dangerous keys;
- values around `Number.MAX_SAFE_INTEGER`, PostgreSQL `int8` bounds, negative
  values, timestamp microseconds, DST-independent instants, and cursor ties;
- option precedence, explicit zero/empty/null, and batch heterogeneity;
- duplicate unique keys within one batch and concurrent across languages;
- sync throws, rejections, non-Error throws, causes, handler timeouts, remote
  cancellation, shutdown cancellation, and late results;
- event-loop blocking detection and worker-thread cooperative/forced stop;
- async queue abort/close races, subscription lag, listener reconnect, fatal
  task supervision, and no unhandled rejections/open handles;
- hook/middleware/plugin ordering around successful, failed, snoozed, cancelled,
  externally finalized, and rolled-back work;
- caller-owned pools/databases are never closed and transaction operations never
  escape their connection;
- package archives contain only intended files and no private/source secrets.

Use deterministic fault injection at semantic backend boundaries for JavaScript
local tests; PostgreSQL harness fault scenarios still use the real database.

### Shared adapter and scenarios

Implement every method in `conformance/adapter/contract.json` exactly and report
the sorted inventory/version/capabilities at startup. Normalize `bigint` and
Temporal values only at the JSON-RPC edge. Stdout is protocol-only; diagnostics
go to stderr with secrets redacted.

Run:

- Go reference + JavaScript PostgreSQL candidate for the complete core profile;
- Go reference + JavaScript SQLite candidate for portable storage and runtime;
- all restart/fault/performance/soak tiers applicable to the backend; and
- direct mixed smoke scenarios in which Rust and JavaScript insert/work,
  compete, cancel, notify, lead/fail over, and recover one another's abandoned
  attempts.

Keep Go as the hub oracle rather than building an O(n²) complete matrix for
every future language. Extend the harness to accept multiple candidate
descriptors for a small three-engine smoke/soak profile. This catches
candidate-candidate interaction while retaining one canonical scenario
definition.

Add neutral fixtures for JavaScript-specific danger zones only when they
describe universal protocol behavior: JSON key ordering/selection, Unicode,
unsafe numeric values, exact IDs/timestamps, and error serialization. Do not add
a public-API-name manifest or assert that constants have the same spelling.

### Coverage accounting

Create a JavaScript coverage matrix keyed by the existing stable scenario IDs
and local test identifiers. A generator verifies that every applicable
PostgreSQL/SQLite capability has both a shared executable scenario and the
necessary JavaScript-native tests. Do not use a hand-checked percentage alone;
line/branch coverage is supplementary evidence, not behavioral parity.

## Performance plan

Add `@riverqueue/cli`'s `riverqueue bench` with the same workload and default
database behavior as Go's `river bench` and Rust's `riverqueue bench`. A typical
development command should be unambiguous, for example:

```sh
pnpm --filter @riverqueue/cli exec riverqueue bench \
  --database-url postgres://localhost/river_js_test
```

The command must warn before truncating the selected table, accept an explicit
database URL, handle signals, drain completions, and report worked/inserted
jobs, interval and overall jobs/sec, p95 latency, duration, and relevant runtime
diagnostics. A Make target in the River integration checkout may wrap it, but
the package CLI works independently.

Benchmark release-built JavaScript with the same PostgreSQL server, schema,
pool size, worker concurrency, job function, insertion mechanism, and warm-up as
Go/Rust. Focus optimization on the worker/completion path rather than winning by
changing only the producer mechanism.

Instrument before optimizing:

- claim query and decode time;
- pool wait and active/dedicated connection counts;
- handler scheduling/run time;
- completion queue depth, batch fill, query latency, and second-query rate;
- notification-to-fetch latency and polling wakeups;
- event-loop delay/utilization;
- heap, allocation, GC pause, and retained job records; and
- worker-thread queue/run time when isolation is enabled.

Likely high-value optimizations are exact custom row decoding with array mode,
fetch batch sizing, conservative local prefetch, prepared statement reuse,
object-allocation reduction, and bounded two-way completion persistence. Do not
parallelize hooks or per-attempt transitions in a way that changes their
ordering. Do not use unbounded `Promise.all` or one database promise per job.

Set release gates from measured baselines on controlled hardware:

- no unexplained multi-fold throughput gap caused by serial JavaScript code;
- p95 remains bounded under sustained equal-rate insert/work load;
- throughput and latency do not degrade materially across a long soak;
- no unbounded heap, listener, waiter, locked-job, or connection growth; and
- correctness/fault scenarios pass with performance features enabled.

Do not prescribe that one Node process must beat Go on every workload. Document
the single-process result and a multi-process result. Node can scale I/O work
well in one event loop and scale CPU/control overhead with ordinary process
replicas; River should expose this cleanly rather than hide a process manager in
the library.

## Documentation and API quality

Generate browsable API documentation with TypeDoc, analogous to godoc/rustdoc,
and check in an API Extractor-style declaration report for reviewable semver
diffs. Every package README is compiled/tested from source snippets so examples
cannot drift.

Write JavaScript-native conceptual docs in `riverqueue-js`, drawing from the
homepage and Go/Rust docs without editing or copying Go syntax mechanically:

- install/runtime support and the 0.1 migration;
- define, validate, insert, work, and test jobs;
- transactions with pg and Prisma;
- JSON, `bigint`, Temporal, and JSON-safe serialization;
- concurrency, the event loop, CPU isolation, and process scaling;
- retries, errors, cancellation, stuck jobs, and graceful shutdown;
- unique, scheduled, periodic, and resumable jobs;
- queues, pausing, querying, subscriptions, hooks, middleware, and plugins;
- migrations, custom PostgreSQL schemas, PgBouncer, and pool sizing;
- PostgreSQL/SQLite behavior and future backend policy;
- logging, metrics, diagnostics channel, and OpenTelemetry;
- mixed Go/Rust/JavaScript deployments, version matching, rollback, and rescue;
- benchmark interpretation and production checklist; and
- private Pro package docs in the private repository.

Prefer small runnable examples. Add a producer-only example, an in-process
worker, a thread-isolated CPU worker, node-postgres transaction, Prisma
transaction, SQLite worker, graceful shutdown, hooks/metrics, and mixed-language
example. Run examples against packed packages, not workspace source aliases.

Use `publint`, `@arethetypeswrong/cli`, API/declaration review, license checks,
dependency review, source-map inspection, and `npm pack --dry-run`/archive
consumer installs. Configure future trusted publishing/provenance without
supplying credentials or invoking `npm publish`.

## Security and operational constraints

- Parameterize all values and centrally validate/quote identifiers.
- Never merge decoded args/metadata into option objects with `Object.assign` or
  object spread unless dangerous keys and prototypes are neutralized. Prefer
  null-prototype dictionaries or explicit key copies at trust boundaries.
- Bound payloads, errors, stacks, event buffers, queues, and batch sizes.
- Validate job args on insertion and work; TypeScript types alone are not a
  trust boundary.
- Keep database credentials in caller-owned pools/configuration and out of
  errors, process-adapter output, snapshots, and package artifacts.
- Treat worker threads as availability isolation, not hostile-code containment.
- Pin runtime dependencies and audit licenses/supply-chain provenance. Keep
  core runtime dependencies minimal; validation libraries, OTel, and thread
  pools remain optional/structural.
- Do not execute migrations automatically on ordinary client construction or
  start. Migrations are an explicit deploy/CLI/library action.
- Do not install process-global handlers or mutate global pg/BigInt behavior.
- Fail startup on a newer unsupported migration/protocol version rather than
  attempting to run.

## Maintaining alignment after the port

Every upstream change follows this protocol:

1. Fetch and inspect the commit range from the implementation branch's merge
   base to each repository's latest `origin/master`; do not rely only on a
   changelog.
2. Classify each commit as persisted protocol, shared runtime behavior,
   backend-specific behavior, language-local API, docs/tooling, or no analogue.
3. For protocol behavior, change the canonical River artifact/scenario first,
   then Go, Rust, JavaScript, and private Pro where applicable.
4. Generate migration/package mirrors; never edit mirrors.
5. Update the manifest's matched versions/capabilities only after every
   applicable implementation passes.
6. Add TypeScript-local tests for its native hazards and shared scenarios for
   cross-language behavior.
7. Record intentional language differences in the compatibility docs.
8. Run public package/conformance gates and the private downstream suite before
   merging the public protocol change.

CI should have a scheduled alignment job that checks out the manifest-pinned
River and JavaScript commits, regenerates/compares artifacts, and runs the full
mixed suite. A bot may open an issue/PR when versions drift, but a human or agent
still audits semantic commits. API Extractor reports catch JavaScript breaking
changes; they are not compared to Go/Rust symbol inventories.

For coordinated releases, merge canonical protocol changes first with old/new
rolling-compatibility behavior where required, then publish matched language
packages only after all repositories are ready. Numeric package alignment is a
convenience; the manifest is the authority.

## Implementation sequence and commit strategy

The implementer should execute all phases without treating them as user
checkpoints. Keep commits reviewable and buildable. Fixups discovered later
should be folded into the commit that introduced the incomplete behavior when
that produces honest history; use a new commit for a distinct later design or
upstream change.

### Phase 0: align and inventory

- Fetch and rebase public River, `riverqueue-js`, and private River Pro feature
  branches on their current `origin/master`.
- Record merge bases and audit all newly introduced commits in order.
- Run existing Go/Rust/JavaScript/Pro tests and capture benchmark baselines.
- Map the Go/Rust feature matrix to JavaScript work items and existing scenario
  IDs.
- Confirm no local untracked/user files are touched.

Gate: clean baselines or documented pre-existing failures with reproduction.

### Phase 1: modern producer/API foundation

- Update Node/TypeScript/package tooling and exact-value/JSON codecs.
- Add `defineJob`, Standard Schema support, typed insertion/batches, structured
  results/errors, and migration fixtures.
- Refactor the narrow insert driver contract without breaking the ability of pg
  and Prisma adapters to transact.
- Add package consumer, codemod, and 0.1 migration tests.
- Port neutral unique/protocol fixtures into executable JavaScript tests.

Gate: producer-only Go-to-JS/JS-to-Go insert/unique/transaction compatibility,
packed consumer matrix, no lossy values.

### Phase 2: backend operations, migrations, and queries

- Generate the canonical migration package and verification hashes.
- Implement the complete PostgreSQL semantic operation set and exact decoding.
- Implement explicit migrator/CLI operations and historical/custom-schema
  migration tests.
- Implement public get/list/update/delete/retry/cancel/queue operations and
  transaction variants.
- Build enough of the conformance adapter to run storage/codec/query scenarios.

Gate: all PostgreSQL storage/migration/query scenarios pass against Go.

### Phase 3: runtime happy path and lifecycle

- Add worker registry, typed validation, client configuration, run supervisor,
  queue producers, attempt executor, completion coordinator, notifications, and
  graceful/cancel stop.
- Port default retry/error behavior and complete/snooze/discard outcomes.
- Add async context, structured logging, and fatal-background-task handling.

Gate: ordinary insert/work/complete/retry/snooze and lifecycle scenarios pass;
no unhandled rejections/open handles.

### Phase 4: race-hardening and extensions

- Implement attempt identity, remote cancellation, external-terminal races,
  rescues, stuck detection, bounded two-way completion, hook/middleware/plugin
  order, events, subscriptions, and lag.
- Add deterministic adversarial/fault tests for all combinations, including a
  full second completion batch threshold before the parallel query starts.
- Add optional worker-thread execution and forced termination tests.

Gate: completion/cancellation/fault/extension suites and repeated stress runs
pass with zero lost/duplicated outcomes.

### Phase 5: queues, leadership, maintenance, and higher-level jobs

- Complete dynamic queues/pause/reconfigure, listener reconnect/poll recovery,
  leader election/renewal/resignation/terms, scheduler, rescuer, cleaners,
  periodic jobs, reindexing, and resumable jobs.
- Match Go/Rust ownership and error behavior under process death/failover.

Gate: every applicable PostgreSQL feature-matrix row and scenario ID complete.

### Phase 6: SQLite

- Add generated migrations and `node:sqlite` backend.
- Complete storage, transactions, outbox notifications, workers/queues,
  cancellation, leadership, scheduler/periodic, subscriptions/extensions, and
  lifecycle profiles.
- Add event-loop blocking documentation and process-isolation example.

Gate: complete portable-storage and SQLite-runtime profiles in both directions.

### Phase 7: conformance, performance, docs, and packaging closure

- Finish the adapter inventory, multi-candidate smoke profile, performance and
  soak gates.
- Implement compatible bench CLI and tune measured bottlenecks without changing
  semantics.
- Complete TypeDoc/API baseline, conceptual docs/examples, security/dependency
  checks, dry-run archives, and release checklist.
- Run the full public Go/Rust/JavaScript gate from clean checkouts.

Gate: no OSS PostgreSQL gap, declared SQLite profile complete, packaging clean,
and no publication performed.

### Phase 8: private River Pro

- Create the private JS workspace/packages and exact-version link to core.
- Run CEL and cryptography spikes with predefined fallbacks.
- Port all Pro semantics/migrations/tests/docs/adapter/benchmarks.
- Run Go/Rust/JavaScript private mixed, migration, fault, performance, and soak
  suites.
- Audit staged/public histories for leakage.

Gate: private feature matrix complete and public repository trees contain no Pro
artifacts.

### Phase 9: final upstream audit

- Fetch every repository again, compare the new merge-base-to-master ranges,
  and integrate any commits that landed during implementation.
- Decide commit-by-commit whether each port belongs folded into an existing
  commit or as a new coherent commit.
- Re-run every gate from clean checkouts and prepare concise PR descriptions.
- Push only if the task invoking this plan explicitly authorizes pushing.
- Never publish npm packages, create releases/tags, reserve names, or modify the
  homepage.

## Default decisions and spike fallbacks

These are not open questions for the implementation agent:

| Question | Default | Fallback without user input |
|---|---|---|
| Minimum runtime | Node 26+ | Wait to publish until LTS; do not lower compatibility |
| Exact IDs | `bigint` | None; JSON edge uses decimal string |
| Exact timestamps | `Temporal.Instant` | None; do not expose lossy `Date` |
| Modules | One compiled ESM artifact | Fix export map/require(esm), do not dual-build |
| Payload validation | Standard Schema or explicit decoder | `unknown` worker args without validation |
| Ordinary concurrency | In-process promises | Scale processes; no threads for I/O by default |
| CPU/hard stop | Optional River thread executor backed by audited Piscina | River-owned worker pool behind same API if Piscina fails audit |
| PostgreSQL | node-postgres full backend | No ORM runtime substitute |
| Prisma | Insert/transaction only | Structured unsupported capability for runtime |
| SQLite | `node:sqlite` preview backend | Keep unpublished/preview until API is suitable; do not drop profile |
| MySQL | Future first-party semantic backend | No placeholder or public arbitrary-driver claim |
| CEL | `@bufbuild/cel` if full goldens pass | CEL C++/Wasm/native adapter in private workflow package |
| Tests | Existing Vitest plus built Node/package tests | Replace only a failing layer, not the evidence requirement |
| Repository | Extend `riverqueue-js`; River owns conformance | Do not duplicate JS source in River |
| Website | Read-only reference | Write local JS docs only |
| Publication | Forbidden in implementation | Dry-run archives only |

An implementation agent may refine private names or internal module boundaries
when compile/performance evidence demands it. It should not pause for stylistic
approval. It should ask only for unavailable credentials/permissions, an
irreversible external action not authorized by the invoking prompt, or a true
product decision not resolved here after exhausting safe fallbacks. Network or
firewall downloads should be retried; one transient failure is not a reason to
stop.

## Definition of done

The plan is complete only when all applicable items are true:

- The latest Go, Rust, JavaScript, and private Pro upstream commits have been
  audited and the branches are rebased without accidental history.
- `riverqueue-js` provides complete PostgreSQL behavior and the declared
  SQLite storage/runtime behavior with an idiomatic API and no lossy IDs/times.
- Current `riverqueue@0.1.0` producer users have a tested migration path; pg and
  Prisma transactions still work.
- Every applicable stable conformance scenario has exactly one executable owner
  and passes with the JavaScript adapter.
- Direct Go/Rust/JavaScript competition/failover smoke and soak tests pass.
- Completion, cancellation, hooks/plugins, lifecycle, and worker-thread
  termination survive repeated adversarial stress without stale transitions,
  duplicate observations, lost completions, or leaked work.
- PostgreSQL and SQLite migration mirrors are generated from canonical sources
  and byte/hash verification rejects drift.
- Performance is measured with equivalent mechanisms; bottlenecks are
  instrumented; no unexplained serialized path or unbounded resource growth
  remains.
- The private Pro feature matrix, CEL/crypto goldens, migrations, and mixed
  suites pass, and public diffs/artifacts contain no Pro material.
- TypeDoc, declaration/API reports, conceptual docs, migration guide, examples,
  and benchmark docs are complete and compile/run against packed artifacts.
- Strict typecheck, lint/format, unit/integration, fault, migration, package,
  dependency/license, conformance, performance, and required soak gates pass
  from clean checkouts.
- Package archives and future release configuration are ready, but no npm
  publish, tag, GitHub release, name reservation, production migration, or
  homepage change occurred.

## One-shot implementation prompt

Use the following as the eventual implementation request. Starting it with
`/goal` explicitly authorizes the agent to create and pursue a persistent goal
instead of stopping after a partial milestone.

```text
/goal Implement docs/js-port-plan.md end to end across the public River,
riverqueue-js, and private River Pro repositories. Treat the plan's defaults as
authoritative and aim to one-shot a complete, idiomatic, fully interoperable
JavaScript/TypeScript implementation rather than returning after scaffolding or
asking me routine design questions.

First fetch/rebase every involved feature branch on its latest origin/master,
inspect every upstream commit since each merge base, and preserve clean,
intentional history. Extend the existing riverqueue-js package/repository; do
not create a duplicate JS implementation. Keep the River repository canonical
for migrations and shared conformance. Keep every River Pro source, fixture,
migration, package, test, and capability private—never commit or push any of it
to a public repository. Treat /Users/bgentry/River/homepage as read-only.

Use the existing candidate-neutral conformance suite, expand it only with
language-neutral executable scenarios, and fully validate Go/Rust/JavaScript
storage, runtime, race, migration, fault, performance, and soak interoperability.
Implement the local API/test/doc/package work and the private Pro work as well.
Use subagents, including adversarial reviewers, for independent bounded reviews
of public API design, completion/cancellation races, database semantics,
conformance coverage, security/private-code leakage, and release packaging;
evaluate their findings rather than applying them mechanically.

Continue autonomously through all phases and safe validation. Retry transient
downloads/firewall failures. Ask me only if blocked on unavailable permission or
credentials, an irreversible external action not authorized here, or a genuinely
material product choice that the plan does not resolve and whose safe fallbacks
have failed. Do not publish or reserve npm packages, create tags/releases, run
production migrations, or change the homepage. Prepare dry-run package archives
and PR-quality commits/descriptions as though we may publish later. Push only if
I explicitly authorize it in the invoking conversation. Do not mark the goal
complete until the full definition of done is satisfied.
```

## Primary research sources

Sources were checked on 2026-08-30. Local River/Rust/Pro source, generated API
docs, the existing `riverqueue-js` repository/package, and the homepage's
conceptual docs were inspected directly; the external technical conclusions use
primary project documentation.

- [Node.js release status and schedule](https://nodejs.org/en/about/previous-releases)
- [Node.js 26 release and default Temporal](https://nodejs.org/en/blog/release/v26.0.0)
- [Node AbortController and AbortSignal](https://nodejs.org/api/globals.html#class-abortcontroller)
- [Node worker threads](https://nodejs.org/api/worker_threads.html)
- [Node AsyncLocalStorage](https://nodejs.org/api/async_context.html)
- [Node diagnostics channel](https://nodejs.org/api/diagnostics_channel.html)
- [Node SQLite](https://nodejs.org/api/sqlite.html)
- [Node test runner](https://nodejs.org/api/test.html)
- [Node package publishing and dual-package hazard](https://nodejs.org/learn/modules/publishing-a-package)
- [TC39 Temporal proposal](https://github.com/tc39/proposal-temporal)
- [TypeScript 6.0 release](https://devblogs.microsoft.com/typescript/announcing-typescript-6-0/)
- [TypeScript 7.0 release and TS6 tool compatibility](https://devblogs.microsoft.com/typescript/announcing-typescript-7-0/)
- [node-postgres data types](https://node-postgres.com/features/types)
- [node-postgres transactions](https://node-postgres.com/features/transactions)
- [node-postgres pool ownership and sizing surface](https://node-postgres.com/apis/pool)
- [Standard Schema](https://github.com/standard-schema/standard-schema)
- [Graphile Worker introduction](https://worker.graphile.org/docs)
- [Graphile Worker task executors](https://worker.graphile.org/docs/tasks)
- [Graphile Worker TypeScript safety](https://worker.graphile.org/docs/typescript)
- [Graphile Worker performance/configuration](https://worker.graphile.org/docs/performance)
- [BullMQ concurrency](https://docs.bullmq.io/guide/workers/concurrency)
- [BullMQ sandboxed processors](https://docs.bullmq.io/guide/workers/sandboxed-processors)
- [pg-boss](https://github.com/timgit/pg-boss)
- [Piscina worker-thread pool](https://github.com/piscinajs/piscina)
- [CEL for ECMAScript](https://github.com/bufbuild/cel-es)
