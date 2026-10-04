# Running alongside Go and Rust

JavaScript, Go, and Rust River services share the database protocol, job states,
uniqueness rules, exact integer/timestamp representation, and migration line.
They do not need identical source-level API names. Treat persisted job kinds and
JSON payload schemas as service contracts.

## Unique arguments and resumable work

Uniqueness hashes depend on the selected argument paths and serialized JSON.
River sorts top-level keys but preserves nested object order, matching Go.
When selecting a whole nested object, match its key order to the Go producer's
struct or map serialization. Select scalar leaf paths (for example,
`byArgs: ["account.id", "account.region"]`) when nested object order should not
be part of identity. Missing selected fields and explicit `null` are distinct.

Resumable step names and cursor shapes must also match between implementations.
Keep names stable and await steps sequentially; nested steps are supported,
but concurrent steps do not define a checkpoint order. Catching a step error
does not make the attempt successful: River retains the failure and checkpoint.
The test helper returns updated metadata so a subsequent test attempt can
resume with the same persisted state as a real worker.

## Rollout

Pin all River packages in a JavaScript application to one exact version line.
Before a mixed rollout, verify that every live language implementation supports
the target migration and capabilities. Apply compatible expand migrations,
deploy producers that can still be read by old workers, deploy workers, and
only then remove old payload shapes or database compatibility.

Keep schemas and decoders tolerant during the transition and make new fields
optional or versioned. A TypeScript rename does not migrate persisted
payloads. Test both directions (a job inserted in one language and worked in
the other) with the payloads your services actually produce.

## Periodic jobs

Only the elected leader inserts periodic jobs, and leadership moves between
languages. Register the same periodic jobs in every language that may lead,
or keep them in one language and set `leaderElectionDisabled: true` on the
others; see
[periodic jobs](./periodic-jobs.md#mixed-language-fleets).

## Rollback and rescue

Retain the previous compatible binaries until the new fleet has completed its
mixed-version soak. A rollback must not cross a migration that the previous
binary cannot read. Rescue abandoned attempts through River's normal attempt
identity and state transitions rather than direct ad hoc table updates.

## Maintenance in mixed fleets

Leadership is shared across languages: whichever client is elected, Go, Rust,
or JavaScript, runs the rescuer, cleaners, and scheduler for the whole
database, and the rescuer handles every stuck job regardless of which language
was working it. As in Go, a stuck job whose kind the leading client has no
worker for is discarded instead of retried.

When job kinds are specific to one language, keep each language's kinds in
queues that only that language's clients work, and register the other
language's kinds on every client that may lead. A client claims only from its
own queues, so such a placeholder handler never runs; it only lets the rescuer
apply the normal retry policy. Give it the same timeout as the real worker so
the rescuer waits just as long before treating the job as stuck. This applies
to both PostgreSQL and SQLite.

JavaScript uses `bigint` and `Temporal.Instant` so it does not silently truncate
values produced by Go or Rust. Serialize with River's JSON-safe helpers at HTTP,
logging, or RPC boundaries; ordinary `JSON.stringify` cannot encode `bigint`.

## Production checklist

- run explicit migrations before workers start;
- bound worker, pool, completion, subscription, and worker-thread concurrency;
- observe fatal runtime completion and graceful-shutdown deadlines;
- compare single-process and multi-process benchmark results on the intended
  database and workload;
- verify no unbounded heap, listener, locked-job, or connection growth in a
  sustained soak; and
- exercise a real rollback and mixed-language rescue before relying on it.
