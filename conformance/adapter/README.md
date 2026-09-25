# Conformance adapter protocol

River implementations expose a private test adapter using JSON-RPC 2.0. Each
request and response is one JSON object followed by a newline. Standard output
is reserved for protocol messages; all diagnostics and library logs go to
standard error.

The harness starts each adapter with `RIVER_CONFORMANCE_DATABASE_URL`, an
explicit `RIVER_CONFORMANCE_DATABASE_KIND` (`postgres` or `sqlite`), and, for
SQLite, `RIVER_CONFORMANCE_PROFILE`. PostgreSQL
uses an externally provisioned disposable database. The SQLite harness creates
one temporary file and both adapters enable WAL, foreign keys, a five-second
busy timeout, and a one-connection pool. Requests are sequential within an
adapter process, while the harness may call different adapters concurrently.
IDs and transaction-independent records returned by one implementation may be
passed to any other implementation attached to the database.
Job IDs are exact signed 64-bit JSON integer tokens, not JavaScript `number`
values. Adapters must accept and emit values above `Number.MAX_SAFE_INTEGER`
without rounding in CRUD parameters, normalized rows, list filters, or opaque
cursors.

The Go implementation is the reference side. By default the candidate is the
Rust adapter described by [`candidates/rust.json`](candidates/rust.json). A
JavaScript or future implementation can run the same suite by placing an object
matching [`candidate.schema.json`](../schema/candidate.schema.json) in its own
repository and setting `RIVER_CONFORMANCE_CANDIDATE_FILE` to its path:

```json
{
  "application_name": "river-conformance-javascript",
  "command": ["node", "dist/conformance-adapter.js"],
  "implementation": "javascript",
  "performance": {
    "enqueue": { "max_p95_ratio": 3, "min_throughput_ratio": 0.25 }
  },
  "profiles": ["portable-storage-v1", "postgres-full-v1", "sqlite-runtime-v1"],
  "start_options": ["elect_interval_ms", "rescuer_interval_ms", "scheduler_interval_ms"],
  "version": "0.47.0-alpha.1"
}
```

For one-off runs, `RIVER_CONFORMANCE_CANDIDATE` accepts the descriptor as an
inline JSON object. Set only one of the file and inline variables. Relative
descriptor paths and every candidate command run from the River repository
root, so a descriptor outside this checkout should use an absolute adapter path
or a command whose arguments select that external project. Command arguments
may reference environment variables as `${NAME}` or `${NAME:-default}`; the
Rust descriptor uses this to follow `CARGO_TARGET_DIR`. Unknown descriptor
fields are rejected.

- `command` starts an adapter process. `build_command`, when present, runs
  once per test process before any adapter starts, so `command` can run the
  built executable directly.
- `restart_command` starts a prebuilt process for crash and restart
  scenarios, which cannot rely on a build wrapper surviving process
  termination. It defaults to `command`, and its executable must exist once
  the build has run, so a stale binary in another target directory is never
  picked up silently.
- `release_build_command` and `release_command` replace the build and
  commands for performance tiers.
- `application_name` is the PostgreSQL `application_name` of the adapter's
  connections. It must start with `river-conformance-`; fault injection only
  terminates connections with that prefix.
- `version`, if present, must equal the handshake's implementation version.
- `profiles` lists the profiles the adapter serves (default
  `portable-storage-v1`, `postgres-full-v1`, and `sqlite-runtime-v1`).
- `start_options` lists optional `start` tuning parameters the adapter
  honors. The harness sends `elect_interval_ms`, `rescuer_interval_ms`, and
  `scheduler_interval_ms` only to adapters that declare them and otherwise
  waits for the implementation's defaults. Go declares none because it does
  not expose those intervals as configuration.
- `performance` declares the candidate's release bounds relative to the
  reference per benchmark mode; omitted modes use the harness defaults.

For PostgreSQL, the candidate must advertise the exact versioned method set in
`contract.json`. For SQLite, it must advertise the exact capabilities and
methods in either `profiles/sqlite.json` or `profiles/sqlite-runtime.json`, as
selected by the profile environment variable. Missing and extra methods both
fail before behavioral scenarios run.

The SQLite `portable-storage-v1` profile intentionally reuses the same adapter
methods and harness helpers for deterministic controls, unique keys, migrations,
insertion, job CRUD/list cursors, raw timestamp encoding, and transactions. It
does not claim custom schemas, queue/runtime behavior, notifications,
leadership, PostgreSQL transaction-abort semantics, `SKIP LOCKED`, fault
injection, performance, or soak coverage.

The `sqlite-runtime-v1` profile is a tested superset. It adds cross-language
workers, competing claims, queue CRUD and dynamic reconfiguration, pause/resume
behavior, durable insert/control notification wakeups, remote cancellation,
leadership and failover, scheduler and periodic work, local subscriptions,
cross-client pause/resume subscription delivery, extensions, and graceful
lifecycle behavior. PostgreSQL-specific schemas,
`COPY`, `SKIP LOCKED`, backend disconnect/transaction-abort fault injection,
reindexing, rescuer/cleaner maintenance, performance, and soak remain outside
that profile.

## Insert-only clients

The `insert-only-v1` profile (`profiles/insert-only.json`) is for clients that
only enqueue jobs, such as producer libraries in languages without a River
worker runtime. Its methods are `handshake`, `insert`, `insert_many`,
`tx_begin`, `tx_insert`, `tx_insert_many`, `tx_commit`, `tx_rollback`, and
`unique_key`, served over PostgreSQL with `RIVER_CONFORMANCE_PROFILE` set to
`insert-only-v1`. The Go reference migrates, observes, and works every job, so
the adapter needs no migrator, reader, or runtime. `TestInsertOnlyConformance`
compares each insert with the reference's own insert field by field, checks
batch order and duplicate reporting, requires transactional inserts to become
visible and notify only on commit, checks unique keys against the goldens and
against reference inserts in both orders, and requires a candidate insert to
wake a reference worker. A descriptor opts in by listing `insert-only-v1` in
`profiles`; full implementations can serve it as a subset.

`profiles/postgres-full.json` names the complete PostgreSQL profile: every
method in `contract.json` and every complete manifest capability.

## Params, results, and errors

`contract.json` gives every method a `params` and a `result` JSON Schema
(shared shapes live in its `$defs`, and normalized jobs and queues reference
`../schema/normalized-job.schema.json` and
`../schema/normalized-queue.schema.json`). The harness validates every request
it sends and every result it receives against them, so a response with a
missing, extra, or mistyped field fails even when no scenario inspects it.
Adapters must reject parameters their method does not declare, including
nested ones, with `invalid_params` instead of ignoring them.

Errors use the stable JSON-RPC codes listed under `errors` in `contract.json`.
Scenarios assert codes, never message text:

| Code | Name | Meaning |
|---|---|---|
| -32700 | `parse_error` | The request line is not JSON. |
| -32600 | `invalid_request` | Not a JSON-RPC 2.0 request. |
| -32601 | `method_not_found` | The method is outside the advertised profile. |
| -32602 | `invalid_params` | Params do not match the method schema. |
| -32000 | `internal` | The adapter itself failed. |
| -32001 | `not_found` | A job, queue, transaction handle, or barrier does not exist. |
| -32002 | `rejected` | The implementation refused or could not complete the request. |
| -32003 | `database_error` | The database reported an error. |
| -32004 | `unsupported` | A valid optional parameter the implementation cannot honor. |

The optional `start` tuning parameters `elect_interval_ms`,
`rescuer_interval_ms`, and `scheduler_interval_ms` return `unsupported` from
an adapter whose implementation does not expose them; the Go reference is one.
`rescue_after_ms` is required of every runtime adapter.

## Discovery and administration

- `handshake`: protocol and adapter versions, implementation identity,
  capabilities, and migration lines.
- `migrate`, `reset`.
- `clock_set`, `rng_seed`, and `retry_delay` evaluate the implementation's
  production default retry policy at a fixed clock. The delay must fall within
  the bounds in `fixtures/protocol_values.json`, which are generated from
  River's Go retry policy. Implementations with seedable jitter use the seed;
  the Go reference's jitter is process-random and ignores it.
- `cron_next` takes `expression`, an RFC 3339 `from` time, and `count`, and
  returns up to `count` successive occurrences as RFC 3339 strings in the
  reference time's offset. It must accept exactly River Go's documented cron
  syntax (robfig/cron `ParseStandard`) and reject everything else; the
  `cron_cases` and `cron_invalid` sections of
  `fixtures/maintenance_values.json` are the goldens.
- `leader`, `request_resign`, `listener_count`, and `connection_count`.

## Jobs and queues

- `insert`, typed `insert_many`, `insert_many_fast`, `get`, `list`, `update`,
  `retry`, `cancel`, `delete`, and `delete_many`. Typed batch results preserve
  input order and include each normalized job and its unique-conflict flag.
- `queue_get`, `queue_list`, `queue_pause`, `queue_resume`, `queue_update`, and
  runtime `queue_add`/`queue_remove`. Like River Go, `queue_pause`,
  `queue_resume`, and `queue_update` don't validate the queue name: a name
  with no queue record, including one that could never be a valid queue name
  (for example one containing a space or longer than 128 characters), returns
  `not_found` rather than `rejected`.
- `start`, `stop`, `wait`, and the compatibility shorthand `work`. `start`
  also accepts optional maintenance tuning: `cancelled_job_retention_ms`,
  `completed_job_retention_ms`, and `discarded_job_retention_ms` (`-1` keeps
  that state forever), `job_timeout_disabled`, `rescue_after_ms`,
  `reindexer_index_names`, and `reindexer_interval_ms`. Interval keys that
  River Go does not expose (`elect_interval_ms`, `job_cleaner_interval_ms`,
  `queue_cleaner_interval_ms`, `rescuer_interval_ms`,
  `scheduler_interval_ms`) only shorten waits and may be ignored.
- `runtime_stats` exposes normalized hook, middleware, periodic, resumable,
  stuck-job, and event-subscription observations without exposing
  language-specific API shapes. `stuck_jobs` counts jobs the runtime reported
  as stuck after ignoring cancellation beyond the stuck threshold. Version 1 observes delivered event kinds but does not expose
  subscriber lag counters; adding normalized lag observations requires a
  contract revision.
- `barrier_create` and `barrier_release` coordinate the `barrier_wait` and
  output-recording `barrier_output` workers without timing races.
- `benchmark_enqueue` performs an in-process insertion workload so JSON-RPC
  framing is not included in enqueue timings.

The built-in `conformance_echo` job accepts `message`, `behavior`, and
`duration_ms`. Behaviors cover success, retryable error, panic, worker cancel,
discard, one-time snooze, recorded output, barrier waiting, timed work,
cooperative remote cancellation, and intentionally ignored cancellation.
`cooperative_cancel` waits for its job context to be cancelled and returns the
implementation's cancellation error (Go's `context.Canceled`, Rust's
`WorkCancelled`), while `cancel_error` and `cancel_panic` wait the same way and
then return an ordinary error or panic, so shutdown can distinguish a
cooperative stop from a genuine failure. The
suite also covers a snoozed job that is immediately refetched and then
cancelled, which exercises cancellation registration and stale-attempt cleanup
in both directions. The last behavior is only run in a disposable adapter
process that the harness may kill.

The `resumable_cursor` behavior preserves `first_attempt`, records cursor `7`
in its second step, and fails the second and third steps once each. The harness
moves successive attempts between implementations and asserts that completed
steps stay skipped and consumed cursors are cleared. `resumable_duplicate`
repeats a step name and must fail even when the repeated step is being skipped.
The ordinary `resumable` behavior also accepts an empty saved checkpoint and
rejects a malformed cursor object before user work begins.

## Transaction handles

`tx_begin` creates a connection-local transaction under a caller-chosen
handle. Transaction operations cover insert, typed `tx_insert_many`, fast
`tx_insert_many_fast`, get/list/update/delete/bulk delete, cancel/retry, and
queue get/list/update/pause/resume. `tx_commit` and `tx_rollback` consume a
handle. `tx_fail` deliberately aborts PostgreSQL state to verify rollback
behavior. Handles never cross adapter processes because a database transaction
is connection-local. Their effects are deliberately observed from the other
language before and after commit. Transactional insert notifications are also
commit-bound: jobs remain invisible before commit, commit wakes an opposite-
language worker whose poll interval is 60 seconds, and rollback produces no
wakeup.

Job lists accept shared ID/kind/metadata/priority/queue/state/tag filters,
ordering, direction, limits, and opaque `after` cursors. Responses return the
last-row cursor so page tokens emitted by one language can be consumed by the
other.

## Fault injection

- `raw_insert_no_notify` proves polling recovers work when notification
  delivery is lost.
- `raw_finalize` forces a running row to an external terminal state, with
  `finalized_at` set to the database's current time, so the suite can prove
  late worker completion preserves that state and error while merging worker
  metadata and delivering the canonical worker-outcome event. A current
  timestamp keeps leader cleaners from deleting the row mid-scenario.
- `fault_disconnect_listeners` terminates the adapter's PostgreSQL listener
  backends and the harness waits for reconnection.
- `fault_disconnect_application` terminates all non-caller connections for one
  adapter application name, which must start with `river-conformance-`.
- `fault_expire_leader` forces the current lease to expire before a replacement
  client starts.

The harness may also kill a disposable adapter process. Process kill is the
only safe way to test a worker that deliberately ignores cancellation.

Normalized jobs include every persisted field. Timestamps use UTC RFC 3339,
unique keys use lowercase hexadecimal, absent values use JSON null, and JSONB
objects remain objects. These representations remove driver-specific byte and
time encodings while retaining protocol-visible data.

Protocol additions must be implemented by every current adapter before its
capability is advertised. Backward-incompatible message changes require a new
`protocol_revision` and a matched-version manifest update.
