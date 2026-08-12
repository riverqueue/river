# Conformance adapter protocol

River implementations expose a private test adapter using JSON-RPC 2.0. Each
request and response is one JSON object followed by a newline. Standard output
is reserved for protocol messages; all diagnostics and library logs go to
standard error.

The harness starts each adapter with `RIVER_CONFORMANCE_DATABASE_URL` pointing
at a disposable PostgreSQL database. Requests are sequential within an adapter
process, while the harness may call different adapters concurrently. IDs and
transaction-independent records returned by one implementation may be passed
to any other implementation attached to the database.

The Go implementation is the reference side. By default the candidate is the
Rust adapter in this repository. Any future implementation, including a
JavaScript port, can run the same suite by setting
`RIVER_CONFORMANCE_CANDIDATE` to a JSON object:

```json
{
  "application_name": "river-conformance-javascript",
  "command": ["node", "dist/conformance-adapter.js"],
  "implementation": "javascript",
  "restart_command": ["node", "dist/conformance-adapter.js"],
  "version": "0.43.0-alpha.1"
}
```

`command` starts the ordinary candidate. `restart_command` must start a
prebuilt process because crash/restart cases cannot rely on a build wrapper
surviving process termination. `application_name` is the PostgreSQL
`application_name` used for connection-fault tests. `version` is optional; if
present, the handshake must match it exactly. The candidate must advertise the
exact versioned method set in `contract.json`; missing and extra methods both
fail before behavioral scenarios run.

## Discovery and administration

- `handshake`: protocol and adapter versions, implementation identity,
  capabilities, and migration lines.
- `migrate`, `reset`.
- `clock_set`, `rng_seed`, and `retry_delay` drive deterministic retry
  goldens without relying on process-global randomness or wall time.
- `leader`, `request_resign`, `listener_count`, and `connection_count`.

## Jobs and queues

- `insert`, typed `insert_many`, `insert_many_fast`, `get`, `list`, `update`,
  `retry`, `cancel`, `delete`, and `delete_many`. Typed batch results preserve
  input order and include each normalized job and its unique-conflict flag.
- `queue_get`, `queue_list`, `queue_pause`, `queue_resume`, `queue_update`, and
  runtime `queue_add`/`queue_remove`.
- `start`, `stop`, `wait`, and the compatibility shorthand `work`.
- `runtime_stats` exposes normalized hook, middleware, periodic, resumable,
  and event-subscription observations without exposing language-specific API
  shapes.
- `barrier_create` and `barrier_release` coordinate the `barrier_wait` worker
  without timing races.
- `benchmark_enqueue` performs an in-process insertion workload so JSON-RPC
  framing is not included in enqueue timings.

The built-in `conformance_echo` job accepts `message`, `behavior`, and
`duration_ms`. Behaviors cover success, retryable error, panic, worker cancel,
discard, one-time snooze, recorded output, barrier waiting, timed work,
cooperative remote cancellation, and intentionally ignored cancellation. The
suite also covers a snoozed job that is immediately refetched and then
cancelled, which exercises cancellation registration and stale-attempt cleanup
in both directions. The last behavior is only run in a disposable adapter
process that the harness may kill.

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
- `fault_disconnect_listeners` terminates the adapter's PostgreSQL listener
  backends and the harness waits for reconnection.
- `fault_disconnect_application` terminates all non-caller connections for one
  allow-listed adapter application name.
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
