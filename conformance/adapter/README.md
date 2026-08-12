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

## Discovery and administration

- `handshake`: protocol and adapter versions, implementation identity,
  capabilities, and migration lines.
- `migrate`, `reset`.
- `clock_set`, `rng_seed`, and `retry_delay` drive deterministic retry
  goldens without relying on process-global randomness or wall time.
- `leader`, `request_resign`, `listener_count`, and `connection_count`.

## Jobs and queues

- `insert`, `insert_many_fast`, `get`, `list`, `update`, `retry`, `cancel`,
  `delete`, and `delete_many`.
- `queue_get`, `queue_list`, `queue_pause`, `queue_resume`, and `queue_update`.
- `start`, `stop`, `wait`, and the compatibility shorthand `work`.
- `barrier_create` and `barrier_release` coordinate the `barrier_wait` worker
  without timing races.
- `benchmark_enqueue` performs an in-process insertion workload so JSON-RPC
  framing is not included in enqueue timings.

The built-in `conformance_echo` job accepts `message`, `behavior`, and
`duration_ms`. Behaviors cover success, retryable error, panic, worker cancel,
discard, one-time snooze, recorded output, barrier waiting, timed work,
cooperative remote cancellation, and intentionally ignored cancellation. The
last behavior is only run in a disposable adapter process that the harness may
kill.

## Transaction handles

`tx_begin` creates a connection-local transaction under a caller-chosen
handle. Transaction operations cover insert, get/list/update/delete/bulk
delete, cancel/retry, and queue get/list/update/pause/resume. `tx_commit` and
`tx_rollback` consume a handle. `tx_fail` deliberately aborts PostgreSQL state
to verify rollback behavior. Handles never cross adapter processes because a
database transaction is connection-local. Their effects are deliberately
observed from the other language before and after commit.

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
