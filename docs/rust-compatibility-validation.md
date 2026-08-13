# Go/Rust compatibility validation

## Status and scope

River's Rust implementation is a preview backend for the protocol implemented
by River Go. PostgreSQL is intended to share a database with Go clients,
workers, migrators, leaders, and maintenance services. SQLite implements the
same logical queue semantics and database layout as River Go's SQLite driver.
The Rust API is native Rust rather than a transliteration of Go's public API.

Compatibility is claimed for the matched pair in
[`conformance/manifest.json`](../conformance/manifest.json) only after the
language-local and shared gates below pass. The crates are not published by
this work, and completing these gates does not itself authorize publication.

The compatibility boundary includes:

- canonical PostgreSQL migrations and custom schemas;
- every persisted job and queue field, state, error, timestamp, and reserved
  metadata transition;
- unique-key hashing, conflict behavior, transactions, fetch ordering,
  completion, cancellation, retry, snooze, and rescue;
- notification topics and wakeups, with polling as loss recovery;
- leadership, maintenance ownership, lifecycle behavior, events, hooks,
  middleware, retry/error policy, periodic jobs, and resumable jobs.

River's Go driver interface itself is outside the Rust API boundary. Rust uses
a sealed, built-in backend boundary instead: PostgreSQL and SQLite are current
implementations, and a future MySQL backend can be added without making worker
or client types generic. River Pro remains in its own private repository and is
validated there against this public worktree; private capabilities and
implementation details must never be added to the public adapter contract or
fixtures.

## Evidence layers

No single test suite is treated as sufficient evidence. Compatibility is
covered in five layers:

| Layer | Purpose |
|---|---|
| Go tests and `riverdrivertest` | Preserve River's existing behavior and exercise each Go database driver. |
| Rust workspace tests | Exercise native APIs, compile-time derives, worker helpers, migrations, runtime services, extension seams, lag behavior, and Rust-specific cancellation. |
| Shared fixtures | Pin normalized rows, retry timing, unique hashes, migration hashes, and JSON/timestamp encodings independent of API shape. |
| Shared process harness | Run Go and a candidate adapter against the same database for storage, runtime, mixed, fault, soak, and performance scenarios. |
| Private downstream suite | Validate River Pro through its private extension and mixed-runtime harness without exposing it publicly. |

The shared adapter protocol is defined by
[`conformance/adapter/contract.json`](../conformance/adapter/contract.json) and
its JSON Schema. Both built-in adapters must report exactly that sorted method
set. A candidate descriptor supplied through `RIVER_CONFORMANCE_CANDIDATE`
lets the same harness run against a future JavaScript implementation without
linking either Go or Rust code into it.

The scenario registry is executable rather than an aspirational checklist.
Every owning test reports the stable IDs it completed, cleanup verifies that it
completed its exact registered set, and artifact validation requires that set
and its tiers to match the applicable inventory: PostgreSQL
[`core.json`](../conformance/scenarios/core.json), SQLite portable storage
[`sqlite-storage.json`](../conformance/scenarios/sqlite-storage.json), or SQLite
runtime [`sqlite-runtime.json`](../conformance/scenarios/sqlite-runtime.json).
Adding a catalog entry without executing it, or executing an unregistered
scenario, fails validation.

## High-risk regressions

The suites give extra weight to cases where two independently correct-looking
clients can disagree:

- simultaneous Go/Rust unique insertion returns one winner to every caller,
  including argument, queue, period, and custom-state keys;
- an explicit `scheduled_at` participates identically in period hashing and is
  persisted as scheduled even when the timestamp is already in the past;
- notifications received after a fetch commits but before the worker attempt
  is registered still cancel that exact attempt;
- a snoozed or immediately retried job may be fetched for its next attempt
  before the prior completion is reaped, without stale cleanup deleting the new
  cancellation token;
- cancellation wins over available, retryable, or scheduled completion and
  produces the canonical cancelled error and event;
- externally finalized rows cannot be overwritten by a late worker result;
- completion bursts drain through shutdown, return every expected job ID once,
  and bound parallel persistence to two full batches;
- rescuers honor disabled, client-default, and worker-specific timeouts and use
  worker retry overrides;
- transactional inserts are invisible before commit, never survive rollback,
  and wake a polling-disabled worker only after commit;
- every historical migration version can move down and up under either
  migrator and remain usable by the other runtime.

## Native differences

The following are intentional API or runtime differences, not database
protocol differences:

- Rust uses `Client::builder`, typed async workers, `WorkOutcome`, builders,
  and explicit SQLx transaction references instead of Go API shapes.
- PostgreSQL uses native `LISTEN`/`NOTIFY`, schemas, advisory locks, and `COPY`.
  SQLite serializes writers and polls its durable notification outbox. These
  backend mechanisms differ, while job and queue semantics remain aligned.
- Go-specific per-job hook/plugin declaration interfaces map to Rust hooks and
  middleware registered on the client. A Rust hook can select by kind from its
  insert/work context; the declaration site is not reproduced.
- Rust insertion middleware observes each ordinary typed-batch element through
  its existing per-insert callbacks; Go middleware can instead wrap an entire
  ordinary batch in one callback. Fast-batch completion has an explicit Rust
  batch callback. This affects local extension ergonomics, not stored rows or
  cross-language execution.
- Runtime queue addition is add-or-reconfigure in Rust. The shared adapter uses
  that normalized behavior even though Go's direct duplicate-add API rejects a
  duplicate.
- Rust may abort a Tokio job task after its cancellation grace period. Tokio
  cannot forcibly stop arbitrary CPU work that never yields or an already
  running blocking task, so this is not advertised as unconditional thread
  termination.
- A job event is emitted only after its transition commits, but concurrently
  completed jobs have no global event-order guarantee. Consumers needing
  stable order must use persisted timestamps and IDs.

## Change protocol

Every protocol-visible change should use this sequence:

1. Classify the change as persisted protocol, shared runtime semantics, or a
   language-local API.
2. Change migrations only in the canonical Go driver migration directory for
   that database. `make generate/rust-migrations` produces the PostgreSQL and
   SQLite Rust package mirrors and shared hashes;
   `make verify/rust-migrations` rejects drift. The Rust SQL files are generated
   copies, not a second migration authority.
3. Add or update a language-neutral fixture for deterministic encodings. For a
   procedural behavior, add a stable scenario ID and implement it through the
   shared adapter contract.
4. Implement and test both built-in adapters. Bump the adapter version when
   the RPC method contract changes, and bump the protocol revision when a
   matched implementation with the old behavior is no longer compatible.
5. Run the full local, mixed, race, migration, packaging, documentation, and
   private downstream gates before changing a capability to `complete`.

The exact adapter method inventory and executable scenario registry make these
steps reusable for another language: implement the JSON-RPC process, point
`RIVER_CONFORMANCE_CANDIDATE` at it, and close failures by scenario ID. No Go
test types or Rust traits are part of that interface.

## Validation commands

Use disposable PostgreSQL databases for database-backed suites:

```sh
make verify
make test
make lint
make test/race

make test/conformance/sqlite

RIVER_RUST_DATABASE_URL=postgres://localhost/river_rust_test \
  make test/rust/postgres

RIVER_CONFORMANCE_DATABASE_URL=postgres://localhost/river_conformance \
  make test/conformance

RIVER_CONFORMANCE_DATABASE_URL=postgres://localhost/river_conformance \
RIVER_CONFORMANCE_PERFORMANCE=1 \
  make test/conformance/performance

make doc/rust
make check/rust/package
make check/rust/dependencies
make check/rust/semver
```

The soak tier is normally a scheduled or pre-release gate because its duration
is intentionally configurable:

```sh
RIVER_CONFORMANCE_DATABASE_URL=postgres://localhost/river_conformance \
RIVER_CONFORMANCE_SOAK_DURATION=10m \
  make test/conformance/soak
```

River Pro additionally runs its private Rust and mixed suites and its Go modules
through a temporary workspace that points at the local public River checkout.
Those commands and artifacts stay in the private repository.
