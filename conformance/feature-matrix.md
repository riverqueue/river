# Backend feature matrix

The full matrix covers River on PostgreSQL. Every PostgreSQL entry is complete
for the matched Go, Rust, and JavaScript implementations in `manifest.json`.
SQLite has an executable
backend-neutral `portable-storage-v1` profile and an executable
`sqlite-runtime-v1` superset.

| Area | Compatibility surface | Status |
|---|---|---|
| Migrations | Main line 1–7, every historical start, down/up, hashes, custom schemas | Complete |
| Job rows | Every persisted field, eight states, errors, metadata, timestamp and JSON conversion | Complete |
| Insert | Defaults, scheduling, pending, tags, metadata, typed batches and optimized fast insertion | Complete; Go/Rust use `COPY`, JavaScript retains equivalent batch semantics without a candidate-only producer path |
| Unique insert | Args, selected fields, period, queue, states, kind exclusion and Go-compatible hashing | Complete |
| Transactions | Insert, CRUD, cancel, completion, commit/rollback and aborted transaction visibility | Complete |
| Fetch | Priority/schedule/ID ordering, kind filtering, paused queues and `SKIP LOCKED` | Complete |
| Completion | Batched complete, retry, discard, snooze, cancel, cancellation override and external-terminal races | Complete |
| Runtime | Typed workers, panic/exception capture, timeout, stuck detection, task abort and graceful/hard stop | Complete |
| Queues | CRUD, pause/resume, dynamic add/reconfigure/remove and persisted metadata | Complete |
| Notifications | Insert, control, leadership, listener reconnect and polling recovery | Complete |
| Leadership | Lease election/renewal, resignation, term changes and mixed failover both directions | Complete |
| Maintenance | Job/queue cleaners, rescuer, scheduler, periodic enqueuer and UTC reindex schedule | Complete |
| Public querying | Get/list/keyset pagination/update/delete/retry plus transaction variants | Complete |
| Extensions | Ordered hooks/middleware, plugins, retry/error policy, events and lag reporting | Complete |
| Higher-level jobs | Cron/interval periodic jobs, dynamic updates and resumable steps/cursors | Complete |
| Test support | Typed language-native helpers, stateful Go/Rust/JavaScript adapters, codec/storage/runtime/chaos/performance tiers | Complete |

## Scope decisions

- PostgreSQL remains the only backend with custom schemas, `COPY`, `SKIP
  LOCKED`, backend fault injection, rescuer/cleaner and UTC-reindex maintenance,
  performance, and soak compatibility claims.
- SQLite compatibility currently covers main-line migrations; deterministic
  retry and unique-key controls; typed and backend-optimized insertion; job
  get/list/update/cancel/retry/delete; cross-language cursor ordering; exact
  millisecond timestamp storage; and transaction commit, rollback, batch
  atomicity, and visibility. Every selected candidate is exercised in both
  directions with Go against one WAL database; the same candidate-neutral
  profile is run independently for Rust and JavaScript.
- SQLite `sqlite-runtime-v1` additionally covers work in both directions,
  competing workers, queue CRUD/dynamic reconfiguration/pause/resume,
  transactional and ordinary notification wakeups, cancellation, leadership
  and failover, scheduler/periodic work, poll-only recovery, resumable retries,
  hook/middleware ordering, local subscriptions, cross-client pause/resume
  subscription delivery, and graceful lifecycle behavior. Subscriber lag
  counters are not exposed by the version 1 process
  adapter and remain covered only by language-local tests; a future contract
  revision should add normalized lag observations before claiming them here.
- SQLite custom schemas, PostgreSQL aborted-transaction behavior, `COPY`,
  `SKIP LOCKED`, backend fault injection, rescuer/cleaner maintenance,
  reindexing, performance, and soak are explicitly outside the SQLite profiles.
- Rust uses builders, typed async workers, cancellation tokens and explicit
  transaction connections rather than reproducing Go API shapes.
- JavaScript uses `bigint`, Temporal instants, promises, `AbortSignal`, and
  optional worker-thread execution rather than narrowing protocol values to
  JavaScript numbers or reproducing Go goroutine APIs. Shared tests exercise
  job IDs above `Number.MAX_SAFE_INTEGER`, including JSON-RPC requests,
  responses, list filters, and cursors.
- `riverqueue-internal` is an exact-version extension seam, published only
  because the public crates require it as dependency plumbing. It is not a
  stable API compatibility promise.
- The Rust crates and JavaScript packages remain unpublished preview packages
  until the full release process is complete; “complete” here means their
  checked compatibility surface, not a published release.
