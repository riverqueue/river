# PostgreSQL feature matrix

This matrix covers River on PostgreSQL. Every entry is complete for the matched
preview pair in `manifest.json`.

| Area | Compatibility surface | Status |
|---|---|---|
| Migrations | Main line 1–7, every historical start, down/up, hashes, custom schemas | Complete |
| Job rows | Every persisted field, eight states, errors, metadata, timestamp and JSON conversion | Complete |
| Insert | Defaults, scheduling, pending, tags, metadata, typed batches and fast `COPY` | Complete |
| Unique insert | Args, selected fields, period, queue, states, kind exclusion and Go-compatible hashing | Complete |
| Transactions | Insert, CRUD, cancel, completion, commit/rollback and aborted transaction visibility | Complete |
| Fetch | Priority/schedule/ID ordering, kind filtering, paused queues and `SKIP LOCKED` | Complete |
| Completion | Batched complete, retry, discard, snooze, cancel and cancellation override | Complete |
| Runtime | Typed workers, panic capture, timeout, stuck detection, task abort and graceful/hard stop | Complete |
| Queues | CRUD, pause/resume, dynamic add/reconfigure/remove and persisted metadata | Complete |
| Notifications | Insert, control, leadership, listener reconnect and polling recovery | Complete |
| Leadership | Lease election/renewal, resignation, term changes and mixed failover both directions | Complete |
| Maintenance | Job/queue cleaners, rescuer, scheduler, periodic enqueuer and UTC reindex schedule | Complete |
| Public querying | Get/list/keyset pagination/update/delete/retry plus transaction variants | Complete |
| Extensions | Ordered hooks/middleware, plugins, retry/error policy, events and lag reporting | Complete |
| Higher-level jobs | Cron/interval periodic jobs, dynamic updates and resumable steps/cursors | Complete |
| Test support | Typed helper crate, stateful Go/Rust adapters, codec/storage/runtime/chaos/performance tiers | Complete |

## Scope decisions

- PostgreSQL is the only Rust backend in this release. Go driver abstractions,
  SQLite-specific APIs, Go command packages, and Go test-only exports are
  outside the Rust compatibility surface.
- Rust uses builders, typed async workers, cancellation tokens and explicit
  transaction connections rather than reproducing Go API shapes.
- `riverqueue-internal` is an exact-version extension seam, published only
  because the public crates require it as dependency plumbing. It is not a
  stable API compatibility promise.
- The Rust crates remain preview packages until the full release process is
  complete; “complete” here means the PostgreSQL implementation and its checked
  conformance surface, not a published release.
