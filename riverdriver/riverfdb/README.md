# FoundationDB driver (experimental)

`riverfdb` stores River jobs directly in FoundationDB's transactional key-value
store. It uses the official Go bindings, tested with FoundationDB **7.3.77** and
API version **730**. There is no SQL server or SQL translation layer.

Install FoundationDB's native client library and headers, then build your
application with `go build -tags foundationdb`. The build tag keeps this native
dependency out of ordinary River builds.

```go
import (
    "github.com/apple/foundationdb/bindings/go/src/fdb"
    "github.com/riverqueue/river"
    "github.com/riverqueue/river/riverdriver/riverfdb"
)

// Select the FoundationDB API version once, before opening any databases.
if err := fdb.APIVersion(730); err != nil {
    return err
}
db, err := fdb.OpenDefault()
if err != nil {
    return err
}
defer db.Close() // Stop all River clients before closing the database.

driver, err := riverfdb.New(db, []byte("my-application/river/"))
if err != nil {
    return err
}
client, err := river.NewClient(driver, &river.Config{
    Queues: map[string]river.QueueConfig{
        river.QueueDefault: {MaxWorkers: 10},
    },
    Workers: workers, // Register your workers as with other River drivers.
})
if err != nil {
    return err
}
// Call client.Start(ctx), client.Insert(...), and client.Stop(ctx) as usual.
```

No migrations are needed. The caller supplies an exclusive, nonempty key prefix;
`Config.Schema` creates additional isolated namespaces within that prefix.
Different drivers must use disjoint prefixes. The storage format is experimental
and may change without a migration path.

For atomic application writes and job insertion, use `client.InsertTx(ctx, tx,
args, opts)` inside `db.Transact(func(tx fdb.Transaction) (any, error) { ... })`.
Write application records outside the River prefix. The entire callback may be
retried, so it must not perform irreversible external side effects. Transactional
completion through `river.JobCompleteTx[*riverfdb.Driver]` is also supported.

Supported operations include inserts and unique inserts, atomic priority-ordered
claims, retries and snoozing, scheduling, cancellation, rescue, completion,
individual deletion and retention cleanup, job lookups and counts, queue state,
and leader leases. Insert and claim conflicts retry automatically for standalone
operations. Explicit transactions return commit conflicts to their owner; retry
the entire transaction. A `commit_unknown_result` is returned to the caller
instead of replaying an operation that might already have committed.

Notifications use a per-schema log and a watched sequence key. `NotifyMany`
appends payloads and advances the sequence in the same transaction as the job or
queue change. Each listener keeps its own cursor and subscriptions, reads batches
of committed payloads, then arms a FoundationDB watch in the same transaction as
its empty log read. A commit between the read and watch registration still wakes
the listener. Watches are cancelled on interruption and rearmed after firing.
Inserts, running-job cancellations, queue pause/resume, and leader resignations
use this path by default; `PollOnly` still disables the listener.

Notifications are ephemeral wakeups. New subscriptions and reconnects start at
the current sequence; they do not replay history. The elected client's
notification cleaner runs every minute and removes entries older than five
minutes, in bounded batches using an expiry index. Slow listeners may miss
expired entries, so River retains its polling fallback. Clients that only insert
jobs do not run maintenance; start a worker client to clean the log.

Limitations:
- `JobList`, `JobDeleteMany`, raw SQL, migrations, SQL introspection, advisory
  locks, and nested transactions return `riverdriver.ErrNotImplemented`.
  River does not start its SQL reindexer for this backend.
- Shared transactional counters serialize job inserts and notification publishers
  within each schema. A single watched key wakes listeners for all topics.
  Some maintenance and reporting operations scan the job keyspace. This is an
  evaluation driver for small datasets, not a production-scale implementation.
- Job records are JSON-encoded and must fit FoundationDB's 100,000-byte value
  limit, including encoding overhead. Transactions must remain within its
  duration and size limits; keep batches small. Large batches fail atomically
  rather than being silently split.
- Scheduling, leader leases, and notification retention use client clocks.
  Keep clocks synchronized.
- River Pro extensions are not implemented or tested.

FoundationDB documents its transaction and key/value limits in
[Known Limitations](https://apple.github.io/foundationdb/known-limitations.html).

From the repository root, with the native client installed and a test cluster
running:

```sh
export FDB_CLUSTER_FILE=/path/to/fdb.cluster
make test/foundationdb
GOFLAGS=-tags=foundationdb make lint
```

For a client installed outside the default compiler search paths, set
`CGO_CFLAGS` and `CGO_LDFLAGS` to its include and library directories. On macOS,
also include an appropriate runtime library search path in `CGO_LDFLAGS`.

The integration suite uses random prefixes and clears only those prefixes when
finished. It runs River's shared core conformance tests and checks a real client,
concurrent claims and unique inserts, cancellation, transaction visibility and
rollback, failed batch atomicity, record limits, and namespace isolation. Shared
listener tests cover fanout, subscription changes, ordering, and log cleanup;
FoundationDB-specific tests cover watch rearming and cancellation. The
normal `make test` does not build or run the native integration tests.

The `FoundationDB` job in `.github/workflows/ci.yaml` runs the suite on pull
requests and pushes to `master`. It downloads matching, checksum-verified client
and server packages, starts an isolated cluster, and runs `TestFoundationDB` with
the race detector and test caching disabled. Failed runs retain server logs as a
GitHub Actions artifact for seven days.
