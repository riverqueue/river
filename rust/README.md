# River for Rust (preview)

This workspace contains the native Rust implementation of River. It targets
the same persistence and coordination protocol as the Go library while
exposing an API designed for Rust and Tokio. PostgreSQL is the mature shared-
database backend; SQLite implements the same logical protocol through River's
sealed built-in backend boundary.

The PostgreSQL surface is implemented and checked against the pinned Go
baseline. The crates remain pre-release while the release process is
unfinished. The compatibility contract lives in
[`../docs/rust-compatibility.md`](../docs/rust-compatibility.md), and shared
cross-language fixtures live in [`../conformance`](../conformance).

## Workspace crates

- `riverqueue`: typed client, worker runtime, CRUD, queues, events, extensions,
  periodic/resumable jobs, and maintenance.
- `riverqueue-macros`: `#[derive(JobArgs)]`.
- `riverqueue-migrate`: canonical River migration line and CLI.
- `riverqueue-test`: typed fixtures and worker-test helpers.
- `riverqueue-internal`: published exact-version dependency plumbing and
  unstable extension SPI; applications should not use it directly.
- `riverqueue-conformance`: private verification package.

The primary API uses a caller-owned SQLx pool, Tokio, typed workers, and
`CancellationToken`. `Client` is deliberately non-generic and accepts built-in
PostgreSQL or SQLite sources; it does not expose Go's driver interface or a
third-party SQL dialect trait. This leaves room for a future built-in MySQL
backend without propagating a driver type through workers and extensions.

## Quick start

```rust,no_run
use riverqueue::{Client, InsertOpts};
use sqlx::PgPool;

# async fn example(pool: PgPool) -> Result<(), riverqueue::Error> {
let client = Client::builder(pool).build()?;
// `EmailArgs` is a Serialize/Deserialize type deriving `JobArgs`.
// client.insert(EmailArgs { /* ... */ }).await?;
// client.insert_with(EmailArgs { /* ... */ }, InsertOpts::default()).await?;
# let _ = (client, InsertOpts::default());
# Ok(())
# }
```

Compiled examples cover workers and graceful stop, cancellation,
transactions, custom schemas, and migrations under the workspace crates'
`examples` directories.

Run the Rust suite from the repository root:

```sh
make lint/rust
make test/rust
make doc/rust
make check/rust/package
```

For basic end-to-end performance figures, the packaged `riverqueue` binary has
the Rust equivalent of `river bench`. It truncates the selected River job table,
so use a disposable database:

```sh
make bench/rust DATABASE_URL=postgres://localhost/river_bench \
  RUST_BENCH_ARGS='--duration 30s'
```

The command supports continuous burn, fixed `--num-total-jobs` burn-down,
custom schemas, tunable worker/pool/batch sizes, periodic jobs/sec output, and a
final jobs/sec plus p95 end-to-end latency summary. Use `riverqueue bench
--help` for all options. The conformance performance gate remains the
reproducible Go/Rust comparison across enqueue-only, worker-only, and mixed
workloads.

PostgreSQL integration tests require a disposable database:

```sh
RIVER_RUST_DATABASE_URL=postgres://localhost/river_rust_test \
  make test/rust/postgres
```

`make check/rust/package` builds the five publishable crate archives with
verification disabled because their exact-version workspace dependencies do
not exist in the registry before the first coordinated release. It does not
publish anything. Release tags use `riverqueue-vX.Y.Z`, independently of Go
module tags.

The repository's [Rust API design record](../docs/rust-api-design.md)
documents the compatibility boundaries and the reasoning behind the public
API and sealed database architecture.
