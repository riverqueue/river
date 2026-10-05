# River for Rust (preview)

This workspace contains River's Rust implementation. It shares River's
database schema and job protocol with River for Go on PostgreSQL and SQLite,
with an API designed for Rust and Tokio. The crates are a pre-release
preview.

## Workspace crates

- `riverqueue`: typed client, worker runtime, CRUD, queues, events, extensions,
  periodic/resumable jobs, and maintenance.
- `riverqueue-macros`: `#[derive(JobArgs)]`.
- `riverqueue-migrate`: canonical River migration lines.
- `riverqueue-cli`: the `riverqueue` command-line program for migrations and
  benchmarks.
- `riverqueue-test`: typed fixtures and worker-test helpers.

The API uses a caller-owned SQLx pool, Tokio, typed workers, and
`CancellationToken`. `Client` isn't generic over the database: it accepts a
PostgreSQL or SQLite pool, and there's no driver trait to implement.

## Quick start

The [`riverqueue` crate README](riverqueue/README.md) walks through defining
a job, registering a worker, inserting, and starting a client.

To run Rust clients alongside River Go against one database, including
version matching, queue and kind layout, unique jobs, and rolling deployment
and rollback, see the
[mixed deployment guide](riverqueue/docs/mixed-deployments.md), also published
as `riverqueue::guide::mixed_deployments`.

Runnable examples in `riverqueue/examples` cover workers and graceful
shutdown, cancellation, transactional completion, unique and periodic jobs,
event subscriptions, custom schemas, SQLite, and a mixed Go and Rust
deployment; `riverqueue-migrate/examples` covers migrations.

Run the Rust suite from the repository root:

```sh
make lint/rust
make test/rust
make doc/rust
make check/rust/package
```

For basic end-to-end performance figures, the `riverqueue` binary from
`riverqueue-cli` has the Rust equivalent of `river bench`. It truncates the selected River job table,
so use a disposable database:

```sh
make bench/rust DATABASE_URL=postgres://localhost/river_bench \
  RUST_BENCH_ARGS='--duration 30s'
```

The command supports continuous burn, fixed `--num-total-jobs` burn-down,
custom schemas, tunable worker/pool/batch sizes, periodic jobs/sec output, and a
final jobs/sec plus p95 end-to-end latency summary. Use `riverqueue bench
--help` for all options.

PostgreSQL integration tests require a disposable database. They build only
with `--cfg river_postgres_tests`, which the Makefile targets pass to rustc
and rustdoc, building into `target/postgres-tests`:

```sh
RIVER_RUST_DATABASE_URL=postgres://localhost/river_rust_test \
  make test/rust/postgres
```

CI runs unit, doc, and SQLite tests on each supported Rust version, and
PostgreSQL tests against versions 14 through 18. Rust tests check unique
keys, retry bounds, cron schedules, and snooze counts against fixtures that
River's Go implementation generates into `conformance/testdata`, which isn't
committed. The `make test/rust` targets generate them first, so Go is needed
to run the tests; when running `cargo test` directly, run `make
generate/fixtures` beforehand. A missing fixture fails its test.

`make check/rust/package` builds the five publishable crate archives and
verifies that each one builds from its packaged sources, resolving the
exact-version workspace dependencies from the other archives. It does not
publish anything. Release tags use `riverqueue-vX.Y.Z`, independently of Go
module tags.
