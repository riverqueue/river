# Rust compatibility and deployment

River Rust is a native API over the same PostgreSQL protocol as River Go. A
Go/Rust pair is supported only when `conformance/manifest.json` lists the Go
and Rust versions, protocol revision, migration line, and a complete capability
set.

## Matched versions

- Rust feature minors track the corresponding River Go feature minor. Patch
  versions may differ.
- The checked manifest, rather than numeric similarity alone, declares a
  matched pair.
- PostgreSQL migrations in the Go repository remain canonical. CI verifies the
  checked Rust mirror byte-for-byte by SHA-256.
- Protocol-visible changes update the manifest, normalized schemas, fixtures,
  scenario inventory, and both adapters together. Language-local API changes
  do not require a protocol revision.

## Rolling deployment

1. Confirm the old and new services advertise the manifest's protocol
   revision and capabilities.
2. Apply migrations once with either matched migrator. Both implementations
   use the same `river_migration` history.
3. Deploy Rust producers or workers alongside Go in a small percentage. The
   mixed suite specifically covers cross-language insertion/work, competing
   workers, pause/resume, cancellation, lost notifications, leadership, and
   rescue after process death.
4. Increase Rust ownership while watching queue depth, retries, listener
   reconnects, leadership terms, completion duration, and database connection
   counts.
5. Keep at least one matched Go deployment available through the observation
   window.

Clients may use `poll_only(true)` where dedicated PostgreSQL listener
connections are undesirable. Polling remains the recovery path even when
listeners are enabled.

The matched Go baseline fetches jobs by queue rather than registered kind. If
a worker is missing for a fetched kind, both implementations record the same
retryable unknown-kind attempt error. Services with intentionally disjoint
worker registries should use disjoint queues; an unregistered kind on a shared
queue is not silently skipped.

## Rollback

Rollback does not revert the canonical schema. Stop Rust clients gracefully
and direct traffic/work back to the matched Go version. Already inserted or
running jobs remain ordinary River rows; a killed worker's running job is
handled by the normal rescuer. Only use migration-down as a separately planned
database operation when no deployed client requires the removed schema.

If a staged protocol change introduces dual-read/write behavior, follow that
change's specific rollback window. Never deploy clients with different
protocol revisions against the same database unless the compatibility document
for that revision explicitly permits it.

## Custom schemas and pools

Pass the same validated schema to `riverqueue_migrate::Migrator` and
`riverqueue::Client`. PostgreSQL identifiers are validated and quoted
centrally. A `Client` clones but does not close its caller-owned `sqlx::PgPool`,
so applications remain responsible for pool sizing and shutdown.

## Cancellation limit

Rust improves stuck-job handling by aborting a Tokio task after the
cancellation grace period. Tokio abort is still cooperative at async yield
points and cannot kill an already-running blocking thread. CPU-bound or
blocking untrusted work requiring hard isolation belongs in a subprocess; the
initial release does not promise native-thread termination.

## Release checklist

- `make verify`, Go test/lint, Rust format/clippy/test/docs, package archives,
  MSRV, semver, dependency, and license checks are green.
- PostgreSQL integration and Go/Rust conformance pass on every PostgreSQL major
  supported by River Go.
- Release-mode performance and the required soak duration pass.
- The feature matrix has no OSS PostgreSQL gap.

The first Rust release has no historical Rust API to compare. Its semver gate
therefore builds and compares the initial rustdoc baseline against itself for
each publishable library crate supported by `cargo-semver-checks`, validating
the gate wiring. The proc-macro crate has no ordinary library target for that
tool; package and compiled-example gates cover it before the first release.
After the first `riverqueue-vX.Y.Z` tag, the Make target automatically uses the
newest such tag as the real baseline; `RUST_SEMVER_BASELINE_REV` can select
another revision.
