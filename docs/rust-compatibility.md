# Rust compatibility and deployment

River Rust is a native API over the database protocols implemented by River
Go. PostgreSQL is the complete compatibility baseline, and SQLite has explicit
portable storage and runtime profiles. A Go/Rust pair is supported only when
`conformance/manifest.json` lists the Go and Rust versions, protocol revision,
migration lines, and a complete applicable capability set.

## Matched versions

- Rust feature minors track the corresponding River Go feature minor. Patch
  versions may differ.
- The checked manifest, rather than numeric similarity alone, declares a
  matched pair.
- PostgreSQL and SQLite migrations in the Go repository remain canonical. CI
  verifies each checked Rust mirror byte-for-byte by SHA-256.
- Protocol-visible changes update the manifest, normalized schemas, fixtures,
  scenario inventory, and both adapters together. Language-local API changes
  do not require a protocol revision.

## Unique arguments and resumable work

Cross-language uniqueness requires matching selected argument paths and JSON
serialization, not merely equivalent decoded values. Like Go, Rust sorts the
selected top-level keys but preserves nested object order. A nested Go struct
therefore needs the same serialized field order in the Rust type; a Go map
needs matching sorted nested keys. Prefer selecting individual scalar fields
when nested object order should not be part of identity. Missing explicitly
selected fields contribute no bytes, whereas an explicit `null` is a value.

Resumable step names and cursor shapes are persisted protocol. Keep names
stable across deployments and languages. Await steps sequentially; nested
steps are supported, but concurrent steps do not define a checkpoint order.
A caught step failure still fails the attempt and preserves its checkpoint.
The shared suite moves cursor-bearing attempts between engines in both
directions, including retries that skip previously completed steps.

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

Clients may use `without_notifications()` where dedicated PostgreSQL listener
connections or SQLite outbox polling are undesirable. Backend fetch polling
continues to discover eligible work. PostgreSQL polling also remains the
recovery path when listeners are enabled; SQLite normally uses a durable
notification outbox and polling rather than `LISTEN`/`NOTIFY`.

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

## Backends, schemas, and pools

For PostgreSQL, pass the same validated schema to `PostgresMigrator` and
`PostgresDatabase`. PostgreSQL identifiers are validated and quoted centrally.
SQLite uses its canonical main schema and `SqliteMigrator`. A `Client` clones
but does not close its caller-owned SQLx pool, so applications remain
responsible for connection options, pool sizing, and shutdown.

The public client and worker APIs are backend-neutral. Backend-specific
behavior stays behind River's sealed built-in database boundary; see
[rust-api-design.md](rust-api-design.md) for the rationale and the requirements
for a future MySQL backend.

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
- SQLite migration, storage, transaction, runtime, notification, leadership,
  scheduler/periodic, and lifecycle scenarios pass in every supported Go/Rust
  direction. Rescuer/cleaner maintenance is not claimed by the SQLite runtime
  profile until it has portable shared scenarios.
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
