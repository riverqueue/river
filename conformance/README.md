# River cross-language conformance

This directory describes the database protocol shared by River implementations.
It complements language-specific unit tests; it does not make the internal Go
`riverdriver` interface public.

`manifest.json` declares the matched implementation versions and enumerates
protocol capabilities. `schema/protocol.schema.json` validates that manifest.
`feature-matrix.md` records the backend scope decision for each area.
Canonical migration hashes, codec goldens, declarative scenarios, and the
process-adapter contract live alongside them.

`scenarios/core.json`, `scenarios/sqlite-storage.json`, and
`scenarios/sqlite-runtime.json` are checked against an executable Go registry.
Every ID has exactly one owning harness test, and an owning test can only pass
after it reports its complete registered set. Missing, stale, duplicate,
mis-tiered, or merely declarative entries therefore fail validation.

An implementation may claim compatibility only when its protocol revision and
capabilities match this manifest and its implementation-local and mixed adapter
suites pass.

The mixed harness is candidate-neutral. It always runs Go as the reference and
uses the checked Rust descriptor by default. `RIVER_CONFORMANCE_CANDIDATE_FILE`
can point it at a descriptor supplied by another repository, while
`RIVER_CONFORMANCE_CANDIDATE` accepts the same object inline. See
[`adapter/README.md`](adapter/README.md) for the candidate descriptor. This is
the entry point for JavaScript and future implementations; it does not require
copying another engine's language-specific tests.

The normal artifact gate is `make verify/conformance`. The full PostgreSQL tier
uses an externally provisioned disposable URL:

```sh
RIVER_CONFORMANCE_DATABASE_URL=postgres://localhost/river_conformance \
  make test/conformance
```

The SQLite gate runs both the backend-neutral `portable-storage-v1` subset and
the `sqlite-runtime-v1` worker/queue profile. It provisions an isolated
temporary database per test, enables WAL and a five-second busy timeout in both
adapters, and needs no database environment variable:

```sh
make test/conformance/sqlite
```

Both commands use either candidate setting when supplied. This lets a
JavaScript adapter run the same PostgreSQL contract and SQLite profiles without
a language-specific checklist.

Performance and soak gates are explicit because they take longer:

```sh
RIVER_CONFORMANCE_DATABASE_URL=postgres://localhost/river_conformance \
RIVER_CONFORMANCE_PERFORMANCE=1 make test/conformance/performance

RIVER_CONFORMANCE_DATABASE_URL=postgres://localhost/river_conformance \
RIVER_CONFORMANCE_SOAK_DURATION=10m make test/conformance/soak
```

Direct three-engine tiers start Go, Rust, and JavaScript simultaneously against
one PostgreSQL database. Supply JavaScript as the ordinary candidate; Rust is
the default peer descriptor. The smoke tier deterministically fills one blocked
worker slot in every engine, forces leadership through all three runtimes,
terminates each engine's database connections, and checks recovery and
connection bounds:

```sh
RIVER_CONFORMANCE_DATABASE_URL=postgres://localhost/river_conformance \
RIVER_CONFORMANCE_CANDIDATE='{...javascript descriptor...}' \
  make test/conformance/three-engine

RIVER_CONFORMANCE_DATABASE_URL=postgres://localhost/river_conformance \
RIVER_CONFORMANCE_CANDIDATE='{...javascript descriptor...}' \
RIVER_CONFORMANCE_THREE_ENGINE_PERFORMANCE=1 \
  make test/conformance/three-engine/performance

RIVER_CONFORMANCE_DATABASE_URL=postgres://localhost/river_conformance \
RIVER_CONFORMANCE_CANDIDATE='{...javascript descriptor...}' \
RIVER_CONFORMANCE_THREE_ENGINE_SOAK_DURATION=10m \
  make test/conformance/three-engine/soak
```

`RIVER_CONFORMANCE_PEER` and `RIVER_CONFORMANCE_PEER_FILE` can replace the
default Rust peer with another descriptor. The tier still requires exactly one
Go, Rust, and JavaScript implementation so it cannot accidentally degrade into
a duplicated pairwise test.

The worker and mixed release benchmarks use the same deterministic 10 ms
timed worker in both languages. Mixed mode provisions enough worker slots to
keep p95 focused on insertion-to-execution latency rather than incidental
queue backlog; throughput still covers the complete concurrent pipeline.
