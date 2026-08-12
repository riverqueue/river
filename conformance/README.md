# River cross-language conformance

This directory describes the PostgreSQL-backed protocol shared by River
implementations. It complements language-specific unit tests; it does not make
the internal Go `riverdriver` interface public.

`manifest.json` declares the matched implementation versions and enumerates
protocol capabilities. `schema/protocol.schema.json` validates that manifest.
`feature-matrix.md` records the PostgreSQL scope decision for each area.
Canonical migration hashes, codec goldens, declarative scenarios, and the
process-adapter contract live alongside them.

An implementation may claim compatibility only when its protocol revision and
capabilities match this manifest and the Go-only, Rust-only, and mixed adapter
suites pass.

The normal artifact gate is `make verify/conformance`. Database-backed tiers
use a disposable URL:

```sh
RIVER_CONFORMANCE_DATABASE_URL=postgres://localhost/river_conformance \
  make test/conformance
```

Performance and soak gates are explicit because they take longer:

```sh
RIVER_CONFORMANCE_DATABASE_URL=postgres://localhost/river_conformance \
RIVER_CONFORMANCE_PERFORMANCE=1 make test/conformance/performance

RIVER_CONFORMANCE_DATABASE_URL=postgres://localhost/river_conformance \
RIVER_CONFORMANCE_SOAK_DURATION=10m make test/conformance/soak
```

The worker and mixed release benchmarks use the same deterministic 10 ms
timed worker in both languages. Mixed mode provisions enough worker slots to
keep p95 focused on insertion-to-execution latency rather than incidental
queue backlog; throughput still covers the complete concurrent pipeline.
