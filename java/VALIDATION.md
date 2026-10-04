# Validation

Validated locally on macOS arm64 with JDK 25, Maven, PostgreSQL 18, and the
SQLite JDBC driver pinned in `pom.xml`. The adapters also ran on the installed
JDK 27. The library targets Java 25; formatting requires JDK 25.

References:

- River Go/Rust: `eb16420fed22ce479f4843f0accd4c4bfba0885e` from
  `bg/plan-interoperable-rust-river-port`.
- River JS: `696c67b606c1202eee221f0718b15ee433260bdf` from
  `bg/interoperable-js-port`.

| Check | Result |
|---|---|
| Maven native tests and formatting | Passed, 112 library tests and 7 CLI tests |
| Insert-only profile | Passed |
| Full PostgreSQL profile, maintenance, resilience | Passed |
| SQLite storage/runtime and resilience | Passed |
| Go + Java + Rust + JS on PostgreSQL and SQLite | Passed |
| Same-host enqueue, worker, and mixed performance gate | Passed with the original throughput and p95 limits |
| Four-engine PostgreSQL soak | Passed, five minutes |
| Four-engine SQLite endurance | Passed, five repetitions of the upstream multi-engine suite |

The native tests include exact unique-key goldens, cron/maintenance goldens,
transaction rollback and hook atomicity, transactional completion, queue drain,
and filtering claims to registered job kinds. The shared harness exercises
cross-process state transitions, notifications, reconnection, leadership,
rescue, periodic insertion, cancellation races, migrations, and raw row equality.

`conformance/scenario-coverage.json` maps shared scenarios to their harness
implementations. `conformance/feature-inventory.json` preserves all 304 upstream
inventory entries and identifies their Java surfaces and API differences.
`conformance/reference-sources.json` records hashes of imported Go resources.

These results cover the pinned contracts and finite soak runs. They are not a
proof of every possible workload, nor a production deployment or a published
Maven release. The separate Pro report records a known Go-reference SQLite
failure; no Go source was changed to suppress it.

## Migration CLI validation

The CLI tests cover environment/argument handling, offline SQL export, dry runs,
targets and step limits, and launching the executable JAR without an external
classpath. Library tests cover concurrent SQLite initialization, migration
rollback, legacy migration history, and PostgreSQL schema creation and removal.
All were run with PostgreSQL enabled; no tests were skipped.

After the migration CLI changes, the PostgreSQL mixed conformance suite and both
SQLite storage/runtime suites passed against the pinned Go reference. The
README quick start was compiled and run; its remaining Java examples compiled,
and its SQLite testing example ran successfully.

## Public API review validation

The API review added regression coverage for typed retrieval and mixed-kind
batches, checked JDBC hook failure recovery, queue notification atomicity,
committed completion followed by a handler exception, and repeated graceful-stop
timeouts without implicit cancellation, and job-aware retry policies. All 112 library and 7 CLI tests passed
with PostgreSQL enabled and no skips; Maven's formatting checks passed.

The PostgreSQL maintenance, mixed-engine, and resilience suites and the SQLite
storage, runtime, and resilience suites were rerun against the pinned Go
reference. All README Java blocks compiled; the quickstart and SQLite JUnit
example executed successfully. The Rust/JS matrix, performance, and soak entries
above record the earlier port validation and were not repeated for this API pass.
