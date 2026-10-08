# Validation

The original port and API review were validated locally on macOS arm64 with
JDK 25, Maven, Postgres 18, and the SQLite JDBC driver pinned in `pom.xml`.
The adapters also ran on JDK 27. The build now targets Java 21; the Java 21
compatibility trial is recorded below. Formatting works on JDK 21 and 25.

References:

- River Go/Rust: `eb16420fed22ce479f4843f0accd4c4bfba0885e` from
  `bg/plan-interoperable-rust-river-port`.
- River JS: `696c67b606c1202eee221f0718b15ee433260bdf` from
  `bg/interoperable-js-port`.

| Check | Result |
|---|---|
| Maven native tests and formatting | Passed, 112 library tests and 7 CLI tests |
| Insert-only profile | Passed |
| Full Postgres profile, maintenance, resilience | Passed |
| SQLite storage/runtime and resilience | Passed |
| Go + Java + Rust + JS on Postgres and SQLite | Passed |
| Same-host enqueue, worker, and mixed performance gate | Passed with the original throughput and p95 limits |
| Four-engine Postgres soak | Passed, five minutes |
| Four-engine SQLite endurance | Passed, five repetitions of the upstream multi-engine suite |

The native tests include exact unique-key goldens, cron/maintenance goldens,
transaction rollback and hook atomicity, transactional completion, queue drain,
and filtering claims to registered job kinds. The shared harness exercises
cross-process state transitions, notifications, reconnection, leadership,
rescue, periodic insertion, cancellation races, migrations, and raw row equality.

`conformance/scenario-coverage.json` maps shared scenarios to their harness
implementations. `conformance/feature-inventory.json` preserves all 304 upstream
inventory entries and identifies their Java surfaces and API differences.
`conformance/reference-sources.json` now records only the legacy adapter contract
hash; migrations and fixture tests follow the current checkout as described below.

These results cover the pinned contracts and finite soak runs. They are not a
proof of every possible workload, nor a production deployment or a published
Maven release. The separate Pro report records a known Go-reference SQLite
failure; no Go source was changed to suppress it.

## Migration CLI validation

The CLI tests cover environment/argument handling, offline SQL export, dry runs,
targets and step limits, and launching the executable JAR without an external
classpath. Library tests cover concurrent SQLite initialization, migration
rollback, legacy migration history, and Postgres schema creation and removal.
All were run with Postgres enabled; no tests were skipped.

After the migration CLI changes, the Postgres mixed conformance suite and both
SQLite storage/runtime suites passed against the pinned Go reference. The
README quick start was compiled and run; its remaining Java examples compiled,
and its SQLite testing example ran successfully.

## Public API review validation

The API review added regression coverage for typed retrieval and mixed-kind
batches, checked JDBC hook failure recovery, queue notification atomicity,
committed completion followed by a handler exception, and repeated graceful-stop
timeouts without implicit cancellation, and job-aware retry policies. All 112 library and 7 CLI tests passed
with Postgres enabled and no skips; Maven's formatting checks passed.

The Postgres maintenance, mixed-engine, and resilience suites and the SQLite
storage, runtime, and resilience suites were rerun against the pinned Go
reference. All README Java blocks compiled; the quickstart and SQLite JUnit
example executed successfully. The Rust/JS matrix, performance, and soak entries
above record the earlier port validation and were not repeated for this API pass.

## Java 21 compatibility trial

Validated on macOS arm64 with Temurin 21.0.12.1 and OpenJDK 25.0.1. On both
JDKs, the full Maven build and formatting checks passed: 114 library tests and
7 CLI tests, with Postgres enabled and no skips. The library and executable
CLI target Java 21 (class file version 65), including when built on JDK 25.

The changes replace unnamed `_` variables with named parameters and use
`ReentrantLock` for queue and leadership operations that can block. Java 21
pins a virtual thread's carrier when it blocks inside a monitor. Two regression
tests launch a JVM with exactly one carrier and use latches to test blocking
claims and leadership callbacks. Both failed before the lock change and pass
after it. No public API or dependency changes were needed.

With both `JAVA_HOME` and `PATH` selecting JDK 21, the Postgres maintenance,
mixed-engine, and resilience suites and the SQLite storage, runtime, and
resilience suites passed against the pinned Go reference. All README Java
blocks compiled on JDK 21; the quickstart and SQLite JUnit example executed.
The Java CI workflow now tests JDK 21 and 25 with its existing path filters.

Rust/JS peer, performance, and soak runs were not repeated for this trial;
the earlier results above do not establish their behavior on JDK 21.

## Runtime and option review (2026-10-05)

The full Maven build and formatting checks passed on Temurin 21.0.12.1 and
OpenJDK 25.0.1: 126 library tests and 7 CLI tests, with Postgres enabled and
no skips. Eleven regression cases reproduced failures before the fixes and
passed afterward. They cover throwing observers/subscribers/error handlers,
throwing or invalid retry policies, terminal attempts bypassing retry policies,
distinct IDs when reusing a builder, and immutable query metadata. A further
test verifies the `uniqueBy(String...)` overload against stored uniqueness keys,
including literal dots in field names.

On JDK 21, the Postgres maintenance, mixed-engine, and resilience suites and
the SQLite storage, runtime, and resilience suites passed against the pinned Go
reference. Rust/JS peer, performance, and soak suites were not repeated for this
review. No Go code or shared SQL was changed.

## Current conformance and CI alignment (2026-10-06)

Rebased onto `master` at `4add77d2`, the merged
[PR #1451](https://github.com/riverqueue/river/pull/1451). Java reads all four
Go-generated fixtures from `conformance/testdata`, without committed or classpath
copies. The Java branch leaves `js/` and `rust/` identical to this reference.

| Check | Result |
| --- | --- |
| `make test/java/conformance` on JDK 21 | Passed, 142 fixture tests, no database required |
| Postgres 14, 15, 16, 17, and 18 on JDK 25 | Passed, 175 library tests and 7 CLI tests per version, no skips |
| Postgres 18 on JDK 21 | Passed, 175 library tests and 7 CLI tests, no skips |
| SQLite on JDK 21 and 25 | Passed, 174 library tests and 7 CLI tests per JDK, no skips |
| `make test/rust/conformance` | Passed, 127 tests, including notification dispatch |
| Go `make test` and `make lint` | Passed in the working checkout |
| Migration mirror, formatting, and five publishable JARs | Passed |
| Go module archives | Passed in a clean snapshot excluding unrelated untracked `rust-demo/` artifacts |
| Java and shared conformance workflows | Passed actionlint |

The recent Rust dispatcher test is mirrored in Java: all seven notification
fixtures are dispatched for both the sending client and an observer. Assertions
cover targeted cancellation, unrelated jobs, queue signals, leadership signals,
and ignoring self-resignation. Additional checks cover pending cancellation and
prevent a control payload on another topic from cancelling a job.

Both JDBC listeners now preserve topics when dispatching. Peer resignations wake
leadership election immediately; self-resignations are ignored. Database-backed
tests use readiness signals to verify topic routing, delivery after malformed
JSON, and leadership wakeup with a one-hour poll interval. Both regressions
failed when the previous topic routing and producer-only wakeup behavior was
restored in an isolated copy, and pass with the fixes on both backends.

Earlier fixture checks also exposed and fixed Java's treatment of boolean snooze
metadata: Go counts `true` as one before incrementing. Cron cases with no next
occurrence explicitly assert that the schedule is exhausted.

The CI layout separates quality/package checks, a JDK 21/25 SQLite matrix, and a
Postgres 14–18 matrix on JDK 25. Client and worker tests share assertions across
backends. Postgres tests create and remove a schema per case; the version
matrix left no test schemas behind. The SQLite runs used an unreachable
Postgres URL to verify that the explicit SQLite target remains independent.
The Postgres target fails without its required database URL.

Negative checks during the alignment verified missing-fixture diagnostics,
missing/changed/extra migration detection, rejection of test/fixture/adapter
content in JARs, and Go archive rejection when `java/go.mod` is removed. A
synthetic next canonical migration verified that synchronization updates the
runtime catalog as well as SQL. Fixture generation produced deterministic bytes.

These are local executions of CI commands. Hosted workflows, the historical
cross-process harness, multi-engine storage/runtime suites, and soaks were not
rerun for this alignment. Earlier results remain tied to their recorded revisions.

## Prerelease API and runtime review (2026-10-06)

The review aligned default list ordering with Go's ascending ID order and added
18 regression cases for pagination, timeout supervision, durable completion,
duration validation, and rescue configuration.

| Check | Result |
| --- | --- |
| `make test/java/sqlite` on JDK 21 and 25 | Passed, 192 library tests and 7 CLI tests per JDK, no skips |
| `make test/java/postgres` on JDK 21 and 25, local Postgres | Passed, 193 library tests and 7 CLI tests per JDK, no skips |
| `make lint/java` and `make check/java/package` on JDK 21 | Passed, including adapter compilation and all five JARs |

Tests hold the producer heartbeat or completion transaction behind latches to
verify that timeouts remain independent of dispatch and that a finished handler
cannot be cancelled during acknowledgement. A transient completion failure after
an interrupted handler verifies retry and subsequent reuse of its worker slot.
An oversized snooze must fail the attempt normally rather than strand it.

The new regression cases also ran against the previous production sources in an
isolated copy. They detected the prior ordering, cancellation, completion retry,
timeout, duration, and rescue defects; the ordinary and disabled-timeout rescue
defaults continued to pass. No Go, Rust, JavaScript, or Pro source was changed.
The historical multi-engine and soak suites were not rerun for this review.

## Conformance review after rebase (2026-10-06)

Compared the Java port with `master` at `f36a6452`, including the JavaScript
conformance additions in PR #1460. The fixture generators and Rust conformance
tests are unchanged since the previous alignment. Java now consumes both unique
fixture groups, including duplicate top-level keys and integer-like map keys,
and exercises retry jitter boundaries, fixed attempt-error semantics, and
resumable checkpoint metadata.

New storage tests read Go attempt errors and uniqueness bits from real rows and
verify periodic IDs, insert nonces, output, and rescue counters against the
generated metadata keys. The conformance target selects temporary SQLite
databases explicitly; the full database targets run these same assertions on
their selected backend.

The rescue test exposed a SQLite snapshot-upgrade failure when another writer
committed between selecting and updating a stuck job. Rescue now reserves the
writer before selecting, using the existing lock query. The test attempts a
competing write during retry selection and verifies that it is excluded. In an
isolated copy, this assertion fails against the previous implementation.

| Check | Result |
| --- | --- |
| `make test/java/conformance` on JDK 21 | Passed, 148 tests, with an unreachable Postgres URL |
| `make test/java/sqlite` on JDK 21 and 25 | Passed, 198 library tests and 7 CLI tests per JDK, no skips |
| `make test/java/postgres` on JDK 21 and 25, local Postgres | Passed, 199 library tests and 7 CLI tests per JDK, no skips |
| `make lint/java` and `make check/java/package` on JDK 21 | Passed, including migration verification and all five JARs |

No Go, Rust, JavaScript, or shared fixture source was changed. Postgres's
version matrix and the historical multi-engine and soak suites were not rerun.
