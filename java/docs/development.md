# River Java development

Run commands from the repository root unless a section says otherwise. See the [Java README](../README.md) for application usage.

## Building from source

Install the Go version specified by [`go.work`](../../go.work), Maven, and JDK 21 or 25. Set `JAVA_HOME` and `PATH` to that JDK; the pinned formatter does not run on JDK 27. Go is needed to generate test fixtures, but applications using River do not need Go.

Generate the fixtures and install the Java artifacts into your local Maven repository:

```sh
make generate/fixtures
mvn --batch-mode --no-transfer-progress -f java/pom.xml install
```

The executable migration CLI is built at `java/cli/target/river-cli-0.48.0-alpha.1-all.jar`. For a build without running tests, use `make build/java`.

## Tests and checks

From the repository root, with Go (the version in `go.work`), JDK 21 or 25,
and Maven installed:

```sh
make test/java/conformance
make test/java/sqlite
RIVER_TEST_DATABASE_URL=postgres://localhost/river_test make test/java/postgres
make lint/java
make check/java/package
make verify/java-migrations
make check/modzip
```

`test/java` runs library and executable CLI tests and checks formatting. Client
and worker tests use Postgres when `RIVER_TEST_DATABASE_URL` is set, with a
fresh schema for each test that is removed afterward. Otherwise they use SQLite.
`test/java/postgres` requires that URL; `test/java/sqlite` explicitly uses SQLite
and excludes Postgres-only tests, even if a URL is set in your environment.
`test/java/conformance` needs no external database: it runs the JUnit tests tagged
`conformance`, using temporary SQLite databases for storage checks even if a
Postgres URL is set. The full SQLite and Postgres targets run these checks
against their selected backend. These targets first generate fresh fixtures
directly from this checkout's Go implementation, just like
`test/js/conformance` and `test/rust/conformance`.

Java reads all four files in `conformance/testdata`: uniqueness hashes, cron
schedules, snooze counters, and protocol values (states, metadata keys,
notification payloads, attempt errors, and retry bounds). These files are ignored
by Git and read at test time. Missing fixtures fail with instructions to run
`make generate/fixtures`; there are no bundled fallback goldens. To use Maven or
an IDE directly, generate the fixtures first. `make test` from `java/` delegates
to the root target; `mvn spotless:apply` formats Java sources.

Notification tests check both encoding and dispatch: targeted cancellation, queue
wakeups, leadership signals, and ignoring a client's own resignation.
Database-backed tests also verify topic routing, recovery after malformed
payloads, and immediate leadership wakeup after a peer resigns.

Uniqueness tests include the raw JSON fixtures for duplicate keys and integer-like
map keys. Protocol tests exercise both retry jitter boundaries, decode stored
states and uniqueness bits, and verify periodic IDs, insert nonces, resumable
checkpoints, output, and rescue counters using Go's metadata keys.

`make generate/java-migrations` syncs SQL and the migration catalog from the
canonical Go drivers. `verify/java-migrations` detects changed, missing, or extra
migrations. `check/java/package` checks the library and CLI archives, including
source JARs, for development content and verifies bundled runtime resources.
The Go maintenance tools live in `java/bin/`. `make test/java` and `make lint/java`
include their tests and lint checks; `make test/java/tools` and
`make lint/java/tools` run only the tool checks. From `java/`, use `make test/tools`
and `make lint/tools`. These checks run in Java CI, separately from the root
Go-only `make test` and `make lint` targets.
The legacy adapter is not installed or deployed as a Maven artifact.
`java/go.mod` excludes this directory from Go module archives and package
discovery. The Make targets invoke the Go tools by file from the root workspace;
the module is not part of `go.work` and must not be tagged as a Go module.

The Java CI workflow follows the Rust and JavaScript layout: a quality/package
job, a JDK 21/25 matrix running unit and SQLite tests, and a Postgres 14–18
matrix running client, worker, and migration tests on JDK 25. All jobs use the
shared Java setup action and Maven dependency caching. The whole workflow is
filtered to changes in Java, its build configuration, fixtures, and canonical
migrations. Go changes run the smaller fixture suite in the shared Conformance
workflow alongside Rust and JavaScript.

## Legacy cross-process harness

The original interoperability adapter remains available for broader storage,
runtime, and multi-engine scenarios. It uses the historical harness pinned in
`java/conformance/reference-revision`, which is separate from the current
Go-generated fixtures and is not part of `master`'s conformance module.

From `java/`, using only disposable databases (the harness resets job tables):

```sh
RIVER_CONFORMANCE_DATABASE_URL=postgres://localhost/river_java_conformance \
  python3 conformance/bin/run.py postgres
python3 conformance/bin/run.py sqlite
```

The runner extracts that revision into ignored build storage and registers the
Java candidate there. `--reference /path/to/checkout` uses an existing harness.
`--refresh` opts into the old reference branch's current head; review its
contract and profiles before updating the pin.

See [intentional differences](../DIFFERENCES.md) for the Java API and lifecycle
choices. Passing a conformance profile demonstrates the scenarios in that
profile; it is not a claim about untested behavior or performance.

The full peer matrix uses `multi` with `RIVER_CONFORMANCE_PEER_FILE` set to a
colon-separated list of Rust and JS candidate descriptors, and
`RIVERQUEUE_JS_ROOT` pointing to the JS checkout. `multi-soak` also requires
`RIVER_CONFORMANCE_MULTI_ENGINE_SOAK_DURATION=5m` (or longer). The upstream
harness currently has a Postgres soak; SQLite endurance validation repeats
`TestMultiEngineSQLiteConformance` with `-count=5`.

`python3 bin/import-reference.py --check /path/to/reference` verifies the legacy
adapter contract. Omit `--check` to import it and record its hash. This command
does not overwrite the current migrations or generated fixtures.

See [validation results](../VALIDATION.md) for the tested revisions and remaining
limitations.
