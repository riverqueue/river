# River TypeScript development

## Setup

The JavaScript workspace lives in `js/` of the River repository, next to the
Go and Rust implementations and the shared conformance suite it is tested
against. Run the `pnpm` commands below from `js/`, or use River's top-level
`make` targets (`make lint/js`, `make test/js`, and so on), which delegate to
`pnpm -C js`.

Use an official Node.js 26 build, which includes native Temporal. The
repository's `.node-version` selects Node 26 for version managers such as fnm,
nvm (`nvm use $(cat .node-version)`), and `actions/setup-node`. Some Node.js
builds compiled from source, including some distribution and Homebrew
packages, lack Temporal, and most unit tests then fail with
`ReferenceError: Temporal is not defined`. Check before installing, then use
the pnpm version pinned by `packageManager` in `package.json`:

    node -p "typeof Temporal"   # must print "object"
    pnpm install

TypeScript 6.0 is the minimum compiler for the published declarations, which
reference the `esnext.temporal` library. The workspace builds with TypeScript 6
and also typechecks with the TypeScript-next preview (`typecheck:next`).

## Commands

```sh
pnpm run build             # Build the core package
pnpm run build:all         # Build all public packages
pnpm run clean:all         # Clean all build output
pnpm run fmt               # Format code with Prettier
pnpm run fmt:check         # Check formatting (for CI)
pnpm run lint              # Run ESLint
pnpm run lint:fix          # Run ESLint with auto-fix
pnpm run test              # Run unit tests
pnpm run test:conformance  # Run River's conformance suite against this workspace
pnpm run test:coverage     # Run unit tests with line/branch coverage
pnpm run test:integration  # Run integration tests (requires database)
pnpm run verify:migrations # Verify generated migration files and hashes
pnpm run migration:legacy  # Verify and compile the pinned 0.1.0 fixture
pnpm run docs:snippets     # Typecheck package README examples
pnpm run api:report        # Regenerate the etc/*.api.md declaration reports
pnpm run package:check     # Validate tarballs, consumers, and examples
pnpm audit                 # Check all dependency advisories
```

ESLint's type-aware rules and `typecheck:tests` read `riverqueue` through its
built declarations, as the satellite packages do, so run `pnpm run build`
first after changing `src/`. The unit tests import the other workspace
packages through their build output too, and `@riverqueue/worker-threads`
runs compiled handler modules in its threads, so run `pnpm run build:all`
before `pnpm run test`.

`verify:migrations` compares every file of the generated migration mirror and
its manifest with River's canonical Go migration sources in the surrounding
repository, and checks that the package version matches the JavaScript version
in River's `conformance/manifest.json`. `generate:migrations` refreshes the
mirror after a River migration changes.

`package:check` creates real tarballs, validates them with publint and Are The
Types Wrong, checks that every JavaScript and declaration map resolves to
TypeScript source shipped in the same archive, and inspects licenses, package
metadata, and archive contents. It installs the tarballs into a clean consumer
without type packages, confirms that each library refuses to install beside a
different `riverqueue` version, exercises ESM and Node's `require(esm)`,
compiles strict TS6 and TS-next consumers, and builds every example against the
packed artifacts. It then runs `scripts/packed-tests/*.test.mjs` in that
consumer with Node's built-in `node --test`, so plain JavaScript exercises the
installed tarballs with no transpiler or workspace alias in between: SQLite and
worker-thread jobs end to end with exact int64 args, one shared `riverqueue`
instance and error class hierarchy across packages, `require(esm)`, subpath
exports, the CLI, and the test helpers. Its PostgreSQL tests run in a throwaway
schema when `DATABASE_URL` is set. Database-free examples also run in this
gate. `package:examples` runs the same packed examples with PostgreSQL when
`DATABASE_URL` is set. Neither command publishes anything.

`package:check` and `migration:legacy` reject archive paths that escape the
package root and any packed path or text containing the local checkout or home
directory. Set `RIVER_PACKAGE_DENYLIST` to a comma-separated list of extra
case-insensitive substrings (for example, names of unpublished sibling
checkouts) to reject them as well without committing those names.

`api:report` regenerates the checked-in `etc/*.api.md` declaration reports
from the built packages, and `api:check` (run in CI) fails when they are
stale. Reports include TSDoc on every declaration and member, so
documentation changes show up in review. Both commands also fail when an
exported declaration refers to a type declared in the same package that the
entry point does not export. Export such a type, or allow-list it with a
reason in `scripts/api-reports.mjs`; allow-listed types appear in a separate
section of the report.

`docs:snippets` typechecks the TypeScript fences in every publishable package
README. `migration:legacy` verifies the pinned 0.1.0 npm archive and tagged
documentation snapshot, then compiles its old consumer on both compiler lanes.

Prisma 7.9.1 currently pins `deepmerge-ts` 7.1.5 through its configuration
toolchain. The root override to 8.0.1 carries the upstream fix for
[GHSA-ggr8-5vv4-36mx](https://github.com/advisories/GHSA-ggr8-5vv4-36mx)
until Prisma publishes a stable fixed dependency. Recheck the
[upstream Prisma issue](https://github.com/prisma/orm/issues/30052) and remove
the override only after `pnpm audit --prod` stays clean without it.

Prisma 7.9.1 also pins `mysql2` 3.15.3 for its MySQL tooling, which this
workspace never uses. The `mysql2` 3.24.4 override patches
[GHSA-3f6p-5ww8-9rcr](https://github.com/advisories/GHSA-3f6p-5ww8-9rcr) and
[GHSA-rgwj-5xj2-c3m3](https://github.com/advisories/GHSA-rgwj-5xj2-c3m3) in
that development-only path; remove it once Prisma depends on a fixed release.

The `nanoid` 3.3.18 override patches
[GHSA-2v37-7h3g-55p8](https://github.com/advisories/GHSA-2v37-7h3g-55p8)
in Vite's development-only PostCSS path. It remains within PostCSS's declared
compatible range and can be removed once the ordinary lock resolution is at
least 3.3.18.

## Test guards

Every Vitest configuration loads `scripts/vitest-setup.mjs`. It fails the
running test when an `unhandledRejection` or `uncaughtException` escapes it,
and fails a test file that finishes with more event-loop handles (timers,
sockets, servers, child processes) than it started with. Close pools,
listeners, and clients in `afterAll`, and `unref()` timers that intentionally
outlive a test. The handle check waits up to two seconds for handles that are
already closing, because node-postgres resolves `pool.end()` before its
sockets report closed.

Vitest's `--detectAsyncLeaks` is not used as a gate: it also reports
promises that tests deliberately leave pending (for example a mocked query
that never settles to exercise cancellation), and its async hooks slow the
large batching tests past their timeouts.

## Property tests

Files named `*.property.test.ts` use [fast-check](https://fast-check.dev) to
check invariants over generated inputs: the exact JSON codecs, unique-key
hashing against a reference encoding of River's rules, opaque and portable
job list cursors, SQLite keyset pagination across ties, and model-based
command runs of the periodic job registry and the completion batcher. They
run with the ordinary unit suite and use a fixed seed, so a run is
reproducible. A failure prints its seed and shrink path; replay it, or
explore new inputs, with `FAST_CHECK_SEED`:

    FAST_CHECK_SEED=-1747166622 pnpm test src/json.property.test.ts
    FAST_CHECK_SEED=random pnpm test

## Line and branch coverage

`pnpm run test:coverage` runs the unit suite with V8 coverage and writes a
summary to the terminal and an HTML report to `coverage/`. Coverage is
supplementary evidence only: it shows code no unit test executes, not that
behavior matches River, which the shared conformance scenarios and
JavaScript-native tests establish. It has no threshold and is not a CI gate.

## Integration tests

Integration tests run against a real PostgreSQL database with River's schema.
Create a disposable test database, build the workspace, and apply the exact
generated migrations from this checkout:

    createdb river_test
    pnpm run build:all
    node cli/dist/bin.js migrate-up \
      --database-url "postgres://localhost/river_test"

By default, tests connect to `postgres://localhost:5432/river_test`. Override with `TEST_DATABASE_URL`:

    TEST_DATABASE_URL="postgres://user:pass@host:5432/mydb" pnpm run test:integration

The conformance adapter's PostgreSQL integration tests read River's adapter
contract and manifest from the surrounding repository.

The integration suite includes a bounded multi-client stress test: three
clients share one database while jobs are inserted from every client, one
client is gracefully replaced mid-iteration, and seeded cancellations race
claims, handlers, and completions. Each iteration asserts that every job is
worked at most once, reaches a terminal state that matches its single
observed terminal event, and is never lost. Scale it into a soak run, and
replay a failure by its reported seed, with:

    RIVER_STRESS_ITERATIONS=500 RIVER_STRESS_SEED=7 pnpm run test:integration \
      driver/pg/src/stress.integration.test.ts

## Running the CLI from a checkout

After `pnpm run build:all`, run the workspace's `riverqueue` command with
`node cli/dist/bin.js <command>`. pnpm doesn't link a workspace package's
own `bin` into its `node_modules/.bin`, so
`pnpm --filter=@riverqueue/cli exec riverqueue` can't find it. For example,
to benchmark against a disposable database:

    node cli/dist/bin.js bench \
      --database-url "postgres://localhost/river_bench" --yes --duration 30s

## Cross-language conformance

The canonical suite lives in River's `conformance/` directory and is shared by
the Go, Rust, JavaScript, and future implementations. The workspace owns the
JavaScript candidate descriptor, `conformance/candidate.json`, which tells
River's harness how to start the adapter (`conformance/dist/bin.js`), which
profiles it serves, which optional `start` tuning it honors, and its release
performance bounds. Its `version` must equal the version the adapter reports,
which is the conformance package's; the runner refuses to start otherwise.

`pnpm run test:conformance` is a thin wrapper over River's `make` targets, so
local runs match CI. It sets `RIVER_CONFORMANCE_CANDIDATE_FILE` to the
descriptor and `RIVER_CONFORMANCE_PEER_FILE` to River's Rust descriptor, and
puts the running Node.js first on `PATH`. Build this workspace first. SQLite
needs no external service; a PostgreSQL URL adds the PostgreSQL tiers:

```sh
pnpm run build:all
pnpm run test:conformance
pnpm run test:conformance -- \
  --database-url postgres://user@localhost:5432/river_conformance
```

| Tier                                                                    | `make` target                               | Runs when                      |
| ----------------------------------------------------------------------- | ------------------------------------------- | ------------------------------ |
| SQLite storage, runtime, and resilience                                 | `test/conformance/sqlite`                   | always                         |
| Go and JavaScript on PostgreSQL: mixed, maintenance, and resilience     | `test/conformance`                          | `--database-url`               |
| Insert-only profile through `@riverqueue/driver-prisma`                 | `test/conformance/insert-only`              | `--database-url`               |
| Go, Rust, and JavaScript together, and Rust and JavaScript SQLite pairs | `test/conformance/multi-engine`             | `--multi-engine`               |
| Go and JavaScript release performance                                   | `test/conformance/performance`              | `--performance`                |
| Multi-engine release performance                                        | `test/conformance/multi-engine/performance` | `--multi-engine-performance`   |
| Go and JavaScript soak                                                  | `test/conformance/soak`                     | `--soak-duration`              |
| Multi-engine soak                                                       | `test/conformance/multi-engine/soak`        | `--multi-engine-soak-duration` |

The database URL must name a TCP host and port, not a Unix socket directory:
the resilience scenarios reach PostgreSQL through a fault proxy that rewrites
the URL's address to make the database unavailable to one worker at a time.
Use a disposable database; the harness truncates and migrates River's tables
and creates and drops schemas.

The release tiers use the same adapter and database workload:

```sh
pnpm run test:conformance:performance -- \
  --database-url postgres://user@localhost:5432/river_conformance \
  --performance-jobs 1000
pnpm run test:conformance:soak -- \
  --database-url postgres://user@localhost:5432/river_conformance
pnpm run test:conformance:multi-engine -- \
  --database-url postgres://user@localhost:5432/river_conformance
pnpm run test:conformance:multi-engine:performance -- \
  --database-url postgres://user@localhost:5432/river_conformance
pnpm run test:conformance:multi-engine:soak -- \
  --database-url postgres://user@localhost:5432/river_conformance
```

The soak scripts run for 10 minutes by default; pass `--soak-duration` or
`--multi-engine-soak-duration` (a Go duration such as `2m` or `1h`) to change
that. `--performance-jobs` sets the per-run workload of the performance gates,
which otherwise use the harness default of 200 jobs. Every invocation first
runs the SQLite tier and, with a database URL, the PostgreSQL and insert-only
tiers. Set `RIVER_CONFORMANCE_REQUIRED=1`, as CI does, to turn every skipped
tier or scenario into a failure.

The multi-engine tiers run Go, Rust, and JavaScript simultaneously against one
database for deterministic worker competition, leadership turnover through all
three runtimes, connection-fault recovery, and bounded connection use, and run
the SQLite checks between Rust and JavaScript directly. River's Rust descriptor
is the peer, and its build command compiles River's Rust adapter with `cargo`,
so these tiers need a Rust toolchain. River's own `make
test/conformance/multi-engine` targets add this workspace's descriptor as the
peer of the Rust candidate unless another peer is configured, so they run the
same three implementations once `js/` is built.

### Scenario coverage matrix

Passing the shared scenarios proves compatibility with Go, but a regression
should also fail a JavaScript-native test before it reaches the harness.
`conformance/scenario-coverage.json` maps every scenario ID in River's
PostgreSQL and SQLite catalogs to the Vitest tests that cover the same behavior
(`path > describe > test`), to a documented `gap`, or to a `not_applicable`
reason for scenarios that only mean something with several engines or as a
harness-level measurement.
`docs/conformance-coverage.md` is generated from it.

```sh
pnpm run coverage:scenarios        # check
pnpm run coverage:scenarios:write  # regenerate
```

The check fails when a catalog scenario has no entry, an entry names a
scenario River no longer has, a cited test does not exist, or the generated
matrix is out of date. It lists tests with `vitest list`, so renaming a test
means updating the mapping, and adding a scenario to River's catalogs means
mapping it before regenerating.

## Continuous integration

River's `.github/workflows/js.yaml` runs on pull requests and pushes to
`master` that change `js/`, the shared conformance suite, River's migrations or
SQL, or the workflow itself, and on release tags. Each job installs the
official Node.js build from `js/.node-version` and fails unless
`typeof Temporal` is `object`. The jobs cover build, both type-check lanes,
generated migrations (compared with River's sources), API reports, TypeDoc,
README snippets, the 0.1 fixture, lint, formatting, licenses, `pnpm audit`,
packed archives and examples, unit tests on Node 26.0.0 and the current Node 26
release, and integration tests (including the conformance adapter's) on
PostgreSQL 14 through 18.

Conformance jobs set `RIVER_CONFORMANCE_REQUIRED=1` and run the same `make`
targets as `pnpm run test:conformance` with `conformance/candidate.json`. On
PostgreSQL 14 through 18 they run the SQLite, PostgreSQL (mixed, maintenance,
and resilience), insert-only, and multi-engine tiers, building River's Rust
adapter as the peer with a cached `cargo` build. Pushes to `master` and
release tags also run the Go and JavaScript and the multi-engine performance
gates at 1,000 jobs per run and a 10-minute Go and JavaScript soak. The
performance gates report without failing the workflow while the JavaScript
bounds are recalibrated from GitHub runner measurements; they become blocking
again once they are.

Unit tests compare cron schedules and snooze counting with goldens recorded
from River Go in `src/testdata`, the same values the Rust port checks in its
own fixtures.

`.github/workflows/js-soak.yaml` runs the six-hour multi-engine soak weekly.
Start it manually with a shorter `soak-duration`, such as `1h`, for a release
candidate.

## Preparing a release

The root, drivers, migration library, worker-thread integration, test helpers,
and CLI are intended to be publishable eventually. Examples and conformance
utilities are private. Until the initial release is explicitly approved, do
not reserve names, publish packages, create tags, or create releases.

1. Fetch changes to the repo. Export `VERSION` by incrementing the last tag:

   ```shell
   git checkout master && git pull --rebase
   export VERSION=0.x.y
   git checkout -b $USER-$VERSION
   ```

2. Update every publishable `package.json` to the same version, including the
   exact `workspace:` versions of first-party `dependencies`,
   `peerDependencies`, and `devDependencies`, then regenerate `pnpm-lock.yaml`.
   Libraries take `riverqueue` as an exact peer so a mismatched pair fails at
   install time instead of loading two copies; only the self-contained CLI
   depends on it directly. Do not change the private example or conformance
   package versions.

3. Update `CHANGELOG.md` by moving the release notes from `Unreleased` into a
   heading for the new version.

4. Verify generated migrations, declarations, package contents, and consumer
   installs. Dry runs must not contact the publish endpoint.

   ```shell
   pnpm run verify:migrations
   pnpm run build:all
   pnpm run api:check
   pnpm run docs:snippets
   pnpm run migration:legacy
   pnpm run package:check
   pnpm run license:check
   pnpm audit
   ```

5. Prepare a PR with the version and changelog changes. Publication remains a
   separate, explicitly authorized operation after merge.
