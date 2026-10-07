# River TypeScript development

## Setup

The JavaScript workspace lives in `js/` of the River repository, next to the
Go and Rust implementations. Run the `pnpm` commands below from `js/`, or use
River's top-level `make` targets (`make lint/js`, `make test/js`, and so on),
which delegate to `pnpm -C js`.

Use an official Node.js 26 build, which includes native Temporal. The
workspace's `.node-version` selects Node 26 for version managers such as fnm,
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
repository. `generate:migrations` refreshes the mirror after a River migration
changes.

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
behavior matches River, which the JavaScript-native tests and River Go's
recorded goldens establish. It has no threshold and is not a CI gate.

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

## Continuous integration

River's `.github/workflows/js.yaml` runs on pull requests and pushes to
`master` that change `js/`, River's migrations or SQL, or the workflow itself,
and on `js/v*` release tags. Each job installs the official Node.js build from
`js/.node-version` and fails unless `typeof Temporal` is `object`. The jobs
cover build, both type-check lanes, generated migrations (compared with
River's sources), API reports, TypeDoc, README snippets, the 0.1 fixture, lint,
formatting, licenses, packed archives and examples, unit tests on Node 26.0.0
and the current Node 26 release, and integration tests on PostgreSQL 14
through 18.

Unit tests compare unique keys, protocol values, notification dispatch,
retry timing, cron schedules, and snooze counting with fixtures that
River's Go implementation generates into `conformance/testdata`, the same
files the Rust port reads. They aren't committed: `make test/js` generates
them first, so Go is needed to run the unit tests, and `pnpm run test` needs
a prior `make generate/fixtures` from the repository root. A missing fixture
fails its test. `make test/js/conformance` runs just these checks, including
both drivers' notification adapters without a PostgreSQL server. The two
raw JSON unique-key cases involving duplicate keys or integer-key insertion
order are Rust-only because JavaScript objects cannot preserve them.

## Preparing a release

Run this section's commands from the repository root. The eight publishable packages are `js/package.json` (`riverqueue`), the three drivers under `js/driver/*`, and `js/{migrate,worker-threads,test,cli}`. They share one version, independently of Go and Rust. Examples are private and stay at `0.0.0`. `VERSION` has no leading `v`; JavaScript Git tags use `js/vX.Y.Z`.

1. Fetch changes and tags, choose the next JavaScript version (including a prerelease suffix when applicable), and create a release branch:

   ```shell
   git checkout master && git pull --rebase
   git fetch --tags
   export VERSION=0.x.y
   git checkout -b "$USER-js-$VERSION"
   ```

2. Set every publishable `package.json` version to `$VERSION` and update its exact `workspace:` references in `dependencies`, `peerDependencies`, and `devDependencies`. Libraries take `riverqueue` as an exact peer; the CLI depends on it directly. Keep private example versions and their `workspace:*` references, and the historical `js/fixtures/migration-0.1` files. Refresh the lockfile:

   ```shell
   pnpm -C js install --lockfile-only
   ```

3. Move `js/CHANGELOG.md` entries from `Unreleased` into a `[$VERSION] - YYYY-MM-DD` section, and update any versioned README examples.

4. Open a PR with the manifests, `js/pnpm-lock.yaml`, changelog, and README changes. Keep [JavaScript CI](#continuous-integration) enabled and merge after checks pass. To verify the package archives locally:

   ```shell
   make check/js/package
   ```

5. After merge, pull the release commit into a clean checkout, confirm its version, and push only its JavaScript tag:

   ```shell
   git checkout master && git pull --rebase
   test "$(node -p 'require("./js/package.json").version')" = "$VERSION"
   git tag "js/v$VERSION" -m "release js/v$VERSION"
   git push origin "js/v$VERSION"
   ```

6. Publish locally from the clean `master` checkout tagged above after its JavaScript checks pass. Use official Node.js 26 and the pinned pnpm version, and log in with an npm account that can publish all eight packages. A publication workflow is optional. Disable [provenance](https://docs.npmjs.com/generating-provenance-statements/) for local publication with `--provenance=false`, overriding the packages' `publishConfig.provenance: true`:

   ```shell
   test "$(git rev-parse HEAD)" = "$(git rev-parse "js/v$VERSION^{commit}")"
   pnpm login
   pnpm -C js install --frozen-lockfile
   pnpm -C js run build:all
   for package in js js/migrate js/driver/pg js/driver/prisma js/driver/sqlite js/worker-threads js/test js/cli; do
     (cd "$package" && pnpm publish --access public --no-git-checks --tag latest --provenance=false) || break
   done
   ```

   Complete npm's authentication prompts as needed. Publish packages individually because pnpm 10.22.0's recursive publication does not forward `--provenance=false` to npm. Run publication inside each package directory: `pnpm -C "$package" publish` in this version forwards extra arguments to npm and fails with `EUSAGE`. The loop publishes dependencies first and stops on failure; after a partial publication, remove the already-published packages from the loop before rerunning it. For a prerelease, use `--tag next` instead of `--tag latest`.

7. Once all eight packages are published, create a [GitHub release](https://github.com/riverqueue/river/releases/new) for `js/v$VERSION` and copy the version's `js/CHANGELOG.md` notes into its body. Mark alpha, beta, and RC versions as prereleases.
