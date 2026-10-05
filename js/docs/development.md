# River TypeScript development

## Setup

The JavaScript workspace lives in `js/` of the River repository, next to the
Go and Rust implementations. Run the `pnpm` commands below from `js/`, or use
River's top-level `make` targets (`make lint/js`, `make test/js`, and so on),
which delegate to `pnpm -C js`.

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
and on release tags. Each job installs the official Node.js build from
`js/.node-version` and fails unless `typeof Temporal` is `object`. The jobs
cover build, both type-check lanes, generated migrations (compared with
River's sources), API reports, TypeDoc, README snippets, the 0.1 fixture, lint,
formatting, licenses, packed archives and examples, unit tests on Node 26.0.0
and the current Node 26 release, and integration tests on PostgreSQL 14
through 18.

Unit tests compare cron schedules and snooze counting with fixtures that
River's Go implementation generates into `conformance/testdata`, the same
files the Rust port reads. They aren't committed: `make test/js` generates
them first, so Go is needed to run the unit tests, and `pnpm run test` needs
a prior `make generate/fixtures` from the repository root. A missing fixture
fails its test.

## Preparing a release

The eight publishable packages are the root `riverqueue` package, the three
drivers under `driver/*`, and `migrate`, `worker-threads`, `test`, and `cli`.
They share one release version. Examples are private and stay at `0.0.0`.
The core and PostgreSQL/Prisma drivers were already released as 0.1.0; this
checklist prepares a release of the expanded workspace.

1. Fetch changes to the repo and choose the target npm version, including a
   prerelease suffix when applicable. Do not infer it from the latest
   repository tag: this repository also contains Go and Rust releases.

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
   depends on it directly. Do not change the private example package
   versions or their `workspace:*` references, or the pinned historical
   `fixtures/migration-0.1` files. After editing the manifests, update the
   lockfile:

   ```shell
   pnpm install --lockfile-only
   ```

3. Update `CHANGELOG.md` by moving the release notes from `Unreleased` into a
   heading for the new version.

4. Open a PR with the version, lockfile, and changelog changes, and let the
   [JavaScript CI workflow](#continuous-integration) validate them. It runs
   the build, tests, lint, package checks, and Node/PostgreSQL matrices.
   Local reruns are optional and useful for debugging CI failures.

5. After merge, release from a `master` commit with passing JavaScript CI
   that includes the release's version and lockfile changes.
   Publication remains a separate, explicitly authorized operation after merge.

There is currently no npm publication workflow in this repository:
`.github/workflows/js.yaml` runs checks on `v*` tags but does not publish
packages. All eight packages set `publishConfig.provenance: true`, so
publication needs a supported CI environment configured for
[npm provenance](https://docs.npmjs.com/generating-provenance-statements/)
(including `id-token: write` on GitHub Actions). A publication workflow and
release-tag convention still need to be established. For a prerelease, use an
explicit [npm distribution tag](https://pnpm.io/10.x/cli/publish#--tag-tag),
such as `next`; `pnpm publish` defaults to `latest`.
