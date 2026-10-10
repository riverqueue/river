# River Rust development

Run commands from the repository root. See the [Rust README](../README.md) for application usage.

## Setup

Install Rust 1.95 or newer and the Go version specified by [`go.work`](../../go.work). Go generates the shared conformance fixtures used by the tests. Postgres integration tests also need a disposable database.

## Tests and checks

```sh
make lint/rust
make test/rust
make doc/rust
make check/rust/package
```

Postgres integration tests require a disposable database. They build only
with `--cfg river_postgres_tests`, which the Makefile targets pass to rustc
and rustdoc, building into `target/postgres-tests`:

```sh
RIVER_RUST_DATABASE_URL=postgres://localhost/river_rust_test \
  make test/rust/postgres
```

CI runs unit, doc, and SQLite tests on each supported Rust version, and
Postgres tests against versions 14 through 18. Rust tests check unique
keys, retry bounds, cron schedules, and snooze counts against fixtures that
River's Go implementation generates into `conformance/testdata`, which isn't
committed. The `make test/rust` targets generate them first, so Go is needed
to run the tests; when running `cargo test` directly, run `make
generate/fixtures` beforehand. A missing fixture fails its test.

The unpublished `riverqueue-conformance` crate is River Rust's adapter for
the cross-language conformance harness in `conformance/harness`, which runs
it against River Go on Postgres and SQLite. `make test/conformance/rust`
builds it and runs the harness's pull request tier against
`TEST_DATABASE_URL`; behavior one implementation shows alone is tested in
the crates' own tests instead.

`make check/rust/package` builds the five publishable crate archives and
verifies that each one builds from its packaged sources, resolving the
exact-version workspace dependencies from the other archives. It does not
publish anything. Release tags use `rust/vX.Y.Z`, independently of Go
module tags.

## Benchmarking

For basic end-to-end performance figures, the `riverqueue` binary from
`riverqueue-cli` has the Rust equivalent of `river bench`. It truncates the selected River job table,
so use a disposable database:

```sh
make bench/rust DATABASE_URL=postgres://localhost/river_bench \
  RUST_BENCH_ARGS='--duration 30s'
```

The command supports continuous burn, fixed `--num-total-jobs` burn-down,
custom schemas, tunable worker/pool/batch sizes, periodic jobs/sec output, and a
final jobs/sec plus p95 end-to-end latency summary. Use `riverqueue bench
--help` for all options.

## Releasing a new version

Run these commands from the repository root. All five Rust crates are versioned and released together. `VERSION` has no leading `v`; Git tags use `rust/vX.Y.Z`, independently of Go module tags.

1. Fetch changes and tags, choose the next Rust version (including a prerelease suffix when applicable), and create a release branch:

    ```shell
    git checkout master && git pull --rebase
    git fetch --tags
    export VERSION=0.x.y
    git checkout -b "$USER-rust-$VERSION"
    ```

2. Set `workspace.package.version` in `rust/Cargo.toml` to `$VERSION` and update every existing exact `=...` dependency on another River crate in `rust/*/Cargo.toml` to match. Update versioned README examples and move `rust/CHANGELOG.md` entries from `Unreleased` into a `[$VERSION] - YYYY-MM-DD` section. Refresh the workspace versions in the lockfile:

    ```shell
    cargo update --manifest-path rust/Cargo.toml --workspace
    ```

3. Open a PR with the manifests, `rust/Cargo.lock`, changelog, and README changes. Keep Rust CI enabled: it runs tests, lint, documentation, and package verification. Have the PR reviewed and merged after checks pass. To verify the crate archives locally without publishing:

    ```shell
    make check/rust/package
    ```

4. After merge, pull the changes and tag the release commit. Use a clean working tree and a commit with passing Rust CI; confirm its workspace version matches `$VERSION`:

    ```shell
    git checkout master && git pull --rebase
    git tag "rust/v$VERSION" -m "release rust/v$VERSION"
    ```

5. Authenticate with a [crates.io API token](https://crates.io/settings/tokens) that can publish all five crates, then publish the workspace. [Cargo handles dependencies between workspace crates](https://doc.rust-lang.org/cargo/CHANGELOG.html#cargo-190-2025-09-18): `riverqueue-macros` and `riverqueue-migrate` precede `riverqueue`, followed by `riverqueue-cli` and `riverqueue-test`.

    ```shell
    cargo login --registry crates-io
    cargo publish --manifest-path rust/Cargo.toml --workspace --locked --registry crates-io
    ```

    Publication can partially succeed. If it fails or times out, check which versions reached crates.io and rerun the publish command with `--exclude <crate>` for each crate already published at `$VERSION`.

6. Once all five crates are published, push the release tag:

    ```shell
    git push origin "rust/v$VERSION"
    ```

7. Create a [GitHub release](https://github.com/riverqueue/river/releases/new), select `rust/v$VERSION`, and copy the version's `rust/CHANGELOG.md` content into the release body. Mark alpha, beta, and RC versions as prereleases.
