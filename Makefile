.DEFAULT_GOAL := help

SQLC ?= sqlc

.PHONY: db/reset
db/reset: ## Drop, create, and migrate dev and test databases
db/reset: db/reset/dev
db/reset: db/reset/test

.PHONY: db/reset/dev
db/reset/dev: ## Drop, create, and migrate dev database
	dropdb river_dev --force --if-exists
	createdb river_dev
	cd cmd/river && go run . migrate-up --database-url "postgres://localhost/river_dev"

.PHONY: db/reset/test
db/reset/test: ## Drop, create, and migrate test databases
	go run ./internal/cmd/testdbman reset

.PHONY: generate
generate: ## Generate generated artifacts
generate: generate/js-migrations
generate: generate/migrations
generate: generate/rust-migrations
generate: generate/sqlc

.PHONY: generate/js-migrations
generate/js-migrations: ## Sync database migrations to JavaScript
	pnpm -C js run generate:migrations

.PHONY: generate/migrations
generate/migrations: ## Sync changes of pgxv5 migrations to database/sql
	rsync -au --delete "riverdriver/riverpgxv5/migration/" "riverdriver/riverdatabasesql/migration/"

.PHONY: generate/rust-migrations
generate/rust-migrations: ## Sync database migrations to Rust
	go run ./internal/cmd/syncrustmigrations

.PHONY: generate/sqlc
generate/sqlc: ## Generate sqlc
	cd riverdriver/riverdatabasesql/internal/dbsqlc && $(SQLC) generate
	cd riverdriver/riverpgxv5/internal/dbsqlc && $(SQLC) generate
	cd riverdriver/riversqlite/internal/dbsqlc && $(SQLC) generate

# Looks at comments using ## on targets and uses them to produce a help output.
.PHONY: help
help: ALIGN=22
help: ## Print this message
	@awk -F '::? .*## ' -- "/^[^':]+::? .*## /"' { printf "'$$(tput bold)'%-$(ALIGN)s'$$(tput sgr0)' %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

# Each directory of a submodule in the Go workspace. Go commands provide no
# built-in way to run for all workspace submodules. Add a new submodule to the
# workspace with `go work use ./driver/new`.
submodules := $(shell go list -f '{{.Dir}}' -m)

ITERATIONS ?= 100
RUST_BENCH_ARGS ?=
RUST_SEMVER_BASELINE_REV ?= $(shell git tag --list 'riverqueue-v*' --sort=-v:refname | head -n 1)

TEST_DATABASE ?= all

# Only filter the shared driver suite. Other packages have SQLite-named tests
# that use PostgreSQL or mocks and should stay in the regular test run.
sqlite_test_pattern := ^(Test.*(LibSQL|SQLite|Turso)|Example_(libSQL|sqlite|turso))
test_submodules := $(submodules)
legacy_driver_test_flags := -run '/WithTx$$'
ifeq ($(TEST_DATABASE),postgres)
    test_submodules := $(filter-out %/riverdriver/riversqlite,$(submodules))
    driver_test_flags := -skip '$(sqlite_test_pattern)'
    legacy_driver_test_flags += -skip '$(sqlite_test_pattern)'
else ifeq ($(TEST_DATABASE),sqlite)
    test_submodules := $(filter %/riverdriver/riverdrivertest %/riverdriver/riversqlite,$(submodules))
    driver_test_flags := -run '$(sqlite_test_pattern)'
    legacy_driver_test_flags := -run '$(sqlite_test_pattern)/WithTx$$'
else ifneq ($(TEST_DATABASE),all)
    $(error TEST_DATABASE must be all, postgres, or sqlite)
endif

# Definitions of following tasks look ugly, but they're done this way because to
# produce the best/most comprehensible output by far (e.g. compared to a shell
# loop).
.PHONY: lint
lint:: ## Run linter (golangci-lint) for all submodules
define lint-target
    lint:: ; cd $1 && golangci-lint run --fix
endef
$(foreach mod,$(submodules),$(eval $(call lint-target,$(mod))))

# Rust targets are separate from `lint` and `test` so Go-only contributors and
# the Go CI jobs do not need a Rust toolchain; the Rust workflow runs them.
.PHONY: lint/rust
lint/rust: ## Run Rust formatting and clippy checks, including single-backend builds
	cd rust && cargo fmt --all -- --check
	cd rust && cargo clippy --workspace --all-targets --all-features --locked -- -D warnings
	cd rust && cargo clippy -p riverqueue -p riverqueue-migrate -p riverqueue-cli -p riverqueue-test --no-default-features --features postgres --all-targets --locked -- -D warnings
	cd rust && cargo clippy -p riverqueue -p riverqueue-migrate -p riverqueue-cli -p riverqueue-test --no-default-features --features sqlite --all-targets --locked -- -D warnings
	cd rust && $(RUST_POSTGRES_TESTS_ENV) cargo clippy -p riverqueue -p riverqueue-migrate --all-targets --all-features --locked -- -D warnings

# JavaScript targets, like the Rust ones, are separate from `lint` and `test`
# and need Node.js 26 and pnpm; they delegate to the workspace's own scripts.
# Run `pnpm -C js install` first.
.PHONY: build/js
build/js: ## Build every JavaScript package
	pnpm -C js run build:all

.PHONY: lint/js
lint/js: ## Run JavaScript lint, formatting, and type checks with both compilers
lint/js: build/js
	pnpm -C js run lint
	pnpm -C js run fmt:check
	pnpm -C js run typecheck:tests
	pnpm -C js run typecheck:next

.PHONY: test
test:: ## Run tests (TEST_DATABASE=all, postgres, or sqlite)
define test-target
    test:: ; cd $1 && go test ./... -timeout 2m $(if $(filter %/riverdriver/riverdrivertest,$1),$(driver_test_flags))
endef
$(foreach mod,$(test_submodules),$(eval $(call test-target,$(mod))))

# Exercise the temporary savepoint fallback as well as default transaction reuse.
test:: ; cd ./riverdriver/riverdrivertest && RIVER_USE_LEGACY_SUBTRANSACTIONS=1 go test . $(legacy_driver_test_flags) -timeout 2m
ifneq ($(TEST_DATABASE),sqlite)
test:: ; cd ./riverdriver/riverdrivertest && RIVER_USE_LEGACY_SUBTRANSACTIONS=1 go test . -run '^TestDriverRiverPgxV5$$/.*/WithTx$$' -timeout 2m
endif

# `--cfg river_postgres_tests` builds the Rust PostgreSQL integration tests.
# It goes to both rustc and rustdoc so any doctest gated on it runs too, and
# into its own target directory so switching it on and off doesn't rebuild
# the ordinary build's artifacts. The default is absolute: trybuild resolves a
# relative target directory from the macros crate's directory.
RUST_POSTGRES_TESTS_ENV = RUSTFLAGS="$$RUSTFLAGS --cfg river_postgres_tests" \
	RUSTDOCFLAGS="$$RUSTDOCFLAGS --cfg river_postgres_tests" \
	CARGO_TARGET_DIR="$${CARGO_TARGET_DIR:-$(CURDIR)/rust/target}/postgres-tests"

# PostgreSQL integration tests need RIVER_RUST_DATABASE_URL. Without it
# test/rust still runs unit, doc, and SQLite integration tests, and fails in CI
# so a missing URL cannot turn the PostgreSQL suite into a silent pass.
.PHONY: test/js
test/js: ## Run JavaScript unit tests
test/js: build/js
	pnpm -C js run test

# Integration tests use TEST_DATABASE_URL (default
# postgres://localhost:5432/river_test), migrated with
# `node js/cli/dist/bin.js migrate-up`.
.PHONY: test/js/integration
test/js/integration: ## Run JavaScript integration tests against PostgreSQL
test/js/integration: build/js
	pnpm -C js run test:integration

.PHONY: test/rust
test/rust: ## Run Rust unit and SQLite tests, plus PostgreSQL tests when RIVER_RUST_DATABASE_URL is set
	@if [ -n "$$RIVER_RUST_DATABASE_URL" ]; then \
		cd rust && $(RUST_POSTGRES_TESTS_ENV) cargo test --workspace --all-features --locked; \
	elif [ -n "$$CI" ]; then \
		echo "RIVER_RUST_DATABASE_URL is required in CI to run the Rust PostgreSQL tests" >&2; exit 1; \
	else \
		echo "RIVER_RUST_DATABASE_URL is unset; skipping Rust PostgreSQL integration tests"; \
		cd rust && cargo test --workspace --features riverqueue/sqlite,riverqueue-migrate/sqlite --locked; \
	fi

.PHONY: test/rust/postgres
test/rust/postgres: ## Run all Rust tests, including PostgreSQL integration tests (requires RIVER_RUST_DATABASE_URL)
	@test -n "$$RIVER_RUST_DATABASE_URL" || { echo "RIVER_RUST_DATABASE_URL is required" >&2; exit 1; }
	cd rust && $(RUST_POSTGRES_TESTS_ENV) cargo test --workspace --all-features --locked

.PHONY: test/rust/sqlite
test/rust/sqlite: ## Run Rust unit, doc, and SQLite integration tests without a PostgreSQL database
	cd rust && cargo test --workspace --features riverqueue/sqlite,riverqueue-migrate/sqlite --locked

.PHONY: doc/js
doc/js: ## Check JavaScript API reports, TypeDoc, and README snippets
doc/js: build/js
	pnpm -C js run api:check
	pnpm -C js run docs:api
	pnpm -C js run docs:snippets

.PHONY: doc/rust
doc/rust: ## Build Rust API documentation, compiled examples, and doctests for each backend feature set
	cd rust && RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked
	cd rust && RUSTDOCFLAGS="-D warnings" cargo test --workspace --all-features --doc --locked
	cd rust && RUSTDOCFLAGS="-D warnings" cargo test -p riverqueue -p riverqueue-migrate -p riverqueue-cli -p riverqueue-test --no-default-features --features postgres --doc --locked
	cd rust && RUSTDOCFLAGS="-D warnings" cargo test -p riverqueue -p riverqueue-migrate -p riverqueue-cli -p riverqueue-test --no-default-features --features sqlite --doc --locked
	cd rust && cargo check --workspace --examples --all-features --locked

.PHONY: doc/rust/docsrs
doc/rust/docsrs: ## Build Rust API documentation as docs.rs does (nightly toolchain, `--cfg docsrs`)
	cd rust && RUSTDOCFLAGS="--cfg docsrs -D warnings" CARGO_TARGET_DIR="$${CARGO_TARGET_DIR:-target}/docsrs" cargo +nightly doc -p riverqueue -p riverqueue-migrate -p riverqueue-test --all-features --no-deps --locked

.PHONY: check/js/dependencies
check/js/dependencies: ## Audit JavaScript advisories and production dependency licenses
	pnpm -C js audit
	pnpm -C js run license:check

# Packs every published package and checks the archives in clean consumers,
# including the 0.1 upgrade fixture. Its PostgreSQL tests run when
# DATABASE_URL is set.
.PHONY: check/js/package
check/js/package: ## Build and verify publishable npm archives without publishing
check/js/package: build/js
	pnpm -C js run migration:legacy
	pnpm -C js run package:check

.PHONY: check/rust/dependencies
check/rust/dependencies: ## Audit Rust advisories, licenses, bans, and sources
	cd rust && cargo deny check

# Cargo caches temporary registry dependencies by path and version. A fresh
# build directory prevents stale sources and binaries after same-version edits.
# Verified archives still go to the normal target/package directory.
.PHONY: check/rust/package
check/rust/package: ## Build and verify publishable crate archives without publishing
	cd rust && package_build_dir=$$(mktemp -d) && \
		trap 'rm -rf "$$package_build_dir"' EXIT && \
		CARGO_BUILD_BUILD_DIR="$$package_build_dir" cargo package --workspace --allow-dirty --locked

# The baseline is the latest published riverqueue-v* tag, and
# cargo-semver-checks infers the allowed change from the version bump. It
# skips every lint while the workspace version is a pre-release, so
# comparing unreleased revisions with each other checks nothing. Until a
# Rust release is tagged the check reports that there is no baseline. Set
# RUST_SEMVER_BASELINE_REV to compare with another revision.
.PHONY: check/rust/semver
check/rust/semver: ## Check Rust APIs against RUST_SEMVER_BASELINE_REV (default: latest Rust tag)
	@if test -z "$(RUST_SEMVER_BASELINE_REV)"; then \
		echo "No published Rust release tag (riverqueue-v*); no public API baseline to compare"; \
	elif ! git cat-file -e "$(RUST_SEMVER_BASELINE_REV):rust/Cargo.toml" 2>/dev/null; then \
		echo "Baseline $(RUST_SEMVER_BASELINE_REV) predates the Rust crates; no public API to compare"; \
	else \
		cd rust && cargo semver-checks --workspace --baseline-rev "$(RUST_SEMVER_BASELINE_REV)"; \
	fi

.PHONY: test/race
test/race:: ## Run tests with race detector (TEST_DATABASE=all, postgres, or sqlite)
define test-race-target
    test/race:: ; cd $1 && go test ./... -race -timeout 2m $(if $(filter %/riverdriver/riverdrivertest,$1),$(driver_test_flags))
endef
$(foreach mod,$(test_submodules),$(eval $(call test-race-target,$(mod))))

test/race:: ; cd ./riverdriver/riverdrivertest && RIVER_USE_LEGACY_SUBTRANSACTIONS=1 go test . -race $(legacy_driver_test_flags) -timeout 2m
ifneq ($(TEST_DATABASE),sqlite)
test/race:: ; cd ./riverdriver/riverdrivertest && RIVER_USE_LEGACY_SUBTRANSACTIONS=1 go test . -race -run '^TestDriverRiverPgxV5$$/.*/WithTx$$' -timeout 2m
endif

.PHONY: bench
bench:: ## Run benchmarks in each submodule (ITERATIONS=100)
define bench-target
    bench:: ; cd $1 && go test -bench=. -benchtime=$(ITERATIONS)x -run=a^ ./...
endef
$(foreach mod,$(submodules),$(eval $(call bench-target,$(mod))))

.PHONY: bench/rust
bench/rust: ## Run the destructive Rust PostgreSQL throughput benchmark
	cd rust && cargo run --release --locked -p riverqueue-cli --bin riverqueue -- bench $(if $(DATABASE_URL),--database-url "$(DATABASE_URL)") $(RUST_BENCH_ARGS)

.PHONY: tidy
tidy:: ## Run `go mod tidy` for all submodules
define tidy-target
    tidy:: ; cd $1 && go mod tidy
endef
$(foreach mod,$(submodules),$(eval $(call tidy-target,$(mod))))

.PHONY: update-mod-go
update-mod-go: ## Update `go`/`toolchain` directives in all submodules to match `go.work`
	go run ./rivershared/cmd/update-mod-go ./go.work

.PHONY: update-mod-version
update-mod-version: ## Update River packages in all submodules to $VERSION
	PACKAGE_PREFIX="github.com/riverqueue/river" go run ./rivershared/cmd/update-mod-version ./go.work

.PHONY: verify
verify: ## Verify generated artifacts
verify: verify/js-migrations
verify: verify/migrations
verify: verify/rust-migrations
verify: verify/sqlc

.PHONY: verify/js-migrations
verify/js-migrations: ## Verify JavaScript migrations match the canonical migrations
	pnpm -C js run verify:migrations

.PHONY: verify/migrations
verify/migrations: ## Verify synced migrations
	diff -qr riverdriver/riverpgxv5/migration riverdriver/riverdatabasesql/migration

.PHONY: verify/rust-migrations
verify/rust-migrations: ## Verify Rust migrations match the canonical migrations
	go run ./internal/cmd/syncrustmigrations -check

.PHONY: verify/sqlc
verify/sqlc: ## Verify generated sqlc
	cd riverdriver/riverdatabasesql/internal/dbsqlc && $(SQLC) diff
	cd riverdriver/riverpgxv5/internal/dbsqlc && $(SQLC) diff
	cd riverdriver/riversqlite/internal/dbsqlc && $(SQLC) diff
