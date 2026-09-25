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
generate: generate/feature-inventory
generate: generate/conformance
generate: generate/migrations
generate: generate/rust-migrations
generate: generate/sqlc

.PHONY: generate/conformance
generate/conformance: ## Generate language-neutral protocol fixtures
	go run ./internal/cmd/generateconformance

.PHONY: generate/feature-inventory
generate/feature-inventory: ## Refresh the cross-language feature inventory and matrix
	go run ./internal/cmd/generatefeatureinventory

.PHONY: generate/migrations
generate/migrations: ## Sync changes of pgxv5 migrations to database/sql
	rsync -au --delete "riverdriver/riverpgxv5/migration/" "riverdriver/riverdatabasesql/migration/"

.PHONY: generate/rust-migrations
generate/rust-migrations: ## Sync database migrations and hashes to Rust
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

.PHONY: lint/conformance
lint/conformance: ## Lint the opt-in shared interoperability suite
	golangci-lint run --build-tags riverconformance ./conformance/harness

lint:: lint/conformance

.PHONY: test
test:: ## Run test suite for all submodules
define test-target
    test:: ; cd $1 && go test ./... -timeout 2m
endef
$(foreach mod,$(submodules),$(eval $(call test-target,$(mod))))

# PostgreSQL integration tests need RIVER_RUST_DATABASE_URL. Without it
# test/rust still runs unit, doc, and SQLite integration tests, and fails in CI
# so a missing URL cannot turn the PostgreSQL suite into a silent pass.
.PHONY: test/rust
test/rust: ## Run Rust unit and SQLite tests, plus PostgreSQL tests when RIVER_RUST_DATABASE_URL is set
	@if [ -n "$$RIVER_RUST_DATABASE_URL" ]; then \
		cd rust && cargo test --workspace --all-features --locked; \
	elif [ -n "$$CI" ]; then \
		echo "RIVER_RUST_DATABASE_URL is required in CI to run the Rust PostgreSQL tests" >&2; exit 1; \
	else \
		echo "RIVER_RUST_DATABASE_URL is unset; skipping Rust PostgreSQL integration tests"; \
		cd rust && cargo test --workspace --features riverqueue/sqlite,riverqueue-migrate/sqlite --locked; \
	fi

.PHONY: test/rust/postgres
test/rust/postgres: ## Run all Rust tests, including PostgreSQL integration tests (requires RIVER_RUST_DATABASE_URL)
	@test -n "$$RIVER_RUST_DATABASE_URL" || { echo "RIVER_RUST_DATABASE_URL is required" >&2; exit 1; }
	cd rust && cargo test --workspace --all-features --locked

.PHONY: test/rust/sqlite
test/rust/sqlite: ## Run Rust unit, doc, and SQLite integration tests without a PostgreSQL database
	cd rust && cargo test --workspace --features riverqueue/sqlite,riverqueue-migrate/sqlite --locked

# `go test -timeout` backstops for the conformance targets. The harness bounds
# each adapter request (two minutes) and exit (thirty seconds) itself, so a
# hung adapter fails with a message naming it long before these fire. Soaks
# check at startup that their duration plus five minutes to finish fits in
# CONFORMANCE_SOAK_TIMEOUT, so raise it with the soak duration.
CONFORMANCE_TIMEOUT ?= 30m
CONFORMANCE_SOAK_TIMEOUT ?= 6h20m

.PHONY: test/conformance
test/conformance: ## Run Go and configured candidate conformance (requires database URL)
	go test -tags riverconformance ./conformance/harness -run '^Test(Maintenance|Mixed|Resilience)Conformance$$' -count=1 -timeout $(CONFORMANCE_TIMEOUT)

.PHONY: test/conformance/insert-only
test/conformance/insert-only: ## Run the insert-only-v1 profile against the configured candidate (requires database URL)
	go test -tags riverconformance ./conformance/harness -run '^TestInsertOnlyConformance$$' -count=1 -timeout $(CONFORMANCE_TIMEOUT)

.PHONY: test/conformance/sqlite
test/conformance/sqlite: ## Run candidate-neutral SQLite storage and runtime conformance
	go test -tags riverconformance ./conformance/harness -run '^Test(MixedSQLite|MixedSQLiteRuntime|ResilienceSQLite)Conformance$$' -count=1 -timeout $(CONFORMANCE_TIMEOUT)

.PHONY: test/conformance/performance
test/conformance/performance: ## Run Go and configured candidate performance gates
	go test -tags riverconformance ./conformance/harness -run '^TestPerformanceGate$$' -count=1 -timeout $(CONFORMANCE_TIMEOUT)

.PHONY: test/conformance/soak
test/conformance/soak: ## Run mixed soak for RIVER_CONFORMANCE_SOAK_DURATION
	go test -tags riverconformance ./conformance/harness -run '^TestMixedSoak$$' -count=1 -timeout $(CONFORMANCE_SOAK_TIMEOUT)

.PHONY: test/conformance/multi-engine
test/conformance/multi-engine: ## Run direct multi-engine competition, failover, fault, and SQLite pair checks
	go test -tags riverconformance ./conformance/harness -run '^TestMultiEngine(Conformance|SQLiteConformance)$$' -count=1 -timeout $(CONFORMANCE_TIMEOUT)

.PHONY: test/conformance/multi-engine/performance
test/conformance/multi-engine/performance: ## Compare release-built reference and candidate adapters together
	go test -tags riverconformance ./conformance/harness -run '^TestMultiEnginePerformanceGate$$' -count=1 -timeout $(CONFORMANCE_TIMEOUT)

.PHONY: test/conformance/multi-engine/soak
test/conformance/multi-engine/soak: ## Run direct multi-engine soak
	go test -tags riverconformance ./conformance/harness -run '^TestMultiEngineSoak$$' -count=1 -timeout $(CONFORMANCE_SOAK_TIMEOUT)

.PHONY: doc/rust
doc/rust: ## Build Rust API documentation and compiled examples
	cd rust && RUSTDOCFLAGS="-D warnings" cargo doc --workspace --all-features --no-deps --locked
	cd rust && RUSTDOCFLAGS="-D warnings" cargo test --workspace --all-features --doc --locked
	cd rust && cargo check --workspace --examples --all-features --locked

.PHONY: check/rust/dependencies
check/rust/dependencies: ## Audit Rust advisories, licenses, bans, and sources
	cd rust && cargo deny check

.PHONY: check/rust/package
check/rust/package: ## Build publishable crate archives without publishing
	cd rust && cargo package --workspace --exclude riverqueue-conformance --allow-dirty --locked --no-verify

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
		cd rust && cargo semver-checks --workspace --exclude riverqueue-conformance --baseline-rev "$(RUST_SEMVER_BASELINE_REV)"; \
	fi

.PHONY: test/race
test/race:: ## Run test suite for all submodules with race detector
define test-race-target
    test/race:: ; cd $1 && go test ./... -race -timeout 2m
endef
$(foreach mod,$(submodules),$(eval $(call test-race-target,$(mod))))

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
verify: verify/conformance
verify: verify/feature-inventory
verify: verify/migrations
verify: verify/rust-migrations
verify: verify/sqlc

.PHONY: verify/conformance
verify/conformance: ## Verify language-neutral protocol fixtures
	go run ./internal/cmd/generateconformance -check

.PHONY: verify/feature-inventory
verify/feature-inventory: ## Fail on Go features missing from the cross-language inventory
	go run ./internal/cmd/generatefeatureinventory -check

.PHONY: verify/migrations
verify/migrations: ## Verify synced migrations
	diff -qr riverdriver/riverpgxv5/migration riverdriver/riverdatabasesql/migration

.PHONY: verify/rust-migrations
verify/rust-migrations: ## Verify Rust migrations and protocol hashes
	go run ./internal/cmd/syncrustmigrations -check

.PHONY: verify/sqlc
verify/sqlc: ## Verify generated sqlc
	cd riverdriver/riverdatabasesql/internal/dbsqlc && $(SQLC) diff
	cd riverdriver/riverpgxv5/internal/dbsqlc && $(SQLC) diff
	cd riverdriver/riversqlite/internal/dbsqlc && $(SQLC) diff
