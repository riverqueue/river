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
generate: generate/conformance
generate: generate/migrations
generate: generate/rust-migrations
generate: generate/sqlc

.PHONY: generate/conformance
generate/conformance: ## Generate language-neutral protocol fixtures
	go run ./internal/cmd/generateconformance

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

.PHONY: lint/rust
lint/rust: ## Run Rust formatting and clippy checks
	cd rust && cargo fmt --all -- --check
	cd rust && cargo clippy --workspace --all-targets --all-features --locked -- -D warnings

lint:: lint/rust

.PHONY: test
test:: ## Run test suite for all submodules
define test-target
    test:: ; cd $1 && go test ./... -timeout 2m
endef
$(foreach mod,$(submodules),$(eval $(call test-target,$(mod))))

.PHONY: test/rust
test/rust: ## Run the Rust workspace test suite
	cd rust && cargo test --workspace --locked

test:: test/rust

.PHONY: test/rust/postgres
test/rust/postgres: ## Run all Rust tests, including PostgreSQL integration tests
	cd rust && cargo test --workspace --all-features --locked

.PHONY: test/conformance
test/conformance: ## Run Go and configured candidate conformance (requires database URL)
	go test -tags riverconformance ./conformance/harness -run TestMixedConformance -count=1

.PHONY: test/conformance/sqlite
test/conformance/sqlite: ## Run candidate-neutral SQLite storage and runtime conformance
	go test -tags riverconformance ./conformance/harness -run '^TestMixedSQLite(Conformance|RuntimeConformance)$$' -count=1

.PHONY: test/conformance/performance
test/conformance/performance: ## Run Go and configured candidate performance gates
	go test -tags riverconformance ./conformance/harness -run TestPerformanceGate -count=1

.PHONY: test/conformance/soak
test/conformance/soak: ## Run mixed soak for RIVER_CONFORMANCE_SOAK_DURATION
	go test -tags riverconformance ./conformance/harness -run TestMixedSoak -count=1 -timeout 6h15m

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

.PHONY: check/rust/semver
check/rust/semver: ## Check Rust APIs against the latest Rust tag or initial baseline
	@if test -n "$(RUST_SEMVER_BASELINE_REV)"; then \
		cd rust && cargo semver-checks --workspace --baseline-rev "$(RUST_SEMVER_BASELINE_REV)"; \
	else \
		echo "No prior riverqueue tag; validating initial rustdoc baseline"; \
		cd rust && cargo semver-checks --workspace --baseline-root .; \
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
	cd rust && cargo run --release --locked -p riverqueue --bin riverqueue -- bench $(if $(DATABASE_URL),--database-url "$(DATABASE_URL)") $(RUST_BENCH_ARGS)

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
verify: verify/migrations
verify: verify/rust-migrations
verify: verify/sqlc

.PHONY: verify/conformance
verify/conformance: ## Verify language-neutral protocol fixtures
	go run ./internal/cmd/generateconformance -check

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
