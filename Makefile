.DEFAULT_GOAL := help

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
generate: generate/migrations
generate: generate/sqlc

.PHONY: generate/migrations
generate/migrations: ## Sync changes of pgxv5 migrations to database/sql
	rsync -au --delete "riverdriver/riverpgxv5/migration/" "riverdriver/riverdatabasesql/migration/"

.PHONY: generate/sqlc
generate/sqlc: ## Generate sqlc
	cd riverdriver/riverdatabasesql/internal/dbsqlc && sqlc generate
	cd riverdriver/riverpgxv5/internal/dbsqlc && sqlc generate
	cd riverdriver/riversqlite/internal/dbsqlc && sqlc generate

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

.PHONY: lint/conformance
lint/conformance: ## Lint the opt-in shared interoperability suite
	golangci-lint run --build-tags riverconformance ./conformance/harness

lint:: lint/conformance

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

.PHONY: test/conformance/performance
test/conformance/performance: ## Run Go and configured candidate performance gates
	go test -tags riverconformance ./conformance/harness -run '^TestPerformanceGate$$' -count=1 -timeout $(CONFORMANCE_TIMEOUT)

.PHONY: test/conformance/soak
test/conformance/soak: ## Run mixed soak for RIVER_CONFORMANCE_SOAK_DURATION
	go test -tags riverconformance ./conformance/harness -run '^TestMixedSoak$$' -count=1 -timeout $(CONFORMANCE_SOAK_TIMEOUT)

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
verify: verify/migrations
verify: verify/sqlc

.PHONY: verify/migrations
verify/migrations: ## Verify synced migrations
	diff -qr riverdriver/riverpgxv5/migration riverdriver/riverdatabasesql/migration

.PHONY: verify/sqlc
verify/sqlc: ## Verify generated sqlc
	cd riverdriver/riverdatabasesql/internal/dbsqlc && sqlc diff
	cd riverdriver/riverpgxv5/internal/dbsqlc && sqlc diff
	cd riverdriver/riversqlite/internal/dbsqlc && sqlc diff
