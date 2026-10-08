.DEFAULT_GOAL := help

TEST_JOBS ?= 4

# Conformance fixtures always come from this checkout's Go implementation.
.PHONY: generate/fixtures test/conformance
generate/fixtures:
	$(MAKE) -C .. generate/fixtures

test/conformance: generate/fixtures ## Run the Go-generated fixture checks without Postgres
	bundle exec rspec spec/conformance_spec.rb

.PHONY: test/yugabyte
test/yugabyte: ## Run both SQL adapters against a real Yugabyte database
	@test -n "$(YUGABYTE_DATABASE_URL)" || { echo "Set YUGABYTE_DATABASE_URL to a disposable test database"; exit 1; }
	cd driver/riverqueue-activerecord && TEST_DATABASE_URL="$(YUGABYTE_DATABASE_URL)" RIVER_YUGABYTE_TEST=1 RIVER_REQUIRE_DATABASES=1 RIVERQUEUE_ROOT_TEST_SUITE=1 bundle exec rspec spec/yugabyte_spec.rb
	cd driver/riverqueue-sequel && TEST_DATABASE_URL="$(YUGABYTE_DATABASE_URL)" RIVER_YUGABYTE_TEST=1 RIVER_REQUIRE_DATABASES=1 RIVERQUEUE_ROOT_TEST_SUITE=1 bundle exec rspec spec/yugabyte_spec.rb

# Looks at comments using ## on targets and uses them to produce a help output.
.PHONY: help
help: ALIGN=14
help: ## Print this message
	@awk -F ': .*## ' -- "/^[^':]+: .*## /"' { printf "'$$(tput bold)'%-$(ALIGN)s'$$(tput sgr0)' %s\n", $$1, $$2 }' $(MAKEFILE_LIST)

.PHONY: install
install: ## Run `bundle install` on gem and all subgems
	bundle install
	cd driver/riverqueue-activerecord && bundle install
	cd driver/riverqueue-sequel && bundle install
	cd rails/riverqueue-rails && bundle install

.PHONY: lint
lint: standardrb frozen-string-literals ## Run linters on gem and all subgems

.PHONY: rspec
rspec: spec

.PHONY: spec
spec: generate/fixtures
	$(MAKE) -j$(TEST_JOBS) spec/all

.PHONY: spec/all spec/core spec/activerecord spec/sequel spec/rails
spec/all: spec/core spec/activerecord spec/sequel spec/rails

spec/core:
	bundle exec rspec

spec/activerecord:
	cd driver/riverqueue-activerecord && bundle exec rspec

spec/sequel:
	cd driver/riverqueue-sequel && bundle exec rspec

spec/rails:
	cd rails/riverqueue-rails && bundle exec rspec

.PHONY: standardrb
standardrb:
	bundle exec standardrb --fix
	cd driver/riverqueue-activerecord && bundle exec standardrb --fix
	cd driver/riverqueue-sequel && bundle exec standardrb --fix
	cd rails/riverqueue-rails && bundle exec standardrb --fix

.PHONY: frozen-string-literals
frozen-string-literals:
	bundle exec rubocop --config .rubocop-frozen-string-literal.yaml --only Style/FrozenStringLiteralComment

.PHONY: steep
steep:
	bundle exec steep check

.PHONY: test
test: spec ## Run test suite (rspec) on gem and all subgems

.PHONY: type-check
type-check: steep ## Run type check with Steep

.PHONY: update
update: ## Run `bundle update` on gem and all subgems
	bundle update
	cd driver/riverqueue-activerecord && bundle update
	cd driver/riverqueue-sequel && bundle update
	cd rails/riverqueue-rails && bundle update

.PHONY: verify
verify: ## Verify bundled migrations against the canonical Go migrations
	ruby scripts/sync_migrations.rb --check

.PHONY: build
build: ## Build the four public gems without publishing
	gem build riverqueue.gemspec
	cd driver/riverqueue-activerecord && gem build riverqueue-activerecord.gemspec
	cd driver/riverqueue-sequel && gem build riverqueue-sequel.gemspec
	cd rails/riverqueue-rails && gem build riverqueue-rails.gemspec
