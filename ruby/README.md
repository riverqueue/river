# River for Ruby

A PostgreSQL and SQLite job queue that shares River's schema with the Go, Rust,
and JavaScript clients. Includes Active Record and Sequel drivers, plus Rails
and Active Job integration.

- [Usage and configuration](docs/README.md)
- [Development and releases](docs/development.md)
- [Conformance coverage](docs/conformance.md)
- [Migrations](docs/migrations.md)

From the repository root:

```sh
make -C ruby install
createdb river_test
RIVER_REQUIRE_DATABASES=1 make test/ruby
make lint/ruby typecheck/ruby
```

Ruby 3.2 or later is required. `make test/ruby/conformance` runs only the
Go-generated fixture checks and needs no PostgreSQL server.
