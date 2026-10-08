# riverqueue-ruby development

## Install dependencies

```shell
$ bundle install
```
## Run tests

Create a test database:

```shell
$ createdb river_test
```

Tests migrate their own disposable schemas using the bundled SQL and leave
existing tables untouched. The database user needs permission to create and drop
schemas. Set `TEST_DATABASE_URL` to use a different Postgres database.

Generate shared fixtures from the repository root with `make generate/fixtures`
before invoking the driver specs directly.

Run all specs:

```shell
$ bundle exec rspec spec
```

## Run lint

```shell
$ standardrb --fix
```

## Code coverage

Running the entire test suite will produce a coverage report, and will fail if line and branch coverage is below 100%. Run the suite and open `coverage/index.html` to find lines or branches that weren't covered:

```shell
$ bundle exec rspec spec
$ open coverage/index.html
```

## Publish a new gem

Release all four gems together using the [Ruby release instructions](../../../docs/development.md#publish-gems).
