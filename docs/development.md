# River

River is an experimental Postgres queue for Go.

## Development

### Run tests

Raise test databases:

    go run ./internal/cmd/testdbman create

Run tests:

    go test ./...

### Run lint

Run the linter and try to autofix:

    golangci-lint run --fix

### Generate sqlc

The project uses sqlc (`brew install sqlc`) to generate Go targets for Postgres
queries. After changing an sqlc `.sql` file, generate Go with:

    make generate