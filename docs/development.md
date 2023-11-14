# River

River is an experimental Postgres queue for Go.

## Development

### Run tests

Raise test databases:

    go run ./internal/cmd/testdbman create

Run tests:

    go test ./... -p 1

### Run lint

Run the linter and try to autofix:

    golangci-lint run --fix

### Generate sqlc

The project uses sqlc (`brew install sqlc`) to generate Go targets for Postgres
queries. After changing an sqlc `.sql` file, generate Go with:

    make generate

## Releasing a new version

First, merge a `CHANGELOG.md` update describing the changes. Next, update the repo locally and push new tags:

```shell
git checkout master && git pull --rebase
VERSION=v0.0.x
git tag riverdriver/riverpgxv5/$VERSION -m "release riverdriver/riverpgxv5/$VERSION"
git tag $VERSION
git push --tags
```
