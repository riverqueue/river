# River development

## Run tests

Raise test databases:

    go run ./internal/cmd/testdbman create

Run tests:

    go test ./... -p 1

## Run lint

Run the linter and try to autofix:

    golangci-lint run --fix

## Generate sqlc

The project uses sqlc (`brew install sqlc`) to generate Go targets for Postgres
queries. After changing an sqlc `.sql` file, generate Go with:

    make generate

## Releasing a new version

1. First, prepare a PR with a `CHANGELOG.md` update describing the changes, and update the root `go.mod` to point to the `riverpgxv5` version that is about to be released.
2. Merge the above PR.
3. Next, fetch the repo locally and push new tags:

```shell
git checkout master && git pull --rebase
VERSION=v0.0.x
git tag cmd/river/$VERSION -m "release cmd/river/$VERSION"
git tag riverdriver/riverpgxv5/$VERSION -m "release riverdriver/riverpgxv5/$VERSION"
git tag $VERSION
git push --tags
```
