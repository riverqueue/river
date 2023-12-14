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
3. Next, fetch the repo locally, bump dependency versions, tag each submodule, and push those tags:

```shell
git checkout master && git pull --rebase
export VERSION=v0.0.x
go run ./internal/cmd/update-submodule-versions/main.go
git tag cmd/river/$VERSION -m "release cmd/river/$VERSION"
git tag riverdriver/$VERSION -m "release riverdriver/$VERSION"
git tag riverdriver/riverpgxv5/$VERSION -m "release riverdriver/riverpgxv5/$VERSION"
git tag riverdriver/riverdatabasesql/$VERSION -m "release riverdriver/riverdatabasesql/$VERSION"
git tag $VERSION
git push --tags
```

### Releasing River CLI

The CLI (`./cmd/river`) is different than other River submodules in that it doesn't use any `replace` directives so that it can stay installable with `go install ...@latest`.

If changes to it don't require updates to its other River dependencies (i.e. they're internal to the CLI only), it can be released normally as shown above.

If updates to River dependencies _are_ required, then a two-phase update is necessary:

1. Release River dependencies with an initial version (e.g. `v0.0.14`).
2. From `./cmd/river`, `go get` to upgrade to the version from (1), run `go mod tidy`, then tag it with the same version (e.g. `v0.0.14`).

    The main `v0.0.14` tag and `cmd/river/v0.0.14` will point to different commits, but this is tolerable.