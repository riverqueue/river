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

1. Fetch changes to the repo and any new tags. Export `VERSION` by incrementing the last tag. Execute `update-submodule-versions` to add it the project's `go.mod` files:

    ```shell
    git checkout master && git pull --rebase
    export VERSION=v0.x.y
    make update-mod-version
    git checkout -b $USER-$VERSION
    ```

2. Prepare a PR with the changes, updating `CHANGELOG.md` with any necessary additions at the same time. Include **`[skip ci]`** in the commit description so that CI doesn't run and pollute the Go Module cache by trying to fetch a version that's not available yet (and it would fail anyway). Have it reviewed and merged.

3. Upon merge, pull down the changes, tag each module with the new version, and push the new tags:

    ```shell
    git tag cmd/river/$VERSION -m "release cmd/river/$VERSION"
    git pull origin master
    git tag riverdriver/$VERSION -m "release riverdriver/$VERSION"
    git tag riverdriver/riverpgxv5/$VERSION -m "release riverdriver/riverpgxv5/$VERSION"
    git tag riverdriver/riverdatabasesql/$VERSION -m "release riverdriver/riverdatabasesql/$VERSION"
    git tag rivershared/$VERSION -m "release rivershared/$VERSION"
    git tag rivertype/$VERSION -m "release rivertype/$VERSION"
    git tag $VERSION
    ```

4. Push new tags to GitHub:

    ```shell
    git push --tags
    ```

5. Cut a new GitHub release by visiting [new release](https://github.com/riverqueue/river/releases/new), selecting the new tag, and copying in the version's `CHANGELOG.md` content as the release body.

### Updating Go or toolchain versions in all `go.mod` files

Modify `go.work` so it contains the new desired version in `go` and/or `toolchain` directives, then run `make update-mod-go` to have it reflect the new version(s) into all the workspace's `go.mod` files:

```shell
make update-mod-go
```