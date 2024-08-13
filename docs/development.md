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

2. Prepare a PR with the changes, updating `CHANGELOG.md` with any necessary additions at the same time. Have it reviewed and merged.

3. Upon merge, pull down the changes, tag each module with the new version, and push the new tags:


    ```shell
    git pull origin master
    git tag riverdriver/$VERSION -m "release riverdriver/$VERSION"
    git tag riverdriver/riverpgxv5/$VERSION -m "release riverdriver/riverpgxv5/$VERSION"
    git tag riverdriver/riverdatabasesql/$VERSION -m "release riverdriver/riverdatabasesql/$VERSION"
    git tag rivershared/$VERSION -m "release rivershared/$VERSION"
    git tag rivertype/$VERSION -m "release rivertype/$VERSION"
    git tag $VERSION
    ```

    If you _don't_ need a new River CLI release that requires API changes in the main River package from `$VERSION`, tag that immediately as well (if you do, skip this command, and see "Releasing River CLI" below):

    ```shell
    git tag cmd/river/$VERSION -m "release cmd/river/$VERSION"
    ```

4. Push new tags to GitHub:

    ```shell
    git push --tags
    ```

5. Cut a new GitHub release by visiting [new release](https://github.com/riverqueue/river/releases/new), selecting the new tag, and copying in the version's `CHANGELOG.md` content as the release body.

### Releasing River CLI

The CLI (`./cmd/river`) is different than other River submodules in that it doesn't use any `replace` directives so that it can stay installable with `go install ...@latest`.

If changes to it don't require updates to its other River dependencies (i.e. they're internal to the CLI only), it can be released normally as shown above.

If updates to River dependencies _are_ required, then a second phase of the release is necessary:

1. Release River dependencies with an initial version (i.e. all the steps above).

2. Comment out `replace` directives to River's top level packages in `./cmd/river/go.mod`. These were probably needed for developing the new feature, but need to be removed because they prevent the module from being `go install`-able.

3. From `./cmd/river`, `go get` to upgrade to the main package versions were just released (make sure you're getting `$VERSION` and not thwarted by shenanigans in Go's module proxy):

    ```shell
    cd ./cmd/river/
    go get -u github.com/riverqueue/river@$VERSION
    go get -u github.com/riverqueue/river/riverdriver@$VERSION
    go get -u github.com/riverqueue/river/riverdriver/riverdatabasesql@$VERSION
    go get -u github.com/riverqueue/river/riverdriver/riverpgxv5@$VERSION
    go get -u github.com/riverqueue/river/rivershared@$VERSION
    go get -u github.com/riverqueue/river/rivertype@$VERSION
    ```

4. Run `go mod tidy`:

    ```shell
    go mod tidy
    ```

5. Prepare a PR with the changes. Have it reviewed and merged.

6. Pull the changes back down, add a tag for `cmd/river/$VERSION`, and push it to GitHub:

    ```shell
    git pull origin master
    git tag cmd/river/$VERSION -m "release cmd/river/$VERSION"
    git push --tags
    ```

    The main `$VERSION` tag and `cmd/river/$VERSION` will point to different commits, and although a little odd, is tolerable.
