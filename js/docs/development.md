# River TypeScript development

## Setup

    pnpm install

## Commands

```sh
pnpm run build             # Build the core package
pnpm run build:all         # Build all packages (core + drivers)
pnpm run clean:all         # Clean all build output
pnpm run fmt               # Format code with Prettier
pnpm run fmt:check         # Check formatting (for CI)
pnpm run lint              # Run ESLint
pnpm run lint:fix          # Run ESLint with auto-fix
pnpm run test              # Run unit tests
pnpm run test:integration  # Run integration tests (requires database)
```

## Integration tests

Integration tests run against a real PostgreSQL database with River's schema. Create the test database and apply migrations:

    createdb river_test
    river migrate-up --database-url "postgres://localhost/river_test" --line main

The `river` CLI can be installed with Go:

    go install github.com/riverqueue/river/cmd/river@latest

By default, tests connect to `postgres://localhost:5432/river_test`. Override with `TEST_DATABASE_URL`:

    TEST_DATABASE_URL="postgres://user:pass@host:5432/mydb" pnpm run test:integration

## Releasing a new version

1. Fetch changes to the repo. Export `VERSION` by incrementing the last tag:

    ```shell
    git checkout master && git pull --rebase
    export VERSION=0.x.y
    ```

2. Update version numbers in all `package.json` files:

    ```shell
    pnpm --filter '*' exec -- npm version $VERSION --no-git-tag-version
    npm version $VERSION --no-git-tag-version
    ```

3. Prepare a PR with the changes, updating `CHANGELOG.md` with any necessary additions at the same time. Have it reviewed and merged.

4. Upon merge, pull down the changes, tag, and push:

    ```shell
    git checkout master && git pull --rebase
    git tag v$VERSION -m "release v$VERSION"
    git push --tags
    ```

5. Publish packages to npm:

    ```shell
    pnpm run build:all
    pnpm publish
    pnpm --filter '@riverqueue/*' publish --access public
    ```

6. Cut a new GitHub release by visiting [new release](https://github.com/riverqueue/riverqueue-js/releases/new), selecting the new tag, and copying in the version's `CHANGELOG.md` content as the release body.
