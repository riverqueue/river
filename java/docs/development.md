# River Java development

Run commands from the repository root unless a section says otherwise. See the [Java README](../README.md) for application usage.

## Building from source

Install the Go version specified by [`go.work`](../../go.work), Maven, and JDK 21 or 25. Set `JAVA_HOME` and `PATH` to that JDK; the pinned formatter does not run on JDK 27. Go is needed to generate test fixtures.

Generate the fixtures and install the Java artifacts into your local Maven repository:

```sh
make generate/fixtures
mvn --batch-mode --no-transfer-progress -f java/pom.xml install
```

The CLI is built at `java/cli/target/river-cli.jar`, with its runtime dependencies
in `java/cli/target/dependency/`. Run it with:

```sh
java --enable-native-access=ALL-UNNAMED \
  -jar java/cli/target/river-cli.jar --help
```

Keep the `dependency/` directory beside the CLI JAR when moving it. For a build
without running tests, use `make build/java`.

## Release artifacts

GitHub releases tagged `java/vVERSION` are the publication source for
`https://riverqueue.com/maven`. The proxy imports the complete asset set into R2
and serves it anonymously to Maven and Gradle. Publishers use `gh`; no Sonatype
account, OpenPGP key, or Cloudflare credential is needed.

| Artifact | GitHub attachments |
| --- | --- |
| `river-java` | Parent POM |
| `river` | POM, library JAR, sources JAR, Javadoc JAR |
| `river-cli` | POM, thin JAR, sources JAR, Javadoc JAR |

Publish all nine files together. The parent POM is required by this release's
library and CLI; the conformance adapter is excluded. The proxy generates
checksums and preserves imported versions, so publish a new version to change
bytes or attachments. New releases do not build or upload a shaded `-all.jar`.
Use [JBang or Maven](../README.md#migration-cli) to resolve CLI dependencies;
older published standalone JARs remain available.

## Releasing a new version

Run from a clean checkout with JDK 21 or 25, Maven 3.9+, Go, `golangci-lint`,
Python 3.9+, and `gh`. A human runs `gh auth login` in their terminal once; a human
or local agent can then use the same authenticated `gh` session. Keep tokens out
of chat, shell arguments, and release files. No additional credential handoff is
needed. Stop if a command fails. Versions omit `v`; tags use `java/vVERSION`.

1. Start from a clean checkout, fetch changes and tags, and choose the release version:

    ```sh
    git checkout master && git pull --rebase
    git fetch --tags
    export VERSION=0.1.1
    git checkout -b "$USER-java-$VERSION"
    ```

2. Update the POMs, CLI tests, documentation examples, and adapter version references together:

    ```sh
    python3 - <<'PY'
    import json, os, pathlib, subprocess, xml.etree.ElementTree as ET
    old = ET.parse("java/pom.xml").findtext("{http://maven.apache.org/POM/4.0.0}version")
    files = subprocess.check_output([
        "git", "grep", "-Il", "-F", old, "--", "java",
        ":!java/CHANGELOG.md", ":!java/VALIDATION.md",
        ":!java/bin/github_release_test.py",
        ":!java/conformance/src/main/resources/manifest.json",
    ]).decode().splitlines()
    for name in files:
        path = pathlib.Path(name)
        path.write_text(path.read_text().replace(old, os.environ["VERSION"]))
    path = pathlib.Path("java/conformance/src/main/resources/manifest.json")
    manifest = json.loads(path.read_text())
    manifest["implementations"]["java"]["version"] = os.environ["VERSION"]
    path.write_text(json.dumps(manifest, indent=2) + "\n")
    PY
    git diff -- java
    ```

    Move `java/CHANGELOG.md` entries from `Unreleased` into a `[$VERSION] - YYYY-MM-DD` section. For the first release, describe the initial library and CLI; for a stable release, remove "Prerelease" from the README introduction.

3. Check the release and open a PR. Keep CI enabled and have the PR reviewed and merged after the Java checks pass:

    ```sh
    make lint/java test/java/tools test/java/sqlite check/java/package
    git add java
    git commit -m "[java] Release v$VERSION"
    git push -u origin HEAD
    gh pr create --fill
    ```

4. After merge, pull the release commit into a clean checkout, confirm its version, and create the local tag:

    ```sh
    git checkout master && git pull --rebase &&
    test -z "$(git status --porcelain)" &&
    test "$(python3 -c 'import xml.etree.ElementTree as E; print(E.parse("java/pom.xml").findtext("{http://maven.apache.org/POM/4.0.0}version"))')" = "$VERSION" &&
    git tag "java/v$VERSION" -m "release java/v$VERSION"
    ```

5. Build, test, and stage the complete release. This uses a temporary **local file repository**, without signing, uploading, or installing release artifacts into your normal Maven cache:

    ```sh
    python3 java/bin/github-release.py prepare
    python3 - <<'PYNOTES'
    import os, pathlib, re
    text = pathlib.Path("java/CHANGELOG.md").read_text()
    match = re.search(r"^## \[" + re.escape(os.environ["VERSION"]) + r"\][^\n]*\n(.*?)(?=^## |\Z)", text, re.M | re.S)
    if not match:
        raise SystemExit("Version missing from Java changelog")
    pathlib.Path("java/target/github-release-notes.md").write_text(match.group(1).strip() + "\n")
    PYNOTES
    ```

6. Push the tag and upload all artifacts to a draft. The script checks the local/remote tag against the build commit, resumes matching draft uploads, and refuses changed assets or an already published release:

    ```sh
    git push origin "java/v$VERSION"
    python3 java/bin/github-release.py upload \
      --notes-file java/target/github-release-notes.md
    gh release view "java/v$VERSION" --repo riverqueue/river
    ```

7. Publish the verified draft. Alpha, beta, and RC versions are marked as prereleases automatically. Keep `--latest=false` for every Java release, including stable versions, so the repository's Latest release remains the Go release:

    ```sh
    gh release edit "java/v$VERSION" --repo riverqueue/river --draft=false --latest=false
    ```

8. Verify the public proxy, all asset checksums, fresh Maven dependency resolution, a compiled consumer, and thin CLI SQLite migrations. This also checks that Maven obtained River artifacts from the proxy rather than Central. The proxy imports on a schedule; requests do not trigger discovery. If the release is not visible yet, wait a minute and retry:

    ```sh
    python3 java/bin/github-release.py verify --version "$VERSION" \
      --directory java/target/github-release
    ```

    To verify an existing release without a local build, omit `--directory`. This
    requires a thin CLI JAR with `Implementation-Version` manifest metadata. The
    originally published alpha.1 thin JAR reports `development` instead of its
    release version and fails the CLI version check.

## Optional Maven Central publication

The GitHub/proxy release path above does not use Central publication. The existing
`release` Maven profile remains available for separately publishing to Central;
it signs the same public coordinates and creates a Portal bundle. A version
published to both repositories must use the same artifact bytes.

### Local tooling and signing

Use JDK 21 or 25, Maven 3.9 or newer, and the Go version from `go.work` for fixture
generation. Signing uses the Maven GPG plugin's pure Java Bouncy Castle signer
(`signer=bc`) with agent access disabled. Neither GnuPG executables nor
`gpg-agent` are needed. Maven downloads the pinned publishing, signing, and
Javadoc plugins automatically; no separate Sonatype CLI is needed. Select the
supported JDK with both `JAVA_HOME` and `PATH`, even if Homebrew's Maven
installation brings a newer JDK.

Supply an exported OpenPGP private key, such as an ASCII-armored file beginning
with `-----BEGIN PGP PRIVATE KEY BLOCK-----`. SSH keys are not supported. For new
keys without GnuPG, [PGPainless](https://pgpainless.readthedocs.io/en/stable/pgpainless-cli/usage.html)
provides a Java/Bouncy Castle OpenPGP tool. For publication, distribute the public
key to a [keyserver supported by Central](https://central.sonatype.org/publish/requirements/gpg/#distributing-your-public-key).
The [keys.openpgp.org upload form](https://keys.openpgp.org/upload) accepts public
key files directly in the browser.

Use `-Dgpg.keyFilePath=/absolute/path/to/private-key.asc` for a local key file
outside the checkout. Supply its passphrase through `MAVEN_GPG_PASSPHRASE`, using
your secret manager or a hidden terminal prompt. For example, enter the passphrase
at the `read` command, then build and test the signed bundle:

```sh
read -r -s MAVEN_GPG_PASSPHRASE
export MAVEN_GPG_PASSPHRASE
make check/java/release MVN='mvn -Dgpg.keyFilePath=/absolute/path/to/private-key.asc'
unset MAVEN_GPG_PASSPHRASE
```

For CI, supply the private key's full contents through `MAVEN_GPG_KEY` and its
passphrase through `MAVEN_GPG_PASSPHRASE`, then run `make check/java/release`.
The environment key takes precedence over the key file. If a file or environment
key contains multiple keys, select the signing key with
`-Dgpg.keyFingerprint=FINGERPRINT` or `MAVEN_GPG_KEY_FINGERPRINT`.
The plugin rejects passphrases supplied as Maven properties or in `settings.xml`.

Maintainers can use separate signing keys and publishing tokens. Each maintainer
supplies their own key locally; the project does not pin a shared key or
fingerprint. Each public key must be available from a supported keyserver, and
each publisher needs access to `com.riverqueue` in the Central Portal.

From `java/`, the equivalent target is `make package/release`. This performs a
clean build, runs SQLite and CLI tests and formatting checks, signs the parent and
both public modules, and writes `java/target/central-publishing/central-bundle.zip`.
It neither uploads the bundle nor installs these artifacts into the local Maven
repository.
No Central account or publishing token is required for this check. It does require
an exported signing key and, for an encrypted key, its passphrase.

### Publishing setup

Before uploading, verify the `com.riverqueue` namespace in the
[Central Publisher Portal](https://central.sonatype.com) using a DNS TXT record
on `riverqueue.com`, and create a
[publishing token](https://central.sonatype.org/publish/generate-portal-token/).
Merge this server entry into your local `~/.m2/settings.xml`, with the token's
username and password supplied through environment variables:

```xml
<settings xmlns="http://maven.apache.org/SETTINGS/1.2.0">
  <servers>
    <server>
      <id>central</id>
      <username>${env.CENTRAL_USERNAME}</username>
      <password>${env.CENTRAL_PASSWORD}</password>
    </server>
  </servers>
</settings>
```

The profile defaults to `central.skipUpload=true`. With uploading enabled, Maven
waits for Central validation; `autoPublish=false` leaves final publication to the
Portal. Ordinary builds do not activate signing or Central publishing. See the
[Central Maven plugin guide](https://central.sonatype.org/publish/publish-portal-maven/)
for the upload and validation lifecycle.

## Tests and checks

From the repository root, with Go (the version in `go.work`), JDK 21 or 25,
Maven, and Python 3.9+ installed:

```sh
make test/java/conformance
make test/java/sqlite
RIVER_TEST_DATABASE_URL=postgres://localhost/river_test make test/java/postgres
make lint/java
make check/java/package
make verify/java-migrations
make check/modzip
```

`test/java` runs library and executable CLI tests and checks formatting. Client
and worker tests use Postgres when `RIVER_TEST_DATABASE_URL` is set, with a
fresh schema for each test that is removed afterward. Otherwise they use SQLite.
`test/java/postgres` requires that URL; `test/java/sqlite` explicitly uses SQLite
and excludes Postgres-only tests, even if a URL is set in your environment.
`test/java/conformance` needs no external database: it runs the JUnit tests tagged
`conformance`, using temporary SQLite databases for storage checks even if a
Postgres URL is set. The full SQLite and Postgres targets run these checks
against their selected backend. These targets first generate fresh fixtures
directly from this checkout's Go implementation, just like
`test/js/conformance` and `test/rust/conformance`.

Java reads all four files in `conformance/testdata`: uniqueness hashes, cron
schedules, snooze counters, and protocol values (states, metadata keys,
notification payloads, attempt errors, and retry bounds). These files are ignored
by Git and read at test time. Missing fixtures fail with instructions to run
`make generate/fixtures`; there are no bundled fallback goldens. To use Maven or
an IDE directly, generate the fixtures first. `make test` from `java/` delegates
to the root target; `mvn spotless:apply` formats Java sources.

Notification tests check both encoding and dispatch: targeted cancellation, queue
wakeups, leadership signals, and ignoring a client's own resignation.
Database-backed tests also verify topic routing, recovery after malformed
payloads, and immediate leadership wakeup after a peer resigns.

Uniqueness tests include the raw JSON fixtures for duplicate keys and integer-like
map keys. Protocol tests exercise both retry jitter boundaries, decode stored
states and uniqueness bits, and verify periodic IDs, insert nonces, resumable
checkpoints, output, and rescue counters using River's reserved metadata keys.

`make generate/java-migrations` syncs SQL and the migration catalog from the
canonical Go drivers. `verify/java-migrations` detects changed, missing, or extra
migrations. `check/java/package` checks the library and CLI archives, including
source JARs, for development content and verifies bundled runtime resources.
The Go maintenance tools live in `java/bin/`. `make test/java` and `make lint/java`
include their tests and lint checks; the tool tests also cover GitHub release staging and upload guards; `make test/java/tools` and
`make lint/java/tools` run only the tool checks. From `java/`, use `make test/tools`
and `make lint/tools`. These checks run in Java CI, separately from the root
Go-only `make test` and `make lint` targets.
The legacy adapter is not installed or deployed as a Maven artifact.
`java/go.mod` excludes this directory from Go module archives and package
discovery. The Make targets invoke the Go tools by file from the root workspace;
the module is not part of `go.work` and must not be tagged as a Go module.

The Java CI workflow follows the Rust and JavaScript layout: a quality/package
job, a JDK 21/25 matrix running unit and SQLite tests, and a Postgres 14–18
matrix running client, worker, and migration tests on JDK 25. All jobs use the
shared Java setup action and Maven dependency caching. The whole workflow is
filtered to changes in Java, its build configuration, fixtures, and canonical
migrations. Go changes run the smaller fixture suite in the shared Conformance
workflow alongside Rust and JavaScript.

## Warning policy

Java builds keep `-Xlint:all` enabled and fail on unsuppressed compiler warnings,
including test compilation. Tests that intentionally hold workers or subscriptions
open for a block use a method-level `@SuppressWarnings("try")` with a short reason.
Other compiler diagnostics remain enabled.

The Make targets run Maven through `bin/check-maven-warnings.sh`. It streams the
complete build output, preserves command failures, and rejects Maven warning
messages from packaging and other plugins. Local checks and CI use these same
targets. Direct Maven invocations also reject compiler warnings through the
parent POM's `failOnWarning` setting.

Javadoc also fails on warnings. Its doclint configuration checks references,
syntax, HTML, and accessibility, but permits missing comments and tags because
the existing API documentation does not annotate every parameter and return value.
Automatic external documentation links are disabled so Javadoc generation does
not depend on another documentation site being reachable.

The development-only conformance adapter JAR merges dependency
`META-INF/LICENSE` and `META-INF/NOTICE` text, retains service registrations, and
generates its own executable manifest.
Dependency module descriptors and signatures are excluded from this combined
archive. Other overlapping classes or resources still produce warnings and fail
the checked Make targets. Library, CLI, and source JARs keep their normal
packaging; CLI dependencies stay in separate JARs.

## Legacy cross-process harness

The original interoperability adapter remains available for broader storage,
runtime, and multi-engine scenarios. It uses the historical harness pinned in
`java/conformance/reference-revision`, which is separate from the current
Go-generated fixtures and is not part of `master`'s conformance module.

From `java/`, using only disposable databases (the harness resets job tables):

```sh
RIVER_CONFORMANCE_DATABASE_URL=postgres://localhost/river_java_conformance \
  python3 conformance/bin/run.py postgres
python3 conformance/bin/run.py sqlite
```

The runner extracts that revision into ignored build storage and registers the
Java candidate there. `--reference /path/to/checkout` uses an existing harness.
`--refresh` opts into the old reference branch's current head; review its
contract and profiles before updating the pin.

See [intentional differences](../DIFFERENCES.md) for the Java API and lifecycle
choices. Passing a conformance profile demonstrates the scenarios in that
profile; it is not a claim about untested behavior or performance.

The full peer matrix uses `multi` with `RIVER_CONFORMANCE_PEER_FILE` set to a
colon-separated list of Rust and JS candidate descriptors, and
`RIVERQUEUE_JS_ROOT` pointing to the JS checkout. `multi-soak` also requires
`RIVER_CONFORMANCE_MULTI_ENGINE_SOAK_DURATION=5m` (or longer). The upstream
harness currently has a Postgres soak; SQLite endurance validation repeats
`TestMultiEngineSQLiteConformance` with `-count=5`.

`python3 bin/import-reference.py --check /path/to/reference` verifies the legacy
adapter contract. Omit `--check` to import it and record its hash. This command
does not overwrite the current migrations or generated fixtures.

See [validation results](../VALIDATION.md) for the tested revisions and remaining
limitations.
