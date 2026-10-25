# River Java development

Run commands from the repository root unless a section says otherwise. See the [Java README](../README.md) for application usage.

## Building from source

Install the Go version specified by [`go.work`](../../go.work), Maven, and JDK 21 or 25. Set `JAVA_HOME` and `PATH` to that JDK; the pinned formatter does not run on JDK 27. Go is needed to generate test fixtures.

Generate the fixtures and install the Java artifacts into your local Maven repository:

```sh
make generate/fixtures
mvn --batch-mode --no-transfer-progress -f java/pom.xml install
```

The executable migration CLI is built at `java/cli/target/river-cli-0.1.0-alpha.1-all.jar`. For a build without running tests, use `make build/java`.

## Maven Central artifacts

Normal builds attach sources and Javadoc JARs for the library and CLI. The
`release` Maven profile additionally signs the artifacts and builds a Central
Publisher Portal bundle with checksums. It includes three coordinates, all under
`com.riverqueue` and using the version in `java/pom.xml`:

| Artifact | Contents |
| --- | --- |
| `river-java` | Parent POM with shared dependencies and publication metadata |
| `river` | Library, sources, Javadoc, and POM |
| `river-cli` | Thin CLI, executable `all` JAR, sources, Javadoc, and POM |

The parent POM must be published with its children so consumers can resolve their
dependencies. `river-conformance` is explicitly excluded from Central publication
and skips signing, Javadoc, and local installation.

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

### Credentials for a terminal or agent release

The same commands below work for a human or an agent. A human prepares credentials
in a terminal with hidden prompts, using the **generated publishing-token username
and password** from [Central](https://central.sonatype.com/usertoken):

```sh
JAVA_RELEASE_CREDENTIALS=$(python3 java/bin/release-credentials.py prepare \
  --key-file /absolute/path/to/private-key.asc) &&
export JAVA_RELEASE_CREDENTIALS &&
printf '%s\n' "$JAVA_RELEASE_CREDENTIALS"
```

The helper copies the private-key export and saves the passphrase and token in
mode-600 files inside a mode-700 temporary directory outside the checkout. These
are local plaintext credentials protected by filesystem permissions. For an
existing GnuPG keyring, replace `--key-file ...` with
`--gpg-homedir "$HOME/.gnupg" --fingerprint PRIMARY_KEY_FINGERPRINT` (use your
custom home directory if applicable). This invokes GnuPG once to export the key;
Maven signing still uses Bouncy Castle. `--fingerprint` also works with `--key-file`.

For an agent release, give the agent only the printed directory path and ask it
to set `JAVA_RELEASE_CREDENTIALS` to that path. The agent must use the helper's
`run` command to supply secrets to publishing tools; never print the handoff's
contents or dump its environment. `run` injects credentials into the child
environment and redacts their values from stdout and stderr. Use trusted commands
without debug logging. The helper refuses prompts when hidden terminal input is unavailable.

If Central returns `401: Invalid token`, the human can replace just the token:

```sh
python3 java/bin/release-credentials.py update-token "$JAVA_RELEASE_CREDENTIALS"
```

Keep the directory until the release finishes, then run the cleanup in step 9.
Run cleanup as well if the release is abandoned; it removes the temporary copies,
leaving the original key and keyring intact.

## Releasing a new version

Run these commands from the repository root, in the same shell, with JDK 21 or 25,
Maven, Go, `golangci-lint`, Python 3.9+, and `gh` available. Complete the signing and
publishing setup above first. Stop if any command fails. Java versions have no
leading `v`; tags use `java/vX.Y.Z`.

1. Start from a clean checkout, fetch changes and tags, and choose the release version:

    ```sh
    git checkout master && git pull --rebase
    git fetch --tags
    export VERSION=0.1.0-alpha.1
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
    make lint/java test/java/sqlite check/java/package
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

5. Prepare `JAVA_RELEASE_CREDENTIALS` as described above. The human or agent can then build, test, sign, and upload the release for validation:

    ```sh
    test -z "$(git status --porcelain)" &&
    test "$(git rev-parse HEAD)" = "$(git rev-parse "java/v$VERSION^{commit}")" &&
    make generate/fixtures verify/java-migrations &&
    python3 java/bin/release-credentials.py run "$JAVA_RELEASE_CREDENTIALS" -- \
      ./java/bin/check-maven-warnings.sh mvn --batch-mode --no-transfer-progress \
      -Dstyle.color=never -f java/pom.xml -Prelease clean deploy \
      -Driver.test.database=sqlite -DexcludedGroups=postgres \
      -Dmaven.install.skip=true -Dcentral.skipUpload=false
    ```

6. In the [Central Portal deployments page](https://central.sonatype.com/publishing/deployments), open the validated deployment, confirm it contains `river-java`, `river`, and `river-cli` at `$VERSION`, and click **Publish**. Wait for **Published**. If uploading timed out, check this page before retrying.

    An agent can use the [Portal API](https://central.sonatype.org/publish/publish-portal-api/) through the credential helper's `run` command. Construct authorization headers from the child environment in memory, never in command arguments. Record the deployment ID; check its status and coordinates before publishing, then wait for `PUBLISHED`. Reuse an existing validated deployment or signed bundle when retrying; do not upload duplicates while publication is pending.

7. Verify the published CLI and its transitive library/parent dependencies using a fresh Maven cache. If Central has not propagated the release yet, wait and rerun with a new temporary directory:

    ```sh
    export JAVA_RELEASE_CHECK_DIR="$(mktemp -d)"
    (cd "$JAVA_RELEASE_CHECK_DIR" && mvn --batch-mode --no-transfer-progress \
      -Dmaven.repo.local="$JAVA_RELEASE_CHECK_DIR/m2" dependency:get \
      -Dartifact="com.riverqueue:river-cli:$VERSION:jar:all") &&
    java -jar "$JAVA_RELEASE_CHECK_DIR/m2/com/riverqueue/river-cli/$VERSION/river-cli-$VERSION-all.jar" --version &&
    git push origin "java/v$VERSION"
    ```

    If Maven reports a stale Central prefixes file or `prefixes: Path ... NOT allowed`, retry the download with `-Daether.remoteRepositoryFilter.prefixes.resolvePrefixFiles.central=false`. This skips Central's automatic prefix index for that invocation; see [Maven's filter configuration](https://maven.apache.org/resolver/configuration.html).

8. Create a [GitHub release](https://github.com/riverqueue/river/releases/new) for `java/v$VERSION`, copy that version's Java changelog notes into the body, and attach the downloaded `river-cli-$VERSION-all.jar`. Mark alpha, beta, and RC versions as prereleases.

9. Remove the temporary credential handoff:

    ```sh
    python3 java/bin/release-credentials.py cleanup "$JAVA_RELEASE_CREDENTIALS"
    unset JAVA_RELEASE_CREDENTIALS
    ```

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
include their tests and lint checks; `make test/java/tools` and
`make lint/java/tools` run only the tool checks. From `java/`, use `make test/tools`
and `make lint/tools`. These checks run in Java CI, separately from the root
Go-only `make test` and `make lint` targets.
The tool tests also cover the Python release credential helper.
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

`java/.mvn/maven.config` disables automatic discovery of Central's repository
prefix index. The index can lag newly published namespaces, rejecting available
River artifacts or emitting warnings that fail these checks. This applies to
Make targets and direct Maven builds; explicit local prefix filters and other
repositories retain their normal behavior. See [Maven's filter configuration](https://maven.apache.org/resolver/configuration.html).

Javadoc also fails on warnings. Its doclint configuration checks references,
syntax, HTML, and accessibility, but permits missing comments and tags because
the existing API documentation does not annotate every parameter and return value.
Automatic external documentation links are disabled so Javadoc generation does
not depend on another documentation site being reachable.

Executable JARs merge dependency `META-INF/LICENSE` and `META-INF/NOTICE` text,
retain service registrations, and generate their own executable manifests.
Dependency module descriptors and signatures are excluded from these combined
archives. Other overlapping classes or resources still produce warnings and fail
the checked Make targets. Library and source JARs keep their normal packaging.

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
