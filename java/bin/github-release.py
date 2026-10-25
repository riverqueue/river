#!/usr/bin/env python3
"""Stage Java release artifacts, upload a GitHub draft, or verify a Maven repository."""

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import shutil
import subprocess
import sys
import tempfile
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
import zipfile

ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DIRECTORY = ROOT / "java/target/github-release"
GROUP = "com.riverqueue"
NS = {"m": "http://maven.apache.org/POM/4.0.0"}


def artifact_files(version):
    result = []
    for artifact in ("river-java", "river", "river-cli"):
        suffixes = [".pom"]
        if artifact != "river-java":
            suffixes += [".jar", "-sources.jar", "-javadoc.jar"]
        if artifact == "river-cli":
            suffixes += ["-all.jar"]
        result.extend(
            (artifact, f"{artifact}-{version}{suffix}") for suffix in suffixes
        )
    return result


def checked_version(version):
    number = r"(?:0|[1-9][0-9]*)"
    prerelease = r"(?:0|[1-9][0-9]*|[0-9]*[A-Za-z-][0-9A-Za-z-]*)"
    if (
        not re.fullmatch(
            rf"{number}\.{number}\.{number}(?:-{prerelease}(?:\.{prerelease})*)?",
            version,
        )
        or "snapshot" in version.lower()
    ):
        raise ValueError(
            "Expected a canonical release version, without v, SNAPSHOT, or build metadata."
        )
    return version


def command(args, **kwargs):
    return subprocess.run(args, check=True, **kwargs)


def gh_json(args):
    return json.loads(command(["gh", *args], capture_output=True, text=True).stdout)


def download(url):
    request = urllib.request.Request(
        url, headers={"User-Agent": "river-java-release/1.0"}
    )
    with urllib.request.urlopen(request, timeout=60) as response:
        return response.read()


def git(*args):
    return command(
        ["git", *args], cwd=ROOT, capture_output=True, text=True
    ).stdout.strip()


def load_manifest(directory):
    manifest = json.loads((directory / "manifest.json").read_text())
    if manifest.get("schema_version") != 1:
        raise ValueError("Unrecognized release manifest.")
    version = checked_version(manifest["version"])
    expected = {name for _, name in artifact_files(version)}
    if set(manifest["files"]) != expected:
        raise ValueError(
            "Release manifest must contain the complete public artifact set."
        )
    for name, info in manifest["files"].items():
        path = directory / name
        if path.is_symlink() or not path.is_file():
            raise ValueError(f"Expected a regular release file: {name}")
        if (
            path.stat().st_size != info["size"]
            or hashlib.sha256(path.read_bytes()).hexdigest() != info["sha256"]
        ):
            raise ValueError(f"Staged artifact changed: {name}")
    return manifest


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    actions = parser.add_subparsers(dest="action", required=True)
    prepare_parser = actions.add_parser(
        "prepare", help="Build, test, and stage all public POMs/JARs locally"
    )
    prepare_parser.add_argument("--directory", type=Path, default=DEFAULT_DIRECTORY)
    upload_parser = actions.add_parser(
        "upload", help="Create or resume a draft and verify every uploaded asset"
    )
    upload_parser.add_argument("--directory", type=Path, default=DEFAULT_DIRECTORY)
    upload_parser.add_argument("--repo", default="riverqueue/river")
    upload_parser.add_argument("--notes-file", required=True, type=Path)
    verify_parser = actions.add_parser(
        "verify", help="Resolve and run a release through a public Maven repository"
    )
    verify_parser.add_argument("--version", required=True, type=checked_version)
    verify_parser.add_argument("--repository", default="https://riverqueue.com/maven")
    verify_parser.add_argument(
        "--directory",
        type=Path,
        help="Also compare downloads with this staged manifest",
    )
    args = parser.parse_args()
    if args.action == "prepare":
        prepare(args.directory.resolve())
    elif args.action == "upload":
        upload(args.directory.resolve(), args.repo, args.notes_file.resolve())
    else:
        verify(args.repository, args.version, args.directory)


def prepare(directory):
    version = checked_version(
        ET.parse(ROOT / "java/pom.xml").findtext("m:version", namespaces=NS)
    )
    commit = git("rev-parse", "HEAD")
    clean = not git("status", "--porcelain")
    if directory.exists() and directory != DEFAULT_DIRECTORY:
        raise ValueError(
            "Choose an empty staging directory; custom directories are never overwritten."
        )
    command(["make", "generate/fixtures", "verify/java-migrations"], cwd=ROOT)
    with tempfile.TemporaryDirectory(prefix="river-java-stage-") as temporary:
        repository = Path(temporary) / "repository"
        destination = "github-staging::" + repository.as_uri()
        # File deployment uses Maven's canonical artifact names/POMs without
        # installing release artifacts locally or activating Central signing.
        command(
            [
                str(ROOT / "java/bin/check-maven-warnings.sh"),
                "mvn",
                "--batch-mode",
                "--no-transfer-progress",
                "-Dstyle.color=never",
                "-f",
                "java/pom.xml",
                "-P!release",
                "clean",
                "verify",
                "org.apache.maven.plugins:maven-deploy-plugin:3.2.0:deploy",
                "-Driver.test.database=sqlite",
                "-DexcludedGroups=postgres",
                "-DaltDeploymentRepository=" + destination,
                "-DaltReleaseDeploymentRepository=" + destination,
            ],
            cwd=ROOT,
        )
        command(["go", "run", "./java/bin/check-packages/main.go"], cwd=ROOT)
        stage = Path(temporary) / "assets"
        stage.mkdir()
        files = {}
        for artifact, filename in artifact_files(version):
            source = repository / "com/riverqueue" / artifact / version / filename
            validate_artifact(source.read_bytes(), artifact, filename, version)
            shutil.copyfile(source, stage / filename)
            files[filename] = {
                "sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
                "size": source.stat().st_size,
            }
        if (repository / "com/riverqueue/river-conformance").exists():
            raise ValueError("The conformance adapter must not be published.")
        if commit != git("rev-parse", "HEAD"):
            raise ValueError("Checkout changed during the build.")
        manifest = {
            "schema_version": 1,
            "version": version,
            "commit": commit,
            "clean": clean and not git("status", "--porcelain"),
            "files": files,
        }
        (stage / "manifest.json").write_text(json.dumps(manifest, indent=2) + "\n")
        directory.parent.mkdir(parents=True, exist_ok=True)
        shutil.move(str(stage), directory)
    print(f"Staged {len(files)} artifacts for {version} in {directory}")
    if not manifest["clean"]:
        print(
            "This was a development build; upload requires artifacts prepared from a clean release commit."
        )


def release_details(repo, tag):
    result = subprocess.run(
        ["gh", "release", "view", tag, "--repo", repo, "--json", "databaseId"],
        capture_output=True,
        text=True,
    )
    if result.returncode:
        if "release not found" in result.stderr.lower():
            return None
        raise RuntimeError(result.stderr.strip())
    release_id = json.loads(result.stdout)["databaseId"]
    return gh_json(["api", f"repos/{repo}/releases/{release_id}"])


def resolved_from_proxy(provenance, filename):
    # Newer Maven Resolver versions qualify repository IDs with a URL hash.
    return (
        re.search(
            rf"^{re.escape(filename)}>river-release-check(?:-[0-9a-f]{{40}})?=$",
            provenance,
            re.M,
        )
        is not None
    )


def upload(directory, repo, notes_file):
    if not re.fullmatch(r"[A-Za-z0-9_.-]+/[A-Za-z0-9_.-]+", repo):
        raise ValueError("Expected a GitHub owner/repository.")
    manifest = load_manifest(directory)
    if (
        not manifest["clean"]
        or git("status", "--porcelain")
        or git("rev-parse", "HEAD") != manifest["commit"]
    ):
        raise ValueError(
            "Upload requires a clean checkout at the commit used to prepare the artifacts."
        )
    if not notes_file.is_file():
        raise ValueError("Release notes file does not exist.")
    tag = "java/v" + manifest["version"]
    if git("rev-parse", tag + "^{commit}") != manifest["commit"]:
        raise ValueError("Local release tag does not match the prepared commit.")
    remote = gh_json(["api", f"repos/{repo}/git/ref/tags/{tag}"])["object"]
    for _ in range(4):
        if remote["type"] != "tag":
            break
        remote = gh_json(["api", f"repos/{repo}/git/tags/{remote['sha']}"])["object"]
    if remote["type"] != "commit" or remote["sha"] != manifest["commit"]:
        raise ValueError("Push the matching release tag before uploading assets.")
    release = release_details(repo, tag)
    if release is None:
        args = [
            "gh",
            "release",
            "create",
            tag,
            "--repo",
            repo,
            "--verify-tag",
            "--draft",
            "--title",
            tag,
            "--notes-file",
            str(notes_file),
        ]
        if "-" in manifest["version"]:
            args += ["--prerelease", "--latest=false"]
        command(args)
        release = release_details(repo, tag)
    if not release["draft"]:
        raise ValueError(
            "Only draft releases may be uploaded by this script; published versions are immutable."
        )
    pending = verify_assets(release["assets"], manifest, allow_missing=True)
    if pending:
        command(
            [
                "gh",
                "release",
                "upload",
                tag,
                "--repo",
                repo,
                *[str(directory / name) for name in pending],
            ]
        )
    verify_assets(release_details(repo, tag)["assets"], manifest)
    print(f"All {len(manifest['files'])} assets verified on draft {tag}.")
    print(f"Publish with: gh release edit {tag} --repo {repo} --draft=false")


def validate_artifact(data, artifact, filename, version):
    if filename.endswith(".pom"):
        if b"<!DOCTYPE" in data.upper() or b"<!ENTITY" in data.upper():
            raise ValueError(f"Unsupported XML declaration in {filename}")
        project = ET.fromstring(data)
        field = lambda name: project.findtext("m:" + name, namespaces=NS)
        inherited = lambda name: field(name) or project.findtext(
            "m:parent/m:" + name, namespaces=NS
        )
        if (
            inherited("groupId") != GROUP
            or field("artifactId") != artifact
            or inherited("version") != version
        ):
            raise ValueError(f"Unexpected POM coordinates: {filename}")
        if (field("packaging") or "jar") != (
            "pom" if artifact == "river-java" else "jar"
        ):
            raise ValueError(f"Unexpected POM packaging: {filename}")
    else:
        import io

        with zipfile.ZipFile(io.BytesIO(data)) as archive:
            if archive.testzip() is not None:
                raise ValueError(f"Corrupt JAR: {filename}")


def verify_assets(assets, manifest, allow_missing=False):
    by_name = {asset["name"]: asset for asset in assets}
    pending = []
    for name, expected in manifest["files"].items():
        if name not in by_name:
            if not allow_missing:
                raise ValueError(f"Missing GitHub asset: {name}")
            pending.append(name)
            continue
        actual = by_name[name]
        if (
            actual["state"] != "uploaded"
            or actual["size"] != expected["size"]
            or actual.get("digest") != "sha256:" + expected["sha256"]
        ):
            raise ValueError(
                f"Existing GitHub asset differs; refusing to overwrite: {name}"
            )
    return pending


def verify(repository, version, directory=None):
    parsed = urllib.parse.urlsplit(repository)
    if (
        parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
        or not parsed.hostname
    ):
        raise ValueError(
            "Expected a public repository URL without credentials, query, or fragment."
        )
    if parsed.scheme != "https" and not (
        parsed.scheme == "http" and parsed.hostname in ("localhost", "127.0.0.1", "::1")
    ):
        raise ValueError("Repository must use HTTPS, except for local loopback tests.")
    manifest = load_manifest(directory) if directory else None
    if manifest and manifest["version"] != version:
        raise ValueError("Staged version differs from the requested version.")
    with tempfile.TemporaryDirectory(prefix="river-java-consumer-") as temporary:
        work = Path(temporary)
        downloaded = {}
        for artifact, filename in artifact_files(version):
            url = f"{repository.rstrip('/')}/com/riverqueue/{artifact}/{version}/{filename}"
            data = download(url)
            checksum = download(url + ".sha256").decode().strip()
            digest = hashlib.sha256(data).hexdigest()
            if digest != checksum or (
                manifest and digest != manifest["files"][filename]["sha256"]
            ):
                raise ValueError(f"Download checksum differs: {filename}")
            validate_artifact(data, artifact, filename, version)
            downloaded[filename] = digest
        settings = work / "settings.xml"
        settings.write_text("<settings/>\n")
        repository_xml = ET.Element("repositories")
        entry = ET.SubElement(repository_xml, "repository")
        ET.SubElement(entry, "id").text = "river-release-check"
        ET.SubElement(entry, "url").text = repository
        ET.SubElement(ET.SubElement(entry, "releases"), "checksumPolicy").text = "fail"
        ET.SubElement(ET.SubElement(entry, "snapshots"), "enabled").text = "false"
        (work / "pom.xml").write_text(
            f"<project><modelVersion>4.0.0</modelVersion><groupId>check</groupId><artifactId>check</artifactId><version>1</version>{ET.tostring(repository_xml, encoding='unicode')}<dependencies><dependency><groupId>{GROUP}</groupId><artifactId>river-cli</artifactId><version>{version}</version></dependency></dependencies></project>"
        )
        environment = os.environ.copy()
        environment.update(MAVEN_ARGS="", MAVEN_OPTS="")
        for name in list(environment):
            if name.startswith(("MAVEN_GPG_", "CENTRAL_")):
                environment.pop(name)
        args = [
            "mvn",
            "--batch-mode",
            "--no-transfer-progress",
            "--strict-checksums",
            "-s",
            str(settings),
            "-gs",
            str(settings),
            "-Dmaven.repo.local=" + str(work / "m2"),
        ]
        plugin = "org.apache.maven.plugins:maven-dependency-plugin:3.11.0"
        for goals in (
            [plugin + ":build-classpath", "-Dmdep.outputFile=classpath.txt"],
            [plugin + ":get", f"-Dartifact={GROUP}:river-cli:{version}:jar:all"],
        ):
            command(args + goals, cwd=work, env=environment)
        for artifact, filename in artifact_files(version):
            if "-sources.jar" in filename or "-javadoc.jar" in filename:
                continue
            path = work / "m2/com/riverqueue" / artifact / version / filename
            if hashlib.sha256(path.read_bytes()).hexdigest() != downloaded[filename]:
                raise ValueError(f"Maven downloaded different bytes: {filename}")
            provenance = (path.parent / "_remote.repositories").read_text()
            if not resolved_from_proxy(provenance, filename):
                raise ValueError(f"Maven resolved {filename} from another repository.")
        classpath = (work / "classpath.txt").read_text().strip()
        (work / "Smoke.java").write_text(
            'public class Smoke { public static void main(String[] args) { if (!com.riverqueue.Client.class.getName().equals("com.riverqueue.Client")) throw new AssertionError(); if (!tools.jackson.databind.json.JsonMapper.builder().build().writeValueAsString("river").equals("\\"river\\"")) throw new AssertionError(); } }'
        )
        command(
            ["javac", "--release", "21", "-cp", classpath, "Smoke.java"],
            cwd=work,
            env=environment,
        )
        command(
            ["java", "-cp", "." + os.pathsep + classpath, "Smoke"],
            cwd=work,
            env=environment,
        )
        cli = (
            work
            / "m2/com/riverqueue/river-cli"
            / version
            / f"river-cli-{version}-all.jar"
        )
        output = command(
            ["java", "-jar", str(cli), "--version"],
            cwd=work,
            env=environment,
            capture_output=True,
            text=True,
        ).stdout.strip()
        if output != "river " + version:
            raise ValueError(f"CLI version differs: {output}")
        print(
            f"Verified all 10 artifacts, fresh Maven resolution, consumer compilation, and {output}."
        )


if __name__ == "__main__":
    try:
        main()
    except (
        OSError,
        ValueError,
        RuntimeError,
        subprocess.CalledProcessError,
        ET.ParseError,
        zipfile.BadZipFile,
    ) as error:
        print(f"Java release: {error}", file=sys.stderr)
        sys.exit(1)
