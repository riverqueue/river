import contextlib
import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location(
    "github_release", Path(__file__).with_name("github-release.py")
)
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)


class GitHubReleaseTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        self.prepare_version("99.88.77-alpha.4")
        self.notes = self.root / "notes.md"
        self.notes.write_text("Release notes")

    def prepare_version(self, version):
        self.version = version
        self.manifest = {
            "schema_version": 1,
            "version": self.version,
            "commit": "a" * 40,
            "clean": True,
            "files": {},
        }
        for _, name in release.artifact_files(self.version):
            data = name.encode()
            (self.root / name).write_bytes(data)
            self.manifest["files"][name] = {
                "sha256": hashlib.sha256(data).hexdigest(),
                "size": len(data),
            }
        self.write_manifest()
        self.assets = [
            {
                "name": name,
                "state": "uploaded",
                "size": data["size"],
                "digest": "sha256:" + data["sha256"],
            }
            for name, data in self.manifest["files"].items()
        ]

    def write_manifest(self):
        (self.root / "manifest.json").write_text(json.dumps(self.manifest))

    def mock_git(self, *args):
        return "" if args[0] == "status" else self.manifest["commit"]

    def test_artifacts_include_parent_without_fat_jar_or_conformance(self):
        names = {name for _, name in release.artifact_files(self.version)}
        self.assertEqual(len(names), 9)
        self.assertIn("river-java-" + self.version + ".pom", names)
        self.assertIn("river-cli-" + self.version + ".jar", names)
        self.assertNotIn("river-cli-" + self.version + "-all.jar", names)
        self.assertFalse(any("conformance" in name for name in names))

    def test_manifest_rejects_missing_extra_changed_and_symlinked_files(self):
        release.load_manifest(self.root)
        name = next(iter(self.manifest["files"]))
        (self.root / name).write_text("changed")
        with self.assertRaisesRegex(ValueError, "changed"):
            release.load_manifest(self.root)
        (self.root / name).unlink()
        (self.root / name).symlink_to(self.notes)
        with self.assertRaisesRegex(ValueError, "regular"):
            release.load_manifest(self.root)
        del self.manifest["files"][name]
        self.write_manifest()
        with self.assertRaisesRegex(ValueError, "complete"):
            release.load_manifest(self.root)
        self.manifest["files"]["../unexpected.jar"] = {}
        self.write_manifest()
        with self.assertRaisesRegex(ValueError, "complete"):
            release.load_manifest(self.root)

    def test_pom_coordinates_parent_inheritance_and_packaging(self):
        parent = f"<parent><groupId>com.riverqueue</groupId><artifactId>river-java</artifactId><version>{self.version}</version></parent>"
        xml = f'<project xmlns="http://maven.apache.org/POM/4.0.0">{parent}<artifactId>river</artifactId></project>'
        release.validate_artifact(xml.encode(), "river", "river.pom", self.version)
        with self.assertRaisesRegex(ValueError, "coordinates"):
            release.validate_artifact(
                xml.encode(), "river-cli", "river-cli.pom", self.version
            )
        xml = xml.replace(
            "<artifactId>river</artifactId>", "<artifactId>river-java</artifactId>"
        )
        with self.assertRaisesRegex(ValueError, "packaging"):
            release.validate_artifact(
                xml.encode(), "river-java", "river-java.pom", self.version
            )
        release.validate_artifact(
            xml.replace("</project>", "<packaging>pom</packaging></project>").encode(),
            "river-java",
            "river-java.pom",
            self.version,
        )

    def test_repository_tracking_accepts_old_and_url_qualified_ids(self):
        name = "river-" + self.version + ".jar"
        for repository in ("river-release-check", "river-release-check-" + "a" * 40):
            self.assertTrue(
                release.resolved_from_proxy(f"{name}>{repository}=\n", name)
            )
        for repository in (
            "",
            "central",
            "central-" + "a" * 40,
            "river-release-check-other",
        ):
            self.assertFalse(
                release.resolved_from_proxy(f"{name}>{repository}=\n", name)
            )
        self.assertFalse(
            release.resolved_from_proxy(f"different.jar>river-release-check=\n", name)
        )

    def test_upload_creates_drafts_without_replacing_latest(self):
        for version, prerelease in (("99.88.77-alpha.4", True), ("99.88.77", False)):
            with self.subTest(version=version):
                self.prepare_version(version)
                before = {"draft": True, "assets": []}
                after = {"draft": True, "assets": self.assets}
                remote = [
                    {"object": {"type": "tag", "sha": "b" * 40}},
                    {"object": {"type": "commit", "sha": self.manifest["commit"]}},
                ]
                output = io.StringIO()
                with (
                    patch.object(release, "git", side_effect=self.mock_git),
                    patch.object(release, "gh_json", side_effect=remote),
                    patch.object(
                        release, "release_details", side_effect=[None, before, after]
                    ),
                    patch.object(release, "command") as command,
                    contextlib.redirect_stdout(output),
                ):
                    release.upload(self.root, "riverqueue/river", self.notes)
                create, upload = [call.args[0] for call in command.call_args_list]
                self.assertEqual(
                    create[:4], ["gh", "release", "create", "java/v" + version]
                )
                for flag in ("--draft", "--verify-tag", "--latest=false"):
                    self.assertIn(flag, create)
                self.assertEqual("--prerelease" in create, prerelease)
                self.assertEqual(
                    create[create.index("--notes-file") + 1], str(self.notes)
                )
                self.assertEqual(upload[:3], ["gh", "release", "upload"])
                self.assertEqual(len(upload[6:]), 9)
                self.assertIn(
                    f"gh release edit java/v{version} --repo riverqueue/river --draft=false --latest=false",
                    output.getvalue(),
                )

    def test_upload_resumes_matching_draft_and_never_clobbers(self):
        before = {"draft": True, "assets": self.assets[:2]}
        after = {"draft": True, "assets": self.assets}
        remote = {"object": {"type": "commit", "sha": self.manifest["commit"]}}
        with (
            patch.object(release, "git", side_effect=self.mock_git),
            patch.object(release, "gh_json", return_value=remote),
            patch.object(release, "release_details", side_effect=[before, after]),
            patch.object(release, "command") as command,
            contextlib.redirect_stdout(io.StringIO()),
        ):
            release.upload(self.root, "riverqueue/river", self.notes)
        args = command.call_args.args[0]
        self.assertEqual(args[:3], ["gh", "release", "upload"])
        self.assertNotIn("--clobber", args)
        self.assertEqual(len(args[6:]), 7)

    def test_upload_refuses_dirty_build_wrong_tag_and_published_release(self):
        self.manifest["clean"] = False
        self.write_manifest()
        with self.assertRaisesRegex(ValueError, "clean"):
            release.upload(self.root, "riverqueue/river", self.notes)
        self.manifest["clean"] = True
        self.write_manifest()
        remote = {"object": {"type": "commit", "sha": "b" * 40}}
        with (
            patch.object(release, "git", side_effect=self.mock_git),
            patch.object(release, "gh_json", return_value=remote),
        ):
            with self.assertRaisesRegex(ValueError, "matching release tag"):
                release.upload(self.root, "riverqueue/river", self.notes)
        remote["object"]["sha"] = self.manifest["commit"]
        with (
            patch.object(release, "git", side_effect=self.mock_git),
            patch.object(release, "gh_json", return_value=remote),
            patch.object(release, "release_details", return_value={"draft": False}),
        ):
            with self.assertRaisesRegex(ValueError, "Only draft"):
                release.upload(self.root, "riverqueue/river", self.notes)

    def test_verify_assets_refuses_mismatches_before_uploading_anything(self):
        self.assets[-1]["digest"] = "sha256:wrong"
        with self.assertRaisesRegex(ValueError, "refusing to overwrite"):
            release.verify_assets(self.assets[2:], self.manifest, allow_missing=True)
        with self.assertRaisesRegex(ValueError, "Missing"):
            release.verify_assets([], self.manifest)

    def test_verify_refuses_credentials_and_non_https_urls(self):
        for url in (
            "https://user:secret@example.com/maven",
            "http://example.com/maven",
            "https://example.com/?token=secret",
        ):
            with self.subTest(url=url), self.assertRaises(ValueError):
                release.verify(url, self.version)

    def test_versions_reject_snapshots_aliases_and_paths(self):
        for value in (
            "1.0",
            "v1.0.0",
            "01.0.0",
            "1.0.0-SNAPSHOT",
            "1.0.0+build",
            "../1.0.0",
            "1.0.0-01",
        ):
            with self.subTest(version=value), self.assertRaises(ValueError):
                release.checked_version(value)
        self.assertEqual(release.checked_version(self.version), self.version)


if __name__ == "__main__":
    unittest.main()
