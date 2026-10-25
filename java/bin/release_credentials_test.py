import argparse
import base64
import contextlib
import importlib.util
import io
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch


SPEC = importlib.util.spec_from_file_location("release_credentials", Path(__file__).with_name("release-credentials.py"))
release = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(release)
KEY = "-----BEGIN PGP PRIVATE KEY BLOCK-----\nTEST-PRIVATE-MATERIAL\n-----END PGP PRIVATE KEY BLOCK-----\n"


class ReleaseCredentialsTest(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()
        self.key_file = self.root / "original.asc"
        self.key_file.write_text(KEY)
        self.umask = os.umask(0o077)
        self.addCleanup(os.umask, self.umask)

    def prepare(self, fingerprint=""):
        args = argparse.Namespace(key_file=self.key_file, gpg_homedir=None, fingerprint=fingerprint)
        output = io.StringIO()
        with patch.object(release.tempfile, "tempdir", str(self.root)), patch.object(release.getpass, "getpass", side_effect=["key-passphrase", "token-user", "token-password"]), contextlib.redirect_stdout(output):
            release.prepare(args)
        return Path(output.getvalue().strip())

    def test_cleanup_preserves_original_key_and_refuses_unexpected_files(self):
        directory = self.prepare()
        extra = directory / "unrelated"
        extra.write_text("keep")
        with self.assertRaisesRegex(ValueError, "unexpected files"):
            release.cleanup(directory)
        self.assertTrue((directory / "credentials.json").exists())
        extra.unlink()
        with contextlib.redirect_stdout(io.StringIO()):
            release.cleanup(directory)
        self.assertFalse(directory.exists())
        self.assertEqual(self.key_file.read_text(), KEY)

    def test_cli_runs_command_after_separator(self):
        directory = self.prepare()
        result = subprocess.run([
            sys.executable, str(Path(__file__).with_name("release-credentials.py")),
            "run", str(directory), "--", sys.executable, "-c",
            "import os; print(os.environ['CENTRAL_PASSWORD']); print('finished')",
        ], capture_output=True, text=True)
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertNotIn("token-password", result.stdout + result.stderr)
        self.assertIn("[REDACTED]", result.stdout)
        self.assertIn("finished", result.stdout)

    def test_failed_export_removes_partial_handoff(self):
        args = argparse.Namespace(key_file=None, gpg_homedir=self.root, fingerprint="A" * 40)
        before = set(self.root.iterdir())
        failure = subprocess.CompletedProcess([], 1, stdout="", stderr="bad key-passphrase")
        with patch.object(release.tempfile, "tempdir", str(self.root)), patch.object(release.getpass, "getpass", side_effect=["key-passphrase", "token-user", "token-password"]), patch.object(release.subprocess, "run", return_value=failure):
            with self.assertRaises(ValueError) as error:
                release.prepare(args)
        self.assertNotIn("key-passphrase", str(error.exception))
        self.assertEqual(set(self.root.iterdir()), before)

    def test_gpg_export_passes_passphrase_over_stdin(self):
        args = argparse.Namespace(key_file=None, gpg_homedir=self.root, fingerprint="A" * 40)
        def export(command, **kwargs):
            self.assertNotIn("key-passphrase", command)
            self.assertEqual(kwargs["input"], "key-passphrase\n")
            self.assertIn("--passphrase-fd", command)
            Path(command[command.index("--output") + 1]).write_text(KEY)
            return subprocess.CompletedProcess(command, 0, stdout="", stderr="")
        output = io.StringIO()
        with patch.object(release.tempfile, "tempdir", str(self.root)), patch.object(release.getpass, "getpass", side_effect=["key-passphrase", "token-user", "token-password"]), patch.object(release.subprocess, "run", side_effect=export), contextlib.redirect_stdout(output):
            release.prepare(args)
        directory = Path(output.getvalue().strip())
        self.assertEqual(release.load(directory)["MAVEN_GPG_KEY_FINGERPRINT"], "A" * 40)

    def test_handoff_permissions_and_no_gpg_for_exported_key(self):
        with patch.object(release.subprocess, "run", side_effect=AssertionError("GnuPG must not run")):
            directory = self.prepare()
        self.assertEqual(directory.stat().st_mode & 0o777, 0o700)
        for name in release.FILES:
            self.assertEqual((directory / name).stat().st_mode & 0o777, 0o600)
        self.assertEqual((directory / "private-key.asc").read_text(), KEY)
        self.assertEqual(release.load(directory)["CENTRAL_USERNAME"], "token-user")

    def test_missing_hidden_input_does_not_save_credentials(self):
        args = argparse.Namespace(key_file=self.key_file, gpg_homedir=None, fingerprint="")
        before = set(self.root.iterdir())
        with patch.object(release.getpass, "getpass", side_effect=release.getpass.GetPassWarning("No hidden input")):
            with self.assertRaises(release.getpass.GetPassWarning):
                release.prepare(args)
        self.assertEqual(set(self.root.iterdir()), before)

    def test_rejects_handoffs_inside_checkout(self):
        directory = self.prepare()
        with patch.object(release, "ROOT", self.root):
            with self.assertRaisesRegex(ValueError, "outside the checkout"):
                release.load(directory)

    def test_rejects_loose_permissions_and_symlinks(self):
        directory = self.prepare()
        credentials = directory / "credentials.json"
        credentials.chmod(0o644)
        with self.assertRaisesRegex(ValueError, "permissions 600"):
            release.load(directory)
        credentials.chmod(0o600)
        external = self.root / "external.json"
        credentials.rename(external)
        credentials.symlink_to(external)
        with self.assertRaisesRegex(ValueError, "regular file"):
            release.load(directory)

    def test_rejects_public_key_and_empty_token(self):
        self.key_file.write_text("-----BEGIN PGP PUBLIC KEY BLOCK-----")
        args = argparse.Namespace(key_file=self.key_file, gpg_homedir=None, fingerprint="")
        with self.assertRaisesRegex(ValueError, "private key"):
            release.prepare(args)
        self.key_file.write_text(KEY)
        with patch.object(release.getpass, "getpass", side_effect=["pass", "", "token"]):
            with self.assertRaisesRegex(ValueError, "Both Central"):
                release.prepare(args)

    def test_run_injects_credentials_redacts_output_and_preserves_exit_status(self):
        directory = self.prepare()
        code = '''import base64, json, os, sys
assert os.environ['MAVEN_GPG_KEY'].startswith('-----BEGIN PGP PRIVATE KEY BLOCK-----')
assert 'MAVEN_GPG_KEY_FINGERPRINT' not in os.environ
assert os.environ['MAVEN_GPG_PASSPHRASE'] == 'key-passphrase'
for name in ('CENTRAL_USERNAME', 'CENTRAL_PASSWORD', 'MAVEN_GPG_KEY', 'MAVEN_GPG_PASSPHRASE'):
    print(os.environ[name], flush=True)
print(json.dumps(os.environ['MAVEN_GPG_KEY']), file=sys.stderr, flush=True)
print(base64.b64encode((os.environ['CENTRAL_USERNAME'] + ':' + os.environ['CENTRAL_PASSWORD']).encode()).decode(), flush=True)
print('nonsecret build output', flush=True)
sys.exit(7)
'''
        output = io.StringIO()
        with patch.dict(os.environ, {"MAVEN_GPG_KEY_FINGERPRINT": "inherited-wrong-key"}), contextlib.redirect_stdout(output):
            result = release.run(directory, [sys.executable, "-c", code])
        self.assertEqual(result, 7)
        text = output.getvalue()
        for value in ("token-user", "token-password", "key-passphrase", "TEST-PRIVATE-MATERIAL", base64.b64encode(b"token-user:token-password").decode()):
            self.assertNotIn(value, text)
        self.assertIn("nonsecret build output", text)
        self.assertIn("[REDACTED]", text)

    def test_update_token_preserves_signing_credentials(self):
        directory = self.prepare()
        before = release.load(directory)
        with patch.object(release.getpass, "getpass", side_effect=["new-user", "new-password"]), contextlib.redirect_stdout(io.StringIO()):
            release.update_token(directory)
        after = release.load(directory)
        self.assertEqual(after["CENTRAL_USERNAME"], "new-user")
        self.assertEqual(after["CENTRAL_PASSWORD"], "new-password")
        self.assertEqual(after["MAVEN_GPG_PASSPHRASE"], before["MAVEN_GPG_PASSPHRASE"])
        self.assertEqual((directory / "private-key.asc").read_text(), KEY)
        self.assertEqual({path.name for path in directory.iterdir()}, release.FILES)


if __name__ == "__main__":
    unittest.main()
