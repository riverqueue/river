#!/usr/bin/env python3
"""Keep release credentials in a private local handoff, outside the checkout."""

import argparse
import base64
import getpass
import json
import os
from pathlib import Path
import re
import shutil
import stat
import subprocess
import sys
import tempfile
import warnings


FILES = {"credentials.json", "private-key.asc"}
PREFIX = "river-java-release-credentials-"
ROOT = Path(__file__).resolve().parents[2]


def check_permissions(path, directory=False):
    info = path.lstat()
    expected_type = stat.S_ISDIR if directory else stat.S_ISREG
    if not expected_type(info.st_mode) or info.st_uid != os.getuid():
        raise ValueError(f"Expected an owned {'directory' if directory else 'regular file'}: {path}")
    expected_mode = 0o700 if directory else 0o600
    if stat.S_IMODE(info.st_mode) != expected_mode:
        raise ValueError(f"Expected permissions {expected_mode:o}: {path}")


def cleanup(directory):
    load(directory)
    if {path.name for path in directory.iterdir()} != FILES:
        raise ValueError("Handoff contains unexpected files; refusing to remove it.")
    for name in sorted(FILES):
        (directory / name).unlink()
    directory.rmdir()
    print("Removed the temporary credential handoff.")


def load(directory):
    if not directory.name.startswith(PREFIX):
        raise ValueError("Expected a directory created by the prepare command.")
    check_permissions(directory, directory=True)
    if directory.resolve().is_relative_to(ROOT):
        raise ValueError("Credential handoffs must be outside the checkout.")
    for name in FILES:
        check_permissions(directory / name)
    credentials = json.loads((directory / "credentials.json").read_text())
    fields = ("CENTRAL_USERNAME", "CENTRAL_PASSWORD", "MAVEN_GPG_PASSPHRASE", "MAVEN_GPG_KEY_FINGERPRINT")
    if credentials.get("schema_version") != 1 or any(not isinstance(credentials.get(name), str) for name in fields):
        raise ValueError("Unrecognized credential handoff format.")
    if not credentials["CENTRAL_USERNAME"] or not credentials["CENTRAL_PASSWORD"]:
        raise ValueError("Both Central publishing-token fields are required.")
    return credentials


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="action", required=True)
    prepare_parser = commands.add_parser("prepare", help="Prompt locally and create a private handoff")
    source = prepare_parser.add_mutually_exclusive_group(required=True)
    source.add_argument("--key-file", type=Path, help="Existing ASCII-armored private-key export")
    source.add_argument("--gpg-homedir", type=Path, help="Export once from an existing GnuPG home")
    prepare_parser.add_argument("--fingerprint", default="", help="Full primary-key fingerprint; required with --gpg-homedir")
    for action, help_text in (
        ("cleanup", "Remove the temporary handoff after release or abandonment"),
        ("run", "Run a command with credentials in its environment and redacted output"),
        ("update-token", "Replace only the Central token using hidden prompts"),
    ):
        command = commands.add_parser(action, help=help_text)
        command.add_argument("directory", type=Path)
        if action == "run":
            command.add_argument("command", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    os.umask(0o077)
    warnings.simplefilter("error", getpass.GetPassWarning)
    if args.action == "prepare":
        if args.fingerprint and not re.fullmatch(r"[0-9a-fA-F]{40}|[0-9a-fA-F]{64}", args.fingerprint):
            parser.error("--fingerprint must be a full hexadecimal fingerprint without spaces")
        if args.gpg_homedir and not args.fingerprint:
            parser.error("--gpg-homedir requires --fingerprint")
        prepare(args)
    elif args.action == "cleanup":
        cleanup(args.directory)
    elif args.action == "run":
        command = args.command[1:] if args.command[:1] == ["--"] else args.command
        if not command:
            parser.error("run requires a command after --")
        return run(args.directory, command)
    else:
        update_token(args.directory)
    return 0


def prepare(args):
    if Path(tempfile.gettempdir()).resolve().is_relative_to(ROOT):
        raise ValueError("TMPDIR must be outside the checkout.")
    key = args.key_file.expanduser().read_text() if args.key_file else None
    if key is not None and "-----BEGIN PGP PRIVATE KEY BLOCK-----" not in key:
        raise ValueError("--key-file must contain an ASCII-armored OpenPGP private key.")
    credentials = {
        "schema_version": 1,
        "MAVEN_GPG_KEY_FINGERPRINT": args.fingerprint.upper(),
        "MAVEN_GPG_PASSPHRASE": getpass.getpass("OpenPGP key passphrase (empty if unencrypted): "),
    }
    credentials.update(prompt_token())
    directory = Path(tempfile.mkdtemp(prefix=PREFIX))
    try:
        private_key = directory / "private-key.asc"
        if key is not None:
            private_key.write_text(key)
        else:
            result = subprocess.run([
                "gpg", "--homedir", str(args.gpg_homedir.expanduser()),
                "--batch", "--pinentry-mode", "loopback", "--passphrase-fd", "0",
                "--armor", "--output", str(private_key),
                "--export-secret-keys", args.fingerprint,
            ], input=credentials["MAVEN_GPG_PASSPHRASE"] + "\n", text=True, capture_output=True)
            if result.returncode:
                raise ValueError("GnuPG export failed: " + redactor(credentials)(result.stderr).strip())
            if not private_key.is_file() or "-----BEGIN PGP PRIVATE KEY BLOCK-----" not in private_key.read_text():
                raise ValueError("GnuPG did not export an OpenPGP private key.")
        private_key.chmod(0o600)
        write_credentials(directory, credentials)
    except BaseException:
        shutil.rmtree(directory)
        raise
    # stdout contains only the path, so callers may capture it in a shell variable.
    print(directory)


def prompt_token():
    credentials = {
        "CENTRAL_USERNAME": getpass.getpass("Central publishing-token username: ").strip(),
        "CENTRAL_PASSWORD": getpass.getpass("Central publishing-token password: ").strip(),
    }
    if not all(credentials.values()):
        raise ValueError("Both Central publishing-token fields are required.")
    return credentials


def redactor(credentials, key=""):
    values = [credentials.get(name, "") for name in ("CENTRAL_USERNAME", "CENTRAL_PASSWORD", "MAVEN_GPG_PASSPHRASE")]
    values.extend(key.splitlines())
    values.append(key)
    if credentials.get("CENTRAL_USERNAME") and credentials.get("CENTRAL_PASSWORD"):
        values.append(base64.b64encode((credentials["CENTRAL_USERNAME"] + ":" + credentials["CENTRAL_PASSWORD"]).encode()).decode())
    values.extend(json.dumps(value)[1:-1] for value in list(values))
    pattern = re.compile("|".join(re.escape(value) for value in sorted(set(values), key=len, reverse=True) if value))
    return lambda text: pattern.sub("[REDACTED]", text) if pattern.pattern else text


def run(directory, command):
    credentials = load(directory)
    key = (directory / "private-key.asc").read_text()
    environment = os.environ.copy()
    for name in ("CENTRAL_USERNAME", "CENTRAL_PASSWORD", "MAVEN_GPG_PASSPHRASE", "MAVEN_GPG_KEY_FINGERPRINT"):
        environment[name] = credentials[name]
    environment["MAVEN_GPG_KEY"] = key
    if not credentials["MAVEN_GPG_KEY_FINGERPRINT"]:
        environment.pop("MAVEN_GPG_KEY_FINGERPRINT", None)
    redact = redactor(credentials, key)
    with subprocess.Popen(command, env=environment, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, errors="replace") as process:
        try:
            for line in process.stdout:
                print(redact(line), end="", flush=True)
            code = process.wait()
        except BaseException:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
            raise
    return code if code >= 0 else 128 - code


def update_token(directory):
    credentials = load(directory)
    credentials.update(prompt_token())
    write_credentials(directory, credentials)
    print("Updated the Central token; the signing key and passphrase are unchanged.")


def write_credentials(directory, credentials):
    temporary = None
    try:
        with tempfile.NamedTemporaryFile(mode="w", dir=directory, delete=False) as output:
            temporary = Path(output.name)
            json.dump(credentials, output)
            output.write("\n")
        temporary.chmod(0o600)
        temporary.replace(directory / "credentials.json")
    finally:
        if temporary is not None:
            temporary.unlink(missing_ok=True)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except (EOFError, getpass.GetPassWarning, OSError, ValueError) as error:
        print(f"Release credentials: {error}", file=sys.stderr)
        sys.exit(1)
    except KeyboardInterrupt:
        print("Cancelled.", file=sys.stderr)
        sys.exit(130)
