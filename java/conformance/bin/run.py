#!/usr/bin/env python3
"""Run the upstream harness without modifying the application's Go checkout."""

import argparse
import io
import json
import os
from pathlib import Path
import subprocess
import tarfile


ROOT = Path(__file__).resolve().parents[2]
SUITES = {
    "postgres": "test/conformance",
    "insert": "test/conformance/insert-only",
    "sqlite": "test/conformance/sqlite",
    "multi": "test/conformance/multi-engine",
    "performance": "test/conformance/performance",
    "soak": "test/conformance/soak",
    "multi-soak": "test/conformance/multi-engine/soak",
}


def run(*command, **kwargs):
    return subprocess.run(command, check=True, **kwargs)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("suite", choices=SUITES)
    parser.add_argument("--reference", type=Path, help="Use an existing conformance checkout")
    parser.add_argument("--refresh", action="store_true", help="Fetch and use the current reference branch")
    args = parser.parse_args()
    revision_file = ROOT / "conformance/reference-revision"
    revision = revision_file.read_text().strip()
    if args.refresh:
        run("git", "fetch", "origin", "bg/plan-interoperable-rust-river-port", cwd=ROOT.parent)
        revision = subprocess.check_output(
            ["git", "rev-parse", "origin/bg/plan-interoperable-rust-river-port"],
            cwd=ROOT.parent, text=True,
        ).strip()
    reference = args.reference or ROOT / ".conformance" / "reference" / revision
    if not reference.exists():
        try:
            archive = subprocess.check_output(["git", "archive", revision], cwd=ROOT.parent)
        except subprocess.CalledProcessError:
            run("git", "fetch", "origin", "bg/plan-interoperable-rust-river-port", cwd=ROOT.parent)
            archive = subprocess.check_output(["git", "archive", revision], cwd=ROOT.parent)
        reference.mkdir(parents=True)
        with tarfile.open(fileobj=io.BytesIO(archive)) as bundle:
            bundle.extractall(reference, filter="data")
    manifest_path = reference / "conformance/manifest.json"
    manifest = json.loads(manifest_path.read_text())
    manifest["implementations"]["java"] = {
        "package": "com.riverqueue:river", "registry": "maven", "version": "0.48.0-alpha.1"
    }
    manifest_path.write_text(json.dumps(manifest, indent=2) + "\n")
    environment = os.environ | {
        "RIVER_JAVA_ROOT": str(ROOT),
        "RIVER_CONFORMANCE_CANDIDATE_FILE": str(ROOT / "conformance/candidate.json"),
        "RIVER_CONFORMANCE_REQUIRED": "1",
    }
    if args.suite == "performance":
        environment["RIVER_CONFORMANCE_PERFORMANCE"] = "1"
    if "soak" in args.suite:
        environment["RIVER_CONFORMANCE_SOAK"] = "1"
    print(f"Reference: {revision}; suite: {args.suite}", flush=True)
    run("make", SUITES[args.suite], cwd=reference, env=environment)


if __name__ == "__main__":
    main()
