#!/usr/bin/env python3
"""Serialize Maven builds and publish immutable adapter artifacts atomically."""
import fcntl
import hashlib
from pathlib import Path
import shutil
import subprocess
import zipfile

root = Path(__file__).resolve().parents[2]
artifacts = root / ".conformance" / "artifacts"
artifacts.mkdir(parents=True, exist_ok=True)
with (artifacts.parent / "build.lock").open("w") as lock:
    fcntl.flock(lock, fcntl.LOCK_EX)
    subprocess.run(["mvn", "-q", "-f", str(root / "pom.xml"), "package", "-DskipTests"], check=True)
    jar = root / "conformance/target/river-conformance-0.48.0-alpha.1.jar"
    # A stale shaded jar can otherwise silently retain an older dependency's classes.
    with zipfile.ZipFile(root / "river/target/river-0.48.0-alpha.1.jar") as library, zipfile.ZipFile(jar) as bundled:
        for entry in library.namelist():
            if entry.startswith("com/riverqueue/") and not entry.endswith("/"):
                if library.read(entry) != bundled.read(entry):
                    raise RuntimeError(f"Adapter contains a stale River resource: {entry}")
    digest = hashlib.sha256(jar.read_bytes()).hexdigest()
    snapshot = artifacts / f"{digest}.jar"
    if not snapshot.exists():
        temporary = artifacts / "adapter.tmp"
        shutil.copyfile(jar, temporary)
        temporary.replace(snapshot)
    pointer = artifacts.parent / "adapter-path.tmp"
    pointer.write_text(str(snapshot) + "\n")
    pointer.replace(artifacts.parent / "adapter-path")
