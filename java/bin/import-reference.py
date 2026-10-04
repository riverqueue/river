#!/usr/bin/env python3
"""Import the shared Go migration and golden-fixture resources."""
import argparse
import hashlib
import json
from pathlib import Path
import shutil

root = Path(__file__).resolve().parents[1]
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument('reference', type=Path)
parser.add_argument('--check', action='store_true', help='Verify the vendored bytes without changing them')
args = parser.parse_args()
reference = args.reference.resolve()
sources = {}


def copy(source, destination):
    if args.check:
        if source.read_bytes() != destination.read_bytes():
            raise RuntimeError(f'Reference mismatch: {destination}')
    else:
        destination.parent.mkdir(parents=True, exist_ok=True)
        shutil.copyfile(source, destination)
    sources[str(source.relative_to(reference))] = hashlib.sha256(source.read_bytes()).hexdigest()


for dialect, driver in [('postgres', 'riverpgxv5'), ('sqlite', 'riversqlite')]:
    for source in sorted((reference / f'riverdriver/{driver}/migration/main').glob('*.sql')):
        copy(source, root / 'river/src/main/resources/com/riverqueue/migration' / dialect / source.name)
for name in ['maintenance_values.json', 'unique_keys.json']:
    copy(reference / 'conformance/fixtures' / name, root / 'river/src/test/resources/fixtures' / name)
copy(reference / 'conformance/adapter/contract.json', root / 'conformance/src/main/resources/contract.json')
if not args.check:
    (root / 'conformance/reference-sources.json').write_text(json.dumps(sources, indent=2, sort_keys=True) + '\n')
