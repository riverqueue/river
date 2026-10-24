#!/usr/bin/env bash
# Maven 3 has no general fail-on-warning option. Keep its output visible, then
# reject warnings from packaging and other plugins as well as the compiler.
set -euo pipefail

maven_log=$(mktemp)
trap 'rm -f "$maven_log"' EXIT

"$@" 2>&1 | tee "$maven_log"

if grep -q '^\[WARNING\]' "$maven_log"; then
  echo 'Java build emitted Maven warnings; fix them or document an intentional suppression.' >&2
  exit 1
fi
