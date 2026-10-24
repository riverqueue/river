#!/usr/bin/env bash
set -euo pipefail

script_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
test_dir=$(mktemp -d)
trap 'rm -rf "$test_dir"' EXIT

check() {
  local name=$1 expected=$2 output=$3 message=$4
  local actual=0
  "$script_dir/check-maven-warnings.sh" bash -c "$output" >"$test_dir/output" 2>&1 || actual=$?
  if [[ "$actual" -ne "$expected" ]]; then
    cat "$test_dir/output" >&2
    echo "$name: expected exit $expected, got $actual" >&2
    exit 1
  fi
  if ! grep -Fq "$message" "$test_dir/output"; then
    echo "$name: missing command output: $message" >&2
    exit 1
  fi
}

check clean 0 'echo "[INFO] BUILD SUCCESS"' '[INFO] BUILD SUCCESS'
check warning_stdout 1 'echo "[WARNING] duplicate class"' '[WARNING] duplicate class'
check warning_stderr 1 'echo "[WARNING] duplicate resource" >&2' '[WARNING] duplicate resource'
check command_failure 23 'echo "[ERROR] build failed"; exit 23' '[ERROR] build failed'
check failure_with_warning 17 'echo "[WARNING] warning before failure"; exit 17' '[WARNING] warning before failure'
check ordinary_word 0 'echo "[INFO] test checks WARNING handling"' '[INFO] test checks WARNING handling'
check application_log 0 'echo "WARNING: expected test log"' 'WARNING: expected test log'

echo 'Maven warning enforcement checks passed'
