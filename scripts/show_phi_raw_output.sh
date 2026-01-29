#!/usr/bin/env bash
set -euo pipefail

raw_output="${1:-/tmp/phi_raw_output.txt}"

if [[ ! -f "${raw_output}" ]]; then
  echo "Missing raw output file: ${raw_output}" >&2
  exit 1
fi

cat "${raw_output}"
