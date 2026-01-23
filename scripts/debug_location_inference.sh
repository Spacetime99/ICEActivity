#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ -f .venv/bin/activate ]]; then
  # shellcheck source=/dev/null
  source .venv/bin/activate
fi

URL="${1:-}"
if [[ -z "$URL" ]]; then
  echo "Usage: $0 <article-url>"
  exit 1
fi

python3 scripts/debug_location_inference.py "$URL"
