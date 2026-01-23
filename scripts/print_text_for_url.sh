#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ $# -lt 1 ]]; then
  echo "Usage: bash scripts/print_text_for_url.sh <url> [jsonl] [chars]" >&2
  exit 1
fi

url="$1"
input="${2:-datasets/news_ingest/news_reports_20260114T190618Z.jsonl}"
chars="${3:-2000}"

python3 - <<PY
import json
from pathlib import Path

url = "${url}"
path = Path("${input}")
chars = int("${chars}")

for line in path.read_text(errors="ignore").splitlines():
    if not line.strip():
        continue
    obj = json.loads(line)
    item_url = obj.get("url") or obj.get("source_id") or ""
    if item_url == url:
        raw = obj.get("raw") or {}
        text = raw.get("fetched_content") or obj.get("content") or raw.get("content") or raw.get("body") or raw.get("text") or obj.get("summary") or ""
        print("TITLE:", obj.get("title"))
        print("URL:", item_url)
        print("SOURCE:", obj.get("source"))
        print("TEXT:", text[:chars])
        break
else:
    print("No match for URL in", path)
PY
