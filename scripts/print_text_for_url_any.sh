#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ $# -lt 1 ]]; then
  echo "Usage: bash scripts/print_text_for_url_any.sh <url> [chars]" >&2
  exit 1
fi

url="$1"
chars="${2:-3000}"

python3 - <<PY
import json
from pathlib import Path

url = "${url}"
chars = int("${chars}")
for path in sorted(Path("datasets/news_ingest").glob("news_reports_*.jsonl")):
    for line in path.read_text(errors="ignore").splitlines():
        if not line.strip():
            continue
        obj = json.loads(line)
        item_url = obj.get("url") or obj.get("source_id") or ""
        if item_url == url:
            raw = obj.get("raw") or {}
            text = raw.get("fetched_content") or obj.get("content") or raw.get("content") or raw.get("body") or raw.get("text") or obj.get("summary") or ""
            print("FILE:", path)
            print("TITLE:", obj.get("title"))
            print("URL:", item_url)
            print("SOURCE:", obj.get("source"))
            print("TEXT:", text[:chars])
            raise SystemExit(0)
print("No match for URL across news_reports_*.jsonl")
PY
