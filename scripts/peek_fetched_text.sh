#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

file="${1:-datasets/news_ingest/news_reports_20260114T190618Z.jsonl}"
limit="${2:-1000}"

python3 - <<PY
import json
from pathlib import Path

path = Path("${file}")
limit = int("${limit}")
for line in path.read_text(errors="ignore").splitlines():
    if not line.strip():
        continue
    obj = json.loads(line)
    fetched = (obj.get("raw") or {}).get("fetched_content")
    if fetched:
        print("TITLE:", obj.get("title"))
        print("FETCHED:", fetched[:limit])
        break
PY
