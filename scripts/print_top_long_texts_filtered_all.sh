#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

limit="${1:-10}"
chars="${2:-10000}"

python3 - <<PY
import json
import re
from pathlib import Path

limit = int("${limit}")
chars = int("${chars}")
pattern = re.compile(r"\\bSUGGESTED:\\b")
items = []
for path in sorted(Path("datasets/news_ingest").glob("news_reports_*.jsonl")):
    for line in path.read_text(errors="ignore").splitlines():
        if not line.strip():
            continue
        try:
            obj = json.loads(line)
        except Exception:
            continue
        raw = obj.get("raw") or {}
        text = raw.get("fetched_content") or obj.get("content") or raw.get("content") or raw.get("body") or raw.get("text") or ""
        if len(text) < 8192:
            continue
        url = obj.get("url") or obj.get("source_id") or ""
        if "memeorandum.com" in url:
            continue
        if pattern.search(text):
            continue
        items.append((len(text), obj, text))

items.sort(key=lambda x: x[0], reverse=True)
for length, obj, text in items[:limit]:
    print("LENGTH:", length)
    print("TITLE:", obj.get("title"))
    print("URL:", obj.get("url") or obj.get("source_id"))
    print("SOURCE:", obj.get("source"))
    print("TEXT:", text[:chars])
    print("=" * 60)
PY
