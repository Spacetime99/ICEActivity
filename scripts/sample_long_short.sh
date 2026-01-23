#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

input="${1:-datasets/news_ingest/news_reports_20260114T190618Z.jsonl}"
out_dir="tmp"
mkdir -p "$out_dir"

INPUT_PATH="${input}"
python3 - <<PY
import json
from pathlib import Path

path = Path("${INPUT_PATH}")
items = []
for line in path.read_text(errors="ignore").splitlines():
    if not line.strip():
        continue
    try:
        obj = json.loads(line)
    except Exception:
        continue
    raw = obj.get("raw") or {}
    fetched = raw.get("fetched_content") or ""
    items.append((len(fetched), obj))

long_item = max(items, key=lambda x: x[0])[1] if items else None
short_items = [obj for length, obj in items if not (obj.get("raw") or {}).get("fetched_content")]
short_item = short_items[0] if short_items else None

out_path = Path("${out_dir}") / "sample_long_short.jsonl"
with out_path.open("w", encoding="utf-8") as f:
    if long_item:
        f.write(json.dumps(long_item, ensure_ascii=False) + "\n")
    if short_item:
        f.write(json.dumps(short_item, ensure_ascii=False) + "\n")
print(f"Wrote {out_path}")
PY

echo "Run:"
echo "source .venv/bin/activate"
echo "python3 scripts/extract_triplets.py --input-file ${out_dir}/sample_long_short.jsonl --output-dir datasets/news_ingest --model-id microsoft/Phi-3-mini-128k-instruct --max-new-tokens 160 --repetition-penalty 1.05"
