#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

MODEL_ID="${1:-Qwen/Qwen2.5-7B-Instruct}"

python3 scripts/run_daily.py \
  --out site/data \
  --include-triplets \
  --triplet-article-text \
  --skip-newsroom \
  --ice-report-include-index \
  --ice-report-use-playwright \
  --ice-report-llm-location-enrich \
  --ice-report-llm-model-id "$MODEL_ID"

python3 -m pytest -q tests/services/test_deaths_daily.py
python3 scripts/validate_deaths_output.py --input site/data/deaths.jsonl

mkdir -p frontend/public/data
cp site/data/deaths.jsonl frontend/public/data/deaths.jsonl
cp site/data/deaths.json frontend/public/data/deaths.json
cp site/data/index.json frontend/public/data/index.json

echo "Deaths data refreshed and copied to frontend/public/data."
echo "Next: cd frontend && npm run dev -- --host 0.0.0.0 --port 3000"
