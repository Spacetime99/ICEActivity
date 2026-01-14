#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ -f .venv/bin/activate ]]; then
  # shellcheck source=/dev/null
  source .venv/bin/activate
fi

mapfile -t dumps < <(
  rg --files -g "news_reports_*.jsonl" datasets/news_ingest | sort
)

if [[ ${#dumps[@]} -eq 0 ]]; then
  echo "No news_reports_*.jsonl files found in datasets/news_ingest."
  exit 0
fi

for dump in "${dumps[@]}"; do
  echo "Re-extracting triplets from ${dump}..."
  python scripts/extract_triplets.py \
    --input-file "${dump}" \
    --output-dir datasets/news_ingest \
    --model-id microsoft/Phi-3-mini-128k-instruct \
    --max-new-tokens 160 \
    --repetition-penalty 1.05
done
