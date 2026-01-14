#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ -f .venv/bin/activate ]]; then
  # shellcheck source=/dev/null
  source .venv/bin/activate
fi

# Clean up known bad rows before re-extraction.
bash scripts/remove_incomplete_actor_triplets.sh
bash scripts/remove_inverted_ice_victim_triplets.sh

mapfile -t dumps < <(
  rg --files-with-matches -g "news_reports_*.jsonl" "Renee Good|Renee Nicole Good" datasets/news_ingest \
    | sort -u
)

if [[ ${#dumps[@]} -eq 0 ]]; then
  echo "No news_reports_*.jsonl files found containing Renee Good."
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
