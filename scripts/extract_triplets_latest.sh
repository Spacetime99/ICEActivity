#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ -f .venv/bin/activate ]]; then
  # shellcheck source=/dev/null
  source .venv/bin/activate
fi

python3 scripts/extract_triplets.py \
  --input-dir datasets/news_ingest \
  --output-dir datasets/news_ingest \
  --model-id microsoft/Phi-3-mini-128k-instruct \
  --max-new-tokens 160 \
  --repetition-penalty 1.05
