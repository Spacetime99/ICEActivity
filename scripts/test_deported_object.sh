#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ -f .venv/bin/activate ]]; then
  # shellcheck source=/dev/null
  source .venv/bin/activate
fi

tmp_file="tmp/test_deported_object.jsonl"
mkdir -p tmp
cat > "$tmp_file" <<'JSONL'
{"source":"test","source_id":"https://example.com/test-deported","title":"ICE deportations in California surged, data shows","url":"https://example.com/test-deported","summary":"New data shows ICE agents deported at least 8,200 people from California between January and September 2025, with a sharp surge during the summer months.","published_at":"2026-01-13T14:05:00+00:00","content":null,"raw":{"fetched_content":"New data shows ICE agents deported at least 8,200 people from California between January and September 2025, with a sharp surge during the summer months."}}
JSONL

python3 scripts/extract_triplets.py \
  --input-file "$tmp_file" \
  --output-dir datasets/news_ingest \
  --model-id microsoft/Phi-3-mini-128k-instruct \
  --max-new-tokens 120 \
  --repetition-penalty 1.05
