#!/usr/bin/env bash
set -euo pipefail

if [ "$#" -lt 2 ]; then
  echo "Usage: $0 DUMP_PATH URL_OR_PATTERN [MODEL_ID]"
  exit 1
fi

dump_path="$1"
needle="$2"
model_id="${3:-microsoft/Phi-3-mini-128k-instruct}"

tmp_file="/tmp/single_story_extract_$$.jsonl"
trap 'rm -f "$tmp_file"' EXIT

rg -F "$needle" "$dump_path" > "$tmp_file" || true

if [ ! -s "$tmp_file" ]; then
  echo "No matching story in ${dump_path}"
  exit 1
fi

python3 /home/spacetime/codex/scripts/extract_triplets.py \
--input-file "$tmp_file" \
--output-dir /home/spacetime/codex/datasets/news_ingest \
--model-id "$model_id" \
--max-new-tokens 160 \
--repetition-penalty 1.05 \
--log-level DEBUG

rg -n "$needle" /home/spacetime/codex/datasets/news_ingest/triplets_*.jsonl
