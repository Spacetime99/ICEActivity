#!/usr/bin/env bash
set -euo pipefail
python3 /home/spacetime/codex/scripts/extract_triplets.py --input-file /home/spacetime/codex/datasets/news_ingest/news_reports_20260110T030329Z.jsonl --output-dir /home/spacetime/codex/datasets/news_ingest --model-id microsoft/Phi-3-mini-128k-instruct --max-new-tokens 160 --repetition-penalty 1.05
