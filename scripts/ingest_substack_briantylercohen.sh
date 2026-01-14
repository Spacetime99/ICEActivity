#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

if [[ -f .venv/bin/activate ]]; then
  # shellcheck source=/dev/null
  source .venv/bin/activate
fi

python scripts/ingest_news.py \
  --output-dir datasets/news_ingest \
  --rss-only \
  --rss-feed-name substack-briantylercohen \
  --include-all-rss \
  --disable-filtering \
  --log-level INFO
