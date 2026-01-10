#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${REPO_ROOT}/logs"
mkdir -p "${LOG_DIR}"

cd "${REPO_ROOT}"
if [[ -z "${VIRTUAL_ENV:-}" ]]; then
  # shellcheck source=/dev/null
  source "${REPO_ROOT}/.venv/bin/activate"
fi

timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
echo "[${timestamp}] Starting ingest..." | tee -a "${LOG_DIR}/ingest.log"
python scripts/ingest_news.py \
  --output-dir datasets/news_ingest \
  --fetch-content \
  --resolve-google-news \
  --log-level INFO | tee -a "${LOG_DIR}/ingest.log"

echo "[${timestamp}] Starting triplet extraction..." | tee -a "${LOG_DIR}/ingest.log"
python scripts/extract_triplets.py \
  --input-dir datasets/news_ingest \
  --output-dir datasets/news_ingest \
  --log-level INFO | tee -a "${LOG_DIR}/ingest.log"

echo "[${timestamp}] Run complete." | tee -a "${LOG_DIR}/ingest.log"
