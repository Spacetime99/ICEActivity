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
python3 scripts/ingest_news.py \
  --output-dir datasets/news_ingest \
  --fetch-content \
  --resolve-google-news \
  --log-level INFO | tee -a "${LOG_DIR}/ingest.log"

echo "[${timestamp}] Starting triplet extraction..." | tee -a "${LOG_DIR}/ingest.log"
python3 scripts/extract_triplets.py \
  --input-dir datasets/news_ingest \
  --output-dir datasets/news_ingest \
  --log-level INFO | tee -a "${LOG_DIR}/ingest.log"

echo "[${timestamp}] Exporting triplet slices..." | tee -a "${LOG_DIR}/ingest.log"
bash scripts/export_triplets_static.sh | tee -a "${LOG_DIR}/ingest.log"

echo "[${timestamp}] Building + uploading frontend..." | tee -a "${LOG_DIR}/ingest.log"
bash "${REPO_ROOT}/frontend/push_data_and_redirect.sh" | tee -a "${LOG_DIR}/ingest.log"

echo "[${timestamp}] Run complete." | tee -a "${LOG_DIR}/ingest.log"
