#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
LOG_DIR="${REPO_ROOT}/logs"
mkdir -p "${LOG_DIR}"
RUN_LOG="${LOG_DIR}/ingest.log"

cd "${REPO_ROOT}"
if [[ -z "${VIRTUAL_ENV:-}" ]]; then
  # shellcheck source=/dev/null
  source "${REPO_ROOT}/.venv/bin/activate"
fi

if [[ -f "${REPO_ROOT}/config/.env" ]]; then
  set -a
  # shellcheck source=/dev/null
  source "${REPO_ROOT}/config/.env"
  set +a
fi

export HF_HOME="${HF_HOME:-$REPO_ROOT/.cache/huggingface}"
export HF_HUB_CACHE="${HF_HUB_CACHE:-$HF_HOME/hub}"
export TRANSFORMERS_CACHE="${TRANSFORMERS_CACHE:-$HF_HOME/transformers}"
export HF_DATASETS_CACHE="${HF_DATASETS_CACHE:-$HF_HOME/datasets}"
export HF_HUB_DISABLE_TELEMETRY=1
mkdir -p "$HF_HUB_CACHE" "$TRANSFORMERS_CACHE" "$HF_DATASETS_CACHE"

SMTP_PASSWORD_FILE="${SMTP_PASSWORD_FILE:-$REPO_ROOT/config/gm_app_pw.txt}"
if [[ -z "${SMTP_PASSWORD:-}" && -f "${SMTP_PASSWORD_FILE}" ]]; then
  SMTP_PASSWORD="$(tr -d '\r\n' < "${SMTP_PASSWORD_FILE}")"
  export SMTP_PASSWORD
fi
export SMTP_HOST="${SMTP_HOST:-smtp.gmail.com}"
export SMTP_PORT="${SMTP_PORT:-587}"
export SMTP_STARTTLS="${SMTP_STARTTLS:-1}"
export NOTIFY_EMAIL="${NOTIFY_EMAIL:-jon.skyclad@gmail.com}"
export NOTIFY_FROM="${NOTIFY_FROM:-${SMTP_USER:-${NOTIFY_EMAIL}}}"
export SMTP_USER="${SMTP_USER:-${NOTIFY_FROM}}"

run_cmd() {
  local label="$1"
  shift
  set +e
  "$@" 2>&1 | tee -a "${RUN_LOG}"
  local status=${PIPESTATUS[0]}
  set -e
  if [[ $status -ne 0 ]]; then
    echo "[${timestamp}] Warning: ${label} failed (exit ${status})." | tee -a "${RUN_LOG}"
  fi
  return $status
}

format_step_status() {
  local status="$1"
  if [[ "$status" -eq 0 ]]; then
    echo "success (exit 0)"
  else
    echo "failure (exit ${status})"
  fi
}

timestamp="$(date -u +"%Y-%m-%dT%H:%M:%SZ")"
TRIPLET_EXTRA_ARGS="${TRIPLET_EXTRA_ARGS:-}"
LATEST_REPORT_FILE=""
LATEST_TRIPLET_FILE=""
INGEST_REPORTS_WRITTEN="n/a"
TRIPLET_FILE_ROWS="n/a"
TRIPLET_ARTICLES_PROCESSED="n/a"
TRIPLET_TRIPLETS_EXTRACTED="n/a"
DEATHS_TOTAL="n/a"
DEATHS_DETENTION="n/a"
DEATHS_STREET="n/a"
if [[ "${PRECACHE_MODELS:-0}" = "1" ]]; then
  echo "[${timestamp}] Pre-caching HF models (best effort)..." | tee -a "${RUN_LOG}"
  run_cmd "precache" python3 scripts/precache_models.py --best-effort || true
fi

echo "[${timestamp}] Verifying model caches..." | tee -a "${RUN_LOG}"
TRIPLET_MODEL_ID="${TRIPLET_MODEL_ID:-microsoft/Phi-3-mini-128k-instruct}"
DEATH_MODEL_ID="${DEATH_LLM_MODEL_ID:-Qwen/Qwen2.5-7B-Instruct}"
TRIPLET_CACHE_OK=0
DEATH_CACHE_OK=0
if run_cmd "verify triplet model cache" python3 scripts/precache_models.py --verify --local-only --model-id "${TRIPLET_MODEL_ID}"; then
  TRIPLET_CACHE_OK=1
fi
if run_cmd "verify death model cache" python3 scripts/precache_models.py --verify --local-only --model-id "${DEATH_MODEL_ID}"; then
  DEATH_CACHE_OK=1
fi

echo "[${timestamp}] Starting ingest..." | tee -a "${RUN_LOG}"
run_cmd "ingest" python3 scripts/ingest_news.py \
  --output-dir datasets/news_ingest \
  --fetch-content \
  --resolve-google-news \
  --log-level INFO
INGEST_STATUS=$?
LATEST_REPORT_FILE="$(ls -1t datasets/news_ingest/news_reports_*.jsonl 2>/dev/null | head -n 1 || true)"
if [[ -n "${LATEST_REPORT_FILE}" && -f "${LATEST_REPORT_FILE}" ]]; then
  INGEST_REPORTS_WRITTEN="$(wc -l < "${LATEST_REPORT_FILE}" | tr -d ' ')"
fi

echo "[${timestamp}] Starting triplet extraction..." | tee -a "${RUN_LOG}"
TRIPLET_MODEL_CACHE="${HF_HUB_CACHE}/models--${TRIPLET_MODEL_ID//\//--}"
TRIPLET_STATUS=0
if [[ $TRIPLET_CACHE_OK -eq 1 ]]; then
  run_cmd "triplet extraction" env HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 \
    python3 scripts/extract_triplets.py \
      --input-dir datasets/news_ingest \
      --output-dir datasets/news_ingest \
      --model-id "${TRIPLET_MODEL_ID}" \
      --log-level INFO \
      ${TRIPLET_EXTRA_ARGS}
  TRIPLET_STATUS=$?
else
  TRIPLET_STATUS=3
  echo "[${timestamp}] Skipping triplet extraction: model cache missing." | tee -a "${RUN_LOG}"
fi
LATEST_TRIPLET_FILE="$(ls -1t datasets/news_ingest/triplets_*.jsonl 2>/dev/null | head -n 1 || true)"
if [[ -n "${LATEST_TRIPLET_FILE}" && -f "${LATEST_TRIPLET_FILE}" ]]; then
  TRIPLET_FILE_ROWS="$(wc -l < "${LATEST_TRIPLET_FILE}" | tr -d ' ')"
fi
if [[ $TRIPLET_STATUS -eq 0 && -f "datasets/news_ingest/triplets_index.sqlite" ]]; then
  TRIPLET_RUN_STATS="$(sqlite3 -csv datasets/news_ingest/triplets_index.sqlite \
    "select articles_processed,triplets_extracted from runs order by started_at desc limit 1;" 2>/dev/null || true)"
  if [[ -n "${TRIPLET_RUN_STATS}" ]]; then
    IFS=',' read -r _triplet_articles _triplet_triplets <<< "${TRIPLET_RUN_STATS}"
    TRIPLET_ARTICLES_PROCESSED="${_triplet_articles:-n/a}"
    TRIPLET_TRIPLETS_EXTRACTED="${_triplet_triplets:-n/a}"
  fi
fi

echo "[${timestamp}] Exporting triplet slices..." | tee -a "${RUN_LOG}"
run_cmd "export triplet slices" bash scripts/export_triplets_static.sh
EXPORT_STATUS=$?

echo "[${timestamp}] Updating deaths.jsonl..." | tee -a "${RUN_LOG}"
DEATH_REPORT_URL_FILE="${REPO_ROOT}/config/death_report_urls.txt"
DEATH_REPORT_ARGS=(
  --out "${REPO_ROOT}/site/data"
  --ice-report-include-index
  --ice-report-use-playwright
  --ice-report-llm-location-enrich
  --ice-report-llm-model-id "${DEATH_MODEL_ID}"
  --include-triplets
  --triplet-article-text
)
if [[ -f "${DEATH_REPORT_URL_FILE}" ]]; then
  DEATH_REPORT_ARGS+=(--ice-report-url-file "${DEATH_REPORT_URL_FILE}")
fi
DEATH_MODEL_CACHE="${HF_HUB_CACHE}/models--${DEATH_MODEL_ID//\//--}"
DEATHS_STATUS=0
if [[ $DEATH_CACHE_OK -eq 1 ]]; then
  DEATH_REPORT_ARGS+=(--triplet-llm-enrich)
  run_cmd "deaths update" env HF_HUB_OFFLINE=1 TRANSFORMERS_OFFLINE=1 \
    python3 scripts/run_daily.py "${DEATH_REPORT_ARGS[@]}"
  DEATHS_STATUS=$?
else
  echo "[${timestamp}] Running deaths update without LLM enrichment (model cache missing)." | tee -a "${RUN_LOG}"
  run_cmd "deaths update" python3 scripts/run_daily.py "${DEATH_REPORT_ARGS[@]}"
  DEATHS_STATUS=$?
fi

if [[ -f "${REPO_ROOT}/site/data/index.json" ]]; then
  DEATH_COUNTS="$(python3 -c 'import json, pathlib; d = json.loads(pathlib.Path("site/data/index.json").read_text(encoding="utf-8")); c = d.get("counts", {}) if isinstance(d, dict) else {}; ctx = c.get("context", {}) if isinstance(c, dict) else {}; det = ctx.get("detention", "n/a"); st = ctx.get("street", "n/a"); total = c.get("total", (det + st) if isinstance(det, int) and isinstance(st, int) else "n/a"); print("{}|{}|{}".format(total, det, st))' 2>/dev/null || true)"
  if [[ -n "${DEATH_COUNTS}" ]]; then
    IFS='|' read -r DEATHS_TOTAL DEATHS_DETENTION DEATHS_STREET <<< "${DEATH_COUNTS}"
  fi
fi

UPLOAD_STATUS=0
if [[ $TRIPLET_STATUS -ne 0 ]]; then
  UPLOAD_STATUS=2
  echo "[${timestamp}] Skipping upload: triplet extraction failed." | tee -a "${RUN_LOG}"
else
  echo "[${timestamp}] Building + uploading frontend..." | tee -a "${RUN_LOG}"
  run_cmd "frontend upload" bash "${REPO_ROOT}/frontend/push_data_and_redirect.sh"
  UPLOAD_STATUS=$?
fi

OVERALL_STATUS="SUCCESS"
if [[ $INGEST_STATUS -ne 0 || $TRIPLET_STATUS -ne 0 || $EXPORT_STATUS -ne 0 || $DEATHS_STATUS -ne 0 || $UPLOAD_STATUS -ne 0 ]]; then
  OVERALL_STATUS="FAILURE"
fi

SUMMARY_FILE="$(mktemp)"
{
  echo "ICEActivity run summary"
  echo "Timestamp: ${timestamp}"
  echo "Overall: ${OVERALL_STATUS}"
  echo ""
  echo "Steps:"
  echo "  ingest: $(format_step_status "${INGEST_STATUS}")"
  echo "  triplet_extraction: $(format_step_status "${TRIPLET_STATUS}")"
  echo "  export_triplet_slices: $(format_step_status "${EXPORT_STATUS}")"
  echo "  deaths_update: $(format_step_status "${DEATHS_STATUS}")"
  if [[ $UPLOAD_STATUS -eq 2 ]]; then
    echo "  frontend_upload: skipped (triplets failed)"
  else
    echo "  frontend_upload: $(format_step_status "${UPLOAD_STATUS}")"
  fi
  echo ""
  echo "Data volumes:"
  echo "  ingest_reports_written: ${INGEST_REPORTS_WRITTEN}"
  if [[ -n "${LATEST_REPORT_FILE}" ]]; then
    echo "  ingest_output_file: ${LATEST_REPORT_FILE}"
  fi
  echo "  triplet_articles_processed: ${TRIPLET_ARTICLES_PROCESSED}"
  echo "  triplets_extracted: ${TRIPLET_TRIPLETS_EXTRACTED}"
  echo "  triplet_file_rows: ${TRIPLET_FILE_ROWS}"
  if [[ -n "${LATEST_TRIPLET_FILE}" ]]; then
    echo "  triplet_output_file: ${LATEST_TRIPLET_FILE}"
  fi
  echo "  deaths_total_reported: ${DEATHS_TOTAL}"
  echo "  deaths_detention: ${DEATHS_DETENTION}"
  echo "  deaths_street: ${DEATHS_STREET}"
  echo ""
  echo "Recent log tail:"
  tail -n 80 "${RUN_LOG}"
} > "${SUMMARY_FILE}"

NOTIFY_EMAIL="${NOTIFY_EMAIL:-jon.skyclad@gmail.com}"
python3 scripts/notify_run.py \
  --to "${NOTIFY_EMAIL}" \
  --subject "ICEActivity ${OVERALL_STATUS} ${timestamp}" \
  --body-file "${SUMMARY_FILE}" || true
rm -f "${SUMMARY_FILE}"

echo "[${timestamp}] Run complete." | tee -a "${RUN_LOG}"
