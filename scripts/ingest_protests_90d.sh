#!/usr/bin/env bash
set -euo pipefail

OUTPUT_DIR="${OUTPUT_DIR:-datasets/news_ingest}"
MIN_KEYWORD_MATCHES="${MIN_KEYWORD_MATCHES:-99}"
FETCH_CONTENT="${FETCH_CONTENT:-1}"
FORCE_REFETCH="${FORCE_REFETCH:-0}"
EXPORT_SLICES="${EXPORT_SLICES:-1}"

FROM_DATE="$(date -u -d '90 days ago' +%Y-%m-%d)"
TO_DATE="$(date -u +%Y-%m-%d)"

INGEST_ARGS=(
  --output-dir "${OUTPUT_DIR}"
  --from-date "${FROM_DATE}"
  --to-date "${TO_DATE}"
  --min-keyword-matches "${MIN_KEYWORD_MATCHES}"
)

if [ "${FETCH_CONTENT}" = "1" ]; then
  INGEST_ARGS+=(--fetch-content)
fi

if [ "${FORCE_REFETCH}" = "1" ]; then
  INGEST_ARGS+=(--force-refetch)
fi

echo "Ingesting protest-only news (from ${FROM_DATE} to ${TO_DATE})..."
python3 scripts/ingest_news.py "${INGEST_ARGS[@]}"

echo "Extracting triplets into ${OUTPUT_DIR}..."
python3 scripts/extract_triplets.py \
  --input-dir "${OUTPUT_DIR}" \
  --output-dir "${OUTPUT_DIR}" \
  --allow-protests

if [ "${EXPORT_SLICES}" = "1" ]; then
  echo "Exporting static slices..."
  python3 scripts/export_triplets_static.py
fi

echo "Done."
