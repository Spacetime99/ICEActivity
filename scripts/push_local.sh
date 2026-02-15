#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FRONTEND_DIR="${FRONTEND_DIR:-${REPO_ROOT}/frontend}"
SITE_DATA_DIR="${SITE_DATA_DIR:-${REPO_ROOT}/site/data}"

LOCAL_WEB_ROOT="${LOCAL_WEB_ROOT:-/var/www/ice-map}"
LOCAL_WEB_DIR="${LOCAL_WEB_DIR:-${LOCAL_WEB_ROOT}}"
LOCAL_DATA_DIR="${LOCAL_DATA_DIR:-${LOCAL_WEB_ROOT}/data}"
SUDO="${SUDO:-sudo}"

# Ensure local deploy includes latest deaths dataset.
if [ -f "${SITE_DATA_DIR}/deaths.jsonl" ]; then
  mkdir -p "${FRONTEND_DIR}/public/data"
  cp "${SITE_DATA_DIR}/deaths.jsonl" "${FRONTEND_DIR}/public/data/deaths.jsonl"
fi
if [ ! -f "${SITE_DATA_DIR}/deaths.json" ] && [ -f "${SITE_DATA_DIR}/deaths.jsonl" ]; then
  python3 -c 'import json,pathlib; p=pathlib.Path("site/data/deaths.jsonl"); o=pathlib.Path("site/data/deaths.json"); o.write_text(json.dumps([json.loads(x) for x in p.read_text(encoding="utf-8").splitlines() if x.strip()], ensure_ascii=True, indent=2)+"\n", encoding="utf-8")'
fi
if [ -f "${SITE_DATA_DIR}/deaths.json" ]; then
  mkdir -p "${FRONTEND_DIR}/public/data"
  cp "${SITE_DATA_DIR}/deaths.json" "${FRONTEND_DIR}/public/data/deaths.json"
fi
if [ -f "${SITE_DATA_DIR}/index.json" ]; then
  mkdir -p "${FRONTEND_DIR}/public/data"
  cp "${SITE_DATA_DIR}/index.json" "${FRONTEND_DIR}/public/data/index.json"
fi

cd "$FRONTEND_DIR"
npm run build

if [ ! -d "dist" ]; then
  echo "Build output not found: ${FRONTEND_DIR}/dist" >&2
  exit 1
fi

"$SUDO" mkdir -p "$LOCAL_WEB_DIR" "$LOCAL_DATA_DIR"
"$SUDO" rsync -av --delete "${FRONTEND_DIR}/dist/" "${LOCAL_WEB_DIR}/"
"$SUDO" rsync -av --delete "${FRONTEND_DIR}/public/data/" "${LOCAL_DATA_DIR}/"
