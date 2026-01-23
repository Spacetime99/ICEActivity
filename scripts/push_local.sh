#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
FRONTEND_DIR="${FRONTEND_DIR:-${REPO_ROOT}/frontend}"

LOCAL_WEB_ROOT="${LOCAL_WEB_ROOT:-/var/www/ice-map}"
LOCAL_WEB_DIR="${LOCAL_WEB_DIR:-${LOCAL_WEB_ROOT}}"
LOCAL_DATA_DIR="${LOCAL_DATA_DIR:-${LOCAL_WEB_ROOT}/data}"
SUDO="${SUDO:-sudo}"

cd "$FRONTEND_DIR"
npm run build

if [ ! -d "dist" ]; then
  echo "Build output not found: ${FRONTEND_DIR}/dist" >&2
  exit 1
fi

"$SUDO" mkdir -p "$LOCAL_WEB_DIR" "$LOCAL_DATA_DIR"
"$SUDO" rsync -av --delete "${FRONTEND_DIR}/dist/" "${LOCAL_WEB_DIR}/"
"$SUDO" rsync -av --delete "${FRONTEND_DIR}/public/data/" "${LOCAL_DATA_DIR}/"
