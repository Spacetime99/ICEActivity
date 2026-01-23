#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

npm run build
SFTP_HOST="${SFTP_HOST:-www315.your-server.de}"
SFTP_USER="${SFTP_USER:-efsxfy}"
REMOTE_ROOT="${REMOTE_ROOT:-public_html}"
REMOTE_DIR="${REMOTE_DIR:-${REMOTE_ROOT}/ice}"
SFTP_KEY="${SFTP_KEY:-$HOME/.ssh/hetzner_sftp}"
REDIRECT_ROOT="${REDIRECT_ROOT:-1}"
HTACCESS_PATH="${HTACCESS_PATH:-}"

SFTP_ARGS=()
if [ -f "$SFTP_KEY" ]; then
  SFTP_ARGS=(-i "$SFTP_KEY")
fi

TMP_HTACCESS=""
if [ "$REDIRECT_ROOT" = "1" ] && [ -z "$HTACCESS_PATH" ]; then
  TMP_HTACCESS="$(mktemp)"
  cat > "$TMP_HTACCESS" <<'EOF'
RewriteEngine On
RewriteRule ^$ /ice/ [R=301,L]
EOF
  HTACCESS_PATH="$TMP_HTACCESS"
fi

SFTP_BATCH="$(mktemp)"
{
  echo "-mkdir ${REMOTE_ROOT}"
  echo "-mkdir ${REMOTE_DIR}"
  while IFS= read -r dir; do
    rel_dir="${dir#dist}"
    echo "-mkdir ${REMOTE_DIR}${rel_dir}"
  done < <(find dist -type d | sort)
  while IFS= read -r file; do
    rel_file="${file#dist/}"
    echo "put ${file} ${REMOTE_DIR}/${rel_file}"
  done < <(find dist -type f | sort)
  if [ "$REDIRECT_ROOT" = "1" ] && [ -n "$HTACCESS_PATH" ]; then
    echo "put ${HTACCESS_PATH} ${REMOTE_ROOT}/.htaccess"
  fi
} > "$SFTP_BATCH"

sftp "${SFTP_ARGS[@]}" -b "$SFTP_BATCH" "${SFTP_USER}@${SFTP_HOST}"
rm -f "$SFTP_BATCH"
if [ -n "$TMP_HTACCESS" ]; then
  rm -f "$TMP_HTACCESS"
fi
