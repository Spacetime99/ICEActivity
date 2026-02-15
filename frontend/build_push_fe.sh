#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

REPO_ROOT="$(cd "${SCRIPT_DIR}/.." && pwd)"
SITE_DATA_DIR="${SITE_DATA_DIR:-${REPO_ROOT}/site/data}"
FRONTEND_DATA_DIR="${SCRIPT_DIR}/public/data"

# Ensure frontend build includes latest deaths artifacts.
if [ -f "${SITE_DATA_DIR}/deaths.jsonl" ]; then
  mkdir -p "${FRONTEND_DATA_DIR}"
  cp "${SITE_DATA_DIR}/deaths.jsonl" "${FRONTEND_DATA_DIR}/deaths.jsonl"
fi
if [ ! -f "${SITE_DATA_DIR}/deaths.json" ] && [ -f "${SITE_DATA_DIR}/deaths.jsonl" ]; then
  python3 -c 'import json,pathlib; p=pathlib.Path("site/data/deaths.jsonl"); o=pathlib.Path("site/data/deaths.json"); o.write_text(json.dumps([json.loads(x) for x in p.read_text(encoding="utf-8").splitlines() if x.strip()], ensure_ascii=True, indent=2)+"\n", encoding="utf-8")'
fi
if [ -f "${SITE_DATA_DIR}/deaths.json" ]; then
  mkdir -p "${FRONTEND_DATA_DIR}"
  cp "${SITE_DATA_DIR}/deaths.json" "${FRONTEND_DATA_DIR}/deaths.json"
fi
if [ -f "${SITE_DATA_DIR}/index.json" ]; then
  mkdir -p "${FRONTEND_DATA_DIR}"
  cp "${SITE_DATA_DIR}/index.json" "${FRONTEND_DATA_DIR}/index.json"
fi

npm run build
SFTP_HOST="${SFTP_HOST:-www315.your-server.de}"
SFTP_USER="${SFTP_USER:-efsxfy}"
REMOTE_ROOT="${REMOTE_ROOT:-public_html}"
REMOTE_DIR="${REMOTE_DIR:-${REMOTE_ROOT}/ice}"
SFTP_KEY="${SFTP_KEY:-$HOME/.ssh/hetzner_sftp}"
REDIRECT_ROOT="${REDIRECT_ROOT:-1}"
HTACCESS_PATH="${HTACCESS_PATH:-}"
ALLOW_ROOT_ROBOTS="${ALLOW_ROOT_ROBOTS:-1}"
ROOT_ROBOTS_PATH="${ROOT_ROBOTS_PATH:-}"

SFTP_ARGS=()
if [ -f "$SFTP_KEY" ]; then
  SFTP_ARGS=(-i "$SFTP_KEY")
fi

TMP_HTACCESS=""
if [ "$REDIRECT_ROOT" = "1" ] && [ -z "$HTACCESS_PATH" ]; then
  TMP_HTACCESS="$(mktemp)"
  cat > "$TMP_HTACCESS" <<'EOF'
RewriteEngine On

# 0) Force root and default index files to /ice/
RedirectMatch 301 ^/$ /ice/
RedirectMatch 301 ^/index\.html?$ /ice/

# 1) Canonical host: www -> apex
RewriteCond %{HTTP_HOST} ^www\.icemap\.org$ [NC]
RewriteRule ^ https://icemap.org%{REQUEST_URI} [R=301,L]

# 2) Canonical scheme: http -> https
RewriteCond %{HTTPS} !=on
RewriteRule ^ https://%{HTTP_HOST}%{REQUEST_URI} [R=301,L]

# 3) Friendly redirects for old about/legal URLs
RewriteRule ^about/?$ /ice/#about [R=301,L]
RewriteRule ^es/about/?$ /ice/#about [R=301,L]
RewriteRule ^privacy/?$ /ice/#about [R=301,L]
RewriteRule ^legal/?$ /ice/#about [R=301,L]
RewriteRule ^ice/about/?$ /ice/#about [R=301,L]
RewriteRule ^ice/es/about/?$ /ice/#about [R=301,L]
RewriteRule ^ice/methodology/?$ /ice/#methodology [R=301,L]
RewriteRule ^ice/es/methodology/?$ /ice/#methodology [R=301,L]
RewriteRule ^ice/charts/?$ /ice/charts.html [R=301,L]

# 4) Retire legacy/spam paths (410 Gone)
RewriteRule ^(category|tag)(/|$) - [G,L]
RewriteRule ^(wp-admin|wp-content|wp-includes|wp-json)(/|$) - [G,L]
RewriteRule ^xmlrpc\.php$ - [G,L]
RewriteRule ^(rc|rs)[^/]*\.htm$ - [G,L]
RewriteRule ^news-id\d+\.htm$ - [G,L]
RewriteRule ^mercedes-benz-.*$ - [G,L]

# 5) Canonical path: only redirect the bare root "/" -> "/ice/"
RewriteCond %{THE_REQUEST} \s/+HTTP/ [NC]
RewriteRule ^$ ice/ [R=301,L]
EOF
  HTACCESS_PATH="$TMP_HTACCESS"
fi

TMP_ROBOTS=""
if [ "$ALLOW_ROOT_ROBOTS" = "1" ] && [ -z "$ROOT_ROBOTS_PATH" ]; then
  TMP_ROBOTS="$(mktemp)"
  cat > "$TMP_ROBOTS" <<'EOF'
User-agent: *
Allow: /
Sitemap: https://icemap.org/ice/sitemap.xml
EOF
  ROOT_ROBOTS_PATH="$TMP_ROBOTS"
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
    echo "chmod 644 ${REMOTE_ROOT}/.htaccess"
  fi
  if [ "$ALLOW_ROOT_ROBOTS" = "1" ] && [ -n "$ROOT_ROBOTS_PATH" ]; then
    echo "put ${ROOT_ROBOTS_PATH} ${REMOTE_ROOT}/robots.txt"
    echo "chmod 644 ${REMOTE_ROOT}/robots.txt"
  fi
} > "$SFTP_BATCH"

sftp "${SFTP_ARGS[@]}" -b "$SFTP_BATCH" "${SFTP_USER}@${SFTP_HOST}"
rm -f "$SFTP_BATCH"
if [ -n "$TMP_HTACCESS" ]; then
  rm -f "$TMP_HTACCESS"
fi
if [ -n "$TMP_ROBOTS" ]; then
  rm -f "$TMP_ROBOTS"
fi
