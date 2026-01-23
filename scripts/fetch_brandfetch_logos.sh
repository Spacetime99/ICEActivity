#!/usr/bin/env bash
set -euo pipefail

BRANDFETCH_API_KEY="${BRANDFETCH_API_KEY:-}"
if [ -z "$BRANDFETCH_API_KEY" ]; then
  echo "Set BRANDFETCH_API_KEY before running." >&2
  exit 1
fi

DOMAINS_FILE="${1:-}"
if [ -z "$DOMAINS_FILE" ] || [ ! -f "$DOMAINS_FILE" ]; then
  echo "Usage: $0 <domains.tsv|domains.txt>" >&2
  exit 1
fi

OUT_DIR="${OUT_DIR:-frontend/public/source-logos}"
MODE="${MODE:-download}"

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required." >&2
  exit 1
fi

mkdir -p "$OUT_DIR"

slugify() {
  local value="$1"
  value="${value#www.}"
  value="$(echo "$value" | tr '[:upper:]' '[:lower:]')"
  value="${value//[^a-z0-9]/_}"
  value="${value##_}"
  value="${value%%_}"
  if [ -z "$value" ]; then
    value="source"
  fi
  echo "$value"
}

pick_logo_url() {
  jq -r '
    .logos[]? as $logo
    | ($logo.type // "") as $type
    | select($type == "logo" or $type == "symbol" or $type == "icon" or $type == "brandmark")
    | $logo.formats[]?
    | select(.format == "svg")
    | .src
  ' | head -n 1
}

pick_png_url() {
  jq -r '
    .logos[]? as $logo
    | ($logo.type // "") as $type
    | select($type == "logo" or $type == "symbol" or $type == "icon" or $type == "brandmark")
    | $logo.formats[]?
    | select(.format == "png")
    | .src
  ' | head -n 1
}

while IFS=$'\t' read -r domain _rest; do
  domain="$(echo "${domain}" | xargs)"
  if [ -z "$domain" ] || [[ "$domain" == \#* ]]; then
    continue
  fi
  slug="$(slugify "$domain")"
  api_url="https://api.brandfetch.io/v2/brands/${domain}"
  json="$(curl -fsSL -H "Authorization: Bearer ${BRANDFETCH_API_KEY}" "$api_url")" || {
    echo "Failed: ${domain}" >&2
    continue
  }
  svg_url="$(echo "$json" | pick_logo_url)"
  if [ -n "$svg_url" ] && [ "$svg_url" != "null" ]; then
    dest="${OUT_DIR}/${slug}.svg"
    if [ "$MODE" = "print" ]; then
      echo "curl -L \"$svg_url\" -o \"$dest\""
    else
      curl -L "$svg_url" -o "$dest"
      echo "saved $dest"
    fi
    continue
  fi
  png_url="$(echo "$json" | pick_png_url)"
  if [ -n "$png_url" ] && [ "$png_url" != "null" ]; then
    dest="${OUT_DIR}/${slug}.png"
    if [ "$MODE" = "print" ]; then
      echo "curl -L \"$png_url\" -o \"$dest\""
    else
      curl -L "$png_url" -o "$dest"
      echo "saved $dest"
    fi
    continue
  fi
  echo "No logo found for ${domain}" >&2
done < "$DOMAINS_FILE"
