#!/usr/bin/env bash
set -euo pipefail

usage() {
  cat <<'EOF'
Usage:
  bash scripts/debug_triplet_batch.sh urls.txt
  bash scripts/debug_triplet_batch.sh -- urls...

Outputs a compact report showing the last "Completing action" line before each "Summary:".
EOF
}

if [[ $# -lt 1 ]]; then
  usage
  exit 1
fi

urls=()
if [[ "$1" == "--" ]]; then
  shift
  urls=("$@")
else
  if [[ ! -f "$1" ]]; then
    echo "Missing URL list file: $1" >&2
    exit 1
  fi
  while IFS= read -r line; do
    [[ -z "$line" ]] && continue
    [[ "$line" =~ ^# ]] && continue
    urls+=("$line")
  done < "$1"
fi

if [[ ${#urls[@]} -eq 0 ]]; then
  echo "No URLs provided." >&2
  exit 1
fi

for url in "${urls[@]}"; do
  echo "==== $url ===="
  tmp="$(mktemp)"
  if bash scripts/debug_triplet_single.sh "$url" >"$tmp" 2>&1; then
    awk '
      /Completing action/ {last=$0}
      /Summary:/ {
        if (last != "") {
          print last
        }
        print $0
        print ""
      }
      END {
        if (NR > 0 && last != "" && !found) {
          # no Summary lines; still show last action for context
        }
      }
    ' "$tmp"
    if ! rg -q "Summary:" "$tmp"; then
      echo "No Summary lines found."
      echo ""
    fi
  else
    echo "Debug failed for $url"
  fi
  rm -f "$tmp"
done
