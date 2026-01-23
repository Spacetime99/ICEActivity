#!/usr/bin/env bash
set -euo pipefail

timestamp="$(date +%Y%m%d_%H%M%S)"
log_path="tmp/triplet_run_${timestamp}.log"
report_path="tmp/triplet_report_${timestamp}.txt"

mkdir -p tmp

bash scripts/rerun_triplets_all.sh | tee "${log_path}"
python3 scripts/report_triplet_summaries.py "${log_path}" --out "${report_path}"

echo "Log: ${log_path}"
echo "Report: ${report_path}"
