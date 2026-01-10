#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
PORT="${PORT:-5000}"
HOST="${HOST:-127.0.0.1}"

if [[ -z "${VIRTUAL_ENV:-}" ]]; then
  # shellcheck source=/dev/null
  source "${REPO_ROOT}/.venv/bin/activate"
fi

cd "${REPO_ROOT}"
exec uvicorn src.api.main:app --host "${HOST}" --port "${PORT}"
