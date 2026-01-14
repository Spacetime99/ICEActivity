#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

python3 - <<'PY'
import sqlite3

DB_PATH = "datasets/news_ingest/triplets_index.sqlite"
INCOMPLETE_ACTIONS = {
    "shot",
    "shot and killed",
    "killed",
    "arrested",
    "detained",
    "injured",
    "assaulted",
    "raided",
}

conn = sqlite3.connect(DB_PATH)
with conn:
    placeholders = ",".join("?" for _ in INCOMPLETE_ACTIONS)
    params = [item.lower() for item in INCOMPLETE_ACTIONS]
    cursor = conn.execute(
        f"""
        DELETE FROM triplets
        WHERE LOWER(TRIM(what)) IN ({placeholders})
        """,
        params,
    )
    deleted = cursor.rowcount

print(f"deleted_rows={deleted}")
conn.close()
PY
