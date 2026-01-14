#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

python3 - <<'PY'
import sqlite3

DB_PATH = "datasets/news_ingest/triplets_index.sqlite"
STORY_ID = "https://slashdot.org/firehose.pl?op=view&amp;id=180555658"

conn = sqlite3.connect(DB_PATH)
with conn:
    cursor = conn.execute(
        """
        DELETE FROM triplets
        WHERE story_id = ?
          AND (
            LOWER(what) LIKE '%killed an ice%'
            OR LOWER(what) LIKE '%shot an ice%'
            OR LOWER(what) LIKE '%was killed by renee%'
            OR LOWER(what) LIKE '%was shot by renee%'
          )
        """,
        (STORY_ID,),
    )
    deleted = cursor.rowcount

print(f"deleted_rows={deleted}")
conn.close()
PY
