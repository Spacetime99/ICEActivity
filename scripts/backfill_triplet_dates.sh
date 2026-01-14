#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

python3 - <<'PY'
import re
import sqlite3
from datetime import datetime, timezone

DB_PATH = "datasets/news_ingest/triplets_index.sqlite"
DATE_FROM_URL_PATTERNS = [
    re.compile(r"/(20\d{2})/(0[1-9]|1[0-2])/(0[1-9]|[12]\d|3[01])/(?:[^/]+/)?"),
    re.compile(r"/(20\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])/(?:[^/]+/)?"),
]


def infer(url: str | None) -> str | None:
    if not url:
        return None
    for pattern in DATE_FROM_URL_PATTERNS:
        match = pattern.search(url)
        if not match:
            continue
        year, month, day = match.groups()
        try:
            parsed = datetime(int(year), int(month), int(day), tzinfo=timezone.utc)
        except ValueError:
            continue
        return parsed.isoformat()
    return None

conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
rows = conn.execute(
    "SELECT story_id, url, extracted_at FROM triplets WHERE published_at IS NULL OR published_at = ''"
).fetchall()
updates = []
for row in rows:
    published_at = infer(row["url"])
    if not published_at:
        published_at = row["extracted_at"]
    if published_at:
        updates.append((published_at, row["story_id"]))

with conn:
    conn.executemany(
        "UPDATE triplets SET published_at = ? WHERE story_id = ? AND (published_at IS NULL OR published_at = '')",
        updates,
    )

print(f"updated_rows={len(updates)}")
conn.close()
PY
