#!/usr/bin/env bash
set -euo pipefail

REPO_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$REPO_ROOT"

python3 - <<'PY'
import re
import sqlite3
from datetime import datetime

DB_PATH = "datasets/news_ingest/triplets_index.sqlite"

WHO_SYNONYM_PATTERNS = {
    "ice agents": [
        "ice",
        "ice agents",
        "ice agent",
        "immigration and customs enforcement",
        "immigration and customs enforcement agents",
        "immigration and customs enforcement agent",
        "immigration agents",
        "immigration agent",
        "federal immigration agents",
        "federal immigration agent",
        "immigration authorities",
        "immigration officers",
    ],
    "ice agent": [
        "ice agent",
        "immigration and customs enforcement agent",
        "immigration agent",
        "federal immigration agent",
    ],
}

CANONICAL_WHO_DISPLAY = {
    "ice agents": "ICE agents",
    "ice agent": "ICE agent",
}


def normalize_who(value: str | None) -> str | None:
    if not value:
        return value
    normalized = value.strip()
    if not normalized:
        return value
    lower = normalized.lower()
    for canonical, aliases in WHO_SYNONYM_PATTERNS.items():
        if lower == canonical or lower in aliases:
            return CANONICAL_WHO_DISPLAY.get(canonical, canonical)
    return normalized


conn = sqlite3.connect(DB_PATH)
conn.row_factory = sqlite3.Row
rows = conn.execute("SELECT * FROM triplets").fetchall()

# Deduplicate on normalized key, prefer the newest extracted_at if available.
bucket = {}
for row in rows:
    row = dict(row)
    row["who"] = normalize_who(row.get("who")) or row.get("who")
    key = (row.get("story_id"), row.get("who"), row.get("what"), row.get("where_text"))
    existing = bucket.get(key)
    if not existing:
        bucket[key] = row
        continue
    # Prefer newer extracted_at if present
    try:
        existing_ts = datetime.fromisoformat(existing.get("extracted_at")) if existing.get("extracted_at") else None
        row_ts = datetime.fromisoformat(row.get("extracted_at")) if row.get("extracted_at") else None
    except Exception:
        existing_ts = None
        row_ts = None
    if row_ts and (not existing_ts or row_ts > existing_ts):
        bucket[key] = row

# Replace table contents
with conn:
    conn.execute("DELETE FROM triplets")
    conn.executemany(
        """
        INSERT OR REPLACE INTO triplets (
            story_id, source, url, title, published_at,
            who, what, where_text, latitude, longitude,
            geocode_query, geocode_status, extracted_at, run_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        [
            (
                row.get("story_id"),
                row.get("source"),
                row.get("url"),
                row.get("title"),
                row.get("published_at"),
                row.get("who"),
                row.get("what"),
                row.get("where_text"),
                row.get("latitude"),
                row.get("longitude"),
                row.get("geocode_query"),
                row.get("geocode_status"),
                row.get("extracted_at"),
                row.get("run_id"),
            )
            for row in bucket.values()
        ],
    )

print(f"normalized_rows={len(rows)} deduped_rows={len(bucket)}")
conn.close()
PY
