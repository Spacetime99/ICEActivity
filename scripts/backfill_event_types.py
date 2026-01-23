#!/usr/bin/env python3
"""Populate event_types for existing triplets rows using title/what/who."""

from __future__ import annotations

import argparse
import json
import sqlite3
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.news_triplets import _detect_event_types  # type: ignore  # noqa: E402


def _build_blob(title: str | None, who: str | None, what: str | None) -> str:
    return " ".join(part for part in (title, what, who) if part).strip()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Backfill event_types for triplets that are missing them."
    )
    parser.add_argument(
        "--db",
        type=Path,
        default=Path("datasets/news_ingest/triplets_index.sqlite"),
        help="Path to triplets SQLite DB.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report how many rows would update without writing changes.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Optional row limit for debugging.",
    )
    args = parser.parse_args()

    if not args.db.exists():
        raise SystemExit(f"Triplets DB not found: {args.db}")

    conn = sqlite3.connect(args.db)
    conn.row_factory = sqlite3.Row
    cursor = conn.cursor()

    query = """
        SELECT rowid, title, who, what, event_types
        FROM triplets
        WHERE event_types IS NULL
           OR event_types = ''
           OR event_types = '[]'
    """
    if args.limit:
        query += " LIMIT ?"
        cursor.execute(query, (args.limit,))
    else:
        cursor.execute(query)

    rows = cursor.fetchall()
    updates: list[tuple[str, int]] = []
    for row in rows:
        blob = _build_blob(row["title"], row["who"], row["what"])
        event_types = _detect_event_types(blob)
        if event_types:
            updates.append((json.dumps(event_types), row["rowid"]))

    print(f"Scanned {len(rows)} rows.")
    print(f"Found {len(updates)} rows with event types.")
    if args.dry_run or not updates:
        conn.close()
        return 0

    with conn:
        conn.executemany(
            "UPDATE triplets SET event_types = ? WHERE rowid = ?",
            updates,
        )
    conn.close()
    print("Backfill complete.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
