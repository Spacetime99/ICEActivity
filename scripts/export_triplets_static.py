#!/usr/bin/env python3
"""Export triplets into static JSON slices for the frontend."""

from __future__ import annotations

import argparse
import json
import sqlite3
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path


DEFAULT_DB_PATH = Path("datasets/news_ingest/triplets_index.sqlite")
DEFAULT_OUTPUT_DIR = Path("frontend/public/data")


@dataclass(frozen=True)
class Window:
    label: str
    delta: timedelta | None


WINDOWS = [
    Window("3d", timedelta(days=3)),
    Window("7d", timedelta(days=7)),
    Window("1mo", timedelta(days=30)),
    Window("3mo", timedelta(days=90)),
    Window("all", None),
]


def fetch_rows(conn: sqlite3.Connection, window: Window) -> list[dict]:
    conn.row_factory = sqlite3.Row
    query = """
        SELECT
            story_id AS story_id,
            title AS title,
            who AS who,
            what AS what,
            where_text AS where_text,
            latitude AS lat,
            longitude AS lon,
            url AS url,
            source AS source,
            published_at AS publishedAt,
            event_types AS eventTypes
        FROM triplets
    """
    params: tuple[str, ...] = ()
    if window.delta is not None:
        since = datetime.now(timezone.utc) - window.delta
        query += " WHERE published_at >= ?"
        params = (since.isoformat(),)
    rows = conn.execute(query, params).fetchall()
    payload: list[dict] = []
    for row in rows:
        record = dict(row)
        event_types = record.get("eventTypes")
        if isinstance(event_types, str) and event_types:
            try:
                record["eventTypes"] = json.loads(event_types)
            except json.JSONDecodeError:
                record["eventTypes"] = [
                    part.strip() for part in event_types.split(",") if part.strip()
                ]
        elif event_types is None:
            record["eventTypes"] = []
        payload.append(record)
    return payload


def write_window(output_dir: Path, label: str, payload: list[dict]) -> None:
    output_dir.mkdir(parents=True, exist_ok=True)
    out_path = output_dir / f"triplets_{label}.json"
    with out_path.open("w", encoding="utf-8") as handle:
        json.dump(payload, handle, ensure_ascii=False)


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export triplets into static JSON slices for the frontend."
    )
    parser.add_argument(
        "--db-path",
        type=Path,
        default=DEFAULT_DB_PATH,
        help="Path to triplets_index.sqlite.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory to write triplets_*.json files.",
    )
    args = parser.parse_args()

    if not args.db_path.exists():
        raise SystemExit(f"Triplets DB not found: {args.db_path}")
    with sqlite3.connect(args.db_path) as conn:
        for window in WINDOWS:
            payload = fetch_rows(conn, window)
            write_window(args.output_dir, window.label, payload)
            print(
                f"Wrote {len(payload)} rows to {args.output_dir / f'triplets_{window.label}.json'}"
            )


if __name__ == "__main__":
    main()
