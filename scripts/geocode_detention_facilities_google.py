#!/usr/bin/env python3
"""
Backfill latitude/longitude for ice_detention_facilities.csv using the Google Geocoding API.
Requires GOOGLE_ACC_KEY in the environment.
"""

from __future__ import annotations

import csv
import os
import sys
import time
from pathlib import Path
from typing import Iterable

import requests


def geocode(query: str, api_key: str, session: requests.Session) -> tuple[str, str] | tuple[None, None]:
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    resp = session.get(url, params={"address": query, "key": api_key}, timeout=20)
    resp.raise_for_status()
    data = resp.json()
    if data.get("status") != "OK" or not data.get("results"):
        return None, None
    loc = data["results"][0]["geometry"]["location"]
    return str(loc.get("lat")), str(loc.get("lng"))


def ensure_columns(rows: list[dict[str, str]], fieldnames: Iterable[str]) -> list[str]:
    names = list(fieldnames)
    if "latitude" not in names:
        names.append("latitude")
    if "longitude" not in names:
        names.append("longitude")
    for row in rows:
        row.setdefault("latitude", "")
        row.setdefault("longitude", "")
    return names


def main() -> int:
    api_key = os.getenv("GOOGLE_ACC_KEY")
    if not api_key:
        print("GOOGLE_ACC_KEY env var is required", file=sys.stderr)
        return 1

    src = Path("ice_detention_facilities.csv")
    if not src.exists():
        print(f"File not found: {src}", file=sys.stderr)
        return 1

    with src.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        rows = list(reader)
        fieldnames = ensure_columns(rows, reader.fieldnames or [])

    session = requests.Session()
    updated = 0
    skipped = 0
    for idx, row in enumerate(rows, 1):
        if row.get("latitude") and row.get("longitude"):
            skipped += 1
            continue
        query = row.get("address_full") or " ".join(
            part
            for part in [
                row.get("address_line1"),
                row.get("city"),
                row.get("state"),
                row.get("postal_code"),
                row.get("country") or "United States",
            ]
            if part
        )
        if not query.strip():
            print(f"[{idx}/{len(rows)}] missing address for {row.get('name')}", file=sys.stderr)
            continue
        lat, lon = geocode(query, api_key, session)
        if lat and lon:
            row["latitude"] = lat
            row["longitude"] = lon
            updated += 1
            print(f"[{idx}/{len(rows)}] ok {row.get('name')}: {lat},{lon}")
        else:
            print(f"[{idx}/{len(rows)}] failed {row.get('name')} -> {query}", file=sys.stderr)
        time.sleep(0.2)

    with src.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Updated {updated} facilities ({skipped} already had coordinates).")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
