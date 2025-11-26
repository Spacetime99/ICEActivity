"""
One-off script to backfill latitude/longitude for ICE facilities using Google Geocoding API.
Reads assets/ice_facilities.csv and only geocodes rows missing coordinates.
Requires GOOGLE_ACC_KEY in the environment.
"""

from __future__ import annotations

import csv
import os
import sys
import time
from pathlib import Path

import requests


def geocode(query: str, api_key: str, session: requests.Session) -> tuple[str, str] | tuple[None, None]:
    url = "https://maps.googleapis.com/maps/api/geocode/json"
    resp = session.get(url, params={"address": query, "key": api_key}, timeout=15)
    resp.raise_for_status()
    data = resp.json()
    if data.get("status") != "OK" or not data.get("results"):
        return None, None
    loc = data["results"][0]["geometry"]["location"]
    return str(loc.get("lat")), str(loc.get("lng"))


def main() -> int:
    api_key = os.getenv("GOOGLE_ACC_KEY")
    if not api_key:
        print("GOOGLE_ACC_KEY not set in environment", file=sys.stderr)
        return 1

    src = Path("assets/ice_facilities.csv")
    if not src.exists():
        print(f"File not found: {src}", file=sys.stderr)
        return 1

    with src.open(newline="", encoding="utf-8") as f:
        rows = list(csv.DictReader(f))

    session = requests.Session()
    updated = 0
    missing = 0
    for idx, row in enumerate(rows, 1):
        if row.get("latitude") and row.get("longitude"):
            continue
        query = f"{row['address']}, {row['city']}, {row['state']}, United States"
        lat, lon = geocode(query, api_key, session)
        if lat and lon:
            row["latitude"] = lat
            row["longitude"] = lon
            updated += 1
            print(f"[{idx}/{len(rows)}] OK {row['name']}: {lat},{lon}")
        else:
            missing += 1
            print(f"[{idx}/{len(rows)}] no result {row['name']}")
        time.sleep(0.2)  # be nice to the API

    with src.open("w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["name", "address", "city", "state", "latitude", "longitude"])
        writer.writeheader()
        writer.writerows(rows)

    print(f"Updated {updated} rows; {missing} still missing.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
