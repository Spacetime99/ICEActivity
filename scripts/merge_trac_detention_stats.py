#!/usr/bin/env python3
"""Merge TRAC detention facility stats into ice_detention_facilities.csv."""

from __future__ import annotations

import csv
import json
import re
from pathlib import Path
from typing import Any, Dict, Iterable, Optional, Tuple
from urllib.request import urlopen


REPO_ROOT = Path(__file__).resolve().parents[1]
CSV_PATH = REPO_ROOT / "ice_detention_facilities.csv"
DEFAULT_JSON_PATH = REPO_ROOT / "tmp" / "trac_facilities.json"
TRAC_URL = "https://tracreports.org/immigration/detentionstats/facilities.json"

NEW_COLUMNS = [
    "trac_type_detailed",
    "trac_guaranteed_minimum",
    "trac_average_daily_population",
    "trac_as_of",
]


def normalize(text: str) -> str:
    cleaned = re.sub(r"[^a-z0-9]+", " ", text.lower())
    return re.sub(r"\s+", " ", cleaned).strip()


def load_trac_data(path: Path) -> list[dict[str, Any]]:
    if not path.exists():
        path.parent.mkdir(parents=True, exist_ok=True)
        with urlopen(TRAC_URL) as response:
            payload = response.read()
        path.write_bytes(payload)
    with path.open(encoding="utf-8") as handle:
        data = json.load(handle)
    return [row for row in data if row.get("name") and row.get("name") != "Total"]


def index_trac(rows: Iterable[dict[str, Any]]) -> dict[Tuple[str, str], list[dict[str, Any]]]:
    index: dict[Tuple[str, str], list[dict[str, Any]]] = {}
    for row in rows:
        name = normalize(str(row.get("name", "")))
        state = normalize(str(row.get("detention_facility_state", "")))
        city = normalize(str(row.get("detention_facility_city", "")))
        if name and state:
            index.setdefault((name, state), []).append(row)
        if name and state and city:
            index.setdefault((name, f"{state}|{city}"), []).append(row)
    return index


def find_trac_match(
    row: dict[str, str],
    index: dict[Tuple[str, str], list[dict[str, Any]]],
) -> Optional[dict[str, Any]]:
    name = normalize(row.get("name", ""))
    state = normalize(row.get("state", ""))
    city = normalize(row.get("city", ""))
    zip_code = normalize(row.get("postal_code", ""))
    if not name or not state:
        return None
    candidates = index.get((name, f"{state}|{city}"), [])
    if not candidates:
        candidates = index.get((name, state), [])
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]
    if zip_code:
        for candidate in candidates:
            candidate_zip = normalize(str(candidate.get("detention_facility_zip", "")))
            if candidate_zip and candidate_zip == zip_code:
                return candidate
    return candidates[0]


def format_number(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, int):
        return str(value)
    text = str(value).strip()
    text = text.replace(",", "")
    return text if text else ""


def update_row(row: dict[str, str], trac_row: dict[str, Any] | None) -> dict[str, str]:
    for col in NEW_COLUMNS:
        row.setdefault(col, "")
    if not trac_row:
        return row
    row["trac_type_detailed"] = str(trac_row.get("type_detailed", "")).strip()
    row["trac_guaranteed_minimum"] = format_number(trac_row.get("guaranteed_min_num"))
    row["trac_average_daily_population"] = format_number(trac_row.get("count"))
    row["trac_as_of"] = str(trac_row.get("download_date", "")).strip()
    return row


def main() -> None:
    trac_rows = load_trac_data(DEFAULT_JSON_PATH)
    trac_index = index_trac(trac_rows)
    rows: list[dict[str, str]] = []
    with CSV_PATH.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        fieldnames = list(reader.fieldnames or [])
        for col in NEW_COLUMNS:
            if col not in fieldnames:
                fieldnames.append(col)
        for row in reader:
            rows.append(update_row(row, find_trac_match(row, trac_index)))

    with CSV_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)

    print(f"Updated {len(rows)} facilities with TRAC stats.")


if __name__ == "__main__":
    main()
