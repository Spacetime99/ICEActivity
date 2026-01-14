#!/usr/bin/env python3
"""Build a monthly encounters CSV from CBP nationwide encounter tables."""

from __future__ import annotations

import csv
import re
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Dict, Iterable, Tuple


DATA_DIR = Path("assets/data")
OUTPUT_PATH = DATA_DIR / "encounters_monthly.csv"

MONTH_TO_NUM = {
    "JAN": 1,
    "FEB": 2,
    "MAR": 3,
    "APR": 4,
    "MAY": 5,
    "JUN": 6,
    "JUL": 7,
    "AUG": 8,
    "SEP": 9,
    "OCT": 10,
    "NOV": 11,
    "DEC": 12,
}

PRESIDENCY_BOUNDARIES = [
    (date(2025, 1, 20), "Trump 2"),
    (date(2021, 1, 20), "Biden"),
    (date(2017, 1, 20), "Trump 1"),
]


@dataclass(frozen=True)
class EncounterKey:
    fiscal_year: int
    month_abbr: str


def parse_fiscal_year(value: str) -> int:
    match = re.search(r"(20\d{2})", value or "")
    if not match:
        raise ValueError(f"Unable to parse fiscal year from '{value}'")
    return int(match.group(1))


def month_to_date(fiscal_year: int, month_abbr: str) -> date:
    month_num = MONTH_TO_NUM[month_abbr.upper()]
    year = fiscal_year - 1 if month_num >= 10 else fiscal_year
    return date(year, month_num, 1)


def classify_presidency(month_date: date) -> str:
    for boundary, label in PRESIDENCY_BOUNDARIES:
        if month_date >= boundary:
            return label
    return "Obama"


def parse_int(value: str) -> int:
    cleaned = (value or "").replace(",", "").strip()
    return int(cleaned) if cleaned else 0


def read_encounters(path: Path) -> Dict[EncounterKey, int]:
    totals: Dict[EncounterKey, int] = {}
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            fiscal_year = parse_fiscal_year(row.get("Fiscal Year", ""))
            month_abbr = (row.get("Month (abbv)") or "").strip().upper()
            if not month_abbr:
                continue
            key = EncounterKey(fiscal_year=fiscal_year, month_abbr=month_abbr)
            totals[key] = totals.get(key, 0) + parse_int(row.get("Encounter Count", "0"))
    return totals


def merge_encounters(
    primary: Dict[EncounterKey, int],
    fallback: Dict[EncounterKey, int],
) -> Dict[EncounterKey, int]:
    merged = dict(fallback)
    merged.update(primary)
    return merged


def write_csv(rows: Iterable[dict]) -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = [
        "month",
        "fiscal_year",
        "month_abbr",
        "encounters",
        "presidency",
    ]
    with OUTPUT_PATH.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        for row in rows:
            writer.writerow(row)


def main() -> None:
    primary_path = DATA_DIR / "nationwide-encounters-fy23-fy26-nov-state.csv"
    fallback_path = DATA_DIR / "nationwide-encounters-fy22-fy25-state.csv"
    primary = read_encounters(primary_path) if primary_path.exists() else {}
    fallback = read_encounters(fallback_path) if fallback_path.exists() else {}
    merged = merge_encounters(primary, fallback)
    rows = []
    for key, total in sorted(merged.items(), key=lambda item: (item[0].fiscal_year, item[0].month_abbr)):
        month_date = month_to_date(key.fiscal_year, key.month_abbr)
        rows.append(
            {
                "month": month_date.isoformat(),
                "fiscal_year": key.fiscal_year,
                "month_abbr": key.month_abbr,
                "encounters": total,
                "presidency": classify_presidency(month_date),
            }
        )
    rows.sort(key=lambda row: row["month"])
    write_csv(rows)
    print(f"Wrote {len(rows)} rows to {OUTPUT_PATH}")


if __name__ == "__main__":
    main()
