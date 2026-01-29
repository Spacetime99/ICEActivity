#!/usr/bin/env python3
"""
Pretty-print ICE detainee death reports JSONL.

Usage:
    python3 scripts/print_death_reports.py --input ./site/data/ice_death_reports.jsonl
"""

from __future__ import annotations

import argparse
import json
import textwrap
from pathlib import Path
from typing import Any


def format_value(value: Any, placeholder: str = "unknown") -> str:
    if value is None:
        return placeholder
    if isinstance(value, list):
        return ", ".join(str(item) for item in value) if value else placeholder
    text = str(value).strip()
    return text if text else placeholder


def wrap(text: str, indent: int = 4) -> str:
    return textwrap.fill(text, width=88, subsequent_indent=" " * indent)


def print_record(record: dict[str, Any], index: int, total: int) -> None:
    print(f"Record {index}/{total}")
    print(f"ID: {format_value(record.get('id'))}")
    print(f"Name: {format_value(record.get('person_name'))}")
    print(f"Name (raw): {format_value(record.get('name_raw'))}")
    print(f"Date of birth: {format_value(record.get('date_of_birth'))}")
    print(f"Date of death: {format_value(record.get('date_of_death'))}")
    print(f"Age: {format_value(record.get('age'))}")
    print(f"Gender: {format_value(record.get('gender'))}")
    print(f"Citizenship: {format_value(record.get('country_of_citizenship'))}")
    print(f"Facility: {format_value(record.get('facility_or_location'))}")
    print(f"Context: {format_value(record.get('death_context'))}")
    print(f"Custody status: {format_value(record.get('custody_status'))}")
    print(f"Agency: {format_value(record.get('agency'))}")
    print(f"Source type: {format_value(record.get('source_type'))}")
    print(f"Extracted at: {format_value(record.get('extracted_at'))}")
    print(f"Updated at: {format_value(record.get('updated_at'))}")
    print("Report URLs:")
    urls = record.get("report_urls") or []
    if not urls:
        print("  none")
    for url in urls:
        print(f"  - {url}")
    print("-" * 88)


def main() -> int:
    parser = argparse.ArgumentParser(description="Pretty-print ICE death reports JSONL.")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("./site/data/ice_death_reports.jsonl"),
        help="Path to ice_death_reports.jsonl (default: ./site/data/ice_death_reports.jsonl).",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Optional limit on number of records printed (default: all).",
    )
    parser.add_argument(
        "--filter",
        type=str,
        default="",
        help="Optional case-insensitive filter applied across record text.",
    )
    args = parser.parse_args()

    if not args.input.exists():
        raise SystemExit(f"Missing input file: {args.input}")

    with args.input.open("r", encoding="utf-8") as handle:
        records = [json.loads(line) for line in handle if line.strip()]

    if args.filter:
        needle = args.filter.lower()
        filtered = []
        for record in records:
            blob = json.dumps(record, ensure_ascii=True).lower()
            if needle in blob:
                filtered.append(record)
        records = filtered

    if args.limit and args.limit > 0:
        records = records[: args.limit]

    total = len(records)
    for idx, record in enumerate(records, start=1):
        print_record(record, idx, total)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
