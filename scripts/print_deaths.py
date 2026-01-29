#!/usr/bin/env python3
"""
Pretty-print deaths.jsonl to a human readable format.

Usage:
    python3 scripts/print_deaths.py --input ./site/data/deaths.jsonl
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


def format_coords(lat: Any, lon: Any) -> str:
    if lat is None and lon is None:
        return "unknown"
    return f"{lat}, {lon}"


def wrap(text: str, indent: int = 4) -> str:
    return textwrap.fill(text, width=88, subsequent_indent=" " * indent)


def print_record(record: dict[str, Any], index: int, total: int) -> None:
    print(f"Record {index}/{total}")
    if "report_urls" in record and record.get("source_type") == "ice_detainee_death_report":
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
        return

    print(f"ID: {format_value(record.get('id'))}")
    print(f"Name: {format_value(record.get('person_name'))}")
    print(f"Aliases: {format_value(record.get('aliases'))}")
    print(f"Nationality: {format_value(record.get('nationality'))}")
    print(f"Age: {format_value(record.get('age'))}")
    print(f"Gender: {format_value(record.get('gender'))}")
    print(
        f"Date of death: {format_value(record.get('date_of_death'))} "
        f"({format_value(record.get('date_precision'))})",
    )
    print(f"City: {format_value(record.get('city'))}")
    print(f"County: {format_value(record.get('county'))}")
    print(f"State: {format_value(record.get('state'))}")
    print(f"Facility/Location: {format_value(record.get('facility_or_location'))}")
    print(
        "Coords: "
        f"{format_coords(record.get('lat'), record.get('lon'))} "
        f"({format_value(record.get('geocode_source'))})",
    )
    print(f"Context: {format_value(record.get('death_context'))}")
    print(f"Custody status: {format_value(record.get('custody_status'))}")
    print(f"Agency: {format_value(record.get('agency'))}")
    print(f"Contractor involved: {format_value(record.get('contractor_involved'))}")
    print(f"Cause reported: {format_value(record.get('cause_of_death_reported'))}")
    print(f"Manner: {format_value(record.get('manner_of_death'))}")
    print(f"Homicide status: {format_value(record.get('homicide_status'))}")
    summary = format_value(record.get("summary_1_sentence"))
    print("Summary:")
    print(wrap(summary))
    print(f"Primary report URL: {format_value(record.get('primary_report_url'))}")
    print(f"Confidence: {format_value(record.get('confidence_score'))}")
    print(f"Manual review: {format_value(record.get('manual_review'))}")
    print("Sources:")
    sources = record.get("sources") or []
    if not sources:
        print("  none")
    for source in sources:
        print(f"  - URL: {format_value(source.get('url'))}")
        print(f"    Publisher: {format_value(source.get('publisher'))}")
        print(f"    Publish date: {format_value(source.get('publish_date'))}")
        print(f"    Access date: {format_value(source.get('access_date'))}")
        print(f"    Source type: {format_value(source.get('source_type'))}")
        print(f"    Credibility: {format_value(source.get('credibility_tier'))}")
        snippet = format_value(source.get("snippet"))
        print("    Snippet:")
        print(wrap(snippet, indent=6))
        print(f"    Claim tags: {format_value(source.get('claim_tags'))}")
    print("-" * 88)


def main() -> int:
    parser = argparse.ArgumentParser(description="Pretty-print deaths JSONL data.")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("./site/data/deaths.jsonl"),
        help="Path to deaths.jsonl (default: ./site/data/deaths.jsonl).",
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
