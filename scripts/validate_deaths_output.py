#!/usr/bin/env python3
"""Fail-fast quality gate for generated deaths datasets."""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services import deaths_daily  # noqa: E402


def _load_jsonl(path: Path) -> list[dict]:
    records: list[dict] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            records.append(json.loads(line))
    return records


def _canonical_name(value: str | None) -> str | None:
    return deaths_daily._canonical_person_name(value)  # intentional internal reuse


def _print_group(prefix: str, items: list[str], limit: int = 5) -> None:
    for entry in items[:limit]:
        print(f"{prefix}{entry}")
    extra = len(items) - limit
    if extra > 0:
        print(f"{prefix}... and {extra} more")


def main() -> int:
    parser = argparse.ArgumentParser(description="Validate generated deaths dataset quality.")
    parser.add_argument(
        "--input",
        type=Path,
        default=Path("site/data/deaths.jsonl"),
        help="Path to deaths.jsonl",
    )
    parser.add_argument(
        "--max-unknown-rate",
        type=float,
        default=0.35,
        help="Maximum allowed unknown location rate before failing (0-1).",
    )
    args = parser.parse_args()

    if not args.input.exists():
        print(f"FAIL: dataset not found at {args.input}")
        return 2

    records = _load_jsonl(args.input)
    if not records:
        print("FAIL: dataset is empty")
        return 2

    failures: list[str] = []
    warnings: list[str] = []

    duplicate_groups: dict[tuple[str, str, str], list[str]] = defaultdict(list)
    bad_name_ids: list[str] = []

    unknown_locations = 0
    context_counts: Counter[str] = Counter()

    for record in records:
        record_id = str(record.get("id") or "<missing-id>")
        context = (record.get("death_context") or "unknown").strip().lower()
        context_counts[context] += 1

        canonical = _canonical_name(record.get("person_name"))
        date_of_death = (record.get("date_of_death") or "").strip()
        if canonical and date_of_death and context in {"street", "detention"}:
            duplicate_groups[(canonical, context, date_of_death)].append(record_id)

        if not deaths_daily._is_likely_person_name(record.get("person_name")):  # intentional internal reuse
            bad_name_ids.append(record_id)

        location = (record.get("facility_or_location") or "").strip().lower()
        if not location or location == "unknown":
            unknown_locations += 1

    duplicate_hits = [
        f"{key[0]} | {key[1]} | {key[2]} -> ids={ids}"
        for key, ids in duplicate_groups.items()
        if len(ids) > 1
    ]
    if duplicate_hits:
        failures.append(f"duplicate person/context/date groups: {len(duplicate_hits)}")
        _print_group("  - ", duplicate_hits)

    if bad_name_ids:
        failures.append(f"non-person names present: {len(bad_name_ids)}")
        _print_group("  - id=", bad_name_ids)

    unknown_rate = unknown_locations / max(1, len(records))
    if unknown_rate > args.max_unknown_rate:
        failures.append(
            f"unknown location rate {unknown_rate:.1%} exceeds threshold {args.max_unknown_rate:.1%}",
        )
    elif unknown_rate > 0.20:
        warnings.append(f"unknown location rate is elevated: {unknown_rate:.1%}")

    print(
        "Deaths QA summary:",
        f"records={len(records)}",
        f"street={context_counts.get('street', 0)}",
        f"detention={context_counts.get('detention', 0)}",
        f"unknown_location_rate={unknown_rate:.1%}",
    )

    for warning in warnings:
        print(f"WARN: {warning}")

    if failures:
        for failure in failures:
            print(f"FAIL: {failure}")
        return 1

    print("PASS: deaths dataset quality gates satisfied.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
