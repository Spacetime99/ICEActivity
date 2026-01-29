#!/usr/bin/env python3
"""
CLI entrypoint for ingesting ICE detainee death report PDFs.

Usage:
    python3 scripts/ingest_death_reports.py --include-index --out ./site/data/deaths.jsonl
    python3 scripts/ingest_death_reports.py --url-file config/death_report_urls.txt
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services import death_reports  # noqa: E402
from src.services import deaths_daily  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Ingest ICE detainee death report PDFs.")
    parser.add_argument("--out", type=Path, default=Path("./site/data/deaths.jsonl"))
    parser.add_argument("--url", action="append", default=[])
    parser.add_argument("--url-file", type=Path)
    parser.add_argument("--include-index", action="store_true")
    parser.add_argument("--index-url", type=str, default=death_reports.ICE_REPORTS_INDEX_URL)
    parser.add_argument(
        "--use-playwright",
        action="store_true",
        help="Use Playwright headless browser if index fetch fails.",
    )
    parser.add_argument("--min-death-year", type=int, default=death_reports.MIN_DEATH_YEAR)
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Ignore existing JSONL and rebuild from current sources.",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    urls = list(args.url)
    if args.url_file:
        urls.extend(death_reports.load_url_file(args.url_file))

    if not urls and not args.include_index:
        raise SystemExit("Provide --url/--url-file or pass --include-index.")

    access_date = datetime.now(timezone.utc).date().isoformat()
    report_entries = death_reports.fetch_report_entries(
        urls=urls,
        include_index=args.include_index,
        index_url=args.index_url,
        use_playwright=args.use_playwright,
        min_death_year=args.min_death_year,
    )
    incoming = []
    for report in report_entries:
        record = deaths_daily.ice_report_entry_to_record(
            report,
            access_date=access_date,
            min_year=args.min_death_year,
        )
        if record:
            incoming.append(record)

    existing = {} if args.rebuild else deaths_daily.load_jsonl(args.out)
    merged, diff_entries, summary = deaths_daily.merge_records(existing, incoming)
    ordered = sorted(
        merged.values(),
        key=lambda record: (
            record.get("date_of_death") or "",
            record.get("person_name") or "",
            record.get("id"),
        ),
    )

    if not args.dry_run:
        deaths_daily.write_jsonl_atomic(args.out, ordered)
        deaths_daily.write_json_atomic(
            args.out.parent / "index.json",
            deaths_daily.build_index(ordered),
        )
        diff_path = deaths_daily.build_diff_path(args.out.parent)
        if diff_entries:
            deaths_daily.write_jsonl_atomic(diff_path, diff_entries)
        else:
            diff_path.parent.mkdir(parents=True, exist_ok=True)

    print(
        "Death report ingest complete:",
        f"records={len(ordered)}",
        f"added={summary['added']}",
        f"updated={summary['updated']}",
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
