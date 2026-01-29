#!/usr/bin/env python3
"""
Test ICE newsroom death release extraction in isolation.

Usage:
    python3 scripts/test_newsroom_deaths.py --limit 5
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.newsroom_deaths import fetch_death_releases  # noqa: E402


def main() -> int:
    parser = argparse.ArgumentParser(description="Test ICE newsroom death extraction.")
    parser.add_argument("--limit", type=int, default=5)
    parser.add_argument("--no-playwright", action="store_true")
    args = parser.parse_args()

    results = fetch_death_releases(limit=args.limit, use_playwright=not args.no_playwright)
    for record in results:
        print(json.dumps(record, ensure_ascii=True, indent=2))
        print("-" * 88)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
