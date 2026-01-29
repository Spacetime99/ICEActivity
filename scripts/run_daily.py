#!/usr/bin/env python3
"""
Entry point for the daily deaths JSONL updater.

Usage:
    python3 scripts/run_daily.py --window-days 14 --out ./site/data
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.deaths_daily import main  # noqa: E402


if __name__ == "__main__":
    raise SystemExit(main())
