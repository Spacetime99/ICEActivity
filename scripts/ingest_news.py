#!/usr/bin/env python3
"""
Entry point used by cron to pull ICE-related news articles.

Usage:
    python3 scripts/ingest_news.py --output-dir datasets/news_ingest
"""

from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.news_ingestion import main


if __name__ == "__main__":
    raise SystemExit(main())
