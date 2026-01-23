#!/usr/bin/env python3
"""Debug location inference for a single article."""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.news_ingestion import extract_city_mentions, extract_locations  # noqa: E402
from src.services.news_triplets import _infer_location_from_text  # noqa: E402
from src.services.news_triplets import _is_immigration_related, _keyword_hits  # noqa: E402
from src.services.news_triplets import combine_article_text  # noqa: E402

DATA_DIR = REPO_ROOT / "datasets" / "news_ingest"


def load_article(story_id: str) -> dict:
    for dump_path in sorted(DATA_DIR.glob("news_reports_*.jsonl")):
        with dump_path.open("r", encoding="utf-8") as handle:
            for line in handle:
                record = json.loads(line)
                if record.get("url") == story_id or record.get("source_id") == story_id:
                    return record
    raise SystemExit(f"Story {story_id} not found in news_reports dumps")


def main(story_id: str) -> None:
    record = load_article(story_id)
    article_text = combine_article_text(record)
    inferred = _infer_location_from_text(article_text)
    city_mentions = extract_city_mentions(article_text) or []
    state_mentions = extract_locations(article_text) or []
    title = record.get("title") or ""
    snippet = (article_text or "")[:1200]
    strong = _keyword_hits(snippet, _keyword_hits.__globals__["IMMIGRATION_STRONG_KEYWORDS"])
    weak = _keyword_hits(snippet, _keyword_hits.__globals__["IMMIGRATION_WEAK_KEYWORDS"])
    title_hits = _keyword_hits(title, _keyword_hits.__globals__["IMMIGRATION_STRONG_KEYWORDS"] | _keyword_hits.__globals__["IMMIGRATION_WEAK_KEYWORDS"])
    print("Immigration related:", _is_immigration_related(article_text, title))
    print("Title hits:", sorted(title_hits))
    print("Snippet strong hits:", sorted(strong))
    print("Snippet weak hits:", sorted(weak))
    print("Title:", title)
    print("Snippet:", snippet[:500])
    print("Inferred location:", inferred)
    print("City mentions:", city_mentions[:20])
    print("State mentions:", state_mentions)


if __name__ == "__main__":
    story = sys.argv[1] if len(sys.argv) > 1 else ""
    if not story:
        raise SystemExit("Usage: debug_location_inference.py <article-url>")
    main(story)
