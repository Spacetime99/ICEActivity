#!/usr/bin/env python3
"""Quick harness to re-run triplet extraction for a single article."""

from __future__ import annotations

import json
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.geocoding import NominatimGeocoder
from src.services.news_triplets import (
    Triplet,
    TripletExtractor,
    combine_article_text,
    geocode_triplets,
    sanitize_triplet,
    _complete_incomplete_actions,
    _drop_inverted_triplets,
    _drop_uncompleted_actions,
    _rank_triplets,
    _rewrite_incomplete_actor_triplets,
    _clause_after_action,
    _sentence_window_for_action,
)

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
    extractor = TripletExtractor()
    triplets = extractor.extract(article_text, location_hints=record.get("city_mentions") or [])
    triplets = _complete_incomplete_actions(triplets, extractor, article_text)
    triplets = _drop_uncompleted_actions(triplets)
    triplets = _rewrite_incomplete_actor_triplets(triplets)
    triplets = _drop_inverted_triplets(triplets)
    triplets = _rank_triplets(triplets)[:2]
    print("LLM output:")
    print(json.dumps(triplets, indent=2))
    if triplets:
        print("\nContext windows:")
        for item in triplets:
            who = (item.get("who") or "").strip()
            what = (item.get("what") or "").strip()
            if not what:
                continue
            sentence = _sentence_window_for_action(what, article_text)
            clause = _clause_after_action(what, sentence)
            print(f"- who={who} what={what}")
            print(f"  sentence: {sentence}")
            print(f"  clause: {clause}")
    if not triplets:
        return
    geocoder = NominatimGeocoder(DATA_DIR / "geocache.sqlite")
    triplet_records = []
    for item in triplets:
        triplet_records.append(
            Triplet(
                who=(item.get("who") or "").strip(),
                what=(item.get("what") or "").strip(),
                where_text=item.get("where"),
                latitude=None,
                longitude=None,
                geocode_query=None,
                raw_text=article_text,
                story_id=record.get("url") or record.get("source_id") or "unknown",
                source=record.get("source") or "unknown",
                url=record.get("url") or "",
                title=record.get("title") or "",
                published_at=record.get("published_at"),
                extracted_at="",
                run_id="local-test",
            )
        )
    geocode_triplets(triplet_records, geocoder)
    sanitized = []
    for item in triplet_records:
        sanitized_item = sanitize_triplet(
            item,
            article_text=article_text,
            extractor=extractor,
        )
        if sanitized_item:
            sanitized.append(
                {
                    "who": sanitized_item.who,
                    "what": sanitized_item.what,
                    "where": sanitized_item.where_text,
                }
            )
    print("\nAfter geocoding + sanitize:")
    print(json.dumps(sanitized, indent=2))


if __name__ == "__main__":
    story = sys.argv[1] if len(sys.argv) > 1 else "https://www.latimes.com/california/story/2025-11-25/memo-torres-daily-memo-la-taco"
    main(story)
