"""
Triplet extraction for ICE-related news dumps.

Reads the latest ingestor JSONL snapshot, runs LLM-based who/what/where extraction,
geocodes locations with the shared cache, and writes JSONL + SQLite indexes.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import sqlite3
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Sequence
from uuid import uuid4

from bs4 import BeautifulSoup
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer

from src.services.geocoding import GeocodeResult, NominatimGeocoder

LOGGER = logging.getLogger(__name__)

DEFAULT_MODEL_ID = "microsoft/Phi-3-mini-128k-instruct"


@dataclass
class Triplet:
    who: str
    what: str
    where_text: Optional[str]
    latitude: Optional[float]
    longitude: Optional[float]
    geocode_query: Optional[str]
    raw_text: str
    story_id: str
    source: str
    url: str
    title: str
    published_at: Optional[str]
    extracted_at: str
    run_id: str
    geocode_status: Optional[str] = None


VALID_LAT_RANGE = NominatimGeocoder.US_LAT_RANGE
VALID_LON_RANGE = NominatimGeocoder.US_LON_RANGE


class TripletExtractor:
    """Lightweight wrapper around a local HF model for structured extraction."""

    def __init__(
        self,
        model_id: str = DEFAULT_MODEL_ID,
        temperature: float = 0.0,
        repetition_penalty: float = 1.05,
        max_new_tokens: int = 200,
        stop_text: Optional[str] = None,
    ) -> None:
        dtype = torch.bfloat16 if torch.cuda.is_available() else torch.float32
        self.tokenizer = AutoTokenizer.from_pretrained(model_id)
        self.model = AutoModelForCausalLM.from_pretrained(
            model_id,
            torch_dtype=dtype,
            device_map="auto",
        )
        self.temperature = temperature
        self.repetition_penalty = repetition_penalty
        self.max_new_tokens = max_new_tokens
        self.stop_text = stop_text

    def build_prompt(self, article_text: str, location_hints: Sequence[str] | None = None) -> str:
        location_clause = ""
        if location_hints:
            unique_hints = sorted({hint.strip() for hint in location_hints if hint and hint.strip()})
            if unique_hints:
                hints_blob = "; ".join(unique_hints)
                location_clause = (
                    "\nKnown valid locations from the article metadata (use these exact names "
                    "whenever possible, and do not invent new places):\n"
                    f"{hints_blob}\n"
                )
        return (
            "Extract concise triplets from the news text. "
            "Return a JSON array of objects: "
            '{"who": "<entity or person>", "what": "<short action>", '
            '"where": "<location or null>"}. '
            "Rules:\n"
            "- Only include facts explicitly stated; do not infer or change who did what.\n"
            "- Keep 'who' as the exact subject described "
            "(e.g., 'mother of White House Press Secretary Karoline "
            "Leavitt's nephew'), not a related person.\n"
            "- Preserve titles/qualifiers so roles stay accurate.\n"
            "- Use the smallest explicit location stated in the text for 'where' "
            "(facility > street > city > region). If a city like 'El Centro' is present, "
            "do not return null even if the venue is unknown.\n"
            "- Use null only if no location is given. Never output 'unknown'.\n"
            "- Keep 'what' short and verb-focused "
            "(e.g., 'is detained by immigration authorities').\n"
            "- Output JSON only; no commentary.\n\n"
            f"{location_clause}"
            f"Text:\n{article_text}\n\nJSON:"
        )

    def extract(self, article_text: str, location_hints: Sequence[str] | None = None) -> list[dict[str, str]]:
        prompt = self.build_prompt(article_text, location_hints=location_hints)
        inputs = self.tokenizer(prompt, return_tensors="pt").to(self.model.device)
        outputs = self.model.generate(
            **inputs,
            max_new_tokens=self.max_new_tokens,
            do_sample=self.temperature > 0,
            temperature=self.temperature if self.temperature > 0 else None,
            repetition_penalty=self.repetition_penalty,
            eos_token_id=self.tokenizer.eos_token_id,
            pad_token_id=self.tokenizer.pad_token_id,
        )
        decoded = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
        if self.stop_text:
            decoded = decoded.split(self.stop_text, 1)[0]
        decoded = decoded.strip()
        return self._parse_triplets(decoded)

    def _parse_triplets(self, text: str) -> list[dict[str, str]]:
        """Best-effort JSON extraction; fallback to empty list."""
        first_bracket = text.find("[")
        last_bracket = text.rfind("]")
        if first_bracket == -1 or last_bracket == -1 or last_bracket <= first_bracket:
            return []
        snippet = text[first_bracket : last_bracket + 1]
        try:
            payload = json.loads(snippet)
            if isinstance(payload, list):
                return [item for item in payload if isinstance(item, dict)]
        except json.JSONDecodeError:
            LOGGER.debug("Failed to parse model output as JSON.", exc_info=True)
        return []


class TripletIndex:
    """SQLite index for quick plotting/filtering."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS triplets (
                story_id TEXT,
                source TEXT,
                url TEXT,
                title TEXT,
                published_at TEXT,
                who TEXT,
                what TEXT,
                where_text TEXT,
                latitude REAL,
                longitude REAL,
                geocode_query TEXT,
                geocode_status TEXT,
                extracted_at TEXT,
                run_id TEXT,
                PRIMARY KEY (story_id, who, what, where_text)
            )
            """
        )
        self._ensure_column("triplets", "geocode_status", "TEXT")
        self._ensure_column("triplets", "run_id", "TEXT")
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_triplets_published ON triplets(published_at)"
        )
        self.conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_triplets_location ON triplets(latitude, longitude)"
        )
        self.conn.execute("DROP VIEW IF EXISTS triplets_fast")
        self.conn.execute(
            """
            CREATE VIEW triplets_fast AS
            SELECT
                story_id AS id,
                who,
                what,
                latitude AS lat,
                longitude AS lon,
                published_at AS publishedAt,
                url,
                source
            FROM triplets
            """
        )
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS runs (
                id TEXT PRIMARY KEY,
                started_at TEXT,
                finished_at TEXT,
                articles_processed INTEGER,
                triplets_extracted INTEGER
            )
            """
        )
        self.conn.commit()

    def _ensure_column(self, table: str, column: str, ddl: str) -> None:
        cursor = self.conn.execute(f"PRAGMA table_info({table})")
        existing = {row[1] for row in cursor.fetchall()}
        if column not in existing:
            self.conn.execute(f"ALTER TABLE {table} ADD COLUMN {column} {ddl}")
            self.conn.commit()

    def upsert(self, records: Iterable[Triplet]) -> None:
        with self.conn:
            self.conn.executemany(
                """
                INSERT OR REPLACE INTO triplets (
                    story_id, source, url, title, published_at,
                    who, what, where_text, latitude, longitude,
                    geocode_query, geocode_status, extracted_at, run_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                [
                    (
                        rec.story_id,
                        rec.source,
                        rec.url,
                        rec.title,
                        rec.published_at,
                        rec.who,
                        rec.what,
                        rec.where_text,
                        rec.latitude,
                        rec.longitude,
                        rec.geocode_query,
                        rec.geocode_status,
                        rec.extracted_at,
                        rec.run_id,
                    )
                    for rec in records
                ],
            )

    def record_run(
        self,
        run_id: str,
        started_at: datetime,
        finished_at: datetime,
        articles_processed: int,
        triplets_extracted: int,
    ) -> None:
        with self.conn:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO runs (
                    id, started_at, finished_at, articles_processed, triplets_extracted
                )
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    run_id,
                    started_at.isoformat(),
                    finished_at.isoformat(),
                    articles_processed,
                    triplets_extracted,
                ),
            )


def load_latest_news_dump(output_dir: Path) -> Path:
    candidates = sorted(output_dir.glob("news_reports_*.jsonl"))
    if not candidates:
        raise FileNotFoundError(f"No news_reports_*.jsonl found in {output_dir}")
    return candidates[-1]


def read_articles(path: Path) -> list[dict]:
    articles: list[dict] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                articles.append(json.loads(line))
            except json.JSONDecodeError:
                LOGGER.warning("Skipping malformed line in %s", path)
    return articles


def _strip_html_fragment(value: Optional[str]) -> str:
    if not value:
        return ""
    soup = BeautifulSoup(value, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", text).strip()


def combine_article_text(article: dict) -> str:
    """Prefer fetched/full content but keep fallbacks for sparse feeds."""
    raw = article.get("raw") or {}
    candidates: list[Optional[str]] = [
        raw.get("fetched_content"),
        article.get("content"),
        raw.get("content"),
        raw.get("body"),
        raw.get("text"),
        raw.get("description"),
        article.get("summary"),
        raw.get("summary"),
        article.get("title"),
    ]
    parts: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if not isinstance(candidate, str):
            continue
        cleaned = _strip_html_fragment(candidate)
        if cleaned and cleaned not in seen:
            parts.append(cleaned)
            seen.add(cleaned)
    return "\n\n".join(parts).strip()


def _coordinates_within_bounds(lat: Optional[float], lon: Optional[float]) -> bool:
    if lat is None or lon is None:
        return False
    return VALID_LAT_RANGE[0] <= lat <= VALID_LAT_RANGE[1] and VALID_LON_RANGE[0] <= lon <= VALID_LON_RANGE[1]


def geocode_where(
    where_value: Optional[str],
    geocoder: NominatimGeocoder,
) -> tuple[Optional[float], Optional[float], Optional[str], str]:
    if not where_value:
        return None, None, None, "missing_where"
    result: Optional[GeocodeResult] = geocoder.lookup(where_value)
    if not result:
        return None, None, where_value, "failed"
    lat = result.latitude
    lon = result.longitude
    if not _coordinates_within_bounds(lat, lon):
        return None, None, result.query, "out_of_bounds"
    return lat, lon, result.query, result.source or "external"


def geocode_triplets(records: list[Triplet], geocoder: NominatimGeocoder) -> None:
    for record in records:
        lat, lon, geocode_query, status = geocode_where(record.where_text, geocoder)
        record.latitude = lat
        record.longitude = lon
        record.geocode_query = geocode_query
        record.geocode_status = status


def extract_triplets_from_dump(
    dump_path: Path,
    output_dir: Path,
    geocoder: NominatimGeocoder,
    extractor: TripletExtractor,
    limit: Optional[int] = None,
) -> Path:
    run_id = uuid4().hex
    run_started = datetime.now(timezone.utc)
    articles = read_articles(dump_path)
    if limit:
        articles = articles[:limit]
    # Deduplicate articles by a stable identifier (prefer source_id/url); avoid reprocessing
    # repeated rows in the same dump.
    seen_story_keys: set[str] = set()
    extracted_path = (
        output_dir
        / f"triplets_{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%SZ')}.jsonl"
    )
    extracted_path.parent.mkdir(parents=True, exist_ok=True)
    index = TripletIndex(output_dir / "triplets_index.sqlite")
    extracted_records: list[Triplet] = []
    articles_processed = 0
    for article in articles:
        story_key = article.get("source_id") or article.get("url")
        if story_key:
            if story_key in seen_story_keys:
                LOGGER.debug("Skipping duplicate article with story_id/url=%s", story_key)
                continue
            seen_story_keys.add(story_key)
        article_text = combine_article_text(article)
        if not article_text:
            continue
        articles_processed += 1
        location_hints: list[str] = []
        for key in ("city_mentions", "facility_mentions", "locations"):
            values = article.get(key)
            if isinstance(values, list):
                location_hints.extend(str(value) for value in values if value)
        for key in ("geocode_query", "where_text"):
            value = article.get(key)
            if isinstance(value, str):
                location_hints.append(value)
        model_triplets = extractor.extract(article_text, location_hints=location_hints)
        story_id = article.get("source_id") or article.get("url") or "unknown"
        source = article.get("source") or "unknown"
        url = article.get("url") or ""
        title = article.get("title") or ""
        published_at = article.get("published_at")
        extracted_at = datetime.now(timezone.utc).isoformat()
        for item in model_triplets:
            who = (item.get("who") or "").strip()
            what = (item.get("what") or "").strip()
            where_value = item.get("where")
            if isinstance(where_value, str):
                where_value = where_value.strip()
            if not who and not what and not where_value:
                continue
            record = Triplet(
                who=who,
                what=what,
                where_text=where_value,
                latitude=None,
                longitude=None,
                geocode_query=None,
                raw_text=article_text,
                story_id=story_id,
                source=source,
                url=url,
                title=title,
                published_at=published_at,
                extracted_at=extracted_at,
                run_id=run_id,
            )
            extracted_records.append(record)

    if extracted_records:
        geocode_triplets(extracted_records, geocoder)
        with extracted_path.open("w", encoding="utf-8") as handle:
            for record in extracted_records:
                handle.write(
                    json.dumps(
                        {
                            "story_id": record.story_id,
                            "source": record.source,
                            "url": record.url,
                            "title": record.title,
                            "published_at": record.published_at,
                            "who": record.who,
                            "what": record.what,
                            "where": record.where_text,
                            "latitude": record.latitude,
                            "longitude": record.longitude,
                            "geocode_query": record.geocode_query,
                            "geocode_status": record.geocode_status,
                            "extracted_at": record.extracted_at,
                            "run_id": record.run_id,
                        },
                        ensure_ascii=False,
                    )
                    + "\n"
                )
        index.upsert(extracted_records)
    else:
        extracted_path.touch()
    run_finished = datetime.now(timezone.utc)
    index.record_run(
        run_id=run_id,
        started_at=run_started,
        finished_at=run_finished,
        articles_processed=articles_processed,
        triplets_extracted=len(extracted_records),
    )
    return extracted_path


def build_geocoder(output_dir: Path, google_key: Optional[str]) -> NominatimGeocoder:
    cache_path = output_dir / "geocache.sqlite"
    user_agent = "codex-triplet-extractor/0.1"
    return NominatimGeocoder(
        cache_path=cache_path,
        user_agent=user_agent,
        google_api_key=google_key,
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Extract who/what/where triplets from news dumps.")
    parser.add_argument(
        "--input-dir",
        type=Path,
        default=Path("datasets/news_ingest"),
        help="Directory containing news_reports_*.jsonl files.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("datasets/news_ingest"),
        help="Directory to write triplets JSONL and SQLite index.",
    )
    parser.add_argument(
        "--model-id",
        default=DEFAULT_MODEL_ID,
        help="HF model id or local path for extraction.",
    )
    parser.add_argument(
        "--temperature",
        type=float,
        default=0.0,
        help="Sampling temperature (0 for deterministic).",
    )
    parser.add_argument(
        "--repetition-penalty",
        type=float,
        default=1.05,
        help="Penalty >1 discourages repetition.",
    )
    parser.add_argument(
        "--max-new-tokens",
        type=int,
        default=200,
        help="Generation cap for extraction model.",
    )
    parser.add_argument(
        "--stop-text",
        default=None,
        help="Optional marker to truncate model output.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Process only the first N articles (for debugging).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Python logging level.",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(message)s",
    )

    # Handle hf_transfer flag to avoid crashes if the package is missing.
    if os.environ.get("HF_HUB_ENABLE_HF_TRANSFER") == "1":
        try:
            import hf_transfer  # type: ignore  # noqa: F401
        except ModuleNotFoundError:
            os.environ["HF_HUB_ENABLE_HF_TRANSFER"] = "0"

    dump_path = load_latest_news_dump(args.input_dir)
    LOGGER.info("Using news dump: %s", dump_path)
    geocoder = build_geocoder(args.output_dir, os.getenv("GOOGLE_ACC_KEY"))
    extractor = TripletExtractor(
        model_id=args.model_id,
        temperature=args.temperature,
        repetition_penalty=args.repetition_penalty,
        max_new_tokens=args.max_new_tokens,
        stop_text=args.stop_text,
    )
    output_path = extract_triplets_from_dump(
        dump_path=dump_path,
        output_dir=args.output_dir,
        geocoder=geocoder,
        extractor=extractor,
        limit=args.limit,
    )
    LOGGER.info("Wrote triplets to %s", output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
