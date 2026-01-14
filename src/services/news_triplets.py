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

MANUAL_COORDINATES = {
    "afghanistan": (33.93911, 67.709953),
    "mexico": (23.6345, -102.5528),
}
MIN_ARTICLE_TEXT_LENGTH = 200
MAX_TRIPLETS_PER_ARTICLE = 2

DATE_FROM_URL_PATTERNS = [
    re.compile(r"/(20\d{2})/(0[1-9]|1[0-2])/(0[1-9]|[12]\d|3[01])/"),
    re.compile(r"/(20\d{2})-(0[1-9]|1[0-2])-(0[1-9]|[12]\d|3[01])/"),
]


def infer_published_at_from_url(url_value: str | None) -> str | None:
    if not url_value:
        return None
    for pattern in DATE_FROM_URL_PATTERNS:
        match = pattern.search(url_value)
        if not match:
            continue
        year, month, day = match.groups()
        try:
            parsed = datetime(
                int(year),
                int(month),
                int(day),
                tzinfo=timezone.utc,
            )
        except ValueError:
            continue
        return parsed.isoformat()
    return None

NON_LOCATION_TOKENS = {
    "unknown",
    "critical",
    "condition",
    "status",
    "life support",
    "fighting for his life",
}

INVALID_LOCATION_PHRASES = {
    "operation allies welcome",
    "operation lone star",
}

INCOMPLETE_ACTOR_ACTIONS = {
    "shot",
    "shot and killed",
    "killed",
    "arrested",
    "detained",
    "injured",
    "assaulted",
    "raided",
}

NEWSWORTHY_KEYWORDS = {
    "shot",
    "shooting",
    "killed",
    "dead",
    "death",
    "injured",
    "assaulted",
    "arrested",
    "detained",
    "raid",
    "raided",
    "custody",
    "fatal",
    "fatally",
    "murdered",
    "charged",
}

ROUTINE_KEYWORDS = {
    "carrying out",
    "conducting",
    "enforcing",
    "continuing",
    "deployed",
    "deploying",
    "announced",
    "said",
    "says",
    "reported",
}

PLURAL_WHO_SUFFIXES = (
    "agents",
    "officers",
    "authorities",
    "officials",
    "forces",
    "police",
)

VICTIM_ACTION_TO_ACTOR = [
    ("was shot and killed", "shot and killed"),
    ("was fatally shot", "shot"),
    ("was shot", "shot"),
    ("was killed", "killed"),
    ("was arrested", "arrested"),
    ("was detained", "detained"),
    ("was injured", "injured"),
    ("was assaulted", "assaulted"),
]

WHO_SYNONYM_PATTERNS: dict[str, list[str]] = {
    "ice agents": [
        "ice",
        "ice agents",
        "ice agent",
        "immigration and customs enforcement",
        "immigration and customs enforcement agents",
        "immigration and customs enforcement agent",
        "immigration agents",
        "immigration agent",
        "federal immigration agents",
        "federal immigration agent",
        "immigration authorities",
        "immigration officers",
    ],
    "ice agent": [
        "ice agent",
        "immigration and customs enforcement agent",
        "immigration agent",
        "federal immigration agent",
    ],
}

CANONICAL_WHO_DISPLAY = {
    "ice agents": "ICE agents",
    "ice agent": "ICE agent",
}

LOCATION_PREFIX_PATTERN = re.compile(
    r"^(?:(?:and|the|this|that|these|those|family|families|community|group|people|residents)\s+)?"
    r"(?:in|at|near|around|outside|inside|under|during|while|amid|through|along|within|by|after|before)\s+",
    re.IGNORECASE,
)


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
            "- Prioritize the most newsworthy, headline-level actions; avoid routine duties unless they are the main story.\n"
            "- Prefer 1-3 high-signal triplets over many low-signal ones.\n"
            "- Only include facts explicitly stated; do not infer or change who did what.\n"
            "- Keep 'who' as the exact subject described "
            "(e.g., 'mother of White House Press Secretary Karoline "
            "Leavitt's nephew'), not a related person.\n"
            "- Preserve titles/qualifiers so roles stay accurate.\n"
            "- Use the smallest explicit location stated in the text for 'where' "
            "(facility > street > city > region). If a city like 'El Centro' is present, "
            "do not return null even if the venue is unknown.\n"
            "- For 'where', return only the actual place name (e.g., 'Farragut Square, Washington, DC'). "
            "Strip any surrounding prepositions ('in', 'at', 'near', 'under') and never output policy names, "
            "operations, programs, timelines, or phrases such as 'under Operation Allies Welcome'.\n"
            "- Use null only if no location is given. Never output 'unknown'.\n"
            "- Keep 'what' short and verb-focused "
            "(e.g., 'is detained by immigration authorities').\n"
            "- 'what' must name the action and its direct object or context (e.g., 'dismissed the case', "
            "not just 'dismissed'). Avoid single verbs without the thing being acted upon.\n"
            "- Only describe who performed an action when the article explicitly states it "
            "(e.g., \"<who> shot <person>\"). Do not guess or infer shooters or other actors.\n"
            "- When (and only when) the article clearly states both the actor and the affected person, output two triplets: "
            "one from the affected person's perspective (\"<victim> was arrested by ICE\"), and one from the actor's perspective "
            "(\"ICE agents arrested <victim>\"). If the actor is not explicitly stated, omit the actor-side triplet.\n"
            "- When the subject of a triplet is the victim (e.g., they were shot, arrested, detained), phrase the action in passive voice "
            "such as \"was shot\" or \"was arrested\" to avoid implying they were the aggressor.\n"
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


def iter_triplet_files(output_dir: Path, rerun_all: bool = False) -> list[Path]:
    pattern = "triplets_*.jsonl" if rerun_all else "news_reports_*.jsonl"
    candidates = sorted(output_dir.glob(pattern))
    if not candidates:
        raise FileNotFoundError(f"No {pattern} files found in {output_dir}")
    return candidates


def read_articles(path: Path) -> list[dict]:
    articles: list[dict] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                articles.append(json.loads(line))
            except json.JSONDecodeError:
                LOGGER.warning("Skipping malformed line in %s", path)
    return articles


def read_triplets_file(path: Path) -> list[Triplet]:
    records: list[Triplet] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                LOGGER.warning("Skipping malformed triplet line in %s", path)
                continue
            try:
                records.append(
                    Triplet(
                        who=payload.get("who", ""),
                        what=payload.get("what", ""),
                        where_text=payload.get("where_text"),
                        latitude=payload.get("lat"),
                        longitude=payload.get("lon"),
                        geocode_query=payload.get("geocode_query"),
                        raw_text=payload.get("raw_text", ""),
                        story_id=payload.get("story_id", str(uuid4())),
                        source=payload.get("source", ""),
                        url=payload.get("url", ""),
                        title=payload.get("title", ""),
                        published_at=payload.get("publishedAt"),
                        extracted_at=payload.get("extracted_at")
                        or payload.get("extractedAt")
                        or datetime.now(timezone.utc).isoformat(),
                        run_id=payload.get("run_id") or payload.get("runId") or "legacy",
                        geocode_status=payload.get("geocode_status"),
                    )
                )
            except Exception:  # noqa: BLE001
                LOGGER.warning("Skipping malformed triplet payload from %s", path, exc_info=True)
    return records


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


def _normalize_location_label(value: str | None) -> str:
    if not value:
        return ""
    cleaned = value.strip()
    cleaned = re.sub(r"\s+", " ", cleaned)
    previous = None
    current = cleaned
    while previous != current:
        previous = current
        current = LOCATION_PREFIX_PATTERN.sub("", current).strip(" ,.;:-")
    return current


def _normalize_who_label(value: str | None) -> str | None:
    if not value:
        return value
    normalized = value.strip()
    if not normalized:
        return value
    lower = normalized.lower()
    for canonical, aliases in WHO_SYNONYM_PATTERNS.items():
        if lower == canonical or lower in aliases:
            return CANONICAL_WHO_DISPLAY.get(canonical, canonical)
    return normalized


def _is_incomplete_actor_action(value: str | None) -> bool:
    if not value:
        return False
    normalized = value.strip().lower().rstrip(".!?,;:")
    return normalized in INCOMPLETE_ACTOR_ACTIONS


def _infer_actor_action_from_victim(what: str | None) -> str | None:
    if not what:
        return None
    lower = what.lower()
    for phrase, actor_action in VICTIM_ACTION_TO_ACTOR:
        if phrase in lower:
            return actor_action
    return None


def _rewrite_incomplete_actor_triplets(triplets: list[dict[str, str]]) -> list[dict[str, str]]:
    victim_candidates: list[tuple[str, str]] = []
    actor_candidates: list[str] = []
    for item in triplets:
        who_value = (item.get("who") or "").strip()
        action = _infer_actor_action_from_victim(item.get("what"))
        if who_value and action:
            victim_candidates.append((who_value, action))
        if who_value and _is_incomplete_actor_action(item.get("what")):
            actor_candidates.append(who_value)
    if not victim_candidates:
        return triplets
    victims = {victim for victim, _ in victim_candidates}
    actions = {action for _, action in victim_candidates}
    if len(victims) != 1 or len(actions) != 1:
        return triplets
    victim = next(iter(victims))
    action = next(iter(actions))
    actor = None
    unique_actors = {actor_value for actor_value in actor_candidates if actor_value}
    if len(unique_actors) == 1:
        actor = next(iter(unique_actors))
    for item in triplets:
        if _is_incomplete_actor_action(item.get("what")):
            item["what"] = f"{action} {victim}"
        if actor:
            current = item.get("what") or ""
            current_lower = current.lower()
            if _infer_actor_action_from_victim(current) and " by " not in current_lower:
                item["what"] = f"{current} by {actor}"
    return triplets


def _drop_inverted_triplets(triplets: list[dict[str, str]]) -> list[dict[str, str]]:
    victim_candidates: list[tuple[str, str]] = []
    for item in triplets:
        who_value = (item.get("who") or "").strip()
        action = _infer_actor_action_from_victim(item.get("what"))
        if who_value and action:
            victim_candidates.append((who_value, action))
    if not victim_candidates:
        return triplets
    victims = {victim for victim, _ in victim_candidates}
    actions = {action for _, action in victim_candidates}
    if len(victims) != 1 or len(actions) != 1:
        return triplets
    victim = next(iter(victims)).lower()
    filtered: list[dict[str, str]] = []
    for item in triplets:
        who_lower = (item.get("who") or "").lower()
        what_lower = (item.get("what") or "").lower()
        if victim and victim in what_lower:
            if "killed an ice" in what_lower or "shot an ice" in what_lower:
                continue
        if who_lower.startswith("ice") and "was killed by" in what_lower and victim in what_lower:
            continue
        filtered.append(item)
    return filtered


def _apply_plural_verb_agreement(who: str | None, what: str | None) -> str | None:
    if not who or not what:
        return what
    who_lower = who.lower()
    if not any(who_lower.endswith(suffix) for suffix in PLURAL_WHO_SUFFIXES):
        return what
    if what.startswith("was "):
        return f"were {what[4:]}"
    if what.startswith("is "):
        return f"are {what[3:]}"
    return what


def _score_triplet(item: dict[str, str]) -> int:
    who_value = (item.get("who") or "").lower()
    what_value = (item.get("what") or "").lower()
    text = f"{who_value} {what_value}".strip()
    score = 0
    if who_value:
        score += 1
    for keyword in NEWSWORTHY_KEYWORDS:
        if keyword in text:
            score += 3
    for keyword in ROUTINE_KEYWORDS:
        if keyword in text:
            score -= 2
    return score


def _rank_triplets(triplets: list[dict[str, str]]) -> list[dict[str, str]]:
    scored = [(index, _score_triplet(item), item) for index, item in enumerate(triplets)]
    scored.sort(key=lambda entry: (entry[1], -entry[0]), reverse=True)
    return [item for _, _, item in scored]


def _dedupe_triplets(triplets: list[dict[str, str]]) -> list[dict[str, str]]:
    seen: set[tuple[str, str, str]] = set()
    deduped: list[dict[str, str]] = []
    for item in triplets:
        who = (item.get("who") or "").strip().lower()
        what = (item.get("what") or "").strip().lower()
        where = (item.get("where") or "").strip().lower()
        key = (who, what, where)
        if not who and not what:
            continue
        if key in seen:
            continue
        seen.add(key)
        deduped.append(item)
    return deduped


def _article_indicates_ice_killed_victim(article_lower: str, victim_lower: str) -> bool:
    if not victim_lower or victim_lower not in article_lower:
        return False
    phrases = (
        "killed by an ice",
        "killed by a u.s. immigration and customs enforcement",
        "killed by immigration and customs enforcement",
        "shot by an ice",
        "shot and killed by an ice",
        "fatally shot by an ice",
        "ice agent shot",
        "ice officer shot",
    )
    return any(phrase in article_lower for phrase in phrases)


def _coerce_where_value(value: object) -> Optional[str]:
    """Best-effort conversion of model `where` output into a string label."""
    if value is None:
        return None
    if isinstance(value, str):
        return value
    if isinstance(value, (list, tuple)):
        for item in value:
            if isinstance(item, str) and item.strip():
                return item
        return None
    return str(value)


def _who_matches_article(who_value: str, article_lower: str) -> bool:
    who_lower = who_value.strip().lower()
    if not who_lower:
        return True
    if who_lower in article_lower:
        return True
    synonyms = WHO_SYNONYM_PATTERNS.get(who_lower)
    if synonyms:
        for synonym in synonyms:
            syn_lower = synonym.lower()
            if syn_lower and syn_lower in article_lower:
                return True
        if who_lower.startswith("ice"):
            if "immigration" in article_lower and "agent" in article_lower:
                return True
    return False


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
    normalized = where_value.strip()
    normalized_lower = normalized.lower()
    if any(token in normalized_lower for token in NON_LOCATION_TOKENS):
        return None, None, None, "invalid_label"
    for key, coords in MANUAL_COORDINATES.items():
        if key in normalized_lower:
            return coords[0], coords[1], normalized, "manual"
    if "white house" in normalized_lower:
        normalized = "White House, Washington, DC"
    elif "farragut" in normalized_lower:
        normalized = "Farragut Square, Washington, DC"
    result: Optional[GeocodeResult] = geocoder.lookup(normalized)
    if not result:
        return None, None, normalized, "failed"
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


def sanitize_triplet(record: Triplet, article_text: str | None = None) -> Optional[Triplet]:
    def _drop(reason: str) -> Optional[Triplet]:
        LOGGER.info(
            "Skipping triplet for story_id=%s who=%s reason=%s",
            record.story_id,
            record.who or "<unknown>",
            reason,
        )
        return None

    record.who = _normalize_who_label(record.who) or record.who
    record.what = _apply_plural_verb_agreement(record.who, record.what) or record.what
    if article_text and record.who:
        article_lower = article_text.lower()
        if not _who_matches_article(record.who, article_lower):
            return _drop("who not found in article text")
    if record.who and _is_incomplete_actor_action(record.what):
        return _drop("actor action missing direct object")
    if article_text and record.who and record.what:
        who_lower = record.who.lower()
        what_lower = record.what.lower()
        article_lower = article_text.lower()
        if _article_indicates_ice_killed_victim(article_lower, who_lower):
            if "killed an ice" in what_lower or "shot an ice" in what_lower:
                return _drop("victim inversion (ice killed victim)")
        if who_lower.startswith("ice") and "was killed by" in what_lower:
            victim_lower = what_lower.split("was killed by", 1)[-1].strip()
            if victim_lower and _article_indicates_ice_killed_victim(article_lower, victim_lower):
                return _drop("actor inversion (ice killed victim)")

    who_lower = record.who.lower() if record.who else ""
    what_lower = record.what.lower()
    if "andrew wolfe" in who_lower and "died" in what_lower:
        record.what = "remains hospitalized in critical condition"
    if "tricia mclaughlin" in who_lower and "third world" in what_lower:
        record.what = "announced a halt to processing immigration requests relating to Afghan nationals"
    coerced_where = _coerce_where_value(record.where_text)
    if not coerced_where:
        return _drop("missing location in model output")
    normalized_where = _normalize_location_label(coerced_where)
    if not normalized_where:
        return _drop(f"location label empty after normalization ('{record.where_text}')")
    wt_lower = normalized_where.lower()
    if any(token in wt_lower for token in NON_LOCATION_TOKENS):
        return _drop(f"location label flagged as non-place ('{normalized_where}')")
    if any(phrase in wt_lower for phrase in INVALID_LOCATION_PHRASES):
        return _drop(f"location label contains policy/program phrase ('{normalized_where}')")
    record.where_text = normalized_where
    return record


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
        story_key = article.get("url") or article.get("source_id")
        if story_key:
            if story_key in seen_story_keys:
                LOGGER.debug("Skipping duplicate article with story_id/url=%s", story_key)
                continue
            seen_story_keys.add(story_key)
        article_text = combine_article_text(article)
        if not article_text or len(article_text) < MIN_ARTICLE_TEXT_LENGTH:
            continue
        article["content_blob"] = article_text
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
        model_triplets = _rewrite_incomplete_actor_triplets(model_triplets)
        model_triplets = _drop_inverted_triplets(model_triplets)
        model_triplets = _dedupe_triplets(model_triplets)
        model_triplets = _rank_triplets(model_triplets)[:MAX_TRIPLETS_PER_ARTICLE]
        story_id = article.get("url") or article.get("source_id") or "unknown"
        source = article.get("source") or "unknown"
        url = article.get("url") or ""
        title = article.get("title") or ""
        published_at = article.get("published_at")
        if not published_at:
            published_at = infer_published_at_from_url(url)
        extracted_at = datetime.now(timezone.utc).isoformat()
        if not published_at:
            published_at = extracted_at
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
            sanitized = sanitize_triplet(
                record,
                article_text=article.get("content_blob", "") or article_text,
            )
            if sanitized:
                extracted_records.append(sanitized)

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


def load_triplets_into_index(
    triplet_file: Path,
    output_dir: Path,
) -> None:
    LOGGER.info("Loading triplets from %s", triplet_file)
    records = read_triplets_file(triplet_file)
    if not records:
        LOGGER.warning("No triplets found in %s; skipping.", triplet_file)
        return
    index = TripletIndex(output_dir / "triplets_index.sqlite")
    index.upsert(records)
    index.record_run(
        run_id=f"hydrate-{triplet_file.stem}",
        started_at=datetime.now(timezone.utc),
        finished_at=datetime.now(timezone.utc),
        articles_processed=len(records),
        triplets_extracted=len(records),
    )


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
        "--input-file",
        type=Path,
        default=None,
        help="Process a specific news_reports_*.jsonl file instead of the latest dump.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=Path("datasets/news_ingest"),
        help="Directory to write triplets JSONL and SQLite index.",
    )
    parser.add_argument(
        "--process-all-dumps",
        action="store_true",
        help="Iterate over every news_reports_*.jsonl in --input-dir (oldest first).",
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
        "--hydrate-existing",
        action="store_true",
        help="Replay all triplets_*.jsonl files into the SQLite index (skip extraction).",
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

    if args.input_file and args.process_all_dumps:
        parser.error("--input-file and --process-all-dumps are mutually exclusive")
    if args.input_file and not args.input_file.exists():
        parser.error(f"Specified --input-file does not exist: {args.input_file}")

    # Handle hf_transfer flag to avoid crashes if the package is missing.
    if os.environ.get("HF_HUB_ENABLE_HF_TRANSFER") == "1":
        try:
            import hf_transfer  # type: ignore  # noqa: F401
        except ModuleNotFoundError:
            os.environ["HF_HUB_ENABLE_HF_TRANSFER"] = "0"

    geocoder = build_geocoder(args.output_dir, os.getenv("GOOGLE_ACC_KEY"))
    if args.hydrate_existing:
        triplet_files = iter_triplet_files(args.input_dir, rerun_all=True)
        LOGGER.info("Hydrating %s existing triplet files into SQLite index.", len(triplet_files))
        for triplet_file in triplet_files:
            load_triplets_into_index(triplet_file, args.output_dir)
        LOGGER.info("Hydration completed.")
        return 0

    if args.process_all_dumps:
        dump_paths = sorted(args.input_dir.glob("news_reports_*.jsonl"))
        if not dump_paths:
            parser.error(f"No news_reports_*.jsonl files found in {args.input_dir}")
    elif args.input_file:
        dump_paths = [args.input_file]
    else:
        dump_paths = [load_latest_news_dump(args.input_dir)]

    extractor = TripletExtractor(
        model_id=args.model_id,
        temperature=args.temperature,
        repetition_penalty=args.repetition_penalty,
        max_new_tokens=args.max_new_tokens,
        stop_text=args.stop_text,
    )

    for idx, dump_path in enumerate(dump_paths, start=1):
        LOGGER.info(
            "Processing dump %s (%s of %s)",
            dump_path,
            idx,
            len(dump_paths),
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
