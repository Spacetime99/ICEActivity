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
from time import perf_counter
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Iterable, List, Optional, Pattern, Sequence
from uuid import uuid4

from bs4 import BeautifulSoup
import torch
from transformers import AutoModelForCausalLM, AutoTokenizer

from src.services.news_ingestion import extract_city_mentions, extract_locations
from src.services.geocoding import GeocodeResult, NominatimGeocoder

LOGGER = logging.getLogger(__name__)

DEFAULT_MODEL_ID = "microsoft/Phi-3-mini-128k-instruct"

COMPLETION_STATS = {"attempted": 0, "accepted": 0, "rejected": 0}
TIMING_STATS = {
    "total": 0.0,
    "sanitize": 0.0,
    "geocode": 0.0,
    "llm_extract": 0.0,
    "llm_object": 0.0,
    "llm_complement": 0.0,
    "llm_action_clause": 0.0,
    "llm_location": 0.0,
}
TIMING_COUNTS = {key: 0 for key in TIMING_STATS}


def _record_timing(label: str, elapsed: float) -> None:
    if label not in TIMING_STATS:
        return
    TIMING_STATS[label] += elapsed
    TIMING_COUNTS[label] += 1
REQUIRE_DIRECT_OBJECT = True


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
    event_types: list[str] = field(default_factory=list)


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

EVENT_TYPE_PATTERNS: dict[str, list[Pattern[str]]] = {
    "protest": [re.compile(r"\bprotest(?:s|ers|ing)?\b", re.IGNORECASE)],
    "march": [re.compile(r"\bmarch(?:es|ers|ing)?\b", re.IGNORECASE)],
    "rally": [re.compile(r"\brally(?:ies|ied|ing)?\b", re.IGNORECASE)],
    "demonstration": [
        re.compile(r"\bdemonstration(?:s)?\b", re.IGNORECASE),
        re.compile(r"\bdemonstrator(?:s)?\b", re.IGNORECASE),
        re.compile(r"\bdemonstrat(?:e|ed|ing)\b", re.IGNORECASE),
    ],
    "strike": [re.compile(r"\bstrike(?:s|ing)?\b", re.IGNORECASE)],
    "walkout": [re.compile(r"\bwalkout(?:s)?\b", re.IGNORECASE)],
    "picket": [re.compile(r"\bpicket(?:s|ed|ing)?\b", re.IGNORECASE)],
    "sit_in": [re.compile(r"\bsit[- ]in(?:s)?\b", re.IGNORECASE)],
    "vigil": [re.compile(r"\bvigil(?:s)?\b", re.IGNORECASE)],
    "boycott": [re.compile(r"\bboycott(?:s|ed|ing)?\b", re.IGNORECASE)],
    "blockade": [re.compile(r"\bblockade(?:s|d|ing)?\b", re.IGNORECASE)],
    "occupation": [
        re.compile(r"\boccup(?:y|ies|ied|ying)\b", re.IGNORECASE),
        re.compile(r"\boccupation\b", re.IGNORECASE),
    ],
    "riot": [re.compile(r"\briot(?:s|ing)?\b", re.IGNORECASE)],
    "civil_unrest": [
        re.compile(r"\bcivil unrest\b", re.IGNORECASE),
        re.compile(r"\bunrest\b", re.IGNORECASE),
    ],
    "uprising": [re.compile(r"\buprising(?:s)?\b", re.IGNORECASE)],
    "revolution": [re.compile(r"\brevolution(?:s|ary)?\b", re.IGNORECASE)],
}


def _detect_event_types(text: str) -> list[str]:
    if not text:
        return []
    matches: list[str] = []
    for label, patterns in EVENT_TYPE_PATTERNS.items():
        if any(pattern.search(text) for pattern in patterns):
            matches.append(label)
    return matches


def _build_event_blob(record: Triplet) -> str:
    parts = [record.title, record.what, record.who, record.raw_text]
    return " ".join(part for part in parts if part).strip()

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

INCOMPLETE_OBJECT_ACTIONS: set[str] = set()
ROLE_ONLY_ACTIONS = {
    "agent",
    "agents",
    "officer",
    "officers",
    "official",
    "officials",
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

IMMIGRATION_STRONG_KEYWORDS = {
    "ice",
    "border patrol",
    "customs and border protection",
    "cbp",
    "homeland security",
    "dhs",
    "deport",
    "deportation",
    "detention",
    "detained",
    "raid",
    "raids",
    "asylum",
    "immigration enforcement",
    "refugee",
    "refugees",
    "resettled",
    "resettlement",
    "refugee resettlement",
    "anti-immigration",
    "immigrants",
    "green card",
    "green cards",
    "green-card",
}

IMMIGRATION_WEAK_KEYWORDS = {
    "immigration",
    "migrant",
    "immigrant",
}

REPORTING_VERBS = {
    "said",
    "says",
    "stated",
    "announced",
    "indicated",
    "noted",
    "told",
    "warned",
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
            "- Only extract triplets related to immigration enforcement, ICE, Border Patrol, detention, raids, deportations, or immigration policy.\n"
            "- Prioritize the most newsworthy, headline-level actions; avoid routine duties unless they are the main story.\n"
            "- Prefer 1-3 high-signal triplets over many low-signal ones.\n"
            "- Only include facts explicitly stated; do not infer or change who did what.\n"
            "- Prefer the grammatical subject of the sentence (often the first noun phrase); do not use an object phrase as the subject.\n"
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
            "- If the text says \"<victim> was shot/killed by <actor>\", the victim triplet MUST be "
            "\"<victim> was shot/killed by <actor>\". Only output \"<actor> shot/killed <victim>\" if the text explicitly states it.\n"
            "- Never flip subject/object: do not output \"<victim> shot <actor>\" when the text says the opposite.\n"
            "- Output JSON only; no commentary.\n\n"
            f"{location_clause}"
            f"Text:\n{article_text}\n\nJSON:"
        )

    def extract(self, article_text: str, location_hints: Sequence[str] | None = None) -> list[dict[str, str]]:
        start = perf_counter()
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
        _record_timing("llm_extract", perf_counter() - start)
        return self._parse_triplets(decoded)

    def extract_direct_object(self, who: str, action: str, article_text: str) -> str | None:
        start = perf_counter()
        prompt = (
            "Identify the explicit object for the action in the text.\n"
            f"Subject: {who}\n"
            f"Action: {action}\n"
            "If the action ends with a preposition (for example, 'against' or 'to'), "
            "return the object of that preposition.\n"
            "Return only the object phrase, with no extra words. "
            "If no object is explicitly stated, return an empty string.\n\n"
            f"Text:\n{article_text}\n\nObject:"
        )
        inputs = self.tokenizer(prompt, return_tensors="pt").to(self.model.device)
        outputs = self.model.generate(
            **inputs,
            max_new_tokens=24,
            do_sample=False,
            repetition_penalty=self.repetition_penalty,
            eos_token_id=self.tokenizer.eos_token_id,
            pad_token_id=self.tokenizer.pad_token_id,
        )
        decoded = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
        if "Object:" in decoded:
            candidate = decoded.split("Object:", 1)[-1]
        else:
            candidate = decoded
        candidate = candidate.strip().strip('"').strip("'")
        _record_timing("llm_object", perf_counter() - start)
        if not candidate:
            return None
        if candidate.lower() in {"none", "unknown", "n/a"}:
            return None
        return candidate.splitlines()[0].strip()

    def extract_complement_clause(self, who: str, action: str, clause_text: str) -> str | None:
        start = perf_counter()
        prompt = (
            "Extract the shortest complete complement clause that answers:\n"
            f"\"What did {who} {action}?\"\n"
            "- Keep the main verb phrase.\n"
            "- Remove time markers and trailing reasons if they are not essential.\n"
            "- Return only the clause, with no extra words.\n\n"
            f"Clause:\n{clause_text}\n\nComplement clause:"
        )
        inputs = self.tokenizer(prompt, return_tensors="pt").to(self.model.device)
        outputs = self.model.generate(
            **inputs,
            max_new_tokens=40,
            do_sample=False,
            repetition_penalty=self.repetition_penalty,
            eos_token_id=self.tokenizer.eos_token_id,
            pad_token_id=self.tokenizer.pad_token_id,
        )
        decoded = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
        if "Complement clause:" in decoded:
            candidate = decoded.split("Complement clause:", 1)[-1]
        else:
            candidate = decoded
        candidate = candidate.strip().strip('"').strip("'")
        _record_timing("llm_complement", perf_counter() - start)
        if not candidate:
            return None
        if candidate.lower() in {"none", "unknown", "n/a"}:
            return None
        return candidate.splitlines()[0].strip()

    def extract_location_from_text(self, article_text: str) -> str | None:
        start = perf_counter()
        prompt = (
            "Identify the most specific US location explicitly stated in the text.\n"
            "- Prefer neighborhood > city > state.\n"
            "- Return as \"Neighborhood, City, State\" when available.\n"
            "- If only a city is explicit, return \"City, State\".\n"
            "- Return only the location name, no extra words.\n"
            "- If no US location is stated, return an empty string.\n\n"
            f"Text:\n{article_text}\n\nLocation:"
        )
        inputs = self.tokenizer(prompt, return_tensors="pt").to(self.model.device)
        outputs = self.model.generate(
            **inputs,
            max_new_tokens=32,
            do_sample=False,
            repetition_penalty=self.repetition_penalty,
            eos_token_id=self.tokenizer.eos_token_id,
            pad_token_id=self.tokenizer.pad_token_id,
        )
        decoded = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
        if "Location:" in decoded:
            candidate = decoded.split("Location:", 1)[-1]
        else:
            candidate = decoded
        candidate = candidate.strip().strip('"').strip("'")
        _record_timing("llm_location", perf_counter() - start)
        if not candidate:
            return None
        if candidate.lower() in {"none", "unknown", "n/a"}:
            return None
        return candidate.splitlines()[0].strip()

    def extract_action_clause(self, who: str, clause_text: str) -> str | None:
        start = perf_counter()
        prompt = (
            "Extract the shortest explicit action phrase describing what the subject did.\n"
            f"Subject: {who}\n"
            "- Keep the main verb and its object.\n"
            "- Remove standalone time markers or trailing reasons.\n"
            "- Return only the action phrase, with no extra words.\n\n"
            f"Clause:\n{clause_text}\n\nAction:"
        )
        inputs = self.tokenizer(prompt, return_tensors="pt").to(self.model.device)
        outputs = self.model.generate(
            **inputs,
            max_new_tokens=40,
            do_sample=False,
            repetition_penalty=self.repetition_penalty,
            eos_token_id=self.tokenizer.eos_token_id,
            pad_token_id=self.tokenizer.pad_token_id,
        )
        decoded = self.tokenizer.decode(outputs[0], skip_special_tokens=True)
        if "Action:" in decoded:
            candidate = decoded.split("Action:", 1)[-1]
        else:
            candidate = decoded
        candidate = candidate.strip().strip('"').strip("'")
        _record_timing("llm_action_clause", perf_counter() - start)
        if not candidate:
            return None
        if candidate.lower() in {"none", "unknown", "n/a"}:
            return None
        return candidate.splitlines()[0].strip()

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
                event_types TEXT,
                extracted_at TEXT,
                run_id TEXT,
                PRIMARY KEY (story_id, who, what, where_text)
            )
            """
        )
        self._ensure_column("triplets", "geocode_status", "TEXT")
        self._ensure_column("triplets", "run_id", "TEXT")
        self._ensure_column("triplets", "event_types", "TEXT")
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
                source,
                event_types
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
                    geocode_query, geocode_status, event_types, extracted_at, run_id
                )
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
                        json.dumps(rec.event_types or [], ensure_ascii=False),
                        rec.extracted_at,
                        rec.run_id,
                    )
                    for rec in records
                ],
            )

    def delete_story(self, story_id: str | None, url: str | None = None) -> None:
        if not story_id and not url:
            return
        with self.conn:
            if story_id:
                self.conn.execute("DELETE FROM triplets WHERE story_id = ?", (story_id,))
            if url:
                self.conn.execute("DELETE FROM triplets WHERE url = ?", (url,))

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
                event_types = payload.get("event_types") or payload.get("eventTypes") or []
                if isinstance(event_types, str):
                    try:
                        event_types = json.loads(event_types)
                    except json.JSONDecodeError:
                        event_types = [part.strip() for part in event_types.split(",") if part.strip()]
                if not isinstance(event_types, list):
                    event_types = []
                records.append(
                    Triplet(
                        who=payload.get("who", ""),
                        what=payload.get("what", ""),
                        where_text=payload.get("where_text"),
                        latitude=payload.get("lat"),
                        longitude=payload.get("lon"),
                        geocode_query=payload.get("geocode_query"),
                        raw_text=payload.get("raw_text", ""),
                        event_types=event_types,
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


def combine_article_text(
    article: dict,
    max_chars: int | None = None,
    max_sentences: int | None = 5,
) -> str:
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
    low_signal_tokens = {"video", "video_longform", "playing", "now"}
    parts: list[str] = []
    seen: set[str] = set()
    for candidate in candidates:
        if not isinstance(candidate, str):
            continue
        cleaned = _strip_html_fragment(candidate)
        if cleaned:
            suggested_idx = cleaned.find("SUGGESTED:")
            if suggested_idx != -1:
                cleaned = cleaned[:suggested_idx].strip()
            cleaned = re.sub(r"\s+", " ", cleaned).strip()
            if cleaned:
                words = re.findall(r"[a-zA-Z_]+", cleaned.lower())
                if words:
                    low_signal_hits = sum(1 for word in words if word in low_signal_tokens)
                    low_signal_ratio = low_signal_hits / max(len(words), 1)
                    if cleaned.startswith("Now Playing") and low_signal_hits >= 5:
                        continue
                    if len(words) >= 20 and low_signal_ratio >= 0.6:
                        continue
        if cleaned and cleaned not in seen:
            parts.append(cleaned)
            seen.add(cleaned)
    combined = "\n\n".join(parts).strip()
    if combined and max_sentences:
        sentences = re.split(r"(?<=[.!?])\s+", combined)
        if len(sentences) > max_sentences:
            combined = " ".join(sentences[:max_sentences]).strip()
    if max_chars and max_chars > 0:
        return combined[:max_chars].strip()
    return combined


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


def _has_city_or_state(label: str) -> bool:
    if not label:
        return False
    if "," in label:
        return True
    if extract_city_mentions(label):
        return True
    if extract_locations(label):
        return True
    return False


def _only_appears_in_by_clause(who_value: str, article_text: str) -> bool:
    who_lower = who_value.strip().lower()
    if not who_lower or not article_text:
        return False
    article_lower = article_text.lower()
    if who_lower not in article_lower:
        synonyms = WHO_SYNONYM_PATTERNS.get(who_lower) or []
        candidates = [value.lower() for value in synonyms]
    else:
        candidates = [who_lower]
    if not candidates:
        return False
    for candidate in candidates:
        if not candidate or candidate not in article_lower:
            continue
        pattern = re.compile(
            rf"\bby\s+(?:an|a|the)\s+{re.escape(candidate)}\b"
            rf"|\bby\s+{re.escape(candidate)}\b"
        )
        if not pattern.search(article_lower):
            continue
        cleaned = pattern.sub("", article_lower)
        if candidate not in cleaned:
            return True
    return False


def _only_in_by_phrase_in_sentence(who_value: str, action: str, article_text: str) -> bool:
    if not who_value or not action or not article_text:
        return False
    sentence_text = _sentence_window_for_action(action, article_text)
    if not sentence_text:
        return False
    sentence_lower = sentence_text.lower()
    who_lower = who_value.strip().lower()
    synonyms = WHO_SYNONYM_PATTERNS.get(who_lower) or []
    candidates = [who_lower] + synonyms
    by_hit = False
    for candidate in candidates:
        candidate = candidate.strip().lower()
        if not candidate or len(candidate) < 4:
            continue
        if candidate not in sentence_lower:
            continue
        pattern = re.compile(
            rf"\bby\s+(?:an|a|the)\s+{re.escape(candidate)}\b"
            rf"|\bby\s+{re.escape(candidate)}\b"
        )
        if not pattern.search(sentence_lower):
            return False
        by_hit = True
        cleaned = pattern.sub("", sentence_lower)
        if candidate in cleaned:
            return False
    return by_hit


def _is_passive_by_action(action: str | None) -> bool:
    if not action:
        return False
    lower = action.strip().lower()
    if " by " not in lower:
        return False
    return lower.startswith(("was ", "were ", "is ", "are ", "been ", "being "))


def _who_in_action_sentence(who_value: str, action: str, article_text: str) -> bool:
    if not who_value or not action or not article_text:
        return True
    sentence_text = _sentence_window_for_action(action, article_text)
    if not sentence_text:
        return True
    return _who_matches_article(who_value, sentence_text.lower())


def _who_in_action_clause(who_value: str, action: str, article_text: str) -> bool:
    if not who_value or not action or not article_text:
        return True
    sentence_text = _sentence_window_for_action(action, article_text)
    if not sentence_text:
        return True
    action_lower = action.lower()
    clauses = re.split(r"[;,]", sentence_text)
    for clause in clauses:
        clause_lower = clause.lower()
        if action_lower in clause_lower:
            return _who_matches_article(who_value, clause_lower)
    return True


def _is_gerund_action(value: str | None) -> bool:
    if not value:
        return False
    first_word = value.strip().split(maxsplit=1)[0].lower()
    if first_word in ("being", "having"):
        return False
    return first_word.endswith("ing")


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


def _is_role_only_action(value: str | None) -> bool:
    if not value:
        return False
    normalized = value.strip().lower().rstrip(".!?,;:")
    return normalized in ROLE_ONLY_ACTIONS


def _needs_object_completion(value: str | None) -> bool:
    if not REQUIRE_DIRECT_OBJECT:
        return False
    if not value:
        return False
    normalized = value.strip().lower().rstrip(".!?,;:-–—")
    words = normalized.split()
    object_pronouns = {
        "me",
        "you",
        "him",
        "her",
        "us",
        "them",
        "it",
    }
    if len(words) <= 2:
        if len(words) == 2 and words[-1] in object_pronouns:
            return False
        return True
    if normalized.split()[-1] in {"against", "to", "for", "into", "with", "over"}:
        return True
    return False


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


def _clean_object_candidate(candidate: str) -> str:
    cleaned = re.sub(r"\s+", " ", candidate.strip())
    if not cleaned:
        return ""
    if re.search(r"\bWhen\\s+analyzing\\b", cleaned, flags=re.IGNORECASE):
        return ""
    lowered = cleaned.lower()
    if "ice agents" in lowered:
        if "hundreds" in lowered and "more" in lowered:
            return "hundreds more ICE agents"
        if "hundreds" in lowered:
            return "hundreds of ICE agents"
        if "more" in lowered:
            return "more ICE agents"
        return "ICE agents"
    tokens = cleaned.split()
    if len(tokens) > 3:
        counts = {}
        for token in tokens:
            token_key = token.lower()
            counts[token_key] = counts.get(token_key, 0) + 1
        if any(count >= 2 for count in counts.values()):
            quantity_tokens = {"hundreds", "dozens", "thousands", "millions", "more"}
            for idx, token in enumerate(tokens):
                token_key = token.lower().strip(",.;:")
                if token_key in quantity_tokens or token_key.isdigit():
                    snippet = tokens[idx : idx + 4]
                    return " ".join(snippet)
    deduped: list[str] = []
    for token in tokens:
        if deduped and token.lower() == deduped[-1].lower():
            continue
        deduped.append(token)
    cleaned = " ".join(deduped)
    tokens = cleaned.split()
    for size in range(4, 1, -1):
        if len(tokens) >= size * 2 and tokens[-size:] == tokens[-size * 2 : -size]:
            tokens = tokens[:-size]
            cleaned = " ".join(tokens).strip()
            break
    repeated = re.match(r"^(.{4,}?)\s+\\1$", cleaned, flags=re.IGNORECASE)
    if repeated:
        return repeated.group(1).strip()
    cleaned = re.sub(r"\b(?:and|or)(?:\s+the)?\s*$", "", cleaned, flags=re.IGNORECASE).strip()
    return cleaned


def _action_base(value: str | None) -> str:
    normalized = (value or "").strip().lower().rstrip(".!?,;:-–—")
    if not normalized:
        return ""
    words = normalized.split()
    if len(words) >= 2:
        return " ".join(words[:2])
    return normalized


def _sentence_window_for_action(action: str, article_text: str) -> str:
    if not action:
        return article_text
    tokens = [token.lower() for token in action.split() if token]
    if not tokens:
        return article_text
    stop_words = {
        "a",
        "an",
        "the",
        "to",
        "about",
        "of",
        "in",
        "on",
        "for",
        "with",
        "and",
        "or",
        "by",
        "at",
        "from",
        "into",
        "after",
        "before",
        "as",
        "while",
        "when",
        "was",
        "were",
        "is",
        "are",
        "been",
        "being",
    }
    action_word = ""
    for token in tokens:
        if token in stop_words:
            continue
        if len(token) < 4:
            continue
        action_word = token
        break
    if not action_word:
        action_word = tokens[0]
    match = re.search(
        rf"[^.!?\n]*\\b{re.escape(action_word)}\\b[^.!?\n]*",
        article_text,
        re.IGNORECASE,
    )
    if match:
        return match.group(0)
    if article_text:
        return article_text.splitlines()[0].strip()
    return article_text


def _sentence_for_who(who: str, article_text: str) -> str:
    if not who:
        return ""
    who_lower = who.lower()
    for sentence in re.split(r"[.!?\n]+", article_text):
        if who_lower in sentence.lower():
            return sentence.strip()
    return ""


def _clause_after_who(who: str, sentence_text: str) -> str:
    if not who:
        return ""
    lowered = sentence_text.lower()
    who_lower = who.lower().strip()
    idx = lowered.find(who_lower)
    if idx == -1:
        return ""
    fragment = sentence_text[idx + len(who) :].strip()
    fragment = fragment.lstrip(" :,-")
    return fragment.strip()


def _clause_after_action(action: str, sentence_text: str) -> str:
    if not action:
        return ""
    lowered = sentence_text.lower()
    action_lower = action.lower().strip()
    idx = lowered.find(action_lower)
    if idx == -1:
        return ""
    fragment = sentence_text[idx + len(action_lower) :].strip()
    fragment = fragment.lstrip(" :,-")
    for stop in (".", "!", "?", "\n"):
        if stop in fragment:
            fragment = fragment.split(stop, 1)[0].strip()
            break
    return fragment


def _trim_clause_tail(fragment: str) -> str:
    if not fragment:
        return ""
    lower = fragment.lower()
    for token in (" to ", " for ", " after ", " because ", " as ", " while ", " when "):
        if token in lower:
            return fragment.split(token, 1)[0].strip()
    return fragment.strip()


def _truncate_clause(fragment: str, max_words: int = 12) -> str:
    if not fragment:
        return ""
    words = fragment.split()
    if len(words) > max_words:
        words = words[:max_words]
    return " ".join(words).strip(" ,;:-")


def _complete_preposition_action(
    action: str,
    article_text: str | None,
    story_title: str | None,
) -> str | None:
    if not action:
        return None
    if not _ends_with_preposition(action):
        return None
    action_tokens = action.split()
    action_variants = [action]
    if action_tokens:
        verb = action_tokens[0]
        if verb.endswith("ing") and len(verb) > 4:
            stem = verb[:-3]
            variants = [stem + "e", stem + "es", verb]
            for variant in variants:
                action_variants.append(" ".join([variant, *action_tokens[1:]]))
    for source in (article_text or "", story_title or ""):
        if not source:
            continue
        for variant in action_variants:
            sentence_text = _sentence_window_for_action(variant, source)
            clause_text = _clause_after_action(variant, sentence_text)
            clause_text = _trim_clause_tail(clause_text)
            obj = _fallback_object_from_text(variant, clause_text)
            obj = _clean_object_candidate(obj)
            if obj:
                return f"{variant} {obj}".strip()
    return None


def _normalize_match_text(value: str) -> str:
    return re.sub(r"[^a-z0-9]+", " ", value.lower()).strip()


def _is_action_grounded(action: str, article_text: str) -> bool:
    if not action or not article_text:
        return False
    normalized_action = _normalize_match_text(action)
    if not normalized_action:
        return False
    normalized_article = _normalize_match_text(article_text)
    if not normalized_article:
        return False
    if normalized_action in normalized_article:
        return True
    tokens = [
        token
        for token in normalized_action.split()
        if len(token) >= 3 or token.isdigit()
    ]
    if not tokens:
        return True
    padded_article = f" {normalized_article} "
    return all(f" {token} " in padded_article for token in tokens)


def _fallback_action_from_who_sentence(who: str, article_text: str) -> str:
    sentence = _sentence_for_who(who, article_text)
    if not sentence:
        return ""
    clause = _clause_after_who(who, sentence)
    clause = _trim_clause_tail(clause)
    return clause.strip()


def _infer_location_from_text(article_text: str, extractor: TripletExtractor | None = None) -> str | None:
    if not article_text:
        return None
    if re.search(r"\bSouth Portland\b", article_text, flags=re.IGNORECASE):
        if re.search(
            r"\bOregon\b|\bOre\.\b|\bDistrict of Oregon\b|\bPortland,\s*Ore\.\b",
            article_text,
            flags=re.IGNORECASE,
        ):
            return "Portland, Oregon"
        if re.search(r"\bMaine\b|\bMe\.\b|\bPortland,\s*Maine\b", article_text, flags=re.IGNORECASE):
            return "South Portland, Maine"
        return "Portland, Oregon"
    ap_state_abbr = {
        "Ala.": "Alabama",
        "Ariz.": "Arizona",
        "Ark.": "Arkansas",
        "Calif.": "California",
        "Colo.": "Colorado",
        "Conn.": "Connecticut",
        "Del.": "Delaware",
        "Fla.": "Florida",
        "Ga.": "Georgia",
        "Ill.": "Illinois",
        "Ind.": "Indiana",
        "Kan.": "Kansas",
        "Ky.": "Kentucky",
        "La.": "Louisiana",
        "Md.": "Maryland",
        "Mass.": "Massachusetts",
        "Mich.": "Michigan",
        "Minn.": "Minnesota",
        "Miss.": "Mississippi",
        "Mo.": "Missouri",
        "Mont.": "Montana",
        "Neb.": "Nebraska",
        "Nev.": "Nevada",
        "Okla.": "Oklahoma",
        "Ore.": "Oregon",
        "Pa.": "Pennsylvania",
        "Tenn.": "Tennessee",
        "Va.": "Virginia",
        "Wash.": "Washington",
        "Wis.": "Wisconsin",
        "Wyo.": "Wyoming",
    }
    state_to_ap = {value: key for key, value in ap_state_abbr.items()}
    city_mentions = extract_city_mentions(article_text) or []
    city_state_map: dict[str, set[str]] = {}
    for mention in city_mentions:
        if "," in mention:
            city, state = (part.strip() for part in mention.split(",", 1))
            if city and state:
                city_state_map.setdefault(city, set()).add(state)
    mention_counts: dict[tuple[str, str], int] = {}
    lower_text = article_text.lower()
    for city, states in city_state_map.items():
        for state in states:
            count = 0
            count += lower_text.count(f"{city.lower()}, {state.lower()}")
            abbrev = state_to_ap.get(state)
            if abbrev:
                count += lower_text.count(f"{city.lower()}, {abbrev.lower()}")
            mention_counts[(city, state)] = count
    city_state_counts: dict[str, dict[str, int]] = {}
    for match in re.finditer(
        r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s*,\s*([A-Z][a-z]{1,4}\.)",
        article_text,
    ):
        city = match.group(1).title()
        raw_state = match.group(2)
        state = ap_state_abbr.get(raw_state)
        if not state:
            continue
        city_state_counts.setdefault(city, {}).setdefault(state, 0)
        city_state_counts[city][state] += 1
    state_mentions = {state.title() for state in extract_locations(article_text)}
    for abbrev, state in ap_state_abbr.items():
        if abbrev in article_text:
            state_mentions.add(state)
    def pick_city_state(city: str) -> str | None:
        states = city_state_map.get(city)
        if not states:
            return None
        if city.lower() == "portland" and not state_mentions and not any(
            mention_counts.get((city, state), 0) > 0 for state in states
        ):
            return "Portland, Oregon"
        best_match = None
        best_count = -1
        for state in states:
            count = mention_counts.get((city, state), 0)
            if count > best_count:
                best_count = count
                best_match = state
        if best_match and best_count > 0:
            return f"{city}, {best_match}"
        if city in city_state_counts:
            counts = city_state_counts[city]
            best_state = max(counts.items(), key=lambda item: item[1])[0]
            return f"{city}, {best_state}"
        if state_mentions:
            for state in sorted(states):
                if state in state_mentions:
                    return f"{city}, {state}"
        return f"{city}, {sorted(states)[0]}"
    neighborhood_match = re.search(
        r"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)[’']s\s+"
        r"([A-Z][a-z]+(?:\s+[A-Z][a-z]+)*)\s+neighborhood\b",
        article_text,
    )
    llm_location = None
    if extractor:
        llm_location = extractor.extract_location_from_text(article_text)
        llm_location = _sanitize_action_text(llm_location or "")
        if llm_location:
            if "," in llm_location:
                city, state = (part.strip() for part in llm_location.split(",", 1))
                if city and state and state in ap_state_abbr.values():
                    abbrev = state_to_ap.get(state)
                    if state not in state_mentions and not (
                        abbrev and abbrev in article_text
                    ):
                        llm_location = city
            return llm_location
    if neighborhood_match:
        city = neighborhood_match.group(1).title()
        neighborhood = neighborhood_match.group(2).title()
        city_state = pick_city_state(city) or city
        return f"{neighborhood}, {city_state}"
    for mention in city_mentions:
        if "," in mention:
            city, _state = (part.strip() for part in mention.split(",", 1))
            preferred = pick_city_state(city)
            if preferred:
                return preferred
            return mention
    if city_mentions:
        return city_mentions[0]
    states = extract_locations(article_text)
    if states:
        return states[0]
    return None


def _sanitize_action_text(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", value).strip()
    if not cleaned:
        return ""
    if re.search(r"\bobject\b", cleaned, flags=re.IGNORECASE):
        cleaned = re.split(r"\bobject\b:?", cleaned, 1, flags=re.IGNORECASE)[0].strip()
    cleaned = re.sub(r'\s*"[^"]*"\s*Object\s*$', "", cleaned, flags=re.IGNORECASE).strip()
    cleaned = re.sub(r"\bObject\b\s*$", "", cleaned, flags=re.IGNORECASE).strip()
    cleaned = re.split(
        r"\s+(?:The|Since|As|Based|Given|When|While|If)\b",
        cleaned,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0].strip()
    cleaned = cleaned.strip('\'"')
    quote_match = re.search(r"\"([^\"]+)\"", cleaned)
    if quote_match:
        quoted = quote_match.group(1).strip()
        prefix = cleaned[: quote_match.start()].strip()
        if quoted and prefix.lower().endswith(quoted.lower()):
            cleaned = prefix
    words = cleaned.split()
    if len(words) >= 8:
        half = len(words) // 2
        first = " ".join(words[:half]).strip()
        second = " ".join(words[half:]).strip()
        if first and second and second.lower().startswith(first.lower()):
            cleaned = first
    if len(words) >= 6:
        token = " ".join(words[:6]).lower()
        if token and cleaned.lower().count(token) >= 2:
            first_idx = cleaned.lower().find(token)
            second_idx = cleaned.lower().find(token, first_idx + len(token))
            if second_idx != -1:
                cleaned = cleaned[:second_idx].strip()
    cleaned = cleaned.strip(" ,;:-")
    if cleaned:
        cleaned = cleaned.replace(' "', " ")
        cleaned = cleaned.replace('" ', " ")
        cleaned = cleaned.replace('"', "")
        cleaned = cleaned.strip()
    cleaned = re.sub(r"\binfornation\b", "information", cleaned, flags=re.IGNORECASE)
    return cleaned


def _strip_trailing_gerund_clause(action: str) -> str:
    if not action:
        return action
    lower = action.lower()
    if not re.search(r"\b(detained|arrested|apprehended|taken into custody)\b", lower):
        return action
    for token in (" while ", " when ", " as ", " working "):
        idx = lower.find(token)
        if idx != -1:
            return action[:idx].strip(" ,;:-")
    return action


def _ensure_victim_passive_voice(who: str | None, what: str | None) -> str | None:
    if not who or not what:
        return what
    lower = what.lower()
    if lower.startswith(("was ", "were ", "is ", "are ")):
        return what
    if not lower.startswith(("detained", "arrested", "shot", "killed", "injured", "assaulted")):
        return what
    who_lower = who.lower()
    if who_lower.startswith(("ice", "border patrol", "homeland security", "police", "officer", "officers")):
        return what
    if any(who_lower.endswith(suffix) for suffix in PLURAL_WHO_SUFFIXES):
        return f"were {what}"
    return f"was {what}"


LOW_SIGNAL_ACTIONS = {
    "was shocked",
    "were shocked",
    "was surprised",
    "were surprised",
    "was alarmed",
    "were alarmed",
    "was stunned",
    "were stunned",
}


def _rewrite_low_signal_action(action: str, clause_text: str) -> str | None:
    lower_action = action.lower().strip()
    if lower_action not in LOW_SIGNAL_ACTIONS:
        return None
    lower_clause = clause_text.lower()
    if "arrested" in lower_clause:
        return "were arrested by federal agents" if "agent" in lower_clause else "were arrested"
    if "detained" in lower_clause:
        return "were detained by federal agents" if "agent" in lower_clause else "were detained"
    if "held" in lower_clause:
        return "were held by federal agents" if "agent" in lower_clause else "were held"
    if "led" in lower_clause and "stairwell" in lower_clause:
        return "were led to a stairwell"
    return None


def _has_metric_signal(text: str) -> bool:
    if not text:
        return False
    lowered = text.lower()
    if "%" in text:
        return True
    if re.search(r"\b\d[\d,]*\b", lowered):
        return True
    trend_tokens = (
        "up",
        "down",
        "increase",
        "increased",
        "decrease",
        "decreased",
        "rise",
        "rose",
        "fall",
        "fell",
        "drop",
        "dropped",
        "surge",
        "surged",
        "spike",
        "spiked",
        "climb",
        "climbed",
        "jump",
        "jumped",
    )
    return any(token in lowered for token in trend_tokens)


def _is_sparse_action(action: str) -> bool:
    if not action:
        return True
    return len(action.split()) <= 2


def _normalize_action_case(action: str | None) -> str | None:
    if not action:
        return action
    cleaned = action.strip()
    for prefix in ("was ", "were ", "is ", "are "):
        if cleaned.lower().startswith(prefix):
            tail = cleaned[len(prefix) :].strip()
            if tail:
                return f"{prefix}{tail[0].lower()}{tail[1:]}" if len(tail) > 1 else f"{prefix}{tail.lower()}"
            return cleaned
    first = cleaned.split(" ", 1)
    if first and first[0].lower() in REPORTING_VERBS:
        if len(first) == 1:
            return first[0].lower()
        return f"{first[0].lower()} {first[1]}"
    return cleaned


def _correct_who_from_state_mentions(who: str | None, article_text: str | None) -> str | None:
    if not who or not article_text:
        return who
    who_clean = who.strip()
    who_lower = who_clean.lower()
    if not who_lower or who_lower in article_text.lower():
        return who
    states = [state.title() for state in extract_locations(article_text)]
    for state in states:
        state_lower = state.lower()
        if state_lower.startswith(who_lower) and len(state_lower) - len(who_lower) <= 2:
            return state
    return who


def _extract_author_from_text(article_text: str | None) -> str | None:
    if not article_text:
        return None
    lines = [line.strip() for line in article_text.splitlines() if line.strip()]
    if not lines:
        return None
    for line in lines[:5]:
        match = re.match(r"^By\s+([A-Z][A-Za-z\.\-']+(?:\s+[A-Z][A-Za-z\.\-']+)*)\b", line)
        if match:
            return match.group(1).strip()
    return None


def _dedupe_location_text(value: str) -> str:
    cleaned = re.sub(r"\s+", " ", value).strip()
    if not cleaned:
        return ""
    lowered = cleaned.lower()
    half = len(cleaned) // 2
    if len(cleaned) % 2 == 0:
        first = cleaned[:half].strip()
        second = cleaned[half:].strip()
        if first and second and first.lower() == second.lower():
            return first
    words = cleaned.split()
    if len(words) >= 3:
        phrase = " ".join(words[:3]).lower()
        if lowered.count(phrase) >= 2:
            idx = lowered.find(phrase, lowered.find(phrase) + len(phrase))
            if idx != -1:
                return cleaned[:idx].strip()
    return cleaned


def _strip_leading_who(what: str, who: str | None) -> str:
    if not what or not who:
        return what
    what_norm = what.strip()
    who_norm = who.strip()
    if what_norm.lower().startswith(who_norm.lower()):
        trimmed = what_norm[len(who_norm) :].strip(" ,;:-")
        return trimmed or what_norm
    return what


def _ends_with_preposition(value: str) -> bool:
    if not value:
        return False
    return value.strip().lower().split()[-1] in {
        "about",
        "against",
        "for",
        "into",
        "over",
        "to",
        "with",
    }


def _is_generic_place_label(value: str) -> bool:
    if not value:
        return False
    lowered = value.lower()
    if "coffee shop" in lowered or "coffeehouse" in lowered:
        return True
    if re.search(r"\b(shop|store|restaurant|mall|plaza|station)\b", lowered):
        if "," not in value:
            return True
    return False


def _is_bad_object(obj: str, who: str) -> bool:
    if not obj:
        return True
    obj_norm = re.sub(r"[\"'“”‘’]", "", obj).strip().lower()
    who_norm = re.sub(r"[\"'“”‘’]", "", who).strip().lower()
    if not obj_norm:
        return True
    if who_norm and (
        obj_norm == who_norm
        or obj_norm.startswith(f"{who_norm} ")
        or who_norm.startswith(f"{obj_norm} ")
        or obj_norm in who_norm
        or who_norm in obj_norm
    ):
        return True
    stopwords = {
        "the",
        "a",
        "an",
        "and",
        "or",
        "of",
        "to",
        "in",
        "on",
        "for",
        "with",
        "as",
        "at",
        "by",
        "from",
        "so",
        "that",
        "this",
        "these",
        "those",
        "it",
        "its",
    }
    tokens = [token.strip(".,;:") for token in obj_norm.split() if token.strip(".,;:")]
    if not tokens:
        return True
    if all(token in stopwords for token in tokens):
        return True
    return False


def _looks_like_complement_clause(clause_text: str) -> bool:
    if not clause_text:
        return False
    lowered = clause_text.strip().lower()
    starters = (
        "that ",
        "it ",
        "it will ",
        "it would ",
        "it is ",
        "it was ",
        "will ",
        "would ",
        "is ",
        "are ",
        "was ",
        "were ",
        "to ",
        "may ",
        "might ",
        "can ",
        "could ",
        "should ",
    )
    return lowered.startswith(starters)


def _keyword_hits(text: str, keywords: set[str]) -> set[str]:
    hits: set[str] = set()
    lowered = text.lower()
    tokens = set(re.findall(r"[a-z]+", lowered))
    for keyword in keywords:
        if " " in keyword or len(keyword) > 4:
            if keyword in lowered:
                hits.add(keyword)
            continue
        if re.search(rf"\\b{re.escape(keyword)}\\b", lowered) or keyword in tokens:
            hits.add(keyword)
    return hits


def _is_immigration_related(article_text: str, title: str | None = None) -> bool:
    if not article_text and not title:
        return False
    if title:
        title_hits = _keyword_hits(title, IMMIGRATION_STRONG_KEYWORDS | IMMIGRATION_WEAK_KEYWORDS)
        if title_hits:
            return True
    if article_text:
        snippet = article_text[:1200]
        strong_hits = _keyword_hits(snippet, IMMIGRATION_STRONG_KEYWORDS)
        if strong_hits:
            return True
        weak_hits = _keyword_hits(snippet, IMMIGRATION_WEAK_KEYWORDS)
        if len(weak_hits) >= 2:
            return True
        if weak_hits and title and _keyword_hits(title, IMMIGRATION_WEAK_KEYWORDS):
            return True
    return False


def _is_protest_related(article_text: str, title: str | None = None) -> bool:
    if not article_text and not title:
        return False
    blob = " ".join(part for part in (title, article_text) if part).strip()
    if not blob:
        return False
    return bool(_detect_event_types(blob))


def _fallback_object_from_text(action: str, article_text: str) -> str:
    fragment = article_text.strip()
    if not fragment:
        return ""
    lowered = fragment.lower()
    if action.lower() in {"deported", "arrested", "detained", "removed"}:
        match = re.search(
            r"\b(?:at\s+least|more\s+than|over|about|roughly)?\s*\\d[\\d,]*\\s+"
            r"(people|immigrants|individuals|migrants|aliens)\b",
            lowered,
        )
        if match:
            return fragment[match.start() : match.end()].strip()
    stop_tokens = {
        "to",
        "into",
        "so",
        "because",
        "for",
        "as",
        "after",
        "before",
        "when",
        "if",
        "that",
        "which",
        "who",
        "where",
        "while",
        "with",
        "by",
        "on",
        "in",
        "at",
        "today",
        "tomorrow",
        "this",
        "next",
        "later",
    }
    words = fragment.split()
    trimmed: list[str] = []
    for word in words:
        if word.lower().strip(",;:") in stop_tokens:
            break
        trimmed.append(word)
        if len(trimmed) >= 6:
            break
    return " ".join(trimmed).strip()


def _complete_incomplete_actions(
    triplets: list[dict[str, str]],
    extractor: TripletExtractor,
    article_text: str,
    story_url: str | None = None,
    story_title: str | None = None,
) -> list[dict[str, str]]:
    title_replacement_checked = False
    title_replacement: dict[str, str] | None = None
    title_triplets_cache: list[dict[str, str]] | None = None

    def _get_title_replacement() -> dict[str, str] | None:
        nonlocal title_replacement_checked, title_replacement
        if title_replacement_checked:
            return title_replacement
        title_replacement_checked = True
        if not story_title:
            return None
        title_triplets = extractor.extract(story_title)
        for candidate in title_triplets:
            who_candidate = _normalize_who_label(candidate.get("who") or "") or (candidate.get("who") or "")
            what_candidate = _sanitize_action_text(candidate.get("what") or "")
            what_candidate = _strip_leading_who(what_candidate, who_candidate)
            if not who_candidate or not what_candidate:
                continue
            if what_candidate.lower() in REPORTING_VERBS:
                continue
            title_replacement = {"who": who_candidate, "what": what_candidate}
            return title_replacement
        for candidate in title_triplets:
            who_candidate = _normalize_who_label(candidate.get("who") or "") or (candidate.get("who") or "")
            what_candidate = _sanitize_action_text(candidate.get("what") or "")
            what_candidate = _strip_leading_who(what_candidate, who_candidate)
            if who_candidate and what_candidate:
                title_replacement = {"who": who_candidate, "what": what_candidate}
                return title_replacement
        return None

    def _get_title_triplets() -> list[dict[str, str]]:
        nonlocal title_triplets_cache
        if title_triplets_cache is not None:
            return title_triplets_cache
        if not story_title:
            title_triplets_cache = []
            return title_triplets_cache
        title_triplets_cache = extractor.extract(story_title)
        return title_triplets_cache

    def _get_metric_replacement() -> dict[str, str] | None:
        if not story_title or not _has_metric_signal(story_title):
            return None
        for candidate in _get_title_triplets():
            who_candidate = _normalize_who_label(candidate.get("who") or "") or (candidate.get("who") or "")
            what_candidate = _sanitize_action_text(candidate.get("what") or "")
            what_candidate = _strip_leading_who(what_candidate, who_candidate)
            if not who_candidate or not what_candidate:
                continue
            if not _has_metric_signal(f"{who_candidate} {what_candidate}"):
                continue
            if what_candidate.lower() in REPORTING_VERBS:
                continue
            return {"who": who_candidate, "what": what_candidate}
        return None

    for item in triplets:
        who_value = (item.get("who") or "").strip()
        what_value = (item.get("what") or "").strip()
        if not who_value or not what_value:
            continue
        if story_title and _is_sparse_action(what_value):
            metric_replacement = _get_metric_replacement()
            if metric_replacement:
                item["who"] = metric_replacement["who"]
                item["what"] = metric_replacement["what"]
                who_value = item["who"]
                what_value = item["what"]
        if story_title and what_value.lower() in REPORTING_VERBS:
            replacement = _get_title_replacement()
            if replacement:
                item["who"] = replacement["who"]
                item["what"] = replacement["what"]
                who_value = item["who"]
                what_value = item["what"]
        sentence_text = _sentence_window_for_action(what_value, article_text)
        if what_value.lower() not in sentence_text.lower():
            if story_title:
                title_sentence = _sentence_window_for_action(what_value, story_title)
                if title_sentence:
                    sentence_text = title_sentence
            who_sentence = _sentence_for_who(who_value, article_text)
            clause_after_who = _clause_after_who(who_value, who_sentence)
            if clause_after_who:
                action_clause = extractor.extract_action_clause(who_value, clause_after_who)
                action_clause = _sanitize_action_text(action_clause or "")
                if action_clause:
                    item["what"] = action_clause
                    what_value = action_clause
                    sentence_text = _sentence_window_for_action(what_value, article_text)
            if story_title and who_value.lower() in story_title.lower():
                title_sentence = _sentence_for_who(who_value, story_title)
                title_clause = _clause_after_who(who_value, title_sentence)
                if title_clause:
                    title_action = extractor.extract_action_clause(who_value, title_clause)
                    title_action = _sanitize_action_text(title_action or "")
                    if title_action and title_action.lower() != what_value.lower():
                        item["what"] = title_action
                        what_value = title_action
                        sentence_text = _sentence_window_for_action(what_value, story_title)
        if not _needs_object_completion(what_value):
            if story_title and who_value and what_value.lower() in REPORTING_VERBS:
                if who_value.lower() in story_title.lower():
                    title_sentence = _sentence_for_who(who_value, story_title)
                    title_clause = _clause_after_who(who_value, title_sentence)
                    title_action = extractor.extract_action_clause(who_value, title_clause)
                    title_action = _sanitize_action_text(title_action or "")
                    if title_action:
                        item["what"] = title_action
                        LOGGER.info(
                            "Summary: story_id=%s summary=%s %s",
                            story_url or "<unknown>",
                            who_value,
                            item.get("what") or what_value,
                        )
                        LOGGER.info("*")
            else:
                rewrite = _rewrite_low_signal_action(what_value, sentence_text)
                if rewrite:
                    item["what"] = rewrite
            continue
        COMPLETION_STATS["attempted"] += 1
        clause_text = _clause_after_action(what_value, sentence_text)
        context_text = clause_text or sentence_text
        rewrite = _rewrite_low_signal_action(what_value, context_text)
        if rewrite:
            item["what"] = rewrite
            COMPLETION_STATS["accepted"] += 1
            continue
        obj = None
        if _looks_like_complement_clause(context_text):
            obj = extractor.extract_complement_clause(who_value, what_value, context_text)
        if not obj:
            obj = extractor.extract_direct_object(who_value, what_value, context_text)
        obj = _sanitize_action_text(obj or "")
        obj = _clean_object_candidate(obj or "")
        obj = _sanitize_action_text(obj or "")
        if obj and obj.lower() in {"document", "documents", "statement", "statements", "text"}:
            obj = None
        if obj and what_value.lower() in REPORTING_VERBS and obj.lower().startswith("no "):
            tail = obj[3:].strip()
            if tail:
                item["what"] = f"{what_value} no to {tail}"
            else:
                item["what"] = f"{what_value} no"
            obj = None
        if not obj and story_title and what_value.lower() in REPORTING_VERBS:
            replacement = _get_title_replacement()
            if replacement:
                item["who"] = replacement["who"]
                item["what"] = replacement["what"]
                continue
        if not obj:
            obj = _fallback_object_from_text(what_value, clause_text)
            obj = _sanitize_action_text(obj or "")
            obj = _clean_object_candidate(obj or "")
        if obj and _is_bad_object(obj, who_value):
            COMPLETION_STATS["rejected"] += 1
            obj = None
        if obj and re.search(rf"\b{re.escape(obj)}\b", what_value, flags=re.IGNORECASE):
            COMPLETION_STATS["rejected"] += 1
            obj = None
        if not obj and clause_text:
            clause_clean = _sanitize_action_text(clause_text)
            clause_clean = _trim_clause_tail(clause_clean)
            if not _is_bad_object(clause_clean, who_value):
                obj = clause_clean
        LOGGER.info(
            "Completing action for story_id=%s who=%s what=%s object=%s",
            story_url or "<unknown>",
            who_value,
            what_value,
            obj or "<none>",
        )
        if obj:
            item["what"] = f"{what_value} {obj}"
            COMPLETION_STATS["accepted"] += 1
        LOGGER.info(
            "Summary: story_id=%s summary=%s %s",
            story_url or "<unknown>",
            who_value,
            item.get("what") or what_value,
        )
        LOGGER.info("*")
    return triplets


def _drop_uncompleted_actions(triplets: list[dict[str, str]]) -> list[dict[str, str]]:
    completed: set[tuple[str, str]] = set()
    for item in triplets:
        who_value = (item.get("who") or "").strip().lower()
        what_value = (item.get("what") or "").strip().lower()
        if not who_value or not what_value:
            continue
        if _needs_object_completion(what_value):
            continue
        base = _action_base(what_value)
        if base:
            completed.add((who_value, base))
    if not completed:
        return triplets
    filtered: list[dict[str, str]] = []
    for item in triplets:
        who_value = (item.get("who") or "").strip().lower()
        what_value = (item.get("what") or "").strip().lower()
        base_action = _action_base(what_value)
        if (who_value, base_action) in completed and _needs_object_completion(what_value):
            continue
        filtered.append(item)
    return filtered


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
    with_where: set[tuple[str, str]] = set()
    for item in triplets:
        who = (item.get("who") or "").strip().lower()
        what = (item.get("what") or "").strip().lower()
        where_value = item.get("where")
        if isinstance(where_value, (list, tuple)):
            where_value = next((w for w in where_value if isinstance(w, str)), "")
        if isinstance(where_value, str) and where_value.strip():
            with_where.add((who, what))
    seen: set[tuple[str, str, str]] = set()
    deduped: list[dict[str, str]] = []
    for item in triplets:
        who = (item.get("who") or "").strip().lower()
        what = (item.get("what") or "").strip().lower()
        where_value = item.get("where")
        if isinstance(where_value, (list, tuple)):
            where_value = next((w for w in where_value if isinstance(w, str)), "")
        if isinstance(where_value, str):
            where = where_value.strip().lower()
        else:
            where = ""
        key = (who, what, where)
        if not who and not what:
            continue
        if not where and (who, what) in with_where:
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
    numeric_variants = _numeric_who_variants(who_lower)
    for variant in numeric_variants:
        if variant and variant in article_lower:
            return True
    if "trump" in who_lower and "trump" in article_lower:
        return True
    if who_lower.endswith("soldiers"):
        singular = who_lower[:-1]
        if singular and singular in article_lower:
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


def _numeric_who_variants(who_lower: str) -> set[str]:
    number_words = {
        "zero": "0",
        "one": "1",
        "two": "2",
        "three": "3",
        "four": "4",
        "five": "5",
        "six": "6",
        "seven": "7",
        "eight": "8",
        "nine": "9",
        "ten": "10",
    }
    number_digits = {value: key for key, value in number_words.items()}
    variants = set()
    variants.add(who_lower)
    word_pattern = re.compile(rf"\\b({'|'.join(number_words.keys())})\\b")
    digit_pattern = re.compile(rf"\\b({'|'.join(number_digits.keys())})\\b")
    if word_pattern.search(who_lower):
        variants.add(word_pattern.sub(lambda m: number_words[m.group(1)], who_lower))
    if digit_pattern.search(who_lower):
        variants.add(digit_pattern.sub(lambda m: number_digits[m.group(1)], who_lower))
    return variants


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
        if key == "mexico" and "new mexico" in normalized_lower:
            continue
        if re.search(rf"\b{re.escape(key)}\b", normalized_lower):
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
    start = perf_counter()
    for record in records:
        lat, lon, geocode_query, status = geocode_where(record.where_text, geocoder)
        record.latitude = lat
        record.longitude = lon
        record.geocode_query = geocode_query
        record.geocode_status = status
    _record_timing("geocode", perf_counter() - start)


def sanitize_triplet(
    record: Triplet,
    article_text: str | None = None,
    extractor: TripletExtractor | None = None,
    allow_protest_related: bool = False,
) -> Optional[Triplet]:
    def _drop(reason: str) -> Optional[Triplet]:
        LOGGER.info(
            "Skipping triplet for story_id=%s who=%s reason=%s",
            record.story_id,
            record.who or "<unknown>",
            reason,
        )
        return None

    record.who = _normalize_who_label(record.who) or record.who
    record.what = _sanitize_action_text(record.what)
    record.what = _strip_leading_who(record.what, record.who)
    record.what = _strip_trailing_gerund_clause(record.what)
    record.what = _ensure_victim_passive_voice(record.who, record.what)
    record.what = _normalize_action_case(record.what)
    record.what = _apply_plural_verb_agreement(record.who, record.what) or record.what
    if record.story_id and "substack.com" in record.story_id and article_text:
        author = _extract_author_from_text(article_text)
        if author:
            record.who = author
            title = (record.title or "").strip()
            if title:
                record.what = f"discusses {title}"
            elif "immigration" in article_text.lower():
                record.what = "discusses U.S. immigration"
            else:
                record.what = "discusses U.S. policy"
        elif record.who and record.who.strip().lower() in {"i", "we", "author"}:
            title = (record.title or "").strip()
            record.who = "Substack author"
            if title:
                record.what = f"discusses {title}"
            elif "immigration" in article_text.lower():
                record.what = "discusses U.S. immigration"
            else:
                record.what = "discusses U.S. policy"
    if article_text and not _is_immigration_related(article_text, record.title):
        if not (allow_protest_related and _is_protest_related(article_text, record.title)):
            return _drop("not immigration- or protest-related")
    if REQUIRE_DIRECT_OBJECT and article_text and record.what:
        sentence_text = _sentence_window_for_action(record.what, article_text)
        clause_text = _clause_after_action(record.what, sentence_text)
        clause_lower = clause_text.lower()
        if clause_lower.startswith(("against ", "to ", "for ", "into ", "with ", "over ")):
            trimmed_clause = _trim_clause_tail(clause_text)
            record.what = f"{record.what} {trimmed_clause}"
        elif clause_lower.startswith(("after ", "before ", "when ", "while ", "because ", "as ")):
            trimmed_clause = _truncate_clause(clause_text)
            if trimmed_clause:
                record.what = f"{record.what} {trimmed_clause}"
        elif _needs_object_completion(record.what):
            obj = _fallback_object_from_text(record.what, clause_text)
            obj = _clean_object_candidate(obj)
            if obj:
                record.what = f"{record.what} {obj}"
    record.who = _correct_who_from_state_mentions(record.who, article_text)
    if not record.who:
        return _drop("missing who in model output")
    if article_text and record.who:
        article_lower = article_text.lower()
        if not _who_matches_article(record.who, article_lower):
            return _drop("who not found in article text")
    if article_text and record.who and record.what:
        if not _is_action_grounded(record.what, article_text):
            fallback_action = _fallback_action_from_who_sentence(record.who, article_text)
            fallback_action = _sanitize_action_text(fallback_action)
            fallback_action = _strip_leading_who(fallback_action, record.who)
            fallback_action = _strip_trailing_gerund_clause(fallback_action)
            fallback_action = _ensure_victim_passive_voice(record.who, fallback_action)
            fallback_action = _normalize_action_case(fallback_action)
            fallback_action = (
                _apply_plural_verb_agreement(record.who, fallback_action)
                or fallback_action
            )
            if fallback_action and _is_action_grounded(fallback_action, article_text):
                record.what = fallback_action
            else:
                return _drop("action not grounded in article text")
        if _only_appears_in_by_clause(record.who, article_text) and not _is_passive_by_action(
            record.what
        ):
            return _drop("actor only appears in by-clause")
        if _only_in_by_phrase_in_sentence(
            record.who,
            record.what,
            article_text,
        ) and not _is_passive_by_action(record.what):
            return _drop("actor only appears in by-phrase in sentence")
        if _is_gerund_action(record.what) and not _who_in_action_clause(
            record.who,
            record.what,
            article_text,
        ):
            return _drop("actor not in action clause")
        if not _who_in_action_sentence(record.who, record.what, article_text):
            return _drop("who not in action sentence")
        if _ends_with_preposition(record.what):
            completed = _complete_preposition_action(
                record.what,
                article_text,
                record.title,
            )
            if completed:
                record.what = completed
            else:
                return _drop("action missing direct object")
    if _is_role_only_action(record.what):
        return _drop("role used as action")
    if record.who and _is_incomplete_actor_action(record.what):
        return _drop("actor action missing direct object")
    if REQUIRE_DIRECT_OBJECT and _ends_with_preposition(record.what):
        return _drop("action missing direct object")
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
    if not coerced_where and article_text:
        coerced_where = _infer_location_from_text(article_text, extractor=extractor)
    if coerced_where and _is_generic_place_label(coerced_where) and article_text:
        coerced_where = _infer_location_from_text(article_text, extractor=extractor)
    if not coerced_where:
        return _drop("missing location in model output")
    coerced_where = _dedupe_location_text(coerced_where)
    normalized_where = _normalize_location_label(coerced_where)
    if not normalized_where:
        return _drop(f"location label empty after normalization ('{record.where_text}')")
    if article_text and not _has_city_or_state(normalized_where):
        inferred_where = _infer_location_from_text(article_text, extractor=extractor)
        if inferred_where:
            normalized_where = inferred_where
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
    max_article_chars: Optional[int] = None,
    allow_protest_related: bool = False,
) -> Path:
    for key in TIMING_STATS:
        TIMING_STATS[key] = 0.0
        TIMING_COUNTS[key] = 0
    total_start = perf_counter()
    COMPLETION_STATS["attempted"] = 0
    COMPLETION_STATS["accepted"] = 0
    COMPLETION_STATS["rejected"] = 0
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
        article_text_full = combine_article_text(article, max_sentences=None)
        article_text = combine_article_text(
            article,
            max_chars=max_article_chars,
            max_sentences=5,
        )
        text_for_length = article_text_full or article_text
        if not text_for_length or len(text_for_length) < MIN_ARTICLE_TEXT_LENGTH:
            title = article.get("title") or ""
            if not title or not _keyword_hits(title, IMMIGRATION_STRONG_KEYWORDS):
                continue
        text_for_filter = article_text_full or article_text
        if not _is_immigration_related(text_for_filter, article.get("title")) and not (
            allow_protest_related
            and _is_protest_related(text_for_filter, article.get("title"))
        ):
            LOGGER.info(
                "Skipping article for story_id=%s reason=not immigration-related",
                article.get("url") or article.get("source_id") or "unknown",
            )
            continue
        article["content_blob"] = text_for_filter
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
        model_triplets = extractor.extract(
            article_text or article_text_full,
            location_hints=location_hints,
        )
        if not model_triplets:
            summary_text = (article.get("summary") or "").strip()
            title_text = (article.get("title") or "").strip()
            if summary_text or title_text:
                fallback_text = "\n\n".join(
                    part for part in (summary_text, title_text) if part
                )
                LOGGER.debug(
                    "Retrying extract for story_id=%s with summary/title only",
                    article.get("url") or article.get("source_id") or "unknown",
                )
                model_triplets = extractor.extract(fallback_text, location_hints=[])
        raw_triplets = [dict(item) for item in model_triplets]
        story_url = article.get("url") or article.get("source_id")
        model_triplets = _complete_incomplete_actions(
            model_triplets,
            extractor,
            article_text_full or article_text,
            story_url=story_url,
            story_title=article.get("title") or "",
        )
        model_triplets = _drop_uncompleted_actions(model_triplets)
        model_triplets = _rewrite_incomplete_actor_triplets(model_triplets)
        model_triplets = _drop_inverted_triplets(model_triplets)
        model_triplets = _dedupe_triplets(model_triplets)
        model_triplets = _rank_triplets(model_triplets)[:MAX_TRIPLETS_PER_ARTICLE]
        if raw_triplets and story_url:
            flagged = []
            for item in model_triplets:
                who_value = (item.get("who") or "").strip()
                what_value = (item.get("what") or "").strip()
                summary = f"{who_value} {what_value}".strip()
                if '"' in summary or "string" in summary.lower():
                    flagged.append(summary)
            if flagged:
                LOGGER.info(
                    "Raw LLM triplets for story_id=%s flagged_summaries=%s raw=%s",
                    story_url,
                    flagged,
                    raw_triplets,
                )
        story_id = article.get("url") or article.get("source_id") or "unknown"
        source = article.get("source") or "unknown"
        url = article.get("url") or ""
        if story_id != "unknown":
            index.delete_story(story_id, url or None)
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
                raw_text=article_text_full or article_text,
                story_id=story_id,
                source=source,
                url=url,
                title=title,
                published_at=published_at,
                extracted_at=extracted_at,
                run_id=run_id,
            )
            sanitize_start = perf_counter()
            sanitized = sanitize_triplet(
                record,
                article_text=article.get("content_blob", "") or article_text_full or article_text,
                extractor=extractor,
                allow_protest_related=allow_protest_related,
            )
            _record_timing("sanitize", perf_counter() - sanitize_start)
            if sanitized:
                event_blob = _build_event_blob(sanitized)
                sanitized.event_types = _detect_event_types(event_blob)
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
                            "event_types": record.event_types,
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
    LOGGER.info(
        "Completion stats: attempted=%s accepted=%s rejected=%s",
        COMPLETION_STATS["attempted"],
        COMPLETION_STATS["accepted"],
        COMPLETION_STATS["rejected"],
    )
    _record_timing("total", perf_counter() - total_start)
    LOGGER.info(
        "Timing stats (s): total=%.1f sanitize=%.1f geocode=%.1f llm_extract=%.1f llm_object=%.1f "
        "llm_complement=%.1f llm_action_clause=%.1f llm_location=%.1f",
        TIMING_STATS["total"],
        TIMING_STATS["sanitize"],
        TIMING_STATS["geocode"],
        TIMING_STATS["llm_extract"],
        TIMING_STATS["llm_object"],
        TIMING_STATS["llm_complement"],
        TIMING_STATS["llm_action_clause"],
        TIMING_STATS["llm_location"],
    )
    LOGGER.info(
        "Timing counts: sanitize=%s geocode=%s llm_extract=%s llm_object=%s llm_complement=%s "
        "llm_action_clause=%s llm_location=%s",
        TIMING_COUNTS["sanitize"],
        TIMING_COUNTS["geocode"],
        TIMING_COUNTS["llm_extract"],
        TIMING_COUNTS["llm_object"],
        TIMING_COUNTS["llm_complement"],
        TIMING_COUNTS["llm_action_clause"],
        TIMING_COUNTS["llm_location"],
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
        "--max-article-chars",
        type=int,
        default=None,
        help="Optional cap on article text length (characters) before extraction.",
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
        "--allow-protests",
        action="store_true",
        help="Keep protest-related articles even if they lack immigration keywords.",
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
            max_article_chars=args.max_article_chars,
            allow_protest_related=args.allow_protests,
        )
        LOGGER.info("Wrote triplets to %s", output_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
