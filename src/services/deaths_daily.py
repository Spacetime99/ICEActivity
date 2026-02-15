"""Daily death record pipeline: JSONL canonical store, index, and diffs."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import tempfile
import unicodedata
import uuid
from dataclasses import dataclass
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterable, Sequence
from urllib.parse import urlparse

from src.services import death_reports
from src.services import newsroom_deaths
REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_TRIPLETS_DIR = REPO_ROOT / "datasets" / "news_ingest"
DEFAULT_DEATH_LLM_MODEL_ID = os.getenv("DEATH_LLM_MODEL_ID", "Qwen/Qwen2.5-7B-Instruct")

DEATH_RECORD_NAMESPACE = uuid.UUID("31f4a5a3-2f5f-4e6f-98b8-1e857de534d6")

ALLOWED_DOMAINS = {
    "reuters.com",
    "apnews.com",
    "pbs.org",
    "npr.org",
    "nbcnews.com",
    "propublica.org",
    "nytimes.com",
    "washingtonpost.com",
    "latimes.com",
    "startribune.com",
    "houstonchronicle.com",
    "dallasnews.com",
    "azcentral.com",
    "sfchronicle.com",
    "chicagotribune.com",
    "ice.gov",
    "justice.gov",
    "oig.dhs.gov",
    "ifs.harriscountytx.gov",
    "me.lacounty.gov",
    "cookcountyil.gov",
    "maricopa.gov",
    "pima.gov",
}

BLOCKED_DOMAINS = {
    "freerepublic.com",
    "memeorandum.com",
    "headtopics.com",
    "onenewspage.com",
    "newsnow.co.uk",
    "substack.com",
    "medium.com",
    "blogspot.com",
    "wordpress.com",
    "facebook.com",
    "twitter.com",
    "reddit.com",
    "rumble.com",
    "bitchute.com",
}

OFFICIAL_DOMAINS = {
    "ice.gov",
    "justice.gov",
    "oig.dhs.gov",
    "ifs.harriscountytx.gov",
    "me.lacounty.gov",
    "cookcountyil.gov",
    "maricopa.gov",
    "pima.gov",
}

TRIANGULATION_REQUIRED_DOMAINS = {
    "apnews.com",
    "nbcnews.com",
}


def _bool_env(name: str) -> bool:
    value = os.getenv(name, "").strip().lower()
    return value in {"1", "true", "yes", "on"}


def _resolve_local_model_path(model_id: str) -> str | None:
    if not (_bool_env("HF_HUB_OFFLINE") or _bool_env("TRANSFORMERS_OFFLINE")):
        return None
    try:
        from huggingface_hub import snapshot_download

        return snapshot_download(model_id, local_files_only=True)
    except Exception:
        return None

DEATH_KEYWORDS = [
    "killed",
    "fatally",
    "shot",
    "shot and killed",
    "shot dead",
    "died",
    "death",
    "dead",
    "fatal",
    "murdered",
    "slain",
]
ICE_KEYWORDS = [
    "ice",
    "immigration and customs enforcement",
    "immigration officers",
    "immigration enforcement",
    "ice agent",
    "ice agents",
    "ice officer",
    "border patrol",
    "cbp",
    "homeland security",
    "dhs",
    "hsi",
]
DETENTION_KEYWORDS = [
    "detention",
    "detained",
    "custody",
    "in custody",
    "jail",
    "prison",
    "facility",
    "processing center",
    "detention center",
    "detention facility",
]

DEFAULT_CONTEXT = "street"

ALLOWED_CONTEXTS = {"detention", "street"}
ALLOWED_CUSTODY = {"ICE detention", "ICE transport", "CBP encounter", "unknown"}
ALLOWED_AGENCY = {"ICE", "CBP", "HSI", "DHS", "unknown"}
ALLOWED_HOMICIDE = {"ruled_homicide", "suspected", "not_suspected", "unknown", "under_investigation"}
ALLOWED_LOCATION_CATEGORY = {"facility", "street", "unknown"}

LLM_REQUIRED_FIELDS = {
    "incident_date",
    "incident_time",
    "incident_location",
    "investigation_status",
    "suspect_identified",
    "suspect_name",
    "suspect_role",
    "suspect_agency",
    "suspect_status",
    "facility_name",
}

FIELD_ORDER = [
    "id",
    "person_name",
    "aliases",
    "nationality",
    "age",
    "gender",
    "date_of_death",
    "date_precision",
    "city",
    "county",
    "state",
    "initial_custody_location",
    "death_location",
    "facility_or_location",
    "incident_date",
    "incident_time",
    "incident_location",
    "facility_name",
    "location_category",
    "lat",
    "lon",
    "geocode_source",
    "death_context",
    "custody_status",
    "agency",
    "contractor_involved",
    "cause_of_death_reported",
    "manner_of_death",
    "homicide_status",
    "investigation_status",
    "suspect_identified",
    "suspect_name",
    "suspect_role",
    "suspect_agency",
    "suspect_status",
    "summary_1_sentence",
    "confidence_score",
    "manual_review",
    "primary_report_url",
    "sources",
]

SOURCE_FIELD_ORDER = [
    "url",
    "publisher",
    "publish_date",
    "access_date",
    "source_type",
    "credibility_tier",
    "snippet",
    "claim_tags",
]


@dataclass(frozen=True)
class ChangeLog:
    field: str
    previous_value: Any
    new_value: Any


def _clean_string(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        stripped = value.strip()
        return stripped or None
    return str(value).strip() or None


def _parse_iso_datetime(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _parse_date(value: str | None) -> date | None:
    if not value:
        return None
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", value):
        return date.fromisoformat(value)
    if re.fullmatch(r"\d{4}-\d{2}", value):
        return date.fromisoformat(f"{value}-01")
    if re.fullmatch(r"\d{4}", value):
        return date.fromisoformat(f"{value}-01-01")
    parsed = _parse_iso_datetime(value)
    return parsed.date() if parsed else None


def _iso_date(value: date | datetime) -> str:
    if isinstance(value, datetime):
        return value.date().isoformat()
    return value.isoformat()


def _extract_year(date_value: str | None) -> str | None:
    if not date_value:
        return None
    match = re.match(r"(\d{4})", date_value)
    return match.group(1) if match else None


def _date_precision(date_value: str | None) -> str:
    if not date_value:
        return "unknown"
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", date_value):
        return "day"
    if re.fullmatch(r"\d{4}-\d{2}", date_value):
        return "month"
    if re.fullmatch(r"\d{4}", date_value):
        return "year"
    return "unknown"


def _trim_words(text: str | None, max_words: int) -> str | None:
    if not text:
        return None
    words = text.split()
    if len(words) <= max_words:
        return text
    return " ".join(words[:max_words])


def _is_wikipedia(url: str | None) -> bool:
    if not url:
        return False
    host = urlparse(url).netloc.lower()
    return "wikipedia.org" in host


def _extract_domain(url: str | None) -> str | None:
    if not url:
        return None
    host = urlparse(url).netloc.lower()
    if ":" in host:
        host = host.split(":", 1)[0]
    if host.startswith("www."):
        host = host[4:]
    return host or None


def _normalize_domain(value: str | None) -> str | None:
    if not value:
        return None
    cleaned = value.strip()
    if not cleaned:
        return None
    if cleaned.startswith("http://") or cleaned.startswith("https://"):
        return _extract_domain(cleaned)
    cleaned = cleaned.lower()
    if cleaned.startswith("www."):
        cleaned = cleaned[4:]
    if "/" in cleaned:
        cleaned = cleaned.split("/", 1)[0]
    return cleaned or None


def _domain_matches(host: str, domain: str) -> bool:
    return host == domain or host.endswith(f".{domain}")


def _is_blocked_domain(url: str | None) -> bool:
    host = _extract_domain(url)
    if not host:
        return True
    return any(_domain_matches(host, blocked) for blocked in BLOCKED_DOMAINS)


def _is_allowed_domain(url: str | None) -> bool:
    host = _extract_domain(url)
    if not host:
        return False
    if _is_blocked_domain(url):
        return False
    return any(_domain_matches(host, allowed) for allowed in ALLOWED_DOMAINS)


def _is_official_domain(url: str | None) -> bool:
    host = _extract_domain(url)
    if not host:
        return False
    return any(_domain_matches(host, domain) for domain in OFFICIAL_DOMAINS)


def _make_location_key(record: dict[str, Any]) -> str:
    for key in ("facility_or_location", "city", "county", "state"):
        value = _clean_string(record.get(key))
        if value:
            return value
    return "unknown"


def _name_merge_key(name: str | None) -> str | None:
    if not name:
        return None
    parts = [part for part in re.split(r"\s+", name.strip()) if part]
    if len(parts) < 2:
        return None
    first = parts[0]
    last = parts[-1]
    return f"{first} {last}".lower()


ROLE_NOUNS = {
    "representative",
    "spokesperson",
    "official",
    "officials",
    "officer",
    "officers",
    "agent",
    "agents",
    "witness",
    "attorney",
    "lawyer",
    "family",
    "families",
    "relative",
    "relatives",
    "advocate",
    "activist",
    "protester",
    "protesters",
    "suspect",
    "victim",
    "inspector",
    "general",
}


def _strip_diacritics(value: str) -> str:
    normalized = unicodedata.normalize("NFKD", value)
    return "".join(ch for ch in normalized if not unicodedata.combining(ch))


def _canonical_person_tokens(name: str | None) -> list[str]:
    if not name:
        return []
    text = _strip_diacritics(name)
    text = text.replace("-", " ").replace("'", " ")
    text = re.sub(r"[^A-Za-z ]+", " ", text)
    text = re.sub(r"\s+", " ", text).strip().lower()
    if not text:
        return []
    return [token for token in text.split(" ") if token]


def _canonical_person_name(name: str | None) -> str | None:
    tokens = _canonical_person_tokens(name)
    if len(tokens) < 2:
        return None
    return " ".join(tokens)


def _is_likely_person_name(name: str | None) -> bool:
    tokens = _canonical_person_tokens(name)
    if len(tokens) < 2:
        return False
    if len(tokens) > 5:
        return False
    if any(token in ROLE_NOUNS for token in tokens):
        return False
    if all(len(token) <= 2 for token in tokens):
        return False
    return True


def _dates_within_days(first: str | None, second: str | None, max_days: int) -> bool:
    first_date = _parse_date(first)
    second_date = _parse_date(second)
    if not first_date or not second_date:
        return False
    return abs((first_date - second_date).days) <= max_days


def _source_url_set(record: dict[str, Any]) -> set[str]:
    urls: set[str] = set()
    for source in record.get("sources") or []:
        url = _clean_string(source.get("url"))
        if url:
            urls.add(url)
    primary = _clean_string(record.get("primary_report_url"))
    if primary:
        urls.add(primary)
    return urls


def _record_context(record: dict[str, Any]) -> str:
    return _clean_string(record.get("death_context")) or DEFAULT_CONTEXT


def _dates_within_context_window(first: str | None, second: str | None, context: str) -> bool:
    # Street incidents are usually covered for days; detention reports can lag longer.
    max_days = 14 if context == "street" else 180
    return _dates_within_days(first, second, max_days=max_days)


def _record_quality_score(record: dict[str, Any]) -> tuple[int, int, int]:
    sources = record.get("sources") or []
    source_types = {_clean_string(source.get("source_type")) for source in sources}
    has_official_report = 1 if "official_report" in source_types else 0
    has_known_location = 1 if _make_location_key(record) != "unknown" else 0
    confidence = int(record.get("confidence_score") or 0)
    return has_official_report, has_known_location, confidence


def _prefer_date_of_death(current: str | None, incoming: str | None) -> str | None:
    current_clean = _clean_string(current)
    incoming_clean = _clean_string(incoming)
    if not current_clean:
        return incoming_clean
    if not incoming_clean:
        return current_clean
    current_date = _parse_date(current_clean)
    incoming_date = _parse_date(incoming_clean)
    if current_date and incoming_date:
        return current_clean if current_date <= incoming_date else incoming_clean
    # Keep more specific YYYY-MM-DD over YYYY-MM / YYYY when parse fails or precision differs.
    if len(incoming_clean) > len(current_clean):
        return incoming_clean
    return current_clean


def _merge_duplicate_record(primary: dict[str, Any], duplicate: dict[str, Any]) -> dict[str, Any]:
    merged = dict(primary)
    for field in FIELD_ORDER:
        if field in {"id", "sources"}:
            continue
        current = merged.get(field)
        incoming = duplicate.get(field)
        if field == "date_of_death":
            merged[field] = _prefer_date_of_death(current, incoming)
            continue
        if field == "person_name":
            if not _is_likely_person_name(current) and _is_likely_person_name(incoming):
                merged[field] = incoming
            elif current is None and incoming is not None:
                merged[field] = incoming
            continue
        if field == "manual_review":
            merged[field] = bool(current) or bool(incoming)
            continue
        if field == "confidence_score":
            merged[field] = max(int(current or 0), int(incoming or 0))
            continue
        if current in (None, "", [], {}):
            if incoming not in (None, "", [], {}):
                merged[field] = incoming

    aliases = sorted(set((primary.get("aliases") or [])) | set((duplicate.get("aliases") or [])))
    merged["aliases"] = aliases
    merged_sources = _dedupe_sources(primary.get("sources", []), duplicate.get("sources", []))
    merged["sources"] = merged_sources
    merged["primary_report_url"] = _derive_primary_report_url(merged)
    return _order_fields(merged, FIELD_ORDER)


def _is_duplicate_pair(first: dict[str, Any], second: dict[str, Any]) -> bool:
    first_name = _canonical_person_name(_clean_string(first.get("person_name")))
    second_name = _canonical_person_name(_clean_string(second.get("person_name")))
    if not first_name or first_name != second_name:
        return False
    first_context = _record_context(first)
    second_context = _record_context(second)
    if first_context != second_context:
        return False

    first_urls = _source_url_set(first)
    second_urls = _source_url_set(second)
    if first_urls and second_urls and first_urls.intersection(second_urls):
        return True

    first_date = _clean_string(first.get("date_of_death"))
    second_date = _clean_string(second.get("date_of_death"))
    first_location = _make_location_key(first).lower()
    second_location = _make_location_key(second).lower()

    if first_date and second_date and first_date == second_date:
        if first_context == "detention":
            return True
        if first_location == "unknown" or second_location == "unknown" or first_location == second_location:
            return True

    if (
        first_location != "unknown"
        and second_location != "unknown"
        and first_location == second_location
        and _dates_within_context_window(first_date, second_date, context=first_context)
    ):
        return True

    return False


def collapse_duplicate_records(records: dict[str, dict[str, Any]]) -> dict[str, dict[str, Any]]:
    grouped: dict[tuple[str, str], list[str]] = {}
    for record_id, record in records.items():
        canonical = _canonical_person_name(_clean_string(record.get("person_name")))
        if not canonical:
            continue
        context = _record_context(record)
        grouped.setdefault((canonical, context), []).append(record_id)

    for (_, _), ids in grouped.items():
        if len(ids) < 2:
            continue
        candidate_ids = ids[:]
        consumed: set[str] = set()
        for idx, left_id in enumerate(candidate_ids):
            if left_id in consumed or left_id not in records:
                continue
            left = records[left_id]
            cluster = [left_id]
            for right_id in candidate_ids[idx + 1 :]:
                if right_id in consumed or right_id not in records:
                    continue
                right = records[right_id]
                if _is_duplicate_pair(left, right):
                    cluster.append(right_id)
            if len(cluster) < 2:
                continue

            survivor_id = max(cluster, key=lambda rid: _record_quality_score(records[rid]))
            merged = records[survivor_id]
            for rid in cluster:
                if rid == survivor_id:
                    continue
                merged = _merge_duplicate_record(merged, records[rid])
            merged["id"] = survivor_id
            records[survivor_id] = merged
            for rid in cluster:
                if rid == survivor_id:
                    continue
                consumed.add(rid)
                records.pop(rid, None)

    return records


def _build_uuid(name: str | None, date_value: str | None, location: str, context: str) -> str:
    if name:
        base = f"{name}|{date_value or ''}|{location}"
    else:
        base = f"{date_value or ''}|{location}|{context}"
    return str(uuid.uuid5(DEATH_RECORD_NAMESPACE, base.lower()))


def _normalize_list(value: Any) -> list[str]:
    if not value:
        return []
    if isinstance(value, list):
        cleaned = [_clean_string(item) for item in value]
        return [item for item in cleaned if item]
    if isinstance(value, str):
        parts = [part.strip() for part in value.split(",")]
        return [part for part in parts if part]
    return []


def _normalize_optional_bool(value: Any) -> bool | None:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if isinstance(value, (int, float)):
        return bool(value)
    if isinstance(value, str):
        lowered = value.strip().lower()
        if lowered in {"true", "yes", "1"}:
            return True
        if lowered in {"false", "no", "0"}:
            return False
    return None


def _normalize_location_category(value: str | None) -> str:
    cleaned = _clean_string(value)
    if cleaned in ALLOWED_LOCATION_CATEGORY:
        return cleaned
    return "unknown"


def _sanitize_city_value(value: str | None) -> str | None:
    cleaned = _clean_string(value)
    if not cleaned:
        return None
    if len(cleaned) > 60:
        return None
    parts = [part for part in re.split(r"\s+", cleaned) if part]
    if len(parts) > 4:
        return None
    lowered = f" {cleaned.lower()} "
    if any(token in lowered for token in (" detention ", " custody ", " passed away ", " death ")):
        return None
    if any(char.isdigit() for char in cleaned):
        return None
    return cleaned


def _sanitize_state_value(value: str | None) -> str | None:
    cleaned = _clean_string(value)
    if not cleaned:
        return None
    if len(cleaned) > 30:
        return None
    parts = [part for part in re.split(r"\s+", cleaned) if part]
    if len(parts) > 3:
        return None
    if any(char.isdigit() for char in cleaned):
        return None
    return cleaned


def _sanitize_facility_or_location(value: str | None) -> str | None:
    cleaned = _clean_string(value)
    if not cleaned:
        return None
    sanitized = death_reports._sanitize_location_candidate(cleaned)
    if sanitized:
        return sanitized
    if len(cleaned) > 110:
        return None
    if len(cleaned.split()) > 14:
        return None
    lowered = f" {cleaned.lower()} "
    if any(
        token in lowered
        for token in (" who ", " which ", " that ", " pending ", " noted ", " assessment ", " on the same date ")
    ):
        return None
    return cleaned


def _derive_facility_name(record: dict[str, Any]) -> str | None:
    facility = _clean_string(record.get("facility_name"))
    if facility:
        return facility
    location = _clean_string(record.get("facility_or_location"))
    if not location:
        return None
    lowered = location.lower()
    if record.get("death_context") == "detention":
        return location
    if any(keyword in lowered for keyword in DETENTION_KEYWORDS):
        return location
    if any(token in lowered for token in ("jail", "prison", "detention", "processing center")):
        return location
    return None


def _derive_location_category(record: dict[str, Any], facility_name: str | None) -> str:
    if facility_name:
        return "facility"
    if record.get("death_context") == "detention":
        return "facility"
    if any(_clean_string(record.get(key)) for key in ("facility_or_location", "city", "county", "state")):
        return "street"
    return "unknown"


def _normalize_agency_value(value: str | None) -> str:
    cleaned = _clean_string(value)
    if cleaned in ALLOWED_AGENCY:
        return cleaned
    return "unknown"


def _extract_investigation_status(text: str) -> str | None:
    lowered = text.lower()
    if "under investigation" in lowered or "investigation continues" in lowered:
        return "under_investigation"
    if "awaiting autopsy" in lowered or "autopsy pending" in lowered:
        return "autopsy_pending"
    if "homicide investigation" in lowered or "homicide probe" in lowered:
        return "homicide_investigation"
    if "ruled a homicide" in lowered or "ruled homicide" in lowered:
        return "ruled_homicide"
    if "charged with" in lowered or "charges filed" in lowered:
        return "charges_filed"
    if "no charges" in lowered or "declined to charge" in lowered:
        return "no_charges"
    return None


def _extract_suspect_role_and_agency(text: str) -> tuple[str | None, str | None]:
    lowered = text.lower()
    if "ice agent" in lowered or "ice officer" in lowered or "immigration officer" in lowered:
        return "ICE agent", "ICE"
    if "border patrol" in lowered or "cbp" in lowered:
        return "Border Patrol agent", "CBP"
    if "hsi" in lowered:
        return "HSI agent", "HSI"
    return None, None


def _extract_suspect_identified(text: str) -> bool | None:
    lowered = text.lower()
    if "not been identified" in lowered or "unidentified" in lowered:
        return False
    if "identity has not been released" in lowered or "identity not released" in lowered:
        return False
    if "identified as" in lowered or "was identified" in lowered or "named as" in lowered:
        return True
    return None


def _extract_suspect_status(text: str) -> str | None:
    lowered = text.lower()
    if "charged with" in lowered or "charges filed" in lowered:
        return "charged"
    if "arrested" in lowered:
        return "arrested"
    if "suspended" in lowered or "placed on leave" in lowered:
        return "suspended"
    return None


def _extract_json_object(text: str) -> dict[str, Any]:
    if not text:
        return {}
    start = text.find("{")
    end = text.rfind("}")
    if start == -1 or end == -1 or end <= start:
        return {}
    snippet = text[start : end + 1].strip()
    snippet = snippet.strip("`")
    try:
        parsed = json.loads(snippet)
    except json.JSONDecodeError:
        return {}
    if isinstance(parsed, dict):
        return parsed
    return {}


class DeathDetailExtractor:
    """Local HF model wrapper for death detail extraction."""

    def __init__(
        self,
        model_id: str = DEFAULT_DEATH_LLM_MODEL_ID,
        temperature: float = 0.0,
        repetition_penalty: float = 1.05,
        max_new_tokens: int = 220,
        max_chars: int = 8000,
    ) -> None:
        import torch
        from transformers import AutoModelForCausalLM, AutoTokenizer

        dtype = torch.bfloat16 if torch.cuda.is_available() else torch.float32
        local_only = _bool_env("HF_HUB_OFFLINE") or _bool_env("TRANSFORMERS_OFFLINE")
        cache_dir = os.getenv("HF_HUB_CACHE") or None
        model_ref = model_id
        local_path = _resolve_local_model_path(model_id) if local_only else None
        if local_path:
            model_ref = local_path
        self.tokenizer = AutoTokenizer.from_pretrained(
            model_ref,
            cache_dir=cache_dir,
            local_files_only=local_only,
        )
        self.model = AutoModelForCausalLM.from_pretrained(
            model_ref,
            torch_dtype=dtype,
            device_map="auto",
            cache_dir=cache_dir,
            local_files_only=local_only,
        )
        self.temperature = temperature
        self.repetition_penalty = repetition_penalty
        self.max_new_tokens = max_new_tokens
        self.max_chars = max_chars

    def build_prompt(
        self,
        article_text: str,
        person_name: str | None = None,
        date_hint: str | None = None,
        location_hint: str | None = None,
    ) -> str:
        hints = []
        if person_name:
            hints.append(f"Victim name: {person_name}")
        if date_hint:
            hints.append(f"Known date of death: {date_hint}")
        if location_hint:
            hints.append(f"Known location: {location_hint}")
        hint_block = ""
        if hints:
            hint_block = "Hints:\n" + "\n".join(hints) + "\n\n"
        return (
            "Extract death incident details from the news text. "
            "Return a JSON object with these keys:\n"
            "- incident_date (YYYY-MM-DD or null)\n"
            "- incident_time (HH:MM or null)\n"
            "- incident_location (string or null)\n"
            "- manner_of_death (shooting, stabbing, overdose, suicide, homicide, unknown)\n"
            "- investigation_status (under_investigation, autopsy_pending, homicide_investigation, "
            "charges_filed, no_charges, ruled_homicide, unknown)\n"
            "- suspect_identified (true/false/null)\n"
            "- suspect_name (string or null)\n"
            "- suspect_role (string or null)\n"
            "- suspect_agency (ICE, CBP, HSI, DHS, unknown)\n"
            "- suspect_status (identified, charged, arrested, suspended, unknown)\n"
            "- facility_name (string or null)\n"
            "Rules:\n"
            "- Only use facts explicitly stated in the text.\n"
            "- If the detail is not stated, return null.\n"
            "- Do not invent names, dates, or locations.\n"
            "- Output JSON only, no commentary.\n\n"
            f"{hint_block}"
            f"Text:\n{article_text}\n\nJSON:"
        )

    def extract(
        self,
        article_text: str,
        person_name: str | None = None,
        date_hint: str | None = None,
        location_hint: str | None = None,
    ) -> dict[str, Any]:
        text = article_text.strip()
        if not text:
            return {}
        if len(text) > self.max_chars:
            text = text[: self.max_chars]
        prompt = self.build_prompt(
            text,
            person_name=person_name,
            date_hint=date_hint,
            location_hint=location_hint,
        )
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
        if "JSON:" in decoded:
            decoded = decoded.split("JSON:", 1)[-1]
        return _extract_json_object(decoded)

    def close(self) -> None:
        import gc

        model = getattr(self, "model", None)
        tokenizer = getattr(self, "tokenizer", None)
        if model is not None:
            del model
        if tokenizer is not None:
            del tokenizer
        self.model = None
        self.tokenizer = None
        try:
            import torch

            if torch.cuda.is_available():
                torch.cuda.empty_cache()
                torch.cuda.ipc_collect()
        except Exception:
            pass
        gc.collect()


def _normalize_sources(sources: Any, access_date: str) -> list[dict[str, Any]]:
    if not sources:
        return []
    normalized = []
    for item in sources:
        if not isinstance(item, dict):
            continue
        url = _clean_string(item.get("url"))
        if _is_wikipedia(url):
            continue
        if not url:
            continue
        if not _is_allowed_domain(url):
            continue
        snippet = _trim_words(_clean_string(item.get("snippet")), 25)
        source = {
            "url": url,
            "publisher": _clean_string(item.get("publisher")),
            "publish_date": _clean_string(item.get("publish_date")),
            "access_date": _clean_string(item.get("access_date")) or access_date,
            "source_type": _clean_string(item.get("source_type")),
            "credibility_tier": _clean_string(item.get("credibility_tier")),
            "snippet": snippet,
            "claim_tags": _normalize_list(item.get("claim_tags")),
        }
        normalized.append(_order_fields(source, SOURCE_FIELD_ORDER))
    return normalized


def _normalize_primary_report_url(url: Any) -> str | None:
    cleaned = _clean_string(url)
    if not cleaned:
        return None
    if _is_wikipedia(cleaned) or not _is_allowed_domain(cleaned):
        return None
    return cleaned


def _select_primary_report_url(sources: list[dict[str, Any]]) -> str | None:
    if not sources:
        return None

    def candidate_url(source: dict[str, Any]) -> str | None:
        return _normalize_primary_report_url(source.get("url"))

    for source in sources:
        claim_tags = _normalize_list(source.get("claim_tags"))
        if source.get("source_type") == "official_report" or "ice_death_report" in claim_tags:
            url = candidate_url(source)
            if url:
                return url

    for source in sources:
        claim_tags = _normalize_list(source.get("claim_tags"))
        if source.get("source_type") == "official_release" or "ice_release" in claim_tags:
            url = candidate_url(source)
            if url:
                return url

    return None


def _derive_primary_report_url(record: dict[str, Any]) -> str | None:
    sources = record.get("sources") or []
    selected = _select_primary_report_url(sources)
    if selected:
        return selected
    return _normalize_primary_report_url(record.get("primary_report_url"))


def _filter_sources_by_domain(sources: list[dict[str, Any]]) -> list[dict[str, Any]]:
    filtered: list[dict[str, Any]] = []
    for item in sources:
        if not isinstance(item, dict):
            continue
        url = _clean_string(item.get("url"))
        if not url:
            continue
        if _is_wikipedia(url) or not _is_allowed_domain(url):
            continue
        filtered.append(item)
    return filtered


def _order_fields(record: dict[str, Any], order: list[str]) -> dict[str, Any]:
    ordered: dict[str, Any] = {}
    for key in order:
        ordered[key] = record.get(key)
    return ordered


def normalize_record(record: dict[str, Any], access_date: str) -> dict[str, Any]:
    cleaned = {
        "id": _clean_string(record.get("id")),
        "person_name": _clean_string(record.get("person_name")),
        "aliases": _normalize_list(record.get("aliases")),
        "nationality": _clean_string(record.get("nationality")),
        "age": _clean_string(record.get("age")),
        "gender": _clean_string(record.get("gender")),
        "date_of_death": _clean_string(record.get("date_of_death")),
        "date_precision": _clean_string(record.get("date_precision")),
        "city": _sanitize_city_value(_clean_string(record.get("city"))),
        "county": _clean_string(record.get("county")),
        "state": _sanitize_state_value(_clean_string(record.get("state"))),
        "initial_custody_location": _sanitize_facility_or_location(
            _clean_string(record.get("initial_custody_location")),
        ),
        "death_location": _sanitize_facility_or_location(_clean_string(record.get("death_location"))),
        "facility_or_location": _sanitize_facility_or_location(_clean_string(record.get("facility_or_location"))),
        "incident_date": _clean_string(record.get("incident_date")),
        "incident_time": _clean_string(record.get("incident_time")),
        "incident_location": _clean_string(record.get("incident_location")),
        "facility_name": _clean_string(record.get("facility_name")),
        "location_category": _clean_string(record.get("location_category")),
        "lat": record.get("lat"),
        "lon": record.get("lon"),
        "geocode_source": _clean_string(record.get("geocode_source")),
        "death_context": _clean_string(record.get("death_context")) or DEFAULT_CONTEXT,
        "custody_status": _clean_string(record.get("custody_status")) or "unknown",
        "agency": _clean_string(record.get("agency")) or "unknown",
        "contractor_involved": record.get("contractor_involved", "unknown"),
        "cause_of_death_reported": _clean_string(record.get("cause_of_death_reported")),
        "manner_of_death": _clean_string(record.get("manner_of_death")),
        "homicide_status": _clean_string(record.get("homicide_status")) or "unknown",
        "investigation_status": _clean_string(record.get("investigation_status")),
        "suspect_identified": _normalize_optional_bool(record.get("suspect_identified")),
        "suspect_name": _clean_string(record.get("suspect_name")),
        "suspect_role": _clean_string(record.get("suspect_role")),
        "suspect_agency": _clean_string(record.get("suspect_agency")),
        "suspect_status": _clean_string(record.get("suspect_status")),
        "summary_1_sentence": _clean_string(record.get("summary_1_sentence")),
        "confidence_score": record.get("confidence_score"),
        "manual_review": bool(record.get("manual_review", False)),
        "sources": _normalize_sources(record.get("sources"), access_date),
    }

    if cleaned["death_context"] not in ALLOWED_CONTEXTS:
        cleaned["death_context"] = DEFAULT_CONTEXT
    if cleaned["custody_status"] not in ALLOWED_CUSTODY:
        cleaned["custody_status"] = "unknown"
    if cleaned["agency"] not in ALLOWED_AGENCY:
        cleaned["agency"] = "unknown"
    if cleaned["homicide_status"] not in ALLOWED_HOMICIDE:
        cleaned["homicide_status"] = "unknown"
    cleaned["suspect_agency"] = _normalize_agency_value(cleaned.get("suspect_agency"))
    cleaned["location_category"] = _normalize_location_category(cleaned.get("location_category"))

    if not cleaned["facility_name"]:
        cleaned["facility_name"] = _derive_facility_name(cleaned)
    if not cleaned["incident_location"] and cleaned["facility_or_location"]:
        cleaned["incident_location"] = cleaned["facility_or_location"]
    if cleaned["location_category"] == "unknown":
        cleaned["location_category"] = _derive_location_category(
            cleaned,
            cleaned.get("facility_name"),
        )

    is_news_street = cleaned.get("death_context") == "street" and any(
        source.get("source_type") == "news" for source in cleaned.get("sources") or []
    )
    if is_news_street and not _is_likely_person_name(cleaned.get("person_name")):
        cleaned["person_name"] = None
        cleaned["manual_review"] = True
        cleaned["confidence_score"] = min(int(cleaned.get("confidence_score") or 0), 35)

    if cleaned["suspect_identified"] is None:
        if cleaned.get("suspect_name") or cleaned.get("suspect_role"):
            cleaned["suspect_identified"] = True

    if not cleaned["date_precision"]:
        cleaned["date_precision"] = _date_precision(cleaned["date_of_death"])

    if cleaned["confidence_score"] is None:
        cleaned["confidence_score"] = 0
    cleaned["confidence_score"] = int(max(0, min(100, cleaned["confidence_score"])))

    if not cleaned["id"]:
        location_key = _make_location_key(cleaned)
        cleaned["id"] = _build_uuid(
            cleaned["person_name"],
            cleaned["date_of_death"],
            location_key,
            cleaned["death_context"],
        )

    _apply_source_requirements(cleaned)
    _apply_triangulation_requirements(cleaned)
    cleaned["primary_report_url"] = _derive_primary_report_url(cleaned)

    return _order_fields(cleaned, FIELD_ORDER)


def _dedupe_sources(existing: list[dict[str, Any]], new: list[dict[str, Any]]) -> list[dict[str, Any]]:
    seen = {source.get("url") for source in existing if source.get("url")}
    merged = existing[:]
    for source in new:
        url = source.get("url")
        if not url or url in seen:
            continue
        merged.append(source)
        seen.add(url)
    return merged


def _apply_source_requirements(record: dict[str, Any]) -> list[ChangeLog]:
    changes: list[ChangeLog] = []
    sources_before = record.get("sources") or []
    sources_after = _filter_sources_by_domain(sources_before)
    if sources_after != sources_before:
        changes.append(ChangeLog("sources", sources_before, sources_after))
        record["sources"] = sources_after

    primary_sources = [source for source in sources_after if _is_official_domain(source.get("url"))]
    secondary_sources = [source for source in sources_after if not _is_official_domain(source.get("url"))]
    total_sources = len(primary_sources) + len(secondary_sources)

    manual_review = bool(record.get("manual_review", False))
    confidence = int(record.get("confidence_score") or 0)

    if total_sources < 2:
        if not manual_review:
            changes.append(ChangeLog("manual_review", manual_review, True))
            record["manual_review"] = True
        capped = min(confidence, 45)
        if capped != confidence:
            changes.append(ChangeLog("confidence_score", confidence, capped))
            record["confidence_score"] = capped
    elif primary_sources and secondary_sources:
        boosted = min(100, confidence + 5)
        if boosted != confidence:
            changes.append(ChangeLog("confidence_score", confidence, boosted))
            record["confidence_score"] = boosted

    return changes


def _apply_triangulation_requirements(record: dict[str, Any]) -> list[ChangeLog]:
    changes: list[ChangeLog] = []
    if record.get("death_context") != "street":
        return changes
    sources = record.get("sources") or []
    if not any(source.get("source_type") == "news" for source in sources):
        return changes
    domains = _extract_source_domains(record)
    if not domains:
        return changes
    required = TRIANGULATION_REQUIRED_DOMAINS
    manual_review = bool(record.get("manual_review", False))
    confidence = int(record.get("confidence_score") or 0)
    if not required.issubset(domains):
        if not manual_review:
            changes.append(ChangeLog("manual_review", manual_review, True))
            record["manual_review"] = True
        capped = min(confidence, 45)
        if capped != confidence:
            changes.append(ChangeLog("confidence_score", confidence, capped))
            record["confidence_score"] = capped
        return changes
    boosted = min(100, confidence + 10)
    if boosted != confidence:
        changes.append(ChangeLog("confidence_score", confidence, boosted))
        record["confidence_score"] = boosted
    return changes


def merge_records(
    existing: dict[str, dict[str, Any]],
    incoming: Iterable[dict[str, Any]],
) -> tuple[dict[str, dict[str, Any]], list[dict[str, Any]], dict[str, int]]:
    added = 0
    updated = 0
    manual_review = 0
    diff_entries: list[dict[str, Any]] = []
    name_date_index: dict[str, str] = {}
    name_date_location_index: dict[str, str] = {}
    canonical_name_index: dict[str, list[str]] = {}
    merge_name_index: dict[str, list[str]] = {}
    source_url_index: dict[str, list[str]] = {}

    for record_id, record in existing.items():
        name = _clean_string(record.get("person_name"))
        date_value = _clean_string(record.get("date_of_death"))
        if not name or not date_value:
            continue
        key = f"{name.lower()}|{date_value}"
        name_date_index.setdefault(key, record_id)
        location_key = _make_location_key(record)
        normalized_name = _name_merge_key(name)
        if normalized_name and location_key != "unknown":
            fuzzy_key = f"{normalized_name}|{date_value}|{location_key.lower()}"
            name_date_location_index.setdefault(fuzzy_key, record_id)
        if normalized_name:
            merge_name_index.setdefault(normalized_name, []).append(record_id)
        canonical = _canonical_person_name(name)
        if canonical:
            canonical_name_index.setdefault(canonical, []).append(record_id)
        for url in _source_url_set(record):
            source_url_index.setdefault(url, []).append(record_id)

    for record in incoming:
        record_id = record["id"]
        if record_id not in existing:
            name = _clean_string(record.get("person_name"))
            date_value = _clean_string(record.get("date_of_death"))
            context = _clean_string(record.get("death_context"))
            location_key = _make_location_key(record)
            canonical = _canonical_person_name(name)
            source_urls = _source_url_set(record)
            match_id = None
            if name and date_value:
                key = f"{name.lower()}|{date_value}"
                match_id = name_date_index.get(key)
                if not match_id:
                    normalized_name = _name_merge_key(name)
                    if normalized_name and location_key != "unknown":
                        fuzzy_key = f"{normalized_name}|{date_value}|{location_key.lower()}"
                        match_id = name_date_location_index.get(fuzzy_key)
            if not match_id and canonical:
                candidates = canonical_name_index.get(canonical, [])
                for candidate_id in candidates:
                    candidate = existing.get(candidate_id)
                    if not candidate:
                        continue
                    if context and candidate.get("death_context") != context:
                        continue
                    candidate_location = _make_location_key(candidate)
                    if (
                        location_key != "unknown"
                        and candidate_location != "unknown"
                        and candidate_location.lower() != location_key.lower()
                    ):
                        continue
                    if not _dates_within_days(
                        _clean_string(candidate.get("date_of_death")),
                        date_value,
                        max_days=7,
                    ):
                        continue
                    match_id = candidate_id
                    break
            if not match_id and name:
                merge_name = _name_merge_key(name)
                if merge_name:
                    candidates = merge_name_index.get(merge_name, [])
                    for candidate_id in candidates:
                        candidate = existing.get(candidate_id)
                        if not candidate:
                            continue
                        if context and candidate.get("death_context") != context:
                            continue
                        candidate_location = _make_location_key(candidate)
                        if (
                            location_key != "unknown"
                            and candidate_location != "unknown"
                            and candidate_location.lower() != location_key.lower()
                        ):
                            continue
                        if not _dates_within_days(
                            _clean_string(candidate.get("date_of_death")),
                            date_value,
                            max_days=7,
                        ):
                            continue
                        match_id = candidate_id
                        break
            if not match_id and canonical and source_urls:
                candidates: set[str] = set()
                for url in source_urls:
                    candidates.update(source_url_index.get(url, []))
                for candidate_id in candidates:
                    candidate = existing.get(candidate_id)
                    if not candidate:
                        continue
                    candidate_canonical = _canonical_person_name(
                        _clean_string(candidate.get("person_name")),
                    )
                    if candidate_canonical != canonical:
                        continue
                    if context and candidate.get("death_context") != context:
                        continue
                    match_id = candidate_id
                    break
            if match_id:
                record_id = match_id
                record["id"] = match_id
        if record_id not in existing:
            existing[record_id] = record
            added += 1
            if record.get("manual_review"):
                manual_review += 1
            name = _clean_string(record.get("person_name"))
            date_value = _clean_string(record.get("date_of_death"))
            if name and date_value:
                key = f"{name.lower()}|{date_value}"
                name_date_index.setdefault(key, record_id)
                location_key = _make_location_key(record)
                normalized_name = _name_merge_key(name)
                if normalized_name and location_key != "unknown":
                    fuzzy_key = f"{normalized_name}|{date_value}|{location_key.lower()}"
                    name_date_location_index.setdefault(fuzzy_key, record_id)
                if normalized_name:
                    merge_name_index.setdefault(normalized_name, []).append(record_id)
                canonical = _canonical_person_name(name)
                if canonical:
                    canonical_name_index.setdefault(canonical, []).append(record_id)
            for url in _source_url_set(record):
                source_url_index.setdefault(url, []).append(record_id)
            diff = dict(record)
            diff["change_type"] = "added"
            diff_entries.append(diff)
            continue

        current = existing[record_id]
        change_log: list[ChangeLog] = []

        def update_field(
            field: str,
            new_value: Any,
            placeholders: set[Any] | None = None,
        ) -> None:
            if new_value is None:
                return
            old_value = current.get(field)
            if placeholders and new_value in placeholders and old_value not in placeholders:
                return
            if new_value != old_value:
                change_log.append(ChangeLog(field, old_value, new_value))
                current[field] = new_value

        update_field("person_name", record.get("person_name"))
        if record.get("aliases"):
            merged_aliases = sorted(
                set(current.get("aliases", [])) | set(record.get("aliases", [])),
            )
            if merged_aliases != current.get("aliases"):
                change_log.append(ChangeLog("aliases", current.get("aliases"), merged_aliases))
                current["aliases"] = merged_aliases
        update_field("nationality", record.get("nationality"))
        update_field("age", record.get("age"))
        update_field("gender", record.get("gender"))
        update_field("date_of_death", record.get("date_of_death"))
        update_field("date_precision", record.get("date_precision"))
        update_field("city", record.get("city"))
        update_field("county", record.get("county"))
        update_field("state", record.get("state"))
        update_field("initial_custody_location", record.get("initial_custody_location"))
        update_field("death_location", record.get("death_location"))
        update_field("facility_or_location", record.get("facility_or_location"))
        update_field("incident_date", record.get("incident_date"))
        update_field("incident_time", record.get("incident_time"))
        update_field("incident_location", record.get("incident_location"))
        update_field("facility_name", record.get("facility_name"))
        update_field("location_category", record.get("location_category"), {"unknown"})
        update_field("lat", record.get("lat"))
        update_field("lon", record.get("lon"))
        update_field("geocode_source", record.get("geocode_source"))
        update_field("death_context", record.get("death_context"), {DEFAULT_CONTEXT})
        update_field("custody_status", record.get("custody_status"), {"unknown"})
        update_field("agency", record.get("agency"), {"unknown"})
        update_field("contractor_involved", record.get("contractor_involved"), {"unknown"})
        update_field("cause_of_death_reported", record.get("cause_of_death_reported"))
        update_field("manner_of_death", record.get("manner_of_death"))
        update_field("homicide_status", record.get("homicide_status"), {"unknown"})
        update_field("investigation_status", record.get("investigation_status"))
        update_field("suspect_identified", record.get("suspect_identified"))
        update_field("suspect_name", record.get("suspect_name"))
        update_field("suspect_role", record.get("suspect_role"))
        update_field("suspect_agency", record.get("suspect_agency"), {"unknown"})
        update_field("suspect_status", record.get("suspect_status"))
        update_field("summary_1_sentence", record.get("summary_1_sentence"))
        update_field("primary_report_url", record.get("primary_report_url"))

        new_score = record.get("confidence_score")
        if new_score is not None:
            old_score = current.get("confidence_score")
            best_score = max(old_score or 0, new_score)
            if best_score != old_score:
                change_log.append(ChangeLog("confidence_score", old_score, best_score))
                current["confidence_score"] = best_score

        if record.get("manual_review") and not current.get("manual_review"):
            change_log.append(ChangeLog("manual_review", current.get("manual_review"), True))
            current["manual_review"] = True
            manual_review += 1

        sources_before = current.get("sources", [])
        sources_after = _dedupe_sources(sources_before, record.get("sources", []))
        if len(sources_after) != len(sources_before):
            change_log.append(ChangeLog("sources", sources_before, sources_after))
            current["sources"] = sources_after
        canonical = _canonical_person_name(_clean_string(current.get("person_name")))
        merge_name = _name_merge_key(_clean_string(current.get("person_name")))
        if canonical:
            ids = canonical_name_index.setdefault(canonical, [])
            if record_id not in ids:
                ids.append(record_id)
        if merge_name:
            ids = merge_name_index.setdefault(merge_name, [])
            if record_id not in ids:
                ids.append(record_id)
        for url in _source_url_set(current):
            ids = source_url_index.setdefault(url, [])
            if record_id not in ids:
                ids.append(record_id)

        change_log.extend(_apply_source_requirements(current))
        change_log.extend(_apply_triangulation_requirements(current))
        derived_primary = _derive_primary_report_url(current)
        if derived_primary != current.get("primary_report_url"):
            change_log.append(
                ChangeLog("primary_report_url", current.get("primary_report_url"), derived_primary),
            )
            current["primary_report_url"] = derived_primary

        if change_log:
            updated += 1
            diff = dict(current)
            diff["change_type"] = "updated"
            diff["change_log"] = [
                {
                    "field": entry.field,
                    "previous_value": entry.previous_value,
                    "new_value": entry.new_value,
                }
                for entry in change_log
            ]
            diff_entries.append(diff)

    summary = {"added": added, "updated": updated, "manual_review": manual_review}
    return existing, diff_entries, summary


def build_index(records: Iterable[dict[str, Any]]) -> dict[str, Any]:
    year_counts: dict[str, int] = {}
    context_counts: dict[str, int] = {}
    homicide_counts: dict[str, int] = {}
    min_date: date | None = None
    max_date: date | None = None

    for record in records:
        year = _extract_year(record.get("date_of_death"))
        if year:
            year_counts[year] = year_counts.get(year, 0) + 1
        context = record.get("death_context") or "unknown"
        context_counts[context] = context_counts.get(context, 0) + 1
        homicide = record.get("homicide_status") or "unknown"
        homicide_counts[homicide] = homicide_counts.get(homicide, 0) + 1

        parsed_date = _parse_date(record.get("date_of_death"))
        if parsed_date:
            if min_date is None or parsed_date < min_date:
                min_date = parsed_date
            if max_date is None or parsed_date > max_date:
                max_date = parsed_date

    return {
        "counts": {
            "year": year_counts,
            "context": context_counts,
            "homicide_status": homicide_counts,
        },
        "date_range": {
            "min": _iso_date(min_date) if min_date else None,
            "max": _iso_date(max_date) if max_date else None,
        },
    }


def load_jsonl(path: Path) -> dict[str, dict[str, Any]]:
    if not path.exists():
        return {}
    records: dict[str, dict[str, Any]] = {}
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            record = json.loads(line)
            record_id = record.get("id")
            if not record_id:
                continue
            records[record_id] = record
    return records


def write_jsonl_atomic(path: Path, records: Iterable[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=path.parent, encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=True) + "\n")
        temp_name = handle.name
    os.replace(temp_name, path)


def write_json_atomic(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=path.parent, encoding="utf-8") as handle:
        handle.write(json.dumps(payload, ensure_ascii=True, indent=2) + "\n")
        temp_name = handle.name
    os.replace(temp_name, path)


def _is_death_lead(text: str) -> bool:
    lowered = text.lower()
    return any(keyword in lowered for keyword in DEATH_KEYWORDS)


def _is_ice_related(text: str) -> bool:
    lowered = text.lower()
    return any(keyword in lowered for keyword in ICE_KEYWORDS)


def _infer_death_context(text: str) -> str:
    lowered = text.lower()
    if any(keyword in lowered for keyword in DETENTION_KEYWORDS):
        return "detention"
    return "street"


def _infer_agency(text: str) -> str:
    lowered = text.lower()
    if "hsi" in lowered:
        return "HSI"
    if "cbp" in lowered or "border patrol" in lowered:
        return "CBP"
    if "dhs" in lowered or "homeland security" in lowered:
        return "DHS"
    if "ice" in lowered or "immigration and customs enforcement" in lowered:
        return "ICE"
    return "unknown"


def _infer_custody_status(text: str) -> str:
    lowered = text.lower()
    if "transport" in lowered or "transfer" in lowered:
        return "ICE transport"
    if "cbp" in lowered or "border patrol" in lowered:
        return "CBP encounter"
    if any(keyword in lowered for keyword in DETENTION_KEYWORDS):
        return "ICE detention"
    return "unknown"


def _infer_manner(text: str) -> str | None:
    lowered = text.lower()
    if "shot" in lowered or "shooting" in lowered:
        return "shooting"
    if "stabbed" in lowered or "stabbing" in lowered:
        return "stabbing"
    if "overdose" in lowered:
        return "overdose"
    if "suicide" in lowered:
        return "suicide"
    if "killed" in lowered or "fatally" in lowered:
        return "homicide"
    return None


def _is_generic_actor(name: str | None) -> bool:
    if not name:
        return True
    lowered = name.lower()
    blocked = any(
        phrase in lowered
        for phrase in (
            " a man",
            " a woman",
            "young man",
            "young woman",
            "protester",
            "protesters",
            "protestor",
            "protestors",
            "unknown",
            "unidentified",
            "u.s.",
            "ice agents",
            "ice agent",
            "ice officer",
            "ice officers",
            "border patrol",
            "cbp",
            "dhs",
            "officer",
            "officers",
            "agent",
            "agents",
            "homeland security",
            "immigration officers",
        )
    )
    if blocked:
        return True
    return not _is_likely_person_name(name)


def _parse_location(where_text: str | None) -> tuple[str | None, str | None, str | None]:
    if not where_text:
        return None, None, None
    parts = [part.strip() for part in where_text.split(",") if part.strip()]
    if not parts:
        return None, None, None
    city = parts[0] if parts else None
    state = parts[1] if len(parts) > 1 else None
    county = None
    return city, county, state


def _score_confidence(text: str, person_name: str | None) -> int:
    score = 10
    if _is_death_lead(text):
        score += 40
    if _is_ice_related(text):
        score += 30
    if person_name:
        score += 10
    return min(100, score)


def _build_source(
    triplet: dict[str, Any],
    access_date: str,
    text: str,
) -> dict[str, Any] | None:
    url = _clean_string(triplet.get("url")) or _clean_string(triplet.get("story_id"))
    if _is_wikipedia(url) or not _is_allowed_domain(url):
        return None
    if not url:
        return None
    publish_date = _clean_string(triplet.get("published_at"))
    publisher = _clean_string(triplet.get("source"))
    snippet = _trim_words(_clean_string(triplet.get("title")) or text, 25)
    return _order_fields(
        {
            "url": url,
            "publisher": publisher,
            "publish_date": publish_date,
            "access_date": access_date,
            "source_type": "news",
            "credibility_tier": "unknown",
            "snippet": snippet,
            "claim_tags": [],
        },
        SOURCE_FIELD_ORDER,
    )


def _extract_source_domains(record: dict[str, Any]) -> set[str]:
    domains: set[str] = set()
    for source in record.get("sources") or []:
        url = _clean_string(source.get("url"))
        domain = _extract_domain(url)
        if not domain:
            domain = _normalize_domain(_clean_string(source.get("publisher")))
        if domain:
            domains.add(domain)
    return domains


def _should_drop_record(record: dict[str, Any]) -> bool:
    name = record.get("person_name")
    has_news_or_release_source = any(
        source.get("source_type") in {"news", "official_release"} for source in record.get("sources") or []
    )
    if has_news_or_release_source and not _is_likely_person_name(name):
        return True
    is_news_street = record.get("death_context") == "street" and any(
        source.get("source_type") == "news" for source in record.get("sources") or []
    )
    if is_news_street and not _is_likely_person_name(name):
        return True
    return False




def _extract_article_text(report: dict[str, Any]) -> str | None:
    content = _clean_string(report.get("content")) or _clean_string(report.get("summary"))
    raw = report.get("raw")
    if not content and isinstance(raw, dict):
        content = _clean_string(raw.get("fetched_content"))
    return content


def build_article_text_lookup(
    triplets: Sequence[dict[str, Any]],
    window_days: int,
) -> dict[str, str]:
    urls: set[str] = set()
    for triplet in triplets:
        url = _clean_string(triplet.get("url"))
        story_id = _clean_string(triplet.get("story_id"))
        if url:
            urls.add(url)
        if story_id:
            urls.add(story_id)

    if not urls:
        return {}

    cutoff = datetime.now(timezone.utc) - timedelta(days=window_days)
    lookup: dict[str, str] = {}
    for path in sorted(DEFAULT_TRIPLETS_DIR.glob("news_reports_*.jsonl")):
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                report = json.loads(line)
                published_at = _parse_iso_datetime(_clean_string(report.get("published_at")))
                if published_at and published_at < cutoff:
                    continue
                report_url = _clean_string(report.get("url")) or _clean_string(
                    report.get("source_id"),
                )
                if not report_url or report_url not in urls:
                    continue
                text = _extract_article_text(report)
                if not text:
                    continue
                existing = lookup.get(report_url)
                if not existing or len(text) > len(existing):
                    lookup[report_url] = text
    return lookup


def _apply_enrichment_fields(record: dict[str, Any], enrichment: dict[str, Any]) -> None:
    if not enrichment:
        return
    for key, value in enrichment.items():
        if value is None or value == "":
            continue
        current = record.get(key)
        if current is None or current == "" or current == "unknown":
            record[key] = value


def _needs_llm_enrichment(record: dict[str, Any]) -> bool:
    required = set(LLM_REQUIRED_FIELDS)
    if record.get("death_context") == "street":
        required.discard("facility_name")
    for key in required:
        value = record.get(key)
        if value is None or value == "" or value == "unknown":
            return True
    return False


def triplets_to_records(
    triplets: Iterable[dict[str, Any]],
    access_date: str,
    article_text_lookup: dict[str, str] | None = None,
    llm_extractor: DeathDetailExtractor | None = None,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    for triplet in triplets:
        title = _clean_string(triplet.get("title")) or ""
        who = _clean_string(triplet.get("who")) or ""
        what = _clean_string(triplet.get("what")) or ""
        target = _clean_string(triplet.get("target")) or ""
        where_text = _clean_string(triplet.get("where")) or ""
        base_text = " ".join(part for part in (title, who, what, target, where_text) if part).strip()
        if not base_text:
            continue
        if not _is_death_lead(base_text) or not _is_ice_related(base_text):
            continue

        published_at = _parse_iso_datetime(_clean_string(triplet.get("published_at")))
        if not published_at:
            continue
        if published_at.year not in (2025, 2026):
            continue

        city, county, state = _parse_location(where_text or None)

        person_name = None if _is_generic_actor(who) else who
        if not person_name and target and not _is_generic_actor(target):
            person_name = target
        if not person_name:
            continue
        date_of_death = published_at.date().isoformat()
        death_context = _infer_death_context(base_text)
        agency = _infer_agency(base_text)
        custody_status = _infer_custody_status(base_text)
        confidence = _score_confidence(base_text, person_name)
        manual_review = confidence < 70 or person_name is None

        source = _build_source(triplet, access_date, base_text)
        if not source:
            continue
        sources = [source]

        record_payload: dict[str, Any] = {
            "person_name": person_name,
            "aliases": [],
            "nationality": None,
            "age": None,
            "gender": None,
            "date_of_death": date_of_death,
            "date_precision": "day",
            "city": city,
            "county": county,
            "state": state,
            "facility_or_location": where_text,
            "lat": triplet.get("latitude"),
            "lon": triplet.get("longitude"),
            "geocode_source": _clean_string(triplet.get("geocode_status")),
            "death_context": death_context,
            "custody_status": custody_status,
            "agency": agency,
            "contractor_involved": "unknown",
            "cause_of_death_reported": what if _is_death_lead(what) else None,
            "manner_of_death": _infer_manner(base_text),
            "homicide_status": "suspected" if _infer_manner(base_text) else "unknown",
            "summary_1_sentence": title or what,
            "confidence_score": confidence,
            "manual_review": manual_review,
            "sources": sources,
        }

        article_text = None
        if article_text_lookup:
            url = _clean_string(triplet.get("url"))
            story_id = _clean_string(triplet.get("story_id"))
            if url:
                article_text = article_text_lookup.get(url)
            if not article_text and story_id:
                article_text = article_text_lookup.get(story_id)
        if article_text:
            status = _extract_investigation_status(article_text)
            if status:
                record_payload["investigation_status"] = status
            suspect_role, suspect_agency = _extract_suspect_role_and_agency(article_text)
            if suspect_role:
                record_payload["suspect_role"] = suspect_role
            if suspect_agency:
                record_payload["suspect_agency"] = suspect_agency
            suspect_identified = _extract_suspect_identified(article_text)
            if suspect_identified is not None:
                record_payload["suspect_identified"] = suspect_identified
            suspect_status = _extract_suspect_status(article_text)
            if suspect_status:
                record_payload["suspect_status"] = suspect_status

        if llm_extractor and article_text:
            if _needs_llm_enrichment(record_payload):
                llm_fields = llm_extractor.extract(
                    article_text,
                    person_name=person_name,
                    date_hint=date_of_death,
                    location_hint=where_text,
                )
                _apply_enrichment_fields(record_payload, llm_fields)
        if where_text and not record_payload.get("incident_location"):
            record_payload["incident_location"] = where_text

        record = normalize_record(record_payload, access_date)
        records.append(record)
    return records


def ice_report_entry_to_record(
    report: dict[str, Any],
    access_date: str,
    min_year: int,
) -> dict[str, Any] | None:
    date_of_death = _clean_string(report.get("date_of_death"))
    if not date_of_death:
        return None
    try:
        year = int(date_of_death[:4])
    except ValueError:
        return None
    if year < min_year:
        return None

    person_name = _clean_string(report.get("person_name"))
    report_urls = report.get("report_urls") or []
    sources = []
    for url in report_urls:
        if not url:
            continue
        sources.append(
            {
                "url": url,
                "publisher": "ice.gov",
                "publish_date": None,
                "access_date": access_date,
                "source_type": "official_report",
                "credibility_tier": "high",
                "snippet": "ICE detainee death report",
                "claim_tags": ["ice_death_report"],
            },
        )

    summary = f"ICE detainee death report for {person_name}" if person_name else None
    return normalize_record(
        {
            "person_name": person_name,
            "aliases": [],
            "nationality": _clean_string(report.get("country_of_citizenship")),
            "age": report.get("age"),
            "gender": _clean_string(report.get("gender")),
            "date_of_death": date_of_death,
            "date_precision": "day",
            "city": None,
            "county": None,
            "state": None,
            "initial_custody_location": _clean_string(report.get("initial_custody_location")),
            "death_location": _clean_string(report.get("death_location")),
            "facility_or_location": _clean_string(report.get("facility_or_location")),
            "lat": None,
            "lon": None,
            "geocode_source": None,
            "death_context": "detention",
            "custody_status": "ICE detention",
            "agency": "ICE",
            "contractor_involved": "unknown",
            "cause_of_death_reported": None,
            "manner_of_death": None,
            "homicide_status": "unknown",
            "summary_1_sentence": summary,
            "confidence_score": 90,
            "manual_review": not person_name,
            "sources": sources,
        },
        access_date,
    )


def ice_reports_to_records(
    report_path: Path,
    access_date: str,
    min_year: int,
) -> list[dict[str, Any]]:
    if not report_path.exists():
        return []
    records: list[dict[str, Any]] = []
    with report_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line:
                continue
            report = json.loads(line)
            record = ice_report_entry_to_record(report, access_date, min_year)
            if record:
                records.append(record)
    return records


def fetch_ice_report_records(
    urls: list[str],
    include_index: bool,
    index_url: str,
    use_playwright: bool,
    access_date: str,
    min_year: int,
    llm_location_enrich: bool = False,
    llm_model_id: str = death_reports.DEFAULT_LOCATION_LLM_MODEL_ID,
) -> list[dict[str, Any]]:
    if not urls and not include_index:
        return []
    llm_extractor = None
    if llm_location_enrich:
        try:
            llm_extractor = death_reports.DeathReportLocationExtractor(model_id=llm_model_id)
        except Exception as exc:
            print(
                f"Warning: ICE report location LLM init failed; continuing without it: {exc}",
            )
            llm_extractor = None
    try:
        report_entries = death_reports.fetch_report_entries(
            urls=urls,
            include_index=include_index,
            index_url=index_url,
            use_playwright=use_playwright,
            min_death_year=min_year,
            llm_extractor=llm_extractor,
        )
    finally:
        if llm_extractor is not None:
            llm_extractor.close()
    records: list[dict[str, Any]] = []
    for report in report_entries:
        record = ice_report_entry_to_record(report, access_date=access_date, min_year=min_year)
        if record:
            records.append(record)
    return records


def newsroom_to_records(
    access_date: str,
    limit: int,
    use_playwright: bool,
    debug: bool = False,
    max_pages: int = 1,
    min_year: int | None = None,
    stop_keys: set[str] | None = None,
) -> list[dict[str, Any]]:
    records: list[dict[str, Any]] = []
    releases = newsroom_deaths.fetch_death_releases(
        limit=limit,
        use_playwright=use_playwright,
        debug=debug,
        max_pages=max_pages,
        min_death_year=min_year,
        stop_keys=stop_keys,
    )
    for release in releases:
        date_of_death = release.get("date_of_death")
        if not date_of_death:
            continue
        try:
            year = int(date_of_death[:4])
        except ValueError:
            continue
        if min_year and year < min_year:
            continue

        person_name = _clean_string(release.get("person_name"))
        manual_review = not person_name
        summary = release.get("raw_sentence") or release.get("title")
        homicide_status = (
            "under_investigation" if release.get("under_investigation") else "unknown"
        )
        source = {
            "url": release.get("url"),
            "publisher": "ice.gov",
            "publish_date": release.get("release_date"),
            "access_date": access_date,
            "source_type": "official_release",
            "credibility_tier": "high",
            "snippet": _trim_words(_clean_string(summary), 25),
            "claim_tags": ["ice_release"],
        }
        record = normalize_record(
            {
                "person_name": person_name,
                "aliases": [],
                "nationality": _clean_string(release.get("nationality")),
                "age": release.get("age"),
                "gender": None,
                "date_of_death": date_of_death,
                "date_precision": "day",
                "city": _clean_string(release.get("city")),
                "county": None,
                "state": _clean_string(release.get("state")),
                "facility_or_location": _clean_string(release.get("facility_or_location")),
                "lat": None,
                "lon": None,
                "geocode_source": None,
                "death_context": "detention",
                "custody_status": "ICE detention",
                "agency": "ICE",
                "contractor_involved": "unknown",
                "cause_of_death_reported": None,
                "manner_of_death": None,
                "homicide_status": homicide_status,
                "summary_1_sentence": summary,
                "confidence_score": 85,
                "manual_review": manual_review,
                "sources": [source],
            },
            access_date,
        )
        records.append(record)
    return records


def iter_recent_triplets(window_days: int) -> Iterable[dict[str, Any]]:
    cutoff = datetime.now(timezone.utc) - timedelta(days=window_days)
    for path in sorted(DEFAULT_TRIPLETS_DIR.glob("triplets_*.jsonl")):
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                line = line.strip()
                if not line:
                    continue
                triplet = json.loads(line)
                published_at = _parse_iso_datetime(_clean_string(triplet.get("published_at")))
                if not published_at or published_at < cutoff:
                    continue
                yield triplet


def build_diff_path(out_dir: Path) -> Path:
    run_date = datetime.now(timezone.utc).date().isoformat()
    return out_dir / "diffs" / f"diff_{run_date}.jsonl"


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Daily deaths JSONL updater.")
    parser.add_argument("--window-days", type=int, default=14)
    parser.add_argument(
        "--include-triplets",
        action="store_true",
        help="Include death candidates from news triplets.",
    )
    parser.add_argument(
        "--triplet-article-text",
        action="store_true",
        help="Load article text to enrich triplet-derived deaths.",
    )
    parser.add_argument(
        "--triplet-llm-enrich",
        action="store_true",
        help="Use a local LLM to extract additional death details from articles.",
    )
    parser.add_argument("--triplet-llm-model-id", type=str, default=DEFAULT_DEATH_LLM_MODEL_ID)
    parser.add_argument("--triplet-llm-max-new-tokens", type=int, default=220)
    parser.add_argument("--triplet-llm-temperature", type=float, default=0.0)
    parser.add_argument("--triplet-llm-repetition-penalty", type=float, default=1.05)
    parser.add_argument("--triplet-llm-max-chars", type=int, default=8000)
    parser.add_argument("--out", type=Path, default=Path("./site/data"))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--preview-diff",
        action="store_true",
        help="Print diff entries to stdout (useful with --dry-run).",
    )
    parser.add_argument(
        "--preview-triplet-records",
        action="store_true",
        help="Print triplet-derived death records to stdout before merging.",
    )
    parser.add_argument("--skip-newsroom", action="store_true")
    parser.add_argument("--newsroom-limit", type=int, default=50)
    parser.add_argument("--newsroom-no-playwright", action="store_true")
    parser.add_argument("--newsroom-debug", action="store_true")
    parser.add_argument("--newsroom-max-pages", type=int, default=10)
    parser.add_argument("--newsroom-min-year", type=int, default=2025)
    parser.add_argument("--newsroom-no-stop-on-existing", action="store_true")
    parser.add_argument(
        "--ice-reports-path",
        type=Path,
        default=Path("./site/data/ice_death_reports.jsonl"),
    )
    parser.add_argument("--ice-report-url", action="append", default=[])
    parser.add_argument("--ice-report-url-file", type=Path)
    parser.add_argument("--ice-report-include-index", action="store_true")
    parser.add_argument("--ice-report-index-url", type=str, default=death_reports.ICE_REPORTS_INDEX_URL)
    parser.add_argument("--ice-report-use-playwright", action="store_true")
    parser.add_argument(
        "--ice-report-llm-location-enrich",
        action="store_true",
        help="Use local LLM fallback for custody/death location extraction in ICE report text.",
    )
    parser.add_argument(
        "--ice-report-llm-model-id",
        type=str,
        default=death_reports.DEFAULT_LOCATION_LLM_MODEL_ID,
    )
    parser.add_argument("--skip-ice-reports", action="store_true")
    parser.add_argument("--ice-min-year", type=int, default=2025)
    args = parser.parse_args(argv)

    access_date = datetime.now(timezone.utc).date().isoformat()
    existing_raw = load_jsonl(args.out / "deaths.jsonl")
    existing = {
        record_id: normalize_record(record, access_date)
        for record_id, record in existing_raw.items()
    }
    newsroom_stop_keys: set[str] | None = None
    if not args.newsroom_no_stop_on_existing:
        newsroom_stop_keys = set()
        for record in existing.values():
            name = _clean_string(record.get("person_name"))
            date_value = _clean_string(record.get("date_of_death"))
            if name and date_value:
                newsroom_stop_keys.add(f"{name.lower()}|{date_value}")

    incoming_records: list[dict[str, Any]] = []
    triplet_records: list[dict[str, Any]] = []
    if args.include_triplets:
        triplets = list(iter_recent_triplets(args.window_days))
        article_text_lookup = None
        if args.triplet_article_text or args.triplet_llm_enrich:
            article_text_lookup = build_article_text_lookup(triplets, args.window_days)
        llm_extractor = None
        if args.triplet_llm_enrich:
            try:
                llm_extractor = DeathDetailExtractor(
                    model_id=args.triplet_llm_model_id,
                    temperature=args.triplet_llm_temperature,
                    repetition_penalty=args.triplet_llm_repetition_penalty,
                    max_new_tokens=args.triplet_llm_max_new_tokens,
                    max_chars=args.triplet_llm_max_chars,
                )
            except Exception as exc:
                print(
                    f"Warning: LLM init failed; continuing without LLM enrichment: {exc}",
                    file=sys.stderr,
                )
                llm_extractor = None
        try:
            triplet_records = triplets_to_records(
                triplets,
                access_date,
                article_text_lookup=article_text_lookup,
                llm_extractor=llm_extractor,
            )
        finally:
            if llm_extractor is not None:
                llm_extractor.close()
        incoming_records.extend(triplet_records)

    if not args.skip_ice_reports:
        ice_urls = list(args.ice_report_url)
        if args.ice_report_url_file:
            if args.ice_report_url_file.exists():
                ice_urls.extend(death_reports.load_url_file(args.ice_report_url_file))
            else:
                print(f"Warning: ICE report URL file not found: {args.ice_report_url_file}")

        try:
            incoming_records.extend(
                ice_reports_to_records(
                    report_path=args.ice_reports_path,
                    access_date=access_date,
                    min_year=args.ice_min_year,
                ),
            )
        except Exception as exc:
            print(f"Warning: ICE report ingest failed: {exc}")

        try:
            incoming_records.extend(
                fetch_ice_report_records(
                    urls=ice_urls,
                    include_index=args.ice_report_include_index,
                    index_url=args.ice_report_index_url,
                    use_playwright=args.ice_report_use_playwright,
                    access_date=access_date,
                    min_year=args.ice_min_year,
                    llm_location_enrich=args.ice_report_llm_location_enrich,
                    llm_model_id=args.ice_report_llm_model_id,
                ),
            )
        except Exception as exc:
            print(f"Warning: ICE report fetch failed: {exc}")

    if not args.skip_newsroom:
        try:
            incoming_records.extend(
                newsroom_to_records(
                    access_date=access_date,
                    limit=args.newsroom_limit,
                    use_playwright=not args.newsroom_no_playwright,
                    debug=args.newsroom_debug,
                    max_pages=args.newsroom_max_pages,
                    min_year=args.newsroom_min_year,
                    stop_keys=newsroom_stop_keys,
                ),
            )
        except Exception as exc:
            print(f"Warning: newsroom ingest failed: {exc}")

    if args.preview_triplet_records:
        for record in triplet_records:
            print(json.dumps(_order_fields(record, FIELD_ORDER), ensure_ascii=True))

    merged, diff_entries, summary = merge_records(existing, incoming_records)
    merged = collapse_duplicate_records(merged)
    ordered = sorted(
        [record for record in merged.values() if not _should_drop_record(record)],
        key=lambda record: (
            record.get("date_of_death") or "",
            record.get("person_name") or "",
            record.get("id"),
        ),
    )
    ordered = [_order_fields(record, FIELD_ORDER) for record in ordered]

    if args.preview_diff:
        for entry in diff_entries:
            print(json.dumps(entry, ensure_ascii=True))

    if not args.dry_run:
        write_jsonl_atomic(args.out / "deaths.jsonl", ordered)
        write_json_atomic(args.out / "deaths.json", ordered)
        write_json_atomic(args.out / "index.json", build_index(ordered))
        diff_path = build_diff_path(args.out)
        if diff_entries:
            write_jsonl_atomic(diff_path, diff_entries)
        else:
            diff_path.parent.mkdir(parents=True, exist_ok=True)

    total_manual = sum(1 for record in ordered if record.get("manual_review"))
    print(
        "Deaths update complete:",
        f"records={len(ordered)}",
        f"added={summary['added']}",
        f"updated={summary['updated']}",
        f"manual_review={total_manual}",
    )
    return 0
