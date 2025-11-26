"""
Utilities for collecting ICE-related news articles from multiple upstream providers. The
module exposes a reusable `NewsIngestor` plus concrete source clients that can be
imported by CLI scripts or cron jobs.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sqlite3
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from pathlib import Path
from typing import Any, Dict, Iterable, List, Sequence, Set

import feedparser
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from geotext import GeoText
import geotext.geotext as geotext_module

from src.services.geocoding import GeocodeResult, NominatimGeocoder

LOGGER = logging.getLogger(__name__)

FACILITY_CATALOG_PATH = Path("assets/ice_facilities.csv")

# Broad search tokens that surface ICE activity regardless of outlet.
DEFAULT_SEARCH_TERMS = [
    "ice raid",
    "ice agents",
    "ice detention",
    "immigration enforcement operation",
    "immigration enforcement",
    "immigration arrest",
    "immigration raid",
    "border patrol",
    "deportation",
]

DEFAULT_RELEVANCE_KEYWORDS = [
    "ice",
    "immigration",
    "immigration and customs enforcement",
    "customs enforcement",
    "border patrol",
    "deportation",
    "detention center",
    "immigration raid",
    "immigration court",
]

STATE_NAMES = [
    "Alabama",
    "Alaska",
    "Arizona",
    "Arkansas",
    "California",
    "Colorado",
    "Connecticut",
    "Delaware",
    "Florida",
    "Georgia",
    "Hawaii",
    "Idaho",
    "Illinois",
    "Indiana",
    "Iowa",
    "Kansas",
    "Kentucky",
    "Louisiana",
    "Maine",
    "Maryland",
    "Massachusetts",
    "Michigan",
    "Minnesota",
    "Mississippi",
    "Missouri",
    "Montana",
    "Nebraska",
    "Nevada",
    "New Hampshire",
    "New Jersey",
    "New Mexico",
    "New York",
    "North Carolina",
    "North Dakota",
    "Ohio",
    "Oklahoma",
    "Oregon",
    "Pennsylvania",
    "Rhode Island",
    "South Carolina",
    "South Dakota",
    "Tennessee",
    "Texas",
    "Utah",
    "Vermont",
    "Virginia",
    "Washington",
    "West Virginia",
    "Wisconsin",
    "Wyoming",
]

STATE_ABBREVIATIONS: Dict[str, str] = {
    "AL": "Alabama",
    "AK": "Alaska",
    "AZ": "Arizona",
    "AR": "Arkansas",
    "CA": "California",
    "CO": "Colorado",
    "CT": "Connecticut",
    "DE": "Delaware",
    "FL": "Florida",
    "GA": "Georgia",
    "HI": "Hawaii",
    "ID": "Idaho",
    "IL": "Illinois",
    "IN": "Indiana",
    "IA": "Iowa",
    "KS": "Kansas",
    "KY": "Kentucky",
    "LA": "Louisiana",
    "ME": "Maine",
    "MD": "Maryland",
    "MA": "Massachusetts",
    "MI": "Michigan",
    "MN": "Minnesota",
    "MS": "Mississippi",
    "MO": "Missouri",
    "MT": "Montana",
    "NE": "Nebraska",
    "NV": "Nevada",
    "NH": "New Hampshire",
    "NJ": "New Jersey",
    "NM": "New Mexico",
    "NY": "New York",
    "NC": "North Carolina",
    "ND": "North Dakota",
    "OH": "Ohio",
    "OK": "Oklahoma",
    "OR": "Oregon",
    "PA": "Pennsylvania",
    "RI": "Rhode Island",
    "SC": "South Carolina",
    "SD": "South Dakota",
    "TN": "Tennessee",
    "TX": "Texas",
    "UT": "Utah",
    "VT": "Vermont",
    "VA": "Virginia",
    "WA": "Washington",
    "WV": "West Virginia",
    "WI": "Wisconsin",
    "WY": "Wyoming",
}

STATE_PATTERN = re.compile(r"\b(" + "|".join(STATE_NAMES) + r")\b", re.IGNORECASE)
STATE_NAMES_PATTERN = "|".join(re.escape(name) for name in STATE_NAMES)
STATE_ABBREVIATION_PATTERN = "|".join(STATE_ABBREVIATIONS.keys())

CITY_STATE_PATTERN = re.compile(
    rf"\b([A-Z][a-z]+(?:\s+[A-Z][a-z]+){{0,2}})\s*,\s*(?:(?P<state_full>{STATE_NAMES_PATTERN})|"
    rf"(?P<state_abbr>{STATE_ABBREVIATION_PATTERN}))\b",
    re.IGNORECASE,
)


def extract_locations(text: str) -> List[str]:
    """Return a sorted list of US state names found in the provided text."""
    if not text:
        return []
    matches = {match.group(0).title() for match in STATE_PATTERN.finditer(text)}
    return sorted(matches)


def load_us_city_index() -> Dict[str, Set[str]]:
    """Map lowercase city names to the US states they belong to."""
    city_index: Dict[str, Set[str]] = {}
    data_path = Path(geotext_module.get_data_path("cities15000.txt"))
    if not data_path.exists():
        LOGGER.warning("City dataset at %s not found; city extraction disabled.", data_path)
        return city_index
    with data_path.open("r", encoding="utf-8") as handle:
        for line in handle:
            parts = line.split("\t")
            if len(parts) < 11:
                continue
            country_code = parts[8]
            if country_code != "US":
                continue
            admin1 = parts[10].strip()
            state_name = STATE_ABBREVIATIONS.get(admin1)
            if not state_name:
                continue
            variants = {
                parts[1].strip(),
                parts[2].strip(),
            }
            if len(parts) > 3 and parts[3].strip():
                variants.update(name.strip() for name in parts[3].split(","))
            for variant in variants:
                if not variant:
                    continue
                key = variant.lower()
                city_index.setdefault(key, set()).add(state_name)
    return city_index


CITY_INDEX = load_us_city_index()


def extract_city_mentions(text: str) -> List[str]:
    """Return city-level mentions (City, State) found in the text."""
    if not text or not CITY_INDEX:
        return []
    mentions: Set[str] = set()
    for match in CITY_STATE_PATTERN.finditer(text):
        city = match.group(1)
        raw_state = match.group("state_full") or match.group("state_abbr")
        if not city or not raw_state:
            continue
        state_name = STATE_ABBREVIATIONS.get(raw_state.upper(), raw_state.title())
        mentions.add(f"{city.title()}, {state_name}")
    try:
        geo = GeoText(text, country="US")
    except Exception:  # noqa: BLE001
        LOGGER.debug("GeoText failed to parse text snippet.", exc_info=True)
        return []
    for raw_city in geo.cities:
        normalized = raw_city.strip()
        if not normalized:
            continue
        title_city = normalized.title()
        if title_city in STATE_NAMES:
            continue
        states = CITY_INDEX.get(title_city.lower())
        if not states:
            mentions.add(title_city)
            continue
        for state in states:
            mentions.add(f"{title_city}, {state}")
    return sorted(mentions)


FACILITY_PATTERN = re.compile(
    r"(?:(?:the|a|an)\s+)?((?:[A-Z][\w'&-]*\s){0,4}"
    r"(?:courthouse|detention center|detention facility|processing center|"
    r"immigration court|field office|ice office|county jail|county courthouse|jail|prison))",
    re.IGNORECASE,
)


def extract_facility_mentions(text: str) -> List[str]:
    """Return named facilities such as courthouses or detention centers."""
    if not text:
        return []
    mentions = {
        match.group(1).strip().title()
        for match in FACILITY_PATTERN.finditer(text)
    }
    return sorted(mentions)


def _normalize_title(value: str | None) -> str:
    if not value:
        return ""
    cleaned = re.sub(r"\W+", " ", value).strip().lower()
    return cleaned


def _normalize_text(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"\s+", " ", value).strip().lower()


@dataclass
class NewsReport:
    source: str
    source_id: str
    title: str
    url: str
    summary: str | None
    published_at: datetime | None
    locations: List[str]
    city_mentions: List[str]
    facility_mentions: List[str]
    raw: dict[str, Any]
    latitude: float | None = None
    longitude: float | None = None
    geocode_query: str | None = None

    def to_serializable(self) -> dict[str, Any]:
        return {
            "source": self.source,
            "source_id": self.source_id,
            "title": self.title,
            "url": self.url,
            "summary": self.summary,
            "published_at": self.published_at.isoformat() if self.published_at else None,
            "locations": self.locations,
            "city_mentions": self.city_mentions,
            "facility_mentions": self.facility_mentions,
            "latitude": self.latitude,
            "longitude": self.longitude,
            "geocode_query": self.geocode_query,
            "raw": self.raw,
        }


class BaseNewsSource:
    """Abstract interface for concrete upstream sources."""

    name: str

    def fetch(self, search_terms: Sequence[str]) -> List[NewsReport]:
        raise NotImplementedError


@dataclass
class RSSFeedConfig:
    name: str
    url: str


@dataclass
class FacilityRecord:
    name: str
    address: str
    city: str
    state: str
    latitude: float | None = None
    longitude: float | None = None

    @property
    def geocode_query(self) -> str:
        parts = [self.address, self.city, self.state]
        return ", ".join(part for part in parts if part)


def load_facility_catalog(path: Path = FACILITY_CATALOG_PATH) -> list[FacilityRecord]:
    if not path.exists():
        LOGGER.info("Facility catalog %s not found; continuing without facility hints.", path)
        return []
    records: list[FacilityRecord] = []
    import csv

    try:
        with path.open("r", encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            for row in reader:
                try:
                    name = (row.get("name") or "").strip()
                    address = (row.get("address") or "").strip()
                    city = (row.get("city") or "").strip()
                    state = (row.get("state") or "").strip()
                    lat_raw = (row.get("latitude") or "").strip()
                    lon_raw = (row.get("longitude") or "").strip()
                except Exception:
                    LOGGER.debug("Skipping malformed facility row: %s", row)
                    continue
                if not name or not city or not state or not address:
                    continue
                try:
                    latitude = float(lat_raw) if lat_raw else None
                    longitude = float(lon_raw) if lon_raw else None
                except ValueError:
                    latitude = longitude = None
                records.append(
                    FacilityRecord(
                        name=name,
                        address=address,
                        city=city,
                        state=state,
                        latitude=latitude,
                        longitude=longitude,
                    )
                )
    except Exception:  # noqa: BLE001
        LOGGER.warning("Failed to read facility catalog at %s", path, exc_info=True)
        return []
    LOGGER.info("Loaded %s facility records from %s", len(records), path)
    return records


class RSSFeedSource(BaseNewsSource):
    def __init__(
        self,
        feeds: Sequence[RSSFeedConfig],
        include_all: bool = False,
        timeout: int = 15,
    ) -> None:
        self.name = "rss"
        self.feeds = feeds
        self.include_all = include_all
        self.timeout = timeout

    def fetch(self, search_terms: Sequence[str]) -> List[NewsReport]:
        reports: List[NewsReport] = []
        search_pattern = (
            re.compile("|".join(re.escape(term) for term in search_terms), re.IGNORECASE)
            if search_terms
            else None
        )
        for feed in self.feeds:
            matched = 0
            parsed = feedparser.parse(feed.url)
            if parsed.bozo:
                LOGGER.warning("RSS parse issue for %s: %s", feed.url, parsed.bozo_exception)
            for entry in parsed.entries:
                summary = getattr(entry, "summary", "")
                title = getattr(entry, "title", "")
                text_blob = f"{title}\n{summary}"
                if search_pattern and not self.include_all and not search_pattern.search(text_blob):
                    continue
                matched += 1
                published = self._parse_date(getattr(entry, "published", None))
                city_mentions = extract_city_mentions(text_blob)
                facility_mentions = extract_facility_mentions(text_blob)
                report = NewsReport(
                    source=f"rss:{feed.name}",
                    source_id=getattr(entry, "id", getattr(entry, "link", "")),
                    title=title,
                    url=getattr(entry, "link", ""),
                    summary=summary or None,
                    published_at=published,
                    locations=extract_locations(text_blob),
                    city_mentions=city_mentions,
                    facility_mentions=facility_mentions,
                    raw={
                        key: getattr(entry, key)
                        for key in ("title", "summary", "link", "published", "id")
                        if hasattr(entry, key)
                    },
                )
                reports.append(report)
            LOGGER.debug(
                "RSS feed %s yielded %s filtered articles out of %s entries",
                feed.name,
                matched,
                len(getattr(parsed, "entries", [])),
            )
        return reports

    @staticmethod
    def _parse_date(raw: str | None) -> datetime | None:
        if not raw:
            return None
        try:
            parsed = parsedate_to_datetime(raw)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed.astimezone(timezone.utc)
        except (TypeError, ValueError):
            LOGGER.debug("Unable to parse RSS date %s", raw, exc_info=True)
            return None


class NewsApiSource(BaseNewsSource):
    """Thin wrapper over the News API `/v2/everything` endpoint."""

    endpoint = "https://newsapi.org/v2/everything"

    def __init__(
        self,
        api_key: str | None,
        page_size: int = 100,
        timeout: int = 15,
        from_date: datetime | None = None,
        to_date: datetime | None = None,
    ) -> None:
        self.name = "newsapi"
        self.api_key = api_key
        self.page_size = page_size
        self.timeout = timeout
        self.from_date = from_date
        self.to_date = to_date

    def fetch(self, search_terms: Sequence[str]) -> List[NewsReport]:
        if not self.api_key:
            LOGGER.debug("Skipping NewsAPI source because NEWSAPI_KEY is not configured.")
            return []
        query = " OR ".join(search_terms)
        params = {
            "q": query,
            "language": "en",
            "sortBy": "publishedAt",
            "pageSize": self.page_size,
        }
        if self.from_date:
            params["from"] = self.from_date.strftime("%Y-%m-%d")
        if self.to_date:
            params["to"] = self.to_date.strftime("%Y-%m-%d")
        headers = {"X-Api-Key": self.api_key}
        response = requests.get(self.endpoint, params=params, headers=headers, timeout=self.timeout)
        response.raise_for_status()
        payload = response.json()
        if payload.get("status") != "ok":
            LOGGER.warning("Unexpected NewsAPI payload: %s", payload)
            return []
        articles = payload.get("articles", [])
        reports: List[NewsReport] = []
        for article in articles:
            content_blob = "\n".join(
                part
                for part in (
                    article.get("title"),
                    article.get("description"),
                    article.get("content"),
                )
                if part
            )
            states = extract_locations(content_blob)
            cities = extract_city_mentions(content_blob)
            facilities = extract_facility_mentions(content_blob)
            reports.append(
                NewsReport(
                    source="newsapi",
                    source_id=article.get("url", ""),
                    title=article.get("title", ""),
                    url=article.get("url", ""),
                    summary=article.get("description"),
                    published_at=self._parse_date(article.get("publishedAt")),
                    locations=states,
                    city_mentions=cities,
                    facility_mentions=facilities,
                    raw=article,
                )
            )
        return reports

    @staticmethod
    def _parse_date(raw: str | None) -> datetime | None:
        if not raw:
            return None
        try:
            parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
            return parsed.astimezone(timezone.utc)
        except ValueError:
            LOGGER.debug("Unable to parse NewsAPI date %s", raw, exc_info=True)
            return None


class GdeltDocSource(BaseNewsSource):
    """Fetch data from the GDELT Document API."""

    endpoint = "https://api.gdeltproject.org/api/v2/doc/doc"

    def __init__(
        self,
        max_records: int = 200,
        timeout: int = 20,
        start_date: datetime | None = None,
        end_date: datetime | None = None,
        max_days: int | None = 7,
    ) -> None:
        self.name = "gdelt"
        self.max_records = max_records
        self.timeout = timeout
        self.start_date = start_date
        self.end_date = end_date
        self.max_days = max_days

    def fetch(self, search_terms: Sequence[str]) -> List[NewsReport]:
        sanitized_terms = [term.replace('"', "").strip() for term in search_terms if term.strip()]
        if not sanitized_terms:
            return []
        joined_terms = " OR ".join(f'"{term}"' for term in sanitized_terms)
        query = f"({joined_terms})" if len(sanitized_terms) > 1 else joined_terms
        params = {
            "query": query,
            "format": "json",
            "maxrecords": self.max_records,
            "sort": "datedesc",
        }
        if self.start_date or self.end_date:
            if self.start_date:
                params["startdatetime"] = self._format_date(self.start_date)
            if self.end_date:
                params["enddatetime"] = self._format_date(self.end_date)
        elif self.max_days:
            params["maxdays"] = self.max_days
        response = requests.get(self.endpoint, params=params, timeout=self.timeout)
        response.raise_for_status()
        try:
            payload = response.json()
        except ValueError:
            LOGGER.warning(
                "GDELT returned non-JSON response (status %s): %s",
                response.status_code,
                response.text[:300],
            )
            return []
        articles = payload.get("articles") or []
        reports: List[NewsReport] = []
        for article in articles:
            title = article.get("title", "")
            summary = article.get("seendate")
            text_blob = " ".join(
                value
                for value in (
                    title,
                    article.get("url"),
                    article.get("sourceCountry"),
                    article.get("lang"),
                )
                if value
            )
            states = extract_locations(text_blob)
            cities = extract_city_mentions(text_blob)
            facilities = extract_facility_mentions(text_blob)
            reports.append(
                NewsReport(
                    source="gdelt",
                    source_id=article.get("url", ""),
                    title=title,
                    url=article.get("url", ""),
                    summary=summary,
                    published_at=self._parse_date(article.get("seendate")),
                    locations=states,
                    city_mentions=cities,
                    facility_mentions=facilities,
                    raw=article,
                )
            )
        return reports

    @staticmethod
    def _parse_date(raw: str | None) -> datetime | None:
        if not raw:
            return None
        try:
            parsed = datetime.strptime(raw, "%Y%m%d%H%M%S")
            parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        except ValueError:
            LOGGER.debug("Unable to parse GDELT date %s", raw, exc_info=True)
            return None

    @staticmethod
    def _format_date(value: datetime) -> str:
        return value.strftime("%Y%m%d%H%M%S")


class NewsIngestor:
    """Orchestrates multiple news sources and writes aggregated JSON lines output."""

    def __init__(
        self,
        sources: Sequence[BaseNewsSource],
        output_dir: Path,
        search_terms: Sequence[str] | None = None,
        print_headlines: bool = False,
        relevance_keywords: Sequence[str] | None = None,
        min_keyword_matches: int = 1,
        disable_filtering: bool = False,
        geocoder: NominatimGeocoder | None = None,
        fetch_content: bool = False,
        fetch_content_limit: int | None = None,
        geocode_max_queries: int | None = None,
    ) -> None:
        self.sources = list(sources)
        self.output_dir = output_dir
        self.search_terms = list(search_terms or DEFAULT_SEARCH_TERMS)
        self.print_headlines = print_headlines
        self.relevance_keywords = [
            kw.lower() for kw in (relevance_keywords or DEFAULT_RELEVANCE_KEYWORDS)
        ]
        self.min_keyword_matches = max(1, min_keyword_matches)
        self.disable_filtering = disable_filtering
        self.geocoder = geocoder
        self.story_index_path = output_dir / "story_index.json"
        self.sqlite_path = output_dir / "news_index.sqlite"
        self.fetch_content = fetch_content
        self.fetch_content_limit = fetch_content_limit
        self.geocode_max_queries = geocode_max_queries
        self.facilities = load_facility_catalog()
        self._facility_norms = [
            (
                _normalize_text(record.name),
                _normalize_text(f"{record.city}, {record.state}"),
                record,
            )
            for record in self.facilities
        ]

    def run(self) -> Path | None:
        LOGGER.info(
            "Running NewsIngestor with %s sources; fetch_content=%s (limit=%s)",
            len(self.sources),
            self.fetch_content,
            self.fetch_content_limit,
        )
        metrics = {"facility_hits": 0}
        aggregated: list[NewsReport] = []
        dedup: set[str] = set()
        for source in self.sources:
            LOGGER.info("Fetching from %s", getattr(source, "name", source.__class__.__name__))
            try:
                reports = source.fetch(self.search_terms)
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("Error while fetching from %s: %s", source.name, exc)
                continue
            LOGGER.info("Source %s returned %s candidate reports", source.name, len(reports))
            for report in reports:
                LOGGER.debug("Candidate headline [%s]: %s", report.source, report.title)
                if self.print_headlines:
                    LOGGER.info("Headline [%s]: %s", report.source, report.title)
            for report in reports:
                dedup_key = f"{report.source}:{report.source_id}"
                if dedup_key in dedup:
                    continue
                dedup.add(dedup_key)
                aggregated.append(report)
        filtered_reports = self._apply_relevance_filter(aggregated)
        if self.fetch_content:
            self._enrich_with_full_text(filtered_reports)
        self._annotate_coordinates(filtered_reports, metrics)
        story_index = self._load_story_index()
        LOGGER.info("Loaded story index with %s entries", len(story_index))
        deduped_reports = self._dedupe_with_index(filtered_reports, story_index)
        self._init_sqlite()
        self._upsert_sqlite(filtered_reports)
        if not deduped_reports:
            LOGGER.warning("No news reports collected during this run.")
            return None
        timestamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
        self.output_dir.mkdir(parents=True, exist_ok=True)
        output_path = self.output_dir / f"news_reports_{timestamp}.jsonl"
        with output_path.open("w", encoding="utf-8") as handle:
            for report in deduped_reports:
                handle.write(json.dumps(report.to_serializable()) + "\n")
        self._save_story_index(story_index)
        LOGGER.info("Wrote %s reports to %s", len(deduped_reports), output_path)
        self._write_run_log(deduped_reports, metrics)
        return output_path

    def _apply_relevance_filter(self, reports: list[NewsReport]) -> list[NewsReport]:
        if not reports:
            LOGGER.info("No reports to filter.")
            return reports
        if self.disable_filtering:
            LOGGER.info("Keyword filtering disabled; keeping all %s reports.", len(reports))
            return reports
        filtered: list[NewsReport] = []
        dropped = 0
        for report in reports:
            if self._is_relevant(report):
                filtered.append(report)
            else:
                dropped += 1
        if dropped:
            LOGGER.info("Filtered out %s reports that did not meet keyword criteria.", dropped)
        LOGGER.info("Keeping %s reports after relevance filtering.", len(filtered))
        return filtered

    def _is_relevant(self, report: NewsReport) -> bool:
        text_segments: list[str] = []
        for segment in (report.title, report.summary):
            if segment:
                text_segments.append(segment)
        if isinstance(report.raw, dict):
            for key in ("description", "content", "body", "text"):
                value = report.raw.get(key)
                if isinstance(value, str):
                    text_segments.append(value)
        blob = " ".join(text_segments).lower()
        if not blob:
            return False
        hits = [kw for kw in self.relevance_keywords if kw in blob]
        if len(hits) >= self.min_keyword_matches:
            LOGGER.debug("Report '%s' matched keywords %s", report.title, hits)
            return True
        LOGGER.debug(
            "Report '%s' dropped due to insufficient keyword matches (%s)",
            report.title,
            hits,
        )
        return False

    def _annotate_coordinates(self, reports: list[NewsReport], metrics: dict[str, int]) -> None:
        if not reports or not self.geocoder:
            return
        start = time.time()
        attempts = 0
        resolved = 0
        seen_queries: Set[str] = set()
        LOGGER.info("Geocoding %s reports...", len(reports))
        for idx, report in enumerate(reports, start=1):
            for query in self._iter_geocode_queries(report):
                normalized = query.strip().lower()
                if normalized in seen_queries:
                    LOGGER.debug("Skipping duplicate geocode query '%s'", query)
                    continue
                seen_queries.add(normalized)
                facility_hit = self._lookup_facility(query)
                if facility_hit:
                    LOGGER.info("Geocode resolved via facility catalog: '%s' -> %s", query, facility_hit.query)
                    report.latitude = facility_hit.latitude
                    report.longitude = facility_hit.longitude
                    report.geocode_query = facility_hit.query
                    metrics["facility_hits"] = metrics.get("facility_hits", 0) + 1
                    resolved += 1
                    break
                attempts += 1
                if self.geocode_max_queries is not None and attempts > self.geocode_max_queries:
                    LOGGER.warning(
                        "Geocode max queries (%s) reached; skipping remaining lookups.",
                        self.geocode_max_queries,
                    )
                    duration = time.time() - start
                    LOGGER.info(
                        "Geocoding halted early: resolved %s of %s queries in %.1fs.",
                        resolved,
                        attempts,
                        duration,
                    )
                    return
                LOGGER.info("Geocode attempt %s: '%s'", attempts, query)
                result = self.geocoder.lookup(query)
                if result:
                    source = "cache"
                    if hasattr(self.geocoder, "stats"):
                        stats = self.geocoder.stats
                        # Determine last increment by comparing stats before/after? Not tracked per-call.
                        # We infer source by presence in cache immediately; otherwise rely on order (Nominatim then Google).
                        # For explicitness, the geocoder has already tracked stats; we log a generic success here.
                    LOGGER.info("Geocode resolved via external service: '%s' -> %s,%s", query, result.latitude, result.longitude)
                    report.latitude = result.latitude
                    report.longitude = result.longitude
                    report.geocode_query = result.query
                    resolved += 1
                    break
                else:
                    LOGGER.info("Geocode failed for '%s'", query)
            if idx % 5 == 0:
                LOGGER.info(
                    "Geocoding progress: %s/%s reports (resolved %s of %s queries so far)",
                    idx,
                    len(reports),
                    resolved,
                    attempts,
                )
        duration = time.time() - start
        LOGGER.info(
            "Geocoding complete: resolved %s of %s queries in %.1fs.",
            resolved,
            attempts,
            duration,
        )

    def _enrich_with_full_text(self, reports: list[NewsReport]) -> None:
        """Fetch article bodies and augment location/facility extraction."""
        if not reports:
            LOGGER.info("No reports to fetch full content for.")
            return
        fetched = 0
        for report in reports:
            if self.fetch_content_limit is not None and fetched >= self.fetch_content_limit:
                break
            if not report.url:
                continue
            LOGGER.info("Fetching full content for [%s] %s", report.source, report.title)
            content = self._fetch_article_text(report.url)
            if not content:
                continue
            fetched += 1
            report.raw["fetched_content"] = content
            report.locations = sorted(
                set(report.locations) | set(extract_locations(content))
            )
            report.city_mentions = sorted(
                set(report.city_mentions) | set(extract_city_mentions(content))
            )
            report.facility_mentions = sorted(
                set(report.facility_mentions) | set(extract_facility_mentions(content))
            )
        LOGGER.info("Fetched full content for %s articles.", fetched)

    @staticmethod
    def _fetch_article_text(url: str, timeout: int = 12) -> str | None:
        try:
            response = requests.get(
                url,
                timeout=timeout,
                headers={"User-Agent": "news-ingestor/1.0 (+https://example.org)"},
            )
            response.raise_for_status()
        except Exception:  # noqa: BLE001
            LOGGER.debug("Failed to fetch article body for %s", url, exc_info=True)
            return None
        soup = BeautifulSoup(response.text, "html.parser")
        for tag in soup(["script", "style", "noscript"]):
            tag.decompose()
        text = soup.get_text(separator=" ", strip=True)
        return text or None

    def _dedupe_with_index(
        self, reports: list[NewsReport], story_index: dict[str, dict[str, Any]]
    ) -> list[NewsReport]:
        """Drop stories seen before while enriching a persistent index."""
        if not reports:
            return reports
        now_iso = datetime.now(timezone.utc).isoformat()
        unique_reports: list[NewsReport] = []
        for report in reports:
            key = _normalize_title(report.title)
            if not key:
                unique_reports.append(report)
                continue
            entry = story_index.get(key)
            published_iso = report.published_at.isoformat() if report.published_at else None
            geocode_query = report.geocode_query
            if entry:
                if report.source not in entry["sources"]:
                    entry["sources"].append(report.source)
                if published_iso and published_iso not in entry["published_dates"]:
                    entry["published_dates"].append(published_iso)
                if geocode_query:
                    entry.setdefault("geocodes", [])
                    if geocode_query not in entry["geocodes"]:
                        entry["geocodes"].append(geocode_query)
                entry["last_seen"] = now_iso
                story_index[key] = entry
                continue
            story_index[key] = {
                "title": report.title,
                "first_seen": now_iso,
                "last_seen": now_iso,
                "sources": [report.source],
                "published_dates": [published_iso] if published_iso else [],
                "geocodes": [geocode_query] if geocode_query else [],
            }
            unique_reports.append(report)
        return unique_reports

    def _load_story_index(self) -> dict[str, dict[str, Any]]:
        if not self.story_index_path.exists():
            return {}
        try:
            with self.story_index_path.open("r", encoding="utf-8") as handle:
                payload = json.load(handle)
                if isinstance(payload, dict):
                    return payload
        except Exception:  # noqa: BLE001
            LOGGER.warning("Failed to read story index; starting fresh.", exc_info=True)
        return {}

    def _save_story_index(self, story_index: dict[str, dict[str, Any]]) -> None:
        try:
            with self.story_index_path.open("w", encoding="utf-8") as handle:
                json.dump(story_index, handle, indent=2)
        except Exception:  # noqa: BLE001
            LOGGER.warning("Failed to persist story index to %s", self.story_index_path, exc_info=True)

    def _lookup_facility(self, query: str) -> GeocodeResult | None:
        if not self.facilities:
            return None
        norm_query = _normalize_text(query)
        for name_norm, city_state_norm, facility in self._facility_norms:
            if not norm_query:
                continue
            if (name_norm and name_norm in norm_query) or (
                city_state_norm and city_state_norm in norm_query
            ):
                if facility.latitude is not None and facility.longitude is not None:
                    return GeocodeResult(
                        query=facility.geocode_query,
                        latitude=facility.latitude,
                        longitude=facility.longitude,
                        raw={"source": "facility_catalog", "name": facility.name},
                    )
                if self.geocoder:
                    # Geocode the facility address once and keep it in memory/cache.
                    result = self.geocoder.lookup(facility.geocode_query)
                    if result:
                        facility.latitude = result.latitude
                        facility.longitude = result.longitude
                        return result
        return None

    def _write_run_log(self, reports: list[NewsReport], metrics: dict[str, int]) -> None:
        log_path = self.output_dir / "run_log.csv"
        header = [
            "# fields: timestamp_iso,new_articles,facility_hits,geocode_cache_hits,geocode_nominatim_hits,geocode_google_hits,geocode_failures",
        ]
        timestamp = datetime.now(timezone.utc).isoformat()
        geocode_stats = (
            self.geocoder.stats
            if self.geocoder and hasattr(self.geocoder, "stats")
            else {"cache_hits": 0, "nominatim_hits": 0, "google_hits": 0, "failures": 0}
        )
        line = ",".join(
            [
                timestamp,
                str(len(reports)),
                str(metrics.get("facility_hits", 0)),
                str(geocode_stats.get("cache_hits", 0)),
                str(geocode_stats.get("nominatim_hits", 0)),
                str(geocode_stats.get("google_hits", 0)),
                str(geocode_stats.get("failures", 0)),
            ]
        )
        if not log_path.exists():
            log_path.parent.mkdir(parents=True, exist_ok=True)
            log_path.write_text("\n".join(header + [line]) + "\n", encoding="utf-8")
            return
        with log_path.open("a", encoding="utf-8") as handle:
            handle.write(line + "\n")

    def _init_sqlite(self) -> None:
        self.output_dir.mkdir(parents=True, exist_ok=True)
        conn = sqlite3.connect(self.sqlite_path)
        try:
            conn.executescript(
                """
                PRAGMA journal_mode=WAL;
                CREATE TABLE IF NOT EXISTS stories (
                    id INTEGER PRIMARY KEY,
                    title_norm TEXT UNIQUE,
                    title TEXT,
                    first_seen TEXT,
                    last_seen TEXT
                );
                CREATE TABLE IF NOT EXISTS story_sources (
                    story_id INTEGER,
                    source TEXT,
                    UNIQUE(story_id, source)
                );
                CREATE TABLE IF NOT EXISTS story_publications (
                    story_id INTEGER,
                    published_at TEXT,
                    UNIQUE(story_id, published_at)
                );
                CREATE TABLE IF NOT EXISTS story_locations (
                    story_id INTEGER,
                    geocode_query TEXT,
                    latitude REAL,
                    longitude REAL,
                    source TEXT,
                    url TEXT,
                    published_at TEXT,
                    UNIQUE(story_id, geocode_query, published_at, source, url)
                );
                CREATE INDEX IF NOT EXISTS idx_stories_title_norm ON stories(title_norm);
                CREATE INDEX IF NOT EXISTS idx_story_publications_published ON story_publications(published_at);
                CREATE INDEX IF NOT EXISTS idx_story_locations_published ON story_locations(published_at);
                CREATE INDEX IF NOT EXISTS idx_story_locations_geo ON story_locations(latitude, longitude);
                """
            )
        finally:
            conn.close()

    def _upsert_sqlite(self, reports: list[NewsReport]) -> None:
        if not reports:
            return
        now_iso = datetime.now(timezone.utc).isoformat()
        conn = sqlite3.connect(self.sqlite_path)
        try:
            cur = conn.cursor()
            for report in reports:
                title_norm = _normalize_title(report.title)
                if not title_norm:
                    continue
                cur.execute("SELECT id FROM stories WHERE title_norm = ?", (title_norm,))
                row = cur.fetchone()
                if row:
                    story_id = row[0]
                    cur.execute(
                        "UPDATE stories SET last_seen = ? WHERE id = ?",
                        (now_iso, story_id),
                    )
                else:
                    cur.execute(
                        "INSERT INTO stories (title_norm, title, first_seen, last_seen) VALUES (?, ?, ?, ?)",
                        (title_norm, report.title, now_iso, now_iso),
                    )
                    story_id = cur.lastrowid
                cur.execute(
                    "INSERT OR IGNORE INTO story_sources (story_id, source) VALUES (?, ?)",
                    (story_id, report.source),
                )
                if report.published_at:
                    cur.execute(
                        "INSERT OR IGNORE INTO story_publications (story_id, published_at) VALUES (?, ?)",
                        (story_id, report.published_at.isoformat()),
                    )
                if any((report.geocode_query, report.latitude, report.longitude)):
                    cur.execute(
                        """
                        INSERT OR IGNORE INTO story_locations (
                            story_id, geocode_query, latitude, longitude, source, url, published_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            story_id,
                            report.geocode_query,
                            report.latitude,
                            report.longitude,
                            report.source,
                            report.url,
                            report.published_at.isoformat() if report.published_at else None,
                        ),
                    )
            conn.commit()
        except Exception:  # noqa: BLE001
            LOGGER.warning("Failed to upsert into SQLite index at %s", self.sqlite_path, exc_info=True)
        finally:
            conn.close()

    def _iter_geocode_queries(self, report: NewsReport) -> Iterable[str]:
        seen: Set[str] = set()

        def yield_clean(value: str | None) -> Iterable[str]:
            if not value:
                return []
            cleaned = value.strip()
            if not cleaned or cleaned.lower() in seen:
                return []
            seen.add(cleaned.lower())
            tokens = cleaned.split()
            tokens_lower = [t.lower() for t in tokens]
            if len(tokens) > 8 and not any(
                keyword in tokens_lower for keyword in ("ice", "detention", "processing", "center", "facility")
            ):
                LOGGER.debug("Skipping long geocode candidate without facility keywords: %s", cleaned)
                return []
            # Only attempt geocoding if we have a comma (likely City, State) or a state mention.
            if "," not in cleaned and not STATE_PATTERN.search(cleaned):
                LOGGER.debug("Skipping geocode candidate without location cues: %s", cleaned)
                return []
            return [cleaned]

        combined: List[str] = []
        for facility in report.facility_mentions:
            if report.city_mentions:
                for city in report.city_mentions:
                    combined.append(f"{facility}, {city}")
            combined.append(facility)
        combined.extend(report.city_mentions)
        combined.extend(report.locations)

        for candidate in combined:
            for item in yield_clean(candidate):
                yield item


def build_default_sources(
    include_all_rss: bool = False,
    from_date: datetime | None = None,
    to_date: datetime | None = None,
    gdelt_max_records: int = 200,
    gdelt_max_days: int | None = 7,
    newsapi_page_size: int = 100,
    skip_newsapi: bool = False,
) -> List[BaseNewsSource]:
    rss_feeds = [
        RSSFeedConfig(
            name="reuters-us",
            url="https://feeds.reuters.com/reuters/domesticNews",
        ),
        RSSFeedConfig(name="nbc-us", url="https://feeds.nbcnews.com/nbcnews/public/news"),
        RSSFeedConfig(name="ice-press", url="https://www.ice.gov/rss.xml"),
    ]
    newsapi_key = os.getenv("NEWSAPI_KEY")
    sources: List[BaseNewsSource] = [
        RSSFeedSource(rss_feeds, include_all=include_all_rss),
        GdeltDocSource(
            max_records=gdelt_max_records,
            start_date=from_date,
            end_date=to_date,
            max_days=gdelt_max_days,
        ),
    ]
    if not skip_newsapi:
        sources.append(
            NewsApiSource(
                newsapi_key,
                page_size=newsapi_page_size,
                from_date=from_date,
                to_date=to_date,
            )
        )
    return sources


def _parse_cli_date(value: str) -> datetime:
    try:
        return datetime.strptime(value, "%Y-%m-%d").replace(tzinfo=timezone.utc)
    except ValueError as exc:
        msg = f"Invalid date '{value}'. Expected YYYY-MM-DD."
        raise argparse.ArgumentTypeError(msg) from exc


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Fetch ICE-related news articles.")
    parser.add_argument(
        "--output-dir",
        default="datasets/news_ingest",
        type=Path,
        help="Directory to store JSONL output (default: datasets/news_ingest).",
    )
    parser.add_argument(
        "--log-level",
        default="INFO",
        help="Python logging level (default: INFO).",
    )
    parser.add_argument(
        "--from-date",
        type=_parse_cli_date,
        default=None,
        help="Earliest publication date to include (YYYY-MM-DD, UTC).",
    )
    parser.add_argument(
        "--to-date",
        type=_parse_cli_date,
        default=None,
        help="Latest publication date to include (YYYY-MM-DD, UTC).",
    )
    parser.add_argument(
        "--include-all-rss",
        action="store_true",
        help="Disable RSS keyword filtering so every entry is fetched (useful for debugging).",
    )
    parser.add_argument(
        "--print-headlines",
        action="store_true",
        help="Log every candidate headline at INFO level for inspection.",
    )
    parser.add_argument(
        "--min-keyword-matches",
        type=int,
        default=1,
        help="Minimum number of relevance keywords that must appear in an article (default: 1).",
    )
    parser.add_argument(
        "--newsapi-page-size",
        type=int,
        default=100,
        help="Number of NewsAPI articles to request per page (max 100).",
    )
    parser.add_argument(
        "--gdelt-max-records",
        type=int,
        default=200,
        help="Number of GDELT articles to request (max 250).",
    )
    parser.add_argument(
        "--gdelt-max-days",
        type=int,
        default=7,
        help="Fallback GDELT lookback window in days when --from-date/--to-date are not provided.",
    )
    parser.add_argument(
        "--skip-newsapi",
        action="store_true",
        help="Skip NewsAPI requests (useful when hitting plan limits).",
    )
    parser.add_argument(
        "--fetch-content",
        action="store_true",
        help="Download full article bodies and re-run location extraction (slower).",
    )
    parser.add_argument(
        "--fetch-content-limit",
        type=int,
        default=None,
        help="Max number of articles to fetch full content for (omit for no limit).",
    )
    parser.add_argument(
        "--geocode-max-queries",
        type=int,
        default=None,
        help="Maximum number of geocoding lookups per run (omit for no limit).",
    )
    parser.add_argument(
        "--disable-filtering",
        action="store_true",
        help="Skip relevance keyword filtering and keep every article.",
    )
    parser.add_argument(
        "--disable-geocoding",
        action="store_true",
        help="Skip geocoding lookups (coordinates will be empty).",
    )
    parser.add_argument(
        "--geocode-cache",
        type=Path,
        default=None,
        help="Path to the SQLite cache used for geocoding (default: <output-dir>/geocache.sqlite).",
    )
    args = parser.parse_args(argv)

    logging.basicConfig(
        level=getattr(logging, args.log_level.upper(), logging.INFO),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
        stream=sys.stdout,
        force=True,
    )
    LOGGER.info("Starting news ingestion with args: %s", args)

    dotenv_loaded = load_dotenv(dotenv_path=Path(__file__).resolve().parents[2] / ".env")
    if dotenv_loaded:
        LOGGER.debug("Loaded environment variables from .env file.")

    geocoder = None
    if not args.disable_geocoding:
        cache_path = args.geocode_cache or (args.output_dir / "geocache.sqlite")
        geocoder = NominatimGeocoder(
            cache_path=cache_path,
            google_api_key=os.getenv("GOOGLE_ACC_KEY"),
        )

    ingestor = NewsIngestor(
        build_default_sources(
            include_all_rss=args.include_all_rss,
            from_date=args.from_date,
            to_date=args.to_date,
            gdelt_max_records=args.gdelt_max_records,
            gdelt_max_days=args.gdelt_max_days,
            newsapi_page_size=args.newsapi_page_size,
            skip_newsapi=args.skip_newsapi,
        ),
        args.output_dir,
        print_headlines=args.print_headlines,
        min_keyword_matches=args.min_keyword_matches,
        disable_filtering=args.disable_filtering,
        geocoder=geocoder,
        fetch_content=args.fetch_content,
        fetch_content_limit=args.fetch_content_limit,
        geocode_max_queries=args.geocode_max_queries,
    )
    ingestor.run()
    return 0


if __name__ == "__main__":
    sys.exit(main())
