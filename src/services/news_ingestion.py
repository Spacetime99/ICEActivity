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
from typing import Any, Dict, Iterable, List, Pattern, Sequence, Set
from urllib.parse import urljoin, urlparse

import feedparser
import requests
from bs4 import BeautifulSoup
from dotenv import load_dotenv
from geotext import GeoText
import geotext.geotext as geotext_module

from src.services.geocoding import GeocodeResult, NominatimGeocoder

LOGGER = logging.getLogger(__name__)

FACILITY_CATALOG_PATH = Path("assets/ice_facilities.csv")
DOMAIN_BLACKLIST_PATH = Path("assets/domain_blacklist.txt")

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
    "visa",
    "green card",
    "national guard",
    "asylum",
    "refugee",
]

DEFAULT_RELEVANCE_KEYWORDS = [
    "ICE",
    "immigration",
    "immigration and customs enforcement",
    "customs enforcement",
    "border patrol",
    "deportation",
    "detention center",
    "immigration raid",
    "immigration court",
    "visa",
    "green card",
    "national guard",
    "asylum",
    "refugee",
]

CASE_SENSITIVE_KEYWORDS = frozenset({"ICE"})

RELATED_HEADING_PREFIXES = (
    "related",
    "more stories",
    "more from",
    "recommended",
    "trending",
    "watch next",
    "around the web",
)
RELATED_ATTR_SIGNALS = (
    "related",
    "more-stories",
    "morestories",
    "more_from",
    "recommended",
    "trending",
    "promo",
    "readnext",
    "read-next",
    "watchnext",
)
RELATED_SECTION_PATTERNS = [
    re.compile(r"\brelated (?:topics|stories|coverage)\b", re.IGNORECASE),
    re.compile(r"\bmore (?:stories|from)\b", re.IGNORECASE),
    re.compile(r"\brecommended\b", re.IGNORECASE),
    re.compile(r"\btrending\b", re.IGNORECASE),
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
    r"(?:(?:the|a|an)\s+)?((?:[A-Z][\\w'&-]*\\s){0,4}"
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


def _clean_geocode_phrase(value: str) -> str:
    """Strip leading clauses and keep plausible location signals."""
    cleaned = value
    cleaned = re.sub(r"^(according to|before heading into|came out of|day with|group of|had been|has been|was|were|to be|to reappear at|scheduled to attend|procession at)\s+", "", cleaned, flags=re.IGNORECASE)
    cleaned = cleaned.strip(",;:- ").strip()
    return cleaned


def _clean_html_fragment(value: str | None) -> str:
    """Best-effort HTML to text converter for summaries/descriptions."""
    if not value:
        return ""
    soup = BeautifulSoup(value, "html.parser")
    for tag in soup(["script", "style", "noscript"]):
        tag.decompose()
    text = soup.get_text(" ", strip=True)
    return re.sub(r"\s+", " ", text).strip()


def _strip_related_sections(value: str | None) -> str:
    """Drop trailing related/recommended link lists from extracted article text."""
    if not value:
        return ""
    text = value.strip()
    lowered = text.lower()
    cutoff = len(text)
    for pattern in RELATED_SECTION_PATTERNS:
        match = pattern.search(lowered)
        if match:
            cutoff = min(cutoff, match.start())
    if cutoff < len(text):
        return text[:cutoff].rstrip()
    return text


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
    content: str | None = None
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
            "locations": self.locations or [],
            "city_mentions": self.city_mentions or [],
            "facility_mentions": self.facility_mentions or [],
            "content": self.content,
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
class HtmlPageConfig:
    name: str
    url: str
    selectors: Sequence[str] | None = None


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


LOCAL_RSS_FEEDS: list[RSSFeedConfig] = [
    # ABC owned-and-operated
    RSSFeedConfig(name="abc-wabc", url="https://abc7ny.com/feed"),
    RSSFeedConfig(name="abc-kabc", url="https://abc7.com/feed"),
    RSSFeedConfig(name="abc-kgo", url="https://abc7news.com/feed"),
    RSSFeedConfig(name="abc-wls", url="https://abc7chicago.com/feed"),
    RSSFeedConfig(name="abc-wpvi", url="https://6abc.com/feed"),
    RSSFeedConfig(name="abc-wtvd", url="https://abc11.com/feed"),
    RSSFeedConfig(name="abc-ktrk", url="https://abc13.com/feed"),
    RSSFeedConfig(name="abc-kfsn", url="https://abc30.com/feed"),
    # NBC owned-and-operated + strong affiliates
    RSSFeedConfig(name="nbc-wnbc", url="https://www.nbcnewyork.com/feed/"),
    RSSFeedConfig(name="nbc-knbc", url="https://www.nbclosangeles.com/feed/"),
    RSSFeedConfig(name="nbc-kntv", url="https://www.nbcbayarea.com/feed/"),
    RSSFeedConfig(name="nbc-wmaq", url="https://www.nbcchicago.com/feed/"),
    RSSFeedConfig(name="nbc-wcau", url="https://www.nbcphiladelphia.com/feed/"),
    RSSFeedConfig(name="nbc-wrc", url="https://www.nbcwashington.com/feed/"),
    RSSFeedConfig(name="nbc-wbts", url="https://www.nbcboston.com/feed/"),
    RSSFeedConfig(name="nbc-kxas", url="https://www.nbcdfw.com/feed/"),
    RSSFeedConfig(
        name="nbc-kprc",
        url="https://www.click2houston.com/arc/outboundfeeds/rss/?outputType=xml",
    ),
    RSSFeedConfig(name="nbc-wtvj", url="https://www.nbcmiami.com/feed/"),
    RSSFeedConfig(name="nbc-knsd", url="https://www.nbcsandiego.com/feed/"),
    # CBS owned-and-operated
    # (CBS local feeds responded with 404 HTML; temporarily removed.)
    # Fox owned-and-operated
    RSSFeedConfig(name="fox-wnyw", url="https://www.fox5ny.com/rss/category/news"),
    RSSFeedConfig(name="fox-kttv", url="https://www.foxla.com/rss/category/news"),
    RSSFeedConfig(name="fox-ktvu", url="https://www.ktvu.com/rss/category/news"),
    RSSFeedConfig(name="fox-wfld", url="https://www.fox32chicago.com/rss/category/news"),
    RSSFeedConfig(name="fox-wtxf", url="https://www.fox29.com/rss/category/news"),
    RSSFeedConfig(name="fox-kdfw", url="https://www.fox4news.com/rss/category/news"),
    RSSFeedConfig(name="fox-kriv", url="https://www.fox26houston.com/rss/category/news"),
    RSSFeedConfig(name="fox-waga", url="https://www.fox5atlanta.com/rss/category/news"),
    RSSFeedConfig(name="fox-wttg", url="https://www.fox5dc.com/rss/category/news"),
    RSSFeedConfig(name="fox-ksaz", url="https://www.fox10phoenix.com/rss/category/news"),
    RSSFeedConfig(name="fox-wofl", url="https://www.fox35orlando.com/rss/category/news"),
    # Regional TV groups (Nexstar/Gray/TEGNA examples)
    # NPR member stations
    RSSFeedConfig(name="npr-wnyc", url="https://www.wnyc.org/feeds/articles/"),
    RSSFeedConfig(name="npr-laist", url="https://laist.com/feed"),
    RSSFeedConfig(name="npr-kqed", url="https://www.kqed.org/news/rss"),
    RSSFeedConfig(name="npr-wamu", url="https://wamu.org/feed/"),
    RSSFeedConfig(name="npr-wbur", url="https://www.wbur.org/feed"),
    RSSFeedConfig(name="npr-whyy", url="https://whyy.org/feed/"),
    RSSFeedConfig(name="npr-kut", url="https://www.kut.org/rss"),
    RSSFeedConfig(name="npr-mpr", url="https://www.mprnews.org/stories.rss"),
    RSSFeedConfig(name="npr-cpr", url="https://www.cpr.org/rss/news/"),
    RSSFeedConfig(name="npr-opb", url="https://www.opb.org/feeds/all/"),
    # iHeart local newsrooms
    # (iHeart RSS endpoints redirect to HTML apps; temporarily removed.)
    # Supplemental RSS.app feed (politics stream with immigration coverage)
    RSSFeedConfig(name="rssapp-cnn-politics", url="https://rss.app/feeds/ZaG4vCohPDJWTwru.xml"),
]

WEB_PAGE_SOURCES: list[HtmlPageConfig] = [
    HtmlPageConfig(name="wral-local", url="https://www.wral.com/local", selectors=("article a", "h2 a")),
    HtmlPageConfig(name="wtop-local", url="https://wtop.com/local/", selectors=("article a", "h2 a")),
    HtmlPageConfig(
        name="nydailynews-local",
        url="https://www.nydailynews.com/news/local/",
        selectors=("article a", "h2 a"),
    ),
    HtmlPageConfig(
        name="latimes-california",
        url="https://www.latimes.com/california",
        selectors=("article a", "h2 a"),
    ),
    HtmlPageConfig(
        name="sfchronicle-local",
        url="https://www.sfchronicle.com/local/",
        selectors=("article a", "h2 a"),
    ),
    HtmlPageConfig(
        name="houstonchronicle-local",
        url="https://www.houstonchronicle.com/news/houston-texas/",
        selectors=("article a", "h2 a"),
    ),
    HtmlPageConfig(
        name="azcentral-local",
        url="https://www.azcentral.com/local/",
        selectors=("article a", "h2 a"),
    ),
    HtmlPageConfig(
        name="denverpost-local",
        url="https://www.denverpost.com/news/",
        selectors=("article a", "h2 a"),
    ),
    HtmlPageConfig(
        name="bostonglobe-metro",
        url="https://www.bostonglobe.com/metro/",
        selectors=("article a", "h2 a"),
    ),
    HtmlPageConfig(
        name="wapo-local",
        url="https://www.washingtonpost.com/dc-md-va/",
        selectors=("article a", "h2 a"),
    ),
    HtmlPageConfig(
        name="chicagotribune-local",
        url="https://www.chicagotribune.com/news/breaking/",
        selectors=("article a", "h2 a"),
    ),
    HtmlPageConfig(
        name="nbcchicago-local",
        url="https://www.nbcchicago.com/news/local/",
        selectors=("article a", "h2 a"),
    ),
    HtmlPageConfig(
        name="abc7chicago-local",
        url="https://abc7chicago.com/local-news/",
        selectors=("article a", "h2 a"),
    ),
    HtmlPageConfig(
        name="fox32chicago-local",
        url="https://www.fox32chicago.com/news/local-news",
        selectors=("article a", "h2 a"),
    ),
    HtmlPageConfig(
        name="suntimes-local",
        url="https://chicago.suntimes.com/news",
        selectors=("article a", "h2 a"),
    ),
    HtmlPageConfig(
        name="abc7ny-local",
        url="https://abc7ny.com/local-news/",
        selectors=("article a", "h2 a"),
    ),
    HtmlPageConfig(name="kron4-local", url="https://www.kron4.com/news/", selectors=("article a", "h2 a")),
]

AP_RSS_FEEDS: list[RSSFeedConfig] = [
]

NATIONAL_RSS_FEEDS: list[RSSFeedConfig] = [
    RSSFeedConfig(name="nbc-us", url="https://feeds.nbcnews.com/nbcnews/public/news"),
    RSSFeedConfig(name="ice-press", url="https://www.ice.gov/rss.xml"),
]

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


def load_domain_blacklist(path: Path = DOMAIN_BLACKLIST_PATH) -> set[str]:
    if not path.exists():
        return set()
    try:
        with path.open("r", encoding="utf-8") as handle:
            return {line.strip().lower() for line in handle if line.strip()}
    except Exception:  # noqa: BLE001
        LOGGER.warning("Failed to read domain blacklist at %s", path, exc_info=True)
        return set()


def _build_keyword_pattern(keywords: Sequence[str]) -> Pattern[str] | None:
    """Compile keywords with word boundaries, respecting simple wildcards."""
    if not keywords:
        return None
    processed: list[str] = []
    for term in keywords:
        stripped = term.strip()
        if not stripped:
            continue
        exact = re.escape(stripped)
        exact = exact.replace(r"\*", ".*")
        processed.append(rf"\b{exact}\b")
    if not processed:
        return None
    return re.compile("|".join(processed), re.IGNORECASE)


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
        search_pattern = _build_keyword_pattern(search_terms)
        for feed in self.feeds:
            matched = 0
            parsed = feedparser.parse(feed.url)
            if parsed.bozo:
                LOGGER.warning("RSS parse issue for %s: %s", feed.url, parsed.bozo_exception)
            for entry in parsed.entries:
                raw_summary = getattr(entry, "summary", "")
                summary = _clean_html_fragment(raw_summary)
                title = getattr(entry, "title", "")
                text_blob = " ".join(part for part in (title, summary) if part)
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
                    summary=summary or raw_summary or None,
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


class HtmlPageSource(BaseNewsSource):
    def __init__(
        self,
        pages: Sequence[HtmlPageConfig],
        include_all: bool = False,
        timeout: int = 15,
        max_links: int = 50,
    ) -> None:
        self.name = "html"
        self.pages = pages
        self.include_all = include_all
        self.timeout = timeout
        self.max_links = max_links
        self._headers = {
            "User-Agent": (
                "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                "(KHTML, like Gecko) Chrome/114.0.0.0 Safari/537.36"
            )
        }

    def fetch(self, search_terms: Sequence[str]) -> List[NewsReport]:
        reports: List[NewsReport] = []
        search_pattern = _build_keyword_pattern(search_terms)
        for page in self.pages:
            before_count = len(reports)
            try:
                resp = requests.get(page.url, headers=self._headers, timeout=self.timeout)
                resp.raise_for_status()
            except Exception as exc:  # noqa: BLE001
                LOGGER.warning("Failed to fetch HTML page %s: %s", page.url, exc)
                continue
            soup = BeautifulSoup(resp.text, "html.parser")
            selectors = list(page.selectors or [])
            candidates = []
            if selectors:
                for selector in selectors:
                    candidates.extend(soup.select(selector))
            else:
                candidates.extend(soup.find_all("a"))
            seen_links: set[str] = set()
            for tag in candidates:
                if len(reports) >= self.max_links:
                    break
                href = tag.get("href")
                if not href:
                    continue
                full_url = urljoin(page.url, href)
                if full_url in seen_links:
                    continue
                title = tag.get_text(strip=True)
                if not title:
                    continue
                text_blob = title
                if search_pattern and not self.include_all and not search_pattern.search(text_blob):
                    continue
                seen_links.add(full_url)
                reports.append(
                    NewsReport(
                        source=f"html:{page.name}",
                        source_id=full_url,
                        title=title,
                        url=full_url,
                        summary=None,
                        published_at=None,
                        locations=extract_locations(text_blob),
                        city_mentions=extract_city_mentions(text_blob),
                        facility_mentions=extract_facility_mentions(text_blob),
                        raw={"source_url": page.url},
                    )
                )
            LOGGER.debug(
                "HTML page %s yielded %s candidate links (post-filter)",
                page.name,
                len(reports) - before_count,
            )
        return reports


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
            description = _clean_html_fragment(article.get("description"))
            content = _clean_html_fragment(article.get("content"))
            content_blob = "\n".join(
                part
                for part in (
                    article.get("title"),
                    description,
                    content,
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
                    summary=description or article.get("description"),
                    published_at=self._parse_date(article.get("publishedAt")),
                    locations=states,
                    city_mentions=cities,
                    facility_mentions=facilities,
                    raw=article,
                    content=content or article.get("content"),
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
        try:
            response = requests.get(self.endpoint, params=params, timeout=self.timeout)
            response.raise_for_status()
            payload = response.json()
        except requests.RequestException as exc:
            LOGGER.warning("GDELT request failed: %s", exc)
            return []
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
        raw = raw.strip()
        # Handle common GDELT shapes plus general ISO-8601 (with/without Z).
        formats = [
            "%Y%m%dT%H%M%SZ",  # e.g., 20251126T071500Z
            "%Y%m%d%H%M%S",  # legacy GDELT, no separators
            "%Y-%m-%dT%H:%M:%S%z",  # ISO with offset
            "%Y-%m-%dT%H:%M:%S",  # ISO without offset (assume UTC)
        ]
        for fmt in formats:
            try:
                parsed = datetime.strptime(raw, fmt)
                if parsed.tzinfo is None:
                    parsed = parsed.replace(tzinfo=timezone.utc)
                return parsed
            except ValueError:
                continue
        # Fallback: fromisoformat handles offsets; normalize 'Z' first.
        iso_candidate = raw[:-1] + "+00:00" if raw.endswith("Z") else raw
        try:
            parsed = datetime.fromisoformat(iso_candidate)
            if parsed.tzinfo is None:
                parsed = parsed.replace(tzinfo=timezone.utc)
            return parsed
        except ValueError:
            LOGGER.debug("Unable to parse date %s", raw, exc_info=True)
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
        resolve_google_news: bool = False,
        google_news_resolve_limit: int = 0,
    ) -> None:
        self.sources = list(sources)
        self.output_dir = output_dir
        self.search_terms = list(search_terms or DEFAULT_SEARCH_TERMS)
        self.print_headlines = print_headlines
        raw_keywords = list(relevance_keywords or DEFAULT_RELEVANCE_KEYWORDS)
        self.case_sensitive_keywords: list[tuple[str, Pattern[str]]] = [
            (kw, re.compile(rf"\b{re.escape(kw)}\b")) for kw in raw_keywords if kw in CASE_SENSITIVE_KEYWORDS
        ]
        self.relevance_keywords = [
            kw.lower() for kw in raw_keywords if kw not in CASE_SENSITIVE_KEYWORDS
        ]
        self.min_keyword_matches = max(1, min_keyword_matches)
        self.disable_filtering = disable_filtering
        self.geocoder = geocoder
        self.story_index_path = output_dir / "story_index.json"
        self.sqlite_path = output_dir / "news_index.sqlite"
        self.fetch_content = fetch_content
        if fetch_content_limit is not None and fetch_content_limit <= 0:
            fetch_content_limit = None
        self.fetch_content_limit = fetch_content_limit
        self.geocode_max_queries = geocode_max_queries
        self.facilities = load_facility_catalog()
        self.domain_blacklist = load_domain_blacklist()
        self.force_refetch = False
        self.ignore_geocode_failures = False
        self.disable_geocoding = False
        self.resolve_google_news = resolve_google_news
        self.google_news_resolve_limit = max(0, google_news_resolve_limit or 0)
        self._google_news_resolved = 0
        self._playwright = None
        self._browser = None
        self._gn_page = None
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
        dedup_map: dict[str, NewsReport] = {}
        source_errors: list[str] = []
        source_counts: list[str] = []
        for source in self.sources:
            LOGGER.info("Fetching from %s", getattr(source, "name", source.__class__.__name__))
            try:
                reports = source.fetch(self.search_terms)
            except Exception as exc:  # noqa: BLE001
                LOGGER.exception("Error while fetching from %s: %s", source.name, exc)
                source_errors.append(f"{source.name}: {exc}")
                continue
            LOGGER.info("Source %s returned %s candidate reports", source.name, len(reports))
            source_counts.append(f"{source.name}={len(reports)}")
            for report in reports:
                LOGGER.debug("Candidate headline [%s]: %s", report.source, report.title)
                if self.print_headlines:
                    LOGGER.info("Headline [%s]: %s", report.source, report.title)
            for report in reports:
                if report.locations is None:
                    report.locations = []
                if report.city_mentions is None:
                    report.city_mentions = []
                if report.facility_mentions is None:
                    report.facility_mentions = []
                dedup_key = f"{report.source}:{report.source_id}"
                existing = dedup_map.get(dedup_key)
                if existing:
                    # Prefer entry with fetched content if available.
                    has_body = isinstance(existing.raw, dict) and existing.raw.get("fetched_content")
                    new_has_body = isinstance(report.raw, dict) and report.raw.get("fetched_content")
                    if not has_body and new_has_body:
                        dedup_map[dedup_key] = report
                else:
                    dedup_map[dedup_key] = report
        aggregated = list(dedup_map.values())
        LOGGER.info(
            "Aggregated %s reports from sources (%s)", len(aggregated), ", ".join(source_counts) or "no sources"
        )
        if not aggregated:
            LOGGER.warning("No reports collected from any source.")
            if source_errors:
                LOGGER.warning("Source errors encountered: %s", "; ".join(source_errors))
            return None
        filtered_reports = self._apply_relevance_filter(aggregated)
        LOGGER.info("After filtering: %s reports", len(filtered_reports))
        if self.fetch_content:
            self._enrich_with_full_text(filtered_reports)
        if filtered_reports and not self.disable_geocoding:
            self._annotate_coordinates(filtered_reports, metrics)
        story_index = self._load_story_index() if not self.force_refetch else {}
        LOGGER.info("Loaded story index with %s entries", len(story_index))
        deduped_reports = self._dedupe_with_index(filtered_reports, story_index)
        self._init_sqlite()
        self._upsert_sqlite(filtered_reports)
        if not deduped_reports:
            LOGGER.warning("No news reports collected during this run.")
            if source_errors:
                LOGGER.warning("Source errors encountered: %s", "; ".join(source_errors))
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
        self._close_google_news_browser()
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
        if report.content:
            text_segments.append(report.content)
        if isinstance(report.raw, dict):
            for key in ("description", "content", "body", "text", "fetched_content"):
                value = report.raw.get(key)
                if isinstance(value, str):
                    text_segments.append(value)
        blob = " ".join(text_segments).strip()
        if not blob:
            return False
        blob_lower = blob.lower()
        hits = [kw for kw in self.relevance_keywords if kw in blob_lower]
        for keyword, pattern in self.case_sensitive_keywords:
            if pattern.search(blob):
                hits.append(keyword)
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
            content, status, error = self._fetch_article_text(report)
            if isinstance(report.raw, dict):
                report.raw["fetch_status"] = status
                if error:
                    report.raw["fetch_error"] = error
            if not content:
                continue
            fetched += 1
            report.raw["fetched_content"] = content
            report.content = content
            report.locations = sorted(
                set(report.locations or []) | set(extract_locations(content) or [])
            )
            report.city_mentions = sorted(
                set(report.city_mentions or []) | set(extract_city_mentions(content) or [])
            )
            report.facility_mentions = sorted(
                set(report.facility_mentions or []) | set(extract_facility_mentions(content) or [])
            )
        LOGGER.info("Fetched full content for %s articles.", fetched)

    @staticmethod
    def _strip_junk_nodes(soup: BeautifulSoup) -> None:
        """Remove obvious non-body elements to reduce noise."""

        def _remove_related_block(tag: Any) -> None:
            """Climb to a containing node marked as related content and remove it."""
            candidate = None
            current = tag
            while current and current.parent and current.parent.name not in {"article", "main", "body"}:
                parent = current.parent
                attr_values: list[str] = []
                for attr in ("id", "class", "role", "aria-label", "data-component", "data-testid"):
                    raw_value = parent.get(attr)
                    values = raw_value if isinstance(raw_value, list) else [raw_value]
                    attr_values.extend(str(value).lower() for value in values if value)
                attr_blob = " ".join(attr_values)
                if any(signal in attr_blob for signal in RELATED_ATTR_SIGNALS):
                    candidate = parent
                current = parent
            target = candidate or tag
            target.decompose()

        removable_tags = [
            "script",
            "style",
            "noscript",
            "iframe",
            "svg",
            "button",
            "form",
            "header",
            "footer",
            "nav",
            "aside",
        ]
        for tag in soup(removable_tags):
            tag.decompose()
        junk_signals = (
            "advert",
            "ads-",
            "ad-",
            "ad_",
            "sponsor",
            "subscription",
            "paywall",
            "promo",
            "breadcrumb",
            "comment",
            "newsletter",
            "cookie",
            "share",
            "social",
            "related",
            "recommend",
            "popup",
        )
        for tag in soup.find_all(True):
            if not hasattr(tag, "get") or not getattr(tag, "attrs", None):
                continue
            if tag.name in {"article", "main", "p", "h1", "h2", "h3", "h4"}:
                continue
            attr_values: list[str] = []
            for attr in ("id", "class", "role", "aria-label", "data-component", "data-testid"):
                raw_value = tag.get(attr)
                values = raw_value if isinstance(raw_value, list) else [raw_value]
                attr_values.extend(str(value).lower() for value in values if value)
            attr_blob = " ".join(attr_values)
            if any(signal in attr_blob for signal in junk_signals):
                tag.decompose()
                continue
            if any(signal in attr_blob for signal in RELATED_ATTR_SIGNALS):
                tag.decompose()

        candidate_headings = soup.find_all(["h2", "h3", "h4", "p", "span", "strong"])
        for heading in candidate_headings:
            text = heading.get_text(" ", strip=True).lower()
            if not text:
                continue
            if any(text.startswith(prefix) for prefix in RELATED_HEADING_PREFIXES):
                _remove_related_block(heading)

    @staticmethod
    def _extract_main_text(soup: BeautifulSoup) -> str | None:
        """Select the main article text by favoring semantic containers and paragraphs."""
        NewsIngestor._strip_junk_nodes(soup)

        def collect_text(node: Any) -> str:
            if not node:
                return ""
            paragraphs = [
                p.get_text(" ", strip=True)
                for p in node.find_all("p")
                if p.get_text(strip=True)
            ]
            if paragraphs:
                return "\n\n".join(paragraphs)
            return node.get_text(" ", strip=True)

        candidates: list[tuple[int, str]] = []
        for selector in ("article", "main"):
            for node in soup.find_all(selector):
                text = collect_text(node)
                candidates.append((len(text), text))
        if not candidates:
            for node in soup.find_all("div"):
                text = collect_text(node)
                # Skip very short snippets that are likely nav or captions.
                if len(text) < 200:
                    continue
                candidates.append((len(text), text))
        if not candidates:
            text = collect_text(soup.body or soup)
            if text:
                return re.sub(r"\s+", " ", text).strip()
            return None
        _, best_text = max(candidates, key=lambda item: item[0])
        cleaned = re.sub(r"\s+", " ", best_text).strip()
        return cleaned or None

    def _fetch_article_text(self, report: NewsReport, timeout: int = 12) -> tuple[str | None, str, str | None]:
        url = report.url
        netloc = urlparse(url).netloc.lower()
        if any(netloc.endswith(domain) for domain in self.domain_blacklist):
            LOGGER.info("Skipping body fetch for blacklisted domain %s", netloc)
            return None, "skipped_blacklist", None
        if netloc.endswith("news.google.com") and self.resolve_google_news:
            if self.google_news_resolve_limit and self._google_news_resolved >= self.google_news_resolve_limit:
                LOGGER.info("Google News resolve limit (%s) reached; skipping %s", self.google_news_resolve_limit, url)
                return None, "skipped_google_news_limit", None
            resolved = self._resolve_google_news_url(url)
            if resolved:
                LOGGER.info("Resolved Google News URL to %s", resolved)
                url = resolved
                netloc = urlparse(url).netloc.lower()
                self._google_news_resolved += 1
            else:
                LOGGER.info("Failed to resolve Google News URL %s", url)
                return None, "error_google_news_resolve", None
        try:
            response = requests.get(
                url,
                timeout=timeout,
                headers={
                    "User-Agent": "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/142.0.0.0 Safari/537.36",
                    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8",
                    "Accept-Language": "en-US,en;q=0.9",
                    "Referer": url,
                },
                allow_redirects=True,
            )
            response.raise_for_status()
            soup = BeautifulSoup(response.text, "html.parser")
            text = self._extract_main_text(soup)
            if text:
                text = _strip_related_sections(text)
                if len(text) < 100:
                    newsapi_text = self._newsapi_fallback(report)
                    if newsapi_text:
                        return newsapi_text, "ok_newsapi_fallback", None
                return text, "ok", None
            # As a last resort, try Playwright to render JS-heavy pages.
            rendered = self._fetch_via_playwright(url, timeout=timeout)
            if rendered:
                rendered = _strip_related_sections(rendered)
                if len(rendered) < 100:
                    newsapi_text = self._newsapi_fallback(report)
                    if newsapi_text:
                        return newsapi_text, "ok_newsapi_fallback", None
                return rendered, "ok_playwright", None
            # Fallback to NewsAPI snippet if available.
            newsapi_text = self._newsapi_fallback(report)
            if newsapi_text:
                newsapi_text = _strip_related_sections(newsapi_text)
                return newsapi_text, "ok_newsapi_fallback", None
            return None, "empty", None
        except Exception:  # noqa: BLE001
            LOGGER.warning("Failed to fetch article body for %s", url, exc_info=True)
            rendered = self._fetch_via_playwright(url, timeout=timeout)
            if rendered:
                rendered = _strip_related_sections(rendered)
                return rendered, "ok_playwright", None
            newsapi_text = self._newsapi_fallback(report)
            if newsapi_text:
                newsapi_text = _strip_related_sections(newsapi_text)
                return newsapi_text, "ok_newsapi_fallback", None
            return None, "error", str(getattr(sys.exc_info()[1], "args", ""))  # type: ignore[arg-type]

    def _fetch_via_playwright(self, url: str, timeout: int = 12) -> str | None:
        """Render a page with Playwright and extract main text."""
        try:
            from playwright.sync_api import sync_playwright  # type: ignore
        except Exception:
            return None
        try:
            if self._playwright is None:
                self._playwright = sync_playwright().start()
                self._browser = self._playwright.chromium.launch(headless=True)
                self._gn_page = self._browser.new_page()
            page = self._browser.new_page() if self._browser else None
            if page is None:
                return None
            page.goto(url, wait_until="networkidle", timeout=timeout * 1000)
            try:
                page.wait_for_selector("article", timeout=4000)
            except Exception:
                pass
            try:
                article_text = page.inner_text("article")
                if article_text and len(article_text.split()) > 30:
                    page.close()
                    return re.sub(r"\\s+", " ", article_text).strip()
            except Exception:
                pass
            html = page.content()
            page.close()
            soup = BeautifulSoup(html, "html.parser")
            return self._extract_main_text(soup)
        except Exception:
            LOGGER.debug("Playwright render failed for %s", url, exc_info=True)
            return None

    def _newsapi_fallback(self, report: NewsReport) -> str | None:
        """If Reuters blocks scraping, use NewsAPI snippet as a fallback body."""
        api_key = os.getenv("NEWSAPI_KEY")
        if not api_key or not report.title:
            return None
        try:
            params = {
                "q": report.title,
                "language": "en",
                "pageSize": 1,
                "apiKey": api_key,
            }
            resp = requests.get("https://newsapi.org/v2/everything", params=params, timeout=8)
            resp.raise_for_status()
            payload = resp.json()
            articles = payload.get("articles") or []
            if not articles:
                return None
            candidate = articles[0]
            parts = []
            for field in ("title", "description", "content"):
                value = candidate.get(field)
                if value and isinstance(value, str):
                    parts.append(value)
            text = "\n".join(parts).strip()
            cleaned = _strip_related_sections(text)
            return cleaned or None
        except Exception:
            LOGGER.debug("NewsAPI fallback failed for %s", report.title, exc_info=True)
            return None

    def _resolve_google_news_url(self, url: str) -> str | None:
        """Resolve a news.google.com wrapper to its canonical target.

        Strategy: try the batchexecute API (fast, no JS). If that fails, fall back to a
        headless browser (slower, best-effort)."""
        # Fast path: replicate the Google News batchexecute call.
        try:
            resp = requests.get(
                url,
                timeout=10,
                headers={"User-Agent": "Mozilla/5.0 (compatible; news-ingestor/1.0)"},
            )
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "html.parser")
            node = soup.select_one("c-wiz[data-p]")
            data_p = node.get("data-p") if node else None
            if data_p:
                try:
                    payload_obj = json.loads(data_p.replace("%.@.", '["garturlreq",'))
                    payload = {
                        "f.req": json.dumps(
                            [[["Fbv4je", json.dumps(payload_obj[:-6] + payload_obj[-2:]), "null", "generic"]]]
                        )
                    }
                    headers = {
                        "content-type": "application/x-www-form-urlencoded;charset=UTF-8",
                        "user-agent": "Mozilla/5.0 (compatible; news-ingestor/1.0)",
                    }
                    api_resp = requests.post(
                        "https://news.google.com/_/DotsSplashUi/data/batchexecute",
                        headers=headers,
                        data=payload,
                        timeout=10,
                    )
                    api_resp.raise_for_status()
                    # Strip XSSI prefix and decode nested payload.
                    cleaned = api_resp.text.replace(")]}'", "")
                    outer = json.loads(cleaned)
                    array_string = outer[0][2] if outer and outer[0] else None
                    resolved = None
                    if array_string:
                        parsed = json.loads(array_string)
                        if isinstance(parsed, list) and len(parsed) > 1 and isinstance(parsed[1], str):
                            resolved = parsed[1]
                    if resolved and resolved.startswith("http"):
                        return resolved
                except Exception:  # noqa: BLE001
                    LOGGER.debug("Google News batchexecute resolution failed", exc_info=True)
        except Exception:  # noqa: BLE001
            LOGGER.debug("Initial Google News fetch failed", exc_info=True)

        # Fallback: headless browser best-effort.
        try:
            from playwright.sync_api import sync_playwright  # type: ignore
        except Exception:  # noqa: BLE001
            LOGGER.warning("Playwright is not installed; cannot resolve Google News URL %s", url)
            return None
        try:
            if self._playwright is None:
                self._playwright = sync_playwright().start()
                self._browser = self._playwright.chromium.launch(headless=True)
                self._gn_page = self._browser.new_page()
            page = self._gn_page
            if page is None:
                return None
            captured: list[str] = []

            def _capture(route_url: str) -> None:
                if "news.google.com" in route_url or "googleusercontent.com" in route_url:
                    return
                if route_url.startswith("http"):
                    captured.append(route_url)

            page.on("response", lambda resp: _capture(resp.url))
            page.on("request", lambda req: _capture(req.url))
            page.goto(url, wait_until="networkidle", timeout=15000)
            # Also check meta tags for canonical links.
            canonical = page.eval_on_selector("head link[rel='canonical']", "el => el?.href")  # type: ignore[arg-type]
            if isinstance(canonical, str) and canonical.startswith("http") and "news.google.com" not in canonical:
                return canonical
            if captured:
                # Prefer non-static assets.
                for candidate in captured:
                    if any(
                        candidate.endswith(ext)
                        for ext in (".js", ".css", ".png", ".jpg", ".jpeg", ".svg", ".gif")
                    ):
                        continue
                    return candidate
                return captured[0]
        except Exception:  # noqa: BLE001
            LOGGER.debug("Failed to resolve Google News URL via Playwright", exc_info=True)
            return None
        return None

    def _close_google_news_browser(self) -> None:
        try:
            if self._gn_page:
                self._gn_page.close()
            if self._browser:
                self._browser.close()
            if self._playwright:
                self._playwright.stop()
        except Exception:  # noqa: BLE001
            LOGGER.debug("Failed to close Playwright browser cleanly", exc_info=True)
        finally:
            self._gn_page = None
            self._browser = None
            self._playwright = None

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
            cleaned = _clean_geocode_phrase(value.strip())
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
    rss_feeds = [*AP_RSS_FEEDS, *LOCAL_RSS_FEEDS, *NATIONAL_RSS_FEEDS]
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
    if WEB_PAGE_SOURCES:
        sources.append(HtmlPageSource(WEB_PAGE_SOURCES, include_all=include_all_rss))
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
        "--force-refetch",
        action="store_true",
        help="Ignore story index dedupe for this run (fetch bodies and write all candidates).",
    )
    parser.add_argument(
        "--ignore-geocode-failures",
        action="store_true",
        help="Ignore cached geocode failures for this run and retry all candidates.",
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
    parser.add_argument(
        "--resolve-google-news",
        action="store_true",
        help="Use a headless browser to resolve news.google.com wrapper URLs to canonical links.",
    )
    parser.add_argument(
        "--google-news-resolve-limit",
        type=int,
        default=0,
        help="Maximum number of Google News URLs to resolve per run (0 for no limit).",
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
    cache_path = None
    if not args.disable_geocoding:
        cache_path = args.geocode_cache or (args.output_dir / "geocache.sqlite")
        geocoder = NominatimGeocoder(
            cache_path=cache_path,
            google_api_key=os.getenv("GOOGLE_ACC_KEY"),
            ignore_failures=args.ignore_geocode_failures,
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
        resolve_google_news=args.resolve_google_news,
        google_news_resolve_limit=args.google_news_resolve_limit,
    )
    ingestor.force_refetch = args.force_refetch
    ingestor.ignore_geocode_failures = args.ignore_geocode_failures
    ingestor.disable_geocoding = args.disable_geocoding
    LOGGER.info(
        "Initialized ingestor with %s sources (disable_geocoding=%s)",
        len(ingestor.sources),
        args.disable_geocoding,
    )
    try:
        ingestor.run()
    except Exception:  # noqa: BLE001
        LOGGER.exception("Ingestor run failed.")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
