"""
Simple geocoding helper backed by a SQLite cache and OpenStreetMap's Nominatim API.
"""

from __future__ import annotations

import logging
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Optional

import requests

LOGGER = logging.getLogger(__name__)


@dataclass
class GeocodeResult:
    query: str
    latitude: float
    longitude: float
    raw: dict[str, str]


class SQLiteCache:
    """Lightweight cache that stores query -> coordinates mappings."""

    def __init__(self, db_path: Path) -> None:
        self.db_path = db_path
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self.conn = sqlite3.connect(self.db_path, check_same_thread=False)
        self.conn.execute(
            """
            CREATE TABLE IF NOT EXISTS geocache (
                query TEXT PRIMARY KEY,
                latitude REAL,
                longitude REAL,
                raw_response TEXT,
                fetched_at TEXT
            )
            """
        )
        self.conn.commit()
        self.lock = threading.Lock()

    def get(self, query: str) -> Optional[tuple[float | None, float | None, dict[str, str], str | None]]:
        with self.lock:
            cursor = self.conn.execute(
                "SELECT latitude, longitude, raw_response, fetched_at FROM geocache WHERE query = ?",
                (query,),
            )
            row = cursor.fetchone()
        if not row:
            return None
        raw: dict[str, str] = {}
        if row[2]:
            try:
                import json

                raw = json.loads(row[2])
            except json.JSONDecodeError:
                raw = {}
        return (row[0], row[1], raw, row[3])

    def set(self, query: str, latitude: float | None, longitude: float | None, raw: dict[str, str]) -> None:
        raw_blob = None
        if raw:
            import json

            raw_blob = json.dumps(raw)
        with self.lock:
            self.conn.execute(
                """
                INSERT OR REPLACE INTO geocache (query, latitude, longitude, raw_response, fetched_at)
                VALUES (?, ?, ?, ?, ?)
                """,
                (
                    query,
                    latitude,
                    longitude,
                    raw_blob,
                    datetime.now(timezone.utc).isoformat(),
                ),
            )
            self.conn.commit()


class NominatimGeocoder:
    """Fetch coordinates using the public Nominatim endpoint and cache locally."""

    endpoint = "https://nominatim.openstreetmap.org/search"

    def __init__(
        self,
        cache_path: Path,
        min_interval: float = 1.1,
        user_agent: str = "codex-news-ingestor/0.1 (https://example.com/contact)",
        failure_ttl_days: int = 7,
        google_api_key: str | None = None,
        ignore_failures: bool = False,
    ) -> None:
        self.cache = SQLiteCache(cache_path)
        self.min_interval = min_interval
        self.user_agent = user_agent
        self.failure_ttl_days = failure_ttl_days
        self._last_request = 0.0
        self.google_api_key = google_api_key
        self.ignore_failures = ignore_failures
        self.stats: dict[str, int] = {
            "cache_hits": 0,
            "nominatim_hits": 0,
            "google_hits": 0,
            "failures": 0,
        }

    def lookup(self, query: str) -> Optional[GeocodeResult]:
        query = query.strip()
        if not query:
            return None
        cached = self.cache.get(query)
        if cached:
            lat, lon, raw, fetched_at = cached
            if lat is not None and lon is not None:
                self.stats["cache_hits"] += 1
                return GeocodeResult(query=query, latitude=lat, longitude=lon, raw=raw)
            if fetched_at and not self.ignore_failures:
                ts = None
                try:
                    ts = datetime.fromisoformat(fetched_at)
                    if ts.tzinfo is None:
                        ts = ts.replace(tzinfo=timezone.utc)
                except ValueError:
                    ts = None
                if ts and datetime.now(timezone.utc) - ts < timedelta(days=self.failure_ttl_days):
                    LOGGER.debug("Skipping geocode for '%s' due to recent failure cache.", query)
                    return None
        payload = self._fetch(query)
        source = None
        if payload:
            source = "nominatim"
        elif self.google_api_key:
            payload = self._fetch_google(query)
            if payload:
                source = "google"
        if not payload:
            self.cache.set(query, None, None, raw={})
            self.stats["failures"] += 1
            return None
        result = GeocodeResult(
            query=query,
            latitude=float(payload["lat"]),
            longitude=float(payload["lon"]),
            raw=payload,
        )
        self.cache.set(query, result.latitude, result.longitude, payload)
        if source == "nominatim":
            self.stats["nominatim_hits"] += 1
        elif source == "google":
            self.stats["google_hits"] += 1
        return result

    def _fetch(self, query: str) -> Optional[dict[str, str]]:
        elapsed = time.monotonic() - self._last_request
        if elapsed < self.min_interval:
            time.sleep(self.min_interval - elapsed)
        params = {
            "q": query,
            "format": "json",
            "limit": 1,
        }
        headers = {
            "User-Agent": self.user_agent,
        }
        try:
            response = requests.get(self.endpoint, params=params, headers=headers, timeout=25)
            self._last_request = time.monotonic()
            response.raise_for_status()
            results = response.json()
            if results:
                return results[0]
        except requests.RequestException:
            LOGGER.exception("Geocoding request failed for query '%s'", query)
        return None

    def _fetch_google(self, query: str) -> Optional[dict[str, str]]:
        if not self.google_api_key:
            return None
        url = "https://maps.googleapis.com/maps/api/geocode/json"
        try:
            response = requests.get(
                url,
                params={"address": query, "key": self.google_api_key},
                timeout=15,
            )
            response.raise_for_status()
            payload = response.json()
            if payload.get("status") != "OK" or not payload.get("results"):
                return None
            location = payload["results"][0]["geometry"]["location"]
            return {"lat": location.get("lat"), "lon": location.get("lng")}
        except requests.RequestException:
            LOGGER.exception("Google geocoding failed for query '%s'", query)
        return None
