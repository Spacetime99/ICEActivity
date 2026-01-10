"""
FastAPI app exposing triplet data from the existing SQLite index.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
import logging
import sqlite3
from pathlib import Path
from typing import Iterable, Optional

from fastapi import Depends, FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field

DB_PATH = "datasets/news_ingest/triplets_index.sqlite"
MAX_ROWS = 2000
DEFAULT_SINCE_HOURS = 24
LOGGER = logging.getLogger("triplets_api")
if not LOGGER.handlers:
    LOGGER.setLevel(logging.INFO)
    LOG_PATH = Path("logs")
    LOG_PATH.mkdir(parents=True, exist_ok=True)
    file_handler = logging.FileHandler(LOG_PATH / "api_requests.log")
    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")
    file_handler.setFormatter(formatter)
    LOGGER.addHandler(file_handler)


def get_db():
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    try:
        yield conn
    finally:
        conn.close()


class TripletOut(BaseModel):
    story_id: str
    title: str
    who: str
    what: str
    where_text: Optional[str] = None
    lat: float = Field(..., description="Latitude in decimal degrees")
    lon: float = Field(..., description="Longitude in decimal degrees")
    url: Optional[str] = None
    publishedAt: Optional[str] = None
    source: Optional[str] = None


app = FastAPI(title="ICE Triplets API", version="0.1.0")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["http://localhost:3000", "http://localhost:5173"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)


def _parse_bbox(value: str) -> tuple[float, float, float, float]:
    parts = value.split(",")
    if len(parts) != 4:
        raise ValueError("bbox must have four comma-separated floats (west,south,east,north)")
    west, south, east, north = (float(part.strip()) for part in parts)
    if west >= east or south >= north:
        raise ValueError("bbox must satisfy west < east and south < north")
    return west, south, east, north


def _query_triplets(
    conn: sqlite3.Connection,
    since_hours: int | None,
    bbox: tuple[float, float, float, float] | None = None,
) -> Iterable[sqlite3.Row]:
    sql = """
        SELECT story_id, title, who, what, where_text,
               latitude, longitude, url, published_at, source
        FROM triplets
        WHERE latitude IS NOT NULL
          AND longitude IS NOT NULL
    """
    params: list[object] = []
    if since_hours:
        cutoff = datetime.now(timezone.utc) - timedelta(hours=since_hours)
        cutoff_iso = cutoff.isoformat()
        sql += " AND published_at >= ?"
        params.append(cutoff_iso)
    if bbox:
        west, south, east, north = bbox
        sql += " AND longitude BETWEEN ? AND ? AND latitude BETWEEN ? AND ?"
        params.extend([west, east, south, north])
    sql += " ORDER BY published_at DESC LIMIT ?"
    params.append(MAX_ROWS)
    cursor = conn.execute(sql, params)
    return cursor.fetchall()


@app.get("/health")
def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/api/triplets", response_model=list[TripletOut])
def get_triplets(
    since_hours: int | None = Query(
        DEFAULT_SINCE_HOURS,
        ge=0,
        le=24 * 90,
        description="Number of trailing hours to include (default 24). Pass 0 to fetch all available rows.",
    ),
    bbox: Optional[str] = Query(
        default=None,
        description='Optional bounding box "west,south,east,north" in decimal degrees.',
    ),
    conn: sqlite3.Connection = Depends(get_db),
) -> list[TripletOut]:
    LOGGER.info("Fetching triplets since_hours=%s bbox=%s", since_hours, bbox)
    parsed_bbox = None
    if bbox:
        try:
            parsed_bbox = _parse_bbox(bbox)
        except ValueError as exc:
            raise HTTPException(status_code=400, detail=str(exc)) from exc
    rows = _query_triplets(conn, since_hours=since_hours, bbox=parsed_bbox)
    results = [
        TripletOut(
            story_id=row["story_id"],
            title=row["title"],
            who=row["who"],
            what=row["what"],
            where_text=row["where_text"],
            lat=row["latitude"],
            lon=row["longitude"],
            url=row["url"],
            publishedAt=row["published_at"],
            source=row["source"],
        )
        for row in rows
    ]
    return results
