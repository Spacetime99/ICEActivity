"""Ingest ICE detainee death report PDFs into a JSONL store."""

from __future__ import annotations

import argparse
import json
import os
import re
import shutil
import subprocess
import tempfile
import uuid
from datetime import datetime, timezone
from html.parser import HTMLParser
from pathlib import Path
from typing import Any
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen

ICE_REPORTS_INDEX_URL = "https://www.ice.gov/detain/detainee-death-reporting"
ICE_REPORT_NAMESPACE = uuid.UUID("7a0d9421-1e32-45cb-9e14-5a384c9504e9")
MIN_DEATH_YEAR = 2025

SOURCE_TYPE = "ice_detainee_death_report"
DEFAULT_OUT_PATH = Path("./site/data/ice_death_reports.jsonl")

ALLOWED_DOMAINS = {
    "ice.gov",
}

FIELD_ORDER = [
    "id",
    "person_name",
    "name_raw",
    "date_of_birth",
    "date_of_death",
    "age",
    "gender",
    "country_of_citizenship",
    "facility_or_location",
    "death_context",
    "custody_status",
    "agency",
    "report_urls",
    "source_type",
    "extracted_at",
    "updated_at",
]

FACILITY_PATTERNS = [
    r"transferred .*? to the (?P<facility>[A-Za-z0-9 .,'()-]{3,120}?(?:Processing Center|Detention Center|Detention Facility|Service Processing Center|Correctional|Jail|Prison|Center))",
    r"to the (?P<facility>[A-Za-z0-9 .,'()-]{3,120}?(?:Processing Center|Detention Center|Detention Facility|Service Processing Center|Correctional|Jail|Prison|Center))",
]


class LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        href = dict(attrs).get("href")
        if href:
            self.links.append(href)


def _extract_domain(url: str) -> str:
    host = urlparse(url).netloc.lower()
    if host.startswith("www."):
        host = host[4:]
    if ":" in host:
        host = host.split(":", 1)[0]
    return host


def _domain_allowed(url: str) -> bool:
    host = _extract_domain(url)
    return any(host == domain or host.endswith(f".{domain}") for domain in ALLOWED_DOMAINS)


def _normalize_name(name_raw: str) -> str:
    name_raw = name_raw.strip()
    if "," in name_raw:
        last, first = [part.strip() for part in name_raw.split(",", 1)]
        name_raw = f"{first} {last}"
    return " ".join(part.capitalize() for part in name_raw.split())


def _parse_date_text(value: str | None) -> str | None:
    if not value:
        return None
    value = value.strip()
    for fmt in ("%B %d, %Y", "%b %d, %Y"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _build_id(person_name: str, date_of_death: str | None) -> str:
    base = f"{person_name}|{date_of_death or ''}".lower()
    return str(uuid.uuid5(ICE_REPORT_NAMESPACE, base))


def _extract_field(pattern: str, text: str) -> str | None:
    match = re.search(pattern, text, re.IGNORECASE | re.MULTILINE)
    if not match:
        return None
    return match.group(1).strip()


def _extract_facility(text: str) -> str | None:
    normalized = re.sub(r"\s+", " ", text)
    for pattern in FACILITY_PATTERNS:
        match = re.search(pattern, normalized, re.IGNORECASE)
        if match:
            facility = re.sub(r"\s+", " ", match.group("facility")).strip()
            if facility:
                return facility
    return None


def parse_report_text(
    text: str,
    url: str,
    min_death_year: int = MIN_DEATH_YEAR,
) -> dict[str, Any] | None:
    name_raw = _extract_field(r"Detainee Death Report:\s*(.+)", text)
    if not name_raw:
        return None
    person_name = _normalize_name(name_raw)
    date_of_birth = _parse_date_text(_extract_field(r"Date of Birth:\s*(.+)", text))
    date_of_death_raw = _extract_field(r"Date of (?:Neurologic )?Death:\s*(.+)", text)
    date_of_death = _parse_date_text(date_of_death_raw)
    if not date_of_death:
        return None
    if int(date_of_death[:4]) < min_death_year:
        return None
    age_text = _extract_field(r"Age:\s*(\d+)", text)
    gender = _extract_field(r"(?:Sex|Gender):\s*([A-Za-z]+)", text)
    citizenship = _extract_field(r"Country of Citizenship:\s*(.+)", text)
    facility = _extract_facility(text)

    timestamp = datetime.now(timezone.utc).isoformat()
    record = {
        "id": _build_id(person_name, date_of_death),
        "person_name": person_name,
        "name_raw": name_raw,
        "date_of_birth": date_of_birth,
        "date_of_death": date_of_death,
        "age": int(age_text) if age_text else None,
        "gender": gender,
        "country_of_citizenship": citizenship,
        "facility_or_location": facility,
        "death_context": "detention",
        "custody_status": "ICE detention",
        "agency": "ICE",
        "report_urls": [url],
        "source_type": SOURCE_TYPE,
        "extracted_at": timestamp,
        "updated_at": timestamp,
    }
    return _order_fields(record)


def _order_fields(record: dict[str, Any]) -> dict[str, Any]:
    return {field: record.get(field) for field in FIELD_ORDER}


def _load_jsonl(path: Path) -> dict[str, dict[str, Any]]:
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
            if record_id:
                records[record_id] = record
    return records


def _write_jsonl_atomic(path: Path, records: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with tempfile.NamedTemporaryFile("w", delete=False, dir=path.parent, encoding="utf-8") as handle:
        for record in records:
            handle.write(json.dumps(record, ensure_ascii=True) + "\n")
        temp_name = handle.name
    os.replace(temp_name, path)


def _merge_records(
    existing: dict[str, dict[str, Any]],
    incoming: list[dict[str, Any]],
) -> tuple[list[dict[str, Any]], int, int]:
    added = 0
    updated = 0
    now = datetime.now(timezone.utc).isoformat()

    for record in incoming:
        record_id = record["id"]
        if record_id not in existing:
            existing[record_id] = record
            added += 1
            continue
        current = existing[record_id]
        changed = False

        for field in FIELD_ORDER:
            if field in ("report_urls", "extracted_at", "updated_at"):
                continue
            new_value = record.get(field)
            if new_value is None:
                continue
            old_value = current.get(field)
            if not old_value and new_value:
                current[field] = new_value
                changed = True

        urls = set(current.get("report_urls") or [])
        for url in record.get("report_urls") or []:
            urls.add(url)
        if urls != set(current.get("report_urls") or []):
            current["report_urls"] = sorted(urls)
            changed = True

        if changed:
            current["updated_at"] = now
            updated += 1

    ordered = sorted(
        existing.values(),
        key=lambda item: (
            item.get("date_of_death") or "",
            item.get("person_name") or "",
            item.get("id") or "",
        ),
    )
    return ordered, added, updated


def _download_pdf(url: str) -> Path:
    if not _domain_allowed(url):
        raise ValueError(f"URL not allowed: {url}")
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=20) as resp:
        data = resp.read()
    handle = tempfile.NamedTemporaryFile(delete=False, suffix=".pdf")
    handle.write(data)
    handle.close()
    return Path(handle.name)


def _pdf_to_text(path: Path) -> str:
    if not shutil.which("pdftotext"):
        raise RuntimeError("pdftotext is required to extract PDF text.")
    result = subprocess.run(
        ["pdftotext", str(path), "-"],
        check=True,
        capture_output=True,
        text=True,
    )
    return result.stdout


def _fetch_index_links(index_url: str, use_playwright: bool) -> list[str]:
    req = Request(index_url, headers={"User-Agent": "Mozilla/5.0"})
    try:
        with urlopen(req, timeout=20) as resp:
            html = resp.read().decode("utf-8", errors="ignore")
    except Exception as exc:
        print(f"Warning: failed to fetch index page {index_url}: {exc}")
        if use_playwright:
            return _fetch_index_links_playwright(index_url)
        return []
    parser = LinkParser()
    parser.feed(html)
    links = []
    for link in parser.links:
        if ".pdf" not in link.lower():
            continue
        links.append(urljoin(index_url, link))
    return links


def _fetch_index_links_playwright(index_url: str) -> list[str]:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("Warning: playwright not installed; cannot fetch index via browser.")
        return []

    html = ""
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(index_url, wait_until="networkidle", timeout=30000)
            html = page.content()
            browser.close()
    except Exception as exc:
        print(f"Warning: playwright fetch failed for {index_url}: {exc}")
        return []

    parser = LinkParser()
    parser.feed(html)
    links = []
    for link in parser.links:
        if ".pdf" not in link.lower():
            continue
        links.append(urljoin(index_url, link))
    return links


def load_url_file(path: Path) -> list[str]:
    urls: list[str] = []
    with path.open("r", encoding="utf-8") as handle:
        for line in handle:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            urls.append(line)
    return urls


def fetch_report_entries(
    urls: list[str],
    include_index: bool,
    index_url: str,
    use_playwright: bool,
    min_death_year: int,
) -> list[dict[str, Any]]:
    source_urls = list(urls)
    if include_index:
        source_urls.extend(_fetch_index_links(index_url, use_playwright))

    deduped = []
    seen = set()
    for url in source_urls:
        if url in seen:
            continue
        seen.add(url)
        deduped.append(url)

    incoming: list[dict[str, Any]] = []
    for url in deduped:
        if not _domain_allowed(url):
            continue
        pdf_path = None
        try:
            pdf_path = _download_pdf(url)
            text = _pdf_to_text(pdf_path)
            record = parse_report_text(text, url, min_death_year=min_death_year)
            if record:
                incoming.append(record)
        except Exception as exc:
            print(f"Warning: failed to ingest {url}: {exc}")
        finally:
            if pdf_path and pdf_path.exists():
                pdf_path.unlink(missing_ok=True)
    return incoming


def ingest_reports(
    out_path: Path,
    urls: list[str],
    include_index: bool,
    index_url: str,
    use_playwright: bool,
    min_death_year: int,
    rebuild: bool,
    dry_run: bool,
) -> int:
    incoming = fetch_report_entries(
        urls=urls,
        include_index=include_index,
        index_url=index_url,
        use_playwright=use_playwright,
        min_death_year=min_death_year,
    )
    existing = {} if rebuild else _load_jsonl(out_path)
    ordered, added, updated = _merge_records(existing, incoming)
    if not dry_run:
        _write_jsonl_atomic(out_path, ordered)

    print(
        "Death report ingest complete:",
        f"records={len(ordered)}",
        f"added={added}",
        f"updated={updated}",
    )
    return 0


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="Ingest ICE detainee death report PDFs.")
    parser.add_argument("--out", type=Path, default=DEFAULT_OUT_PATH)
    parser.add_argument("--url", action="append", default=[])
    parser.add_argument("--url-file", type=Path)
    parser.add_argument("--include-index", action="store_true")
    parser.add_argument("--index-url", type=str, default=ICE_REPORTS_INDEX_URL)
    parser.add_argument(
        "--use-playwright",
        action="store_true",
        help="Use Playwright headless browser if index fetch fails.",
    )
    parser.add_argument("--min-death-year", type=int, default=MIN_DEATH_YEAR)
    parser.add_argument(
        "--rebuild",
        action="store_true",
        help="Ignore existing JSONL and rebuild from current sources.",
    )
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args(argv)

    urls = list(args.url)
    if args.url_file:
        urls.extend(load_url_file(args.url_file))

    if not urls and not args.include_index:
        raise SystemExit("Provide --url/--url-file or pass --include-index.")

    return ingest_reports(
        out_path=args.out,
        urls=urls,
        include_index=args.include_index,
        index_url=args.index_url,
        use_playwright=args.use_playwright,
        min_death_year=args.min_death_year,
        rebuild=args.rebuild,
        dry_run=args.dry_run,
    )


if __name__ == "__main__":
    raise SystemExit(main())
