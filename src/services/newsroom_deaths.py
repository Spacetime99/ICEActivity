"""Fetch and parse ICE newsroom death-related releases."""

from __future__ import annotations

import html
import json
import re
from dataclasses import dataclass
from datetime import datetime
from html.parser import HTMLParser
from typing import Any, Iterable
from urllib.parse import urlencode, urljoin
from urllib.request import Request, urlopen

NEWSROOM_URL = "https://www.ice.gov/newsroom"
VIEWS_AJAX_URL = "https://www.ice.gov/views/ajax"

DEATH_KEYWORDS = (
    "death",
    "died",
    "dies",
    "passed away",
    "pass away",
    "custody",
    "deceased",
    "pronounced deceased",
)

DETAIL_KEYWORDS = (
    "died",
    "dies",
    "passed away",
    "passes away",
    "pronounced deceased",
    "pronounced dead",
)

MONTH_PATTERN = (
    r"(?:Jan(?:uary)?|Feb(?:ruary)?|Mar(?:ch)?|Apr(?:il)?|May|Jun(?:e)?|Jul(?:y)?|"
    r"Aug(?:ust)?|Sep(?:t|tember)?\.?|Oct(?:ober)?|Nov(?:ember)?|Dec(?:ember)?)"
)
DATE_PATTERN = rf"{MONTH_PATTERN}\s+\d{{1,2}},\s+\d{{4}}"
MONTH_DAY_PATTERN = rf"{MONTH_PATTERN}\s+\d{{1,2}}"
ISO_DATE_PATTERN = r"\b\d{4}-\d{2}-\d{2}\b"
JSON_STRING_FIELD_PATTERN = re.compile(
    r'"(?P<key>[A-Za-z0-9_]*(?:body|text|summary|content))"\s*:\s*'
    r'"(?P<value>(?:\\.|[^"\\])*)"',
    re.IGNORECASE,
)
DATELINE_DASH_PATTERN = re.compile(
    r"\b([A-Za-z][A-Za-z .'-]+),\s*([A-Za-z.]{2,})\b\s*[-\u2014]\s*",
    re.IGNORECASE,
)
DATELINE_PATTERN = re.compile(
    r"\b([A-Za-z][A-Za-z .'-]+),\s*([A-Za-z.]{2,})\b",
    re.IGNORECASE,
)

STATE_ABBREVIATIONS = {
    "AL",
    "AK",
    "AZ",
    "AR",
    "CA",
    "CO",
    "CT",
    "DE",
    "FL",
    "GA",
    "HI",
    "ID",
    "IL",
    "IN",
    "IA",
    "KS",
    "KY",
    "LA",
    "ME",
    "MD",
    "MA",
    "MI",
    "MN",
    "MS",
    "MO",
    "MT",
    "NE",
    "NV",
    "NH",
    "NJ",
    "NM",
    "NY",
    "NC",
    "ND",
    "OH",
    "OK",
    "OR",
    "PA",
    "RI",
    "SC",
    "SD",
    "TN",
    "TX",
    "UT",
    "VT",
    "VA",
    "WA",
    "WV",
    "WI",
    "WY",
    "DC",
}

STATE_NAMES = {
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
}


@dataclass(frozen=True)
class NewsroomLink:
    url: str
    title: str


class LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[NewsroomLink] = []
        self.current_href: str | None = None
        self.text_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag != "a":
            return
        href = dict(attrs).get("href")
        if href:
            self.current_href = href
            self.text_parts = []

    def handle_endtag(self, tag: str) -> None:
        if tag != "a":
            return
        if self.current_href:
            text = " ".join(part.strip() for part in self.text_parts if part.strip())
            self.links.append(NewsroomLink(self.current_href, text))
        self.current_href = None
        self.text_parts = []

    def handle_data(self, data: str) -> None:
        if self.current_href is not None:
            self.text_parts.append(data)


class ContentExtractor(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.in_script = False
        self.in_style = False
        self.stack: list[dict[str, Any]] = []
        self.blocks: list[dict[str, Any]] = []
        self.all_parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in {"script", "style"}:
            if tag == "script":
                self.in_script = True
            else:
                self.in_style = True
            return
        if tag in {"main", "article", "section", "div"}:
            attrs_map = {key: value or "" for key, value in attrs}
            self.stack.append(
                {
                    "tag": tag,
                    "attrs": attrs_map,
                    "text_parts": [],
                },
            )

    def handle_endtag(self, tag: str) -> None:
        if tag == "script":
            self.in_script = False
            return
        if tag == "style":
            self.in_style = False
            return
        if not self.stack:
            return
        if self.stack[-1]["tag"] != tag:
            return
        block = self.stack.pop()
        text = " ".join(part.strip() for part in block["text_parts"] if part.strip())
        if text:
            block["text"] = text
            self.blocks.append(block)
            if self.stack:
                self.stack[-1]["text_parts"].append(text)

    def handle_data(self, data: str) -> None:
        if self.in_script or self.in_style:
            return
        if self.stack:
            self.stack[-1]["text_parts"].append(data)
        self.all_parts.append(data)


def _extract_jsonld_text(payload: str) -> str:
    blocks = re.findall(
        r"<script[^>]+type=[\"']application/ld\+json[^\"']*[\"'][^>]*>(.*?)</script>",
        payload,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not blocks:
        return ""
    texts: list[str] = []
    for block in blocks:
        block = block.strip()
        if not block:
            continue
        try:
            data = json.loads(block)
        except json.JSONDecodeError:
            continue
        texts.extend(_extract_text_from_json(data))
    return " ".join(texts).strip()


def _extract_text_from_json(data: Any) -> list[str]:
    texts: list[str] = []
    if isinstance(data, dict):
        for key, value in data.items():
            if key in {"articleBody", "headline", "description", "name"} and isinstance(
                value, str
            ):
                texts.append(value)
            if key in {"value", "processed", "summary"} and isinstance(value, str):
                texts.append(value)
            texts.extend(_extract_text_from_json(value))
        if "value" in data and isinstance(data["value"], str):
            texts.append(data["value"])
    elif isinstance(data, list):
        for item in data:
            texts.extend(_extract_text_from_json(item))
    elif isinstance(data, str):
        if len(data.split()) > 10:
            texts.append(data)
    return texts


def _extract_script_json_text(payload: str) -> str:
    blocks = re.findall(
        r"<script[^>]+type=[\"']application/json[^\"']*[\"'][^>]*>(.*?)</script>",
        payload,
        flags=re.IGNORECASE | re.DOTALL,
    )
    if not blocks:
        return ""
    texts: list[str] = []
    for block in blocks:
        block = block.strip()
        if not block:
            continue
        try:
            data = json.loads(block)
        except json.JSONDecodeError:
            continue
        texts.extend(_extract_text_from_json(data))
    cleaned = []
    for text in texts:
        text = re.sub(r"<[^>]+>", " ", text)
        text = html.unescape(text)
        cleaned.append(text)
    return " ".join(cleaned).strip()


def _extract_json_string_fields(payload: str) -> str:
    texts: list[str] = []
    for match in JSON_STRING_FIELD_PATTERN.finditer(payload):
        raw_value = match.group("value")
        if not raw_value or len(raw_value) < 40:
            continue
        try:
            decoded = json.loads(f"\"{raw_value}\"")
        except json.JSONDecodeError:
            decoded = raw_value.replace("\\n", " ").replace("\\t", " ")
            decoded = decoded.replace("\\\"", "\"")
        decoded = html.unescape(decoded)
        texts.append(decoded)
    return " ".join(texts).strip()


def _text_missing_keywords(text: str) -> bool:
    if not text:
        return True
    lower = text.lower()
    return not any(keyword in lower for keyword in DEATH_KEYWORDS)


def _score_block(text: str, attrs: dict[str, str], tag: str) -> int:
    lower = text.lower()
    score = 0
    if any(keyword in lower for keyword in DEATH_KEYWORDS):
        score += 12
    score += min(len(text) // 200, 20)
    if tag in {"main", "article"}:
        score += 5
    attr_blob = " ".join(attrs.values()).lower()
    if any(token in attr_blob for token in ("content", "body", "article", "story", "field")):
        score += 2
    if any(token in attr_blob for token in ("nav", "footer", "header", "menu", "breadcrumb")):
        score -= 6
    return score


def _select_block_text(parser: ContentExtractor) -> str:
    best_text = ""
    best_score = -1
    for block in parser.blocks:
        text = block.get("text", "")
        if not text:
            continue
        score = _score_block(text, block.get("attrs", {}), block.get("tag", ""))
        if score > best_score:
            best_score = score
            best_text = text
    if best_text:
        return best_text
    return ""


def _html_to_text(payload: str) -> str:
    parser = ContentExtractor()
    parser.feed(payload)
    text = _select_block_text(parser)
    if not text:
        text = " ".join(part.strip() for part in parser.all_parts if part.strip())
    text = html.unescape(text).strip()
    if _text_missing_keywords(text):
        json_text = _extract_jsonld_text(payload)
        if json_text:
            text = f"{text} {json_text}".strip() if text else json_text
    if _text_missing_keywords(text):
        json_text = _extract_script_json_text(payload)
        if json_text:
            text = f"{text} {json_text}".strip() if text else json_text
    if _text_missing_keywords(text):
        json_text = _extract_json_string_fields(payload)
        if json_text:
            text = f"{text} {json_text}".strip() if text else json_text
    if _text_missing_keywords(text):
        stripped = re.sub(r"<[^>]+>", " ", payload)
        stripped = html.unescape(stripped)
        stripped = re.sub(r"\s+", " ", stripped).strip()
        text = stripped or text
    return text


def _fetch_html(url: str) -> str:
    req = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    with urlopen(req, timeout=20) as resp:
        return resp.read().decode("utf-8", errors="ignore")


def _fetch_html_playwright(url: str) -> str:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError("Playwright is required for browser fetches.") from exc

    with sync_playwright() as playwright:
        browser = playwright.chromium.launch(headless=True)
        page = browser.new_page()
        page.goto(url, wait_until="networkidle", timeout=30000)
        html = page.content()
        browser.close()
    return html


def _fetch_newsroom_html(use_playwright: bool) -> str:
    if use_playwright:
        return _fetch_html_playwright(NEWSROOM_URL)
    return _fetch_html(NEWSROOM_URL)


def _fetch_newsroom_page_html(page: int, use_playwright: bool) -> str:
    url = NEWSROOM_URL if page <= 0 else f"{NEWSROOM_URL}?page={page}"
    if use_playwright:
        return _fetch_html_playwright(url)
    return _fetch_html(url)


def _extract_drupal_settings(html: str) -> dict[str, Any]:
    match = re.search(r"<script[^>]+drupal-settings-json[^>]*>(.*?)</script>", html, re.DOTALL)
    if not match:
        raise RuntimeError("Unable to locate drupal settings JSON.")
    return json.loads(match.group(1))


def _score_view_config(config: dict[str, Any]) -> int:
    display_id = (config.get("view_display_id") or "").lower()
    view_name = (config.get("view_name") or "").lower()
    base_path = (config.get("view_base_path") or "").lower()
    score = 0
    if display_id.startswith("page"):
        score += 4
    if "page" in display_id:
        score += 2
    if display_id.startswith("block"):
        score -= 2
    if "newsroom" in view_name:
        score += 1
    if "newsroom" in base_path:
        score += 1
    return score


def _select_ajax_view(settings: dict[str, Any]) -> tuple[str, dict[str, Any]]:
    views = settings.get("views", {})
    ajax_views = views.get("ajaxViews", {})
    if not ajax_views:
        raise RuntimeError("No AJAX views found for newsroom page.")
    best_dom_id = ""
    best_config: dict[str, Any] | None = None
    best_score = -10
    for view_dom_id, config in ajax_views.items():
        score = _score_view_config(config)
        if best_config is None or score > best_score:
            best_dom_id = view_dom_id
            best_config = config
            best_score = score
    if best_config is None:
        raise RuntimeError("No AJAX views found for newsroom page.")
    return best_dom_id, best_config


def _view_payload(settings: dict[str, Any]) -> dict[str, str]:
    view_dom_id, config = _select_ajax_view(settings)
    return {
        "view_name": config.get("view_name", ""),
        "view_display_id": config.get("view_display_id", ""),
        "view_args": config.get("view_args", ""),
        "view_path": config.get("view_path", ""),
        "view_base_path": config.get("view_base_path") or "",
        "view_dom_id": view_dom_id.split(":", 1)[1],
        "pager_element": "0",
        "page": "0",
    }


def _apply_page_to_path(path: str, page: int) -> str:
    if not path or page <= 0:
        return path
    if re.search(r"[?&]page=\d+", path):
        return re.sub(r"([?&]page=)\d+", rf"\1{page}", path)
    separator = "&" if "?" in path else "?"
    return f"{path}{separator}page={page}"


def _fetch_view_html(payload: dict[str, str]) -> str:
    encoded = urlencode(payload).encode("utf-8")
    req = Request(
        VIEWS_AJAX_URL,
        data=encoded,
        headers={"Content-Type": "application/x-www-form-urlencoded", "User-Agent": "Mozilla/5.0"},
    )
    with urlopen(req, timeout=20) as resp:
        data = json.loads(resp.read().decode("utf-8", errors="ignore"))
    html_blocks = [item.get("data", "") for item in data if item.get("command") == "insert"]
    return "\n".join(html_blocks)


def _extract_release_links(html: str) -> list[NewsroomLink]:
    parser = LinkParser()
    parser.feed(html)
    links: list[NewsroomLink] = []
    seen: set[str] = set()
    for link in parser.links:
        href = link.url
        if href.startswith("/"):
            href = urljoin(NEWSROOM_URL, href)
        if "/news/releases/" not in href and "/news/statements/" not in href:
            continue
        if href in seen:
            continue
        seen.add(href)
        links.append(NewsroomLink(href, link.title))
    return links


def _filter_links(links: Iterable[NewsroomLink]) -> list[NewsroomLink]:
    filtered: list[NewsroomLink] = []
    for link in links:
        haystack = f"{link.title} {link.url}".lower()
        if any(keyword in haystack for keyword in DEATH_KEYWORDS):
            filtered.append(link)
    return filtered


def _fetch_article_text(url: str, use_playwright: bool) -> str:
    if use_playwright:
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            use_playwright = False
    if use_playwright:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch(headless=True)
            page = browser.new_page()
            page.goto(url, wait_until="domcontentloaded", timeout=30000)
            text = page.locator("main").inner_text().strip()
            browser.close()
            return text
    return _html_to_text(_fetch_html(url))


def _normalize_text(text: str) -> str:
    normalized = re.sub(r"\s+", " ", text)
    normalized = normalized.replace("\u2013", "-")
    normalized = normalized.replace("\u2014", "-")
    normalized = re.sub(r"(\d{4})([A-Z])", r"\1 \2", normalized)
    normalized = re.sub(
        r"\bDETAINEE DEATH NOTIFICATIONS\b",
        "DETAINEE DEATH NOTIFICATIONS.",
        normalized,
        flags=re.IGNORECASE,
    )
    normalized = re.sub(r"\b([A-Z]{2})(DETAINEE)\b", r"\1 \2", normalized)
    normalized = re.sub(
        r"\b(Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Sept|Oct|Nov|Dec)\.",
        r"\1",
        normalized,
    )
    return normalized.strip()


def _parse_date(text: str) -> str | None:
    if not text:
        return None
    value = text.strip().replace("Sept.", "Sep.").replace("Sept", "Sep").title()
    for fmt in ("%B %d, %Y", "%b %d, %Y", "%B %d %Y", "%b %d %Y", "%Y-%m-%d"):
        try:
            return datetime.strptime(value, fmt).date().isoformat()
        except ValueError:
            continue
    return None


def _extract_release_date(text: str) -> str | None:
    normalized = _normalize_text(text)
    match = re.search(DATE_PATTERN, normalized, re.IGNORECASE)
    if match:
        return _parse_date(match.group(0))
    match = re.search(ISO_DATE_PATTERN, normalized)
    if match:
        return _parse_date(match.group(0))
    return None


def _normalize_state(state: str) -> tuple[str | None, bool]:
    cleaned = state.strip().replace(".", "")
    if len(cleaned) == 2:
        code = cleaned.upper()
        if code in STATE_ABBREVIATIONS:
            return code, True
        return None, False
    title = cleaned.title()
    if title in STATE_NAMES:
        return title, False
    return None, False


def _normalize_city(city: str) -> str | None:
    cleaned = re.sub(r"\s+", " ", city.strip(" ,"))
    if not cleaned:
        return None
    if len(cleaned.split()) > 4:
        return None
    if any(len(token.strip(".-")) <= 1 for token in cleaned.split()):
        return None
    if cleaned.upper() in STATE_ABBREVIATIONS:
        return None
    if cleaned.title() in STATE_NAMES:
        return None
    letters = re.sub(r"[^A-Za-z]", "", cleaned)
    if len(letters) < 3:
        return None
    return cleaned.title()


def _extract_dateline(text: str) -> tuple[str | None, str | None]:
    snippet = _normalize_text(text)[:800]
    candidates: list[tuple[int, int, str, str]] = []
    for match in DATELINE_DASH_PATTERN.finditer(snippet):
        city_raw, state_raw = match.group(1), match.group(2)
        city = _normalize_city(city_raw)
        state, is_abbrev = _normalize_state(state_raw)
        if not city or not state:
            continue
        score = 5 if is_abbrev else 4
        if " " in city:
            score += 1
        if len(city) > 8:
            score += 1
        candidates.append((score, match.start(), city, state))
    if not candidates:
        for match in DATELINE_PATTERN.finditer(snippet):
            city_raw, state_raw = match.group(1), match.group(2)
            city = _normalize_city(city_raw)
            state, is_abbrev = _normalize_state(state_raw)
            if not city or not state:
                continue
            score = 3 if is_abbrev else 2
            if " " in city:
                score += 1
            if len(city) > 8:
                score += 1
            candidates.append((score, match.start(), city, state))
    if not candidates:
        return None, None
    candidates.sort(key=lambda item: (-item[0], item[1]))
    _, _, city, state = candidates[0]
    return city, state


def _find_death_sentence(text: str) -> str | None:
    sentences = _split_sentences(_normalize_text(text))
    for sentence in sentences:
        lower = sentence.lower()
        if any(keyword in lower for keyword in DEATH_KEYWORDS):
            return sentence.strip()
    return None


def _extract_date_from_text(text: str) -> str | None:
    match = re.search(DATE_PATTERN, text, re.IGNORECASE)
    if not match:
        return None
    return _parse_date(match.group(0))


def _extract_month_day_from_text(text: str) -> str | None:
    match = re.search(MONTH_DAY_PATTERN, text, re.IGNORECASE)
    if not match:
        return None
    return match.group(0)


def _parse_month_day(text: str, year: int) -> str | None:
    candidate = f"{text} {year}"
    return _parse_date(candidate)


def _strip_dateline_prefix(sentence: str) -> str:
    return re.sub(r"^[A-Z .'-]+,\s*[A-Za-z]{2,}\s+[\u2014-]\s+", "", sentence)


def _extract_name_from_sentence(sentence: str) -> str | None:
    if not sentence:
        return None
    cleaned = _strip_dateline_prefix(sentence)
    match = re.search(
        r"([A-Z][A-Za-z'.-]+(?:\s+[A-Z][A-Za-z'.-]+){1,4})\s*,",
        cleaned,
    )
    if not match:
        return None
    return match.group(1).strip()


def _extract_name_from_text(text: str) -> str | None:
    match = re.search(
        r"\b([A-Z][A-Za-z'.-]+(?:\s+[A-Z][A-Za-z'.-]+){1,4}),\s+(?:a|an)\s+\d{1,3}-year-old\b",
        text,
    )
    if not match:
        return None
    return match.group(1).strip()


def _split_sentences(text: str) -> list[str]:
    return [sentence.strip() for sentence in re.split(r"(?<=[.!?])\s+", text) if sentence.strip()]


def _is_death_detail_sentence(sentence: str) -> bool:
    lower = sentence.lower()
    if "death notifications" in lower:
        return False
    return any(keyword in lower for keyword in DETAIL_KEYWORDS)


def _score_death_sentence(sentence: str) -> int:
    lower = sentence.lower()
    if "death notifications" in lower:
        return -1
    score = 0
    if _is_death_detail_sentence(sentence):
        score += 4
    elif any(keyword in lower for keyword in DEATH_KEYWORDS):
        score += 1
    if _extract_name_from_sentence(sentence):
        score += 2
    if re.search(r"\d{1,3}-year-old", sentence):
        score += 2
    if _extract_month_day_from_text(sentence):
        score += 1
    if _extract_date_from_text(sentence):
        score += 1
    return score


def _select_death_sentence(sentences: list[str]) -> str:
    best_sentence = ""
    best_score = 0
    for sentence in sentences:
        score = _score_death_sentence(sentence)
        if score > best_score:
            best_score = score
            best_sentence = sentence
    return best_sentence


def parse_death_fields(text: str) -> dict[str, Any]:
    dateline_city, dateline_state = _extract_dateline(text)
    normalized = _normalize_text(text)
    release_date = _extract_release_date(text)
    header_match = re.search(r"\bDETAINEE DEATH NOTIFICATIONS\b", normalized, re.IGNORECASE)
    body_text = normalized[header_match.end():].strip() if header_match else normalized
    dateline_match = re.search(
        r"\b[A-Z][A-Z .'-]+,\s*[A-Za-z.]{2,}\b\.?(?:\s*-\s*)?",
        body_text,
    )
    if dateline_match:
        body_text = body_text[dateline_match.end():].strip()
    sentences = _split_sentences(body_text)

    date_of_death = None
    person_name = None
    sentence = _select_death_sentence(sentences)
    if not sentence:
        sentence = _find_death_sentence(body_text) or ""

    if sentence:
        name_match = re.search(
            rf"\bOn ({DATE_PATTERN}),\s*([^,]+),",
            sentence,
            re.IGNORECASE,
        )
        if name_match:
            date_of_death = _parse_date(name_match.group(1))
            if not person_name:
                person_name = name_match.group(2).strip()
        else:
            date_of_death = _extract_date_from_text(sentence)
            if release_date:
                month_day = _extract_month_day_from_text(sentence)
                if month_day:
                    release_year = int(release_date[:4])
                    inferred = _parse_month_day(month_day, release_year)
                    if inferred:
                        date_of_death = inferred

    if not date_of_death:
        date_of_death = _extract_date_from_text(body_text or normalized)
        if date_of_death == release_date:
            date_of_death = None
    if not date_of_death and release_date:
        month_day = _extract_month_day_from_text(body_text or normalized)
        if month_day:
            release_year = int(release_date[:4])
            inferred = _parse_month_day(month_day, release_year)
            if inferred:
                date_of_death = inferred
    if not person_name:
        person_name = _extract_name_from_sentence(sentence)

    if not date_of_death or not person_name:
        detail_text = body_text or normalized
        match = re.search(
            rf"(?P<name>[A-Z][A-Za-z'.-]+(?:\s+[A-Z][A-Za-z'.-]+){{1,4}})"
            rf"[^.]*?\b(died|passed away|was pronounced dead|was pronounced deceased)\b"
            rf"[^.]*?\b(?P<date>{DATE_PATTERN})",
            detail_text,
            re.IGNORECASE,
        )
        if match:
            if not person_name:
                person_name = match.group("name").strip()
            if not date_of_death:
                date_of_death = _parse_date(match.group("date"))

    if not person_name:
        person_name = _extract_name_from_text(body_text or normalized)
    if person_name and dateline_city:
        if dateline_city.lower() in person_name.lower():
            person_name = _extract_name_from_text(body_text or normalized)

    under_investigation = "under investigation" in normalized.lower()
    age_match = re.search(r"(\d{1,3})-year-old", sentence)
    age = int(age_match.group(1)) if age_match else None

    nationality_match = re.search(r"(\d{1,3})-year-old\s+([^,]+?)\s+national", sentence)
    nationality = nationality_match.group(2).strip() if nationality_match else None
    if not nationality:
        from_match = re.search(r"\bfrom\s+([A-Z][A-Za-z .'-]+)", sentence)
        if from_match:
            nationality = from_match.group(1).split(",")[0].strip()

    facility_match = re.search(
        r"at (?:the )?([^.,]+?(?:Detention Center|Processing Center|Service Processing Center|Facility|Center|Hospital|Camp))",
        sentence,
        re.IGNORECASE,
    )
    facility = facility_match.group(1).strip() if facility_match else None

    return {
        "person_name": person_name,
        "date_of_death": date_of_death,
        "age": age,
        "nationality": nationality,
        "facility_or_location": facility,
        "city": dateline_city,
        "state": dateline_state,
        "raw_sentence": sentence or None,
        "release_date": release_date,
        "under_investigation": under_investigation,
    }


def fetch_death_releases(
    limit: int,
    use_playwright: bool,
    debug: bool = False,
    max_pages: int = 1,
    min_death_year: int | None = None,
    stop_keys: set[str] | None = None,
) -> list[dict[str, Any]]:
    html = _fetch_newsroom_html(use_playwright)
    if debug:
        print(f"Newsroom: fetched main page ({len(html)} chars)")
    settings = _extract_drupal_settings(html)
    payload = _view_payload(settings)
    base_view_path = payload.get("view_path") or payload.get("view_base_path") or ""
    display_id = (payload.get("view_display_id") or "").lower()
    if debug:
        print(
            "Newsroom: view payload "
            f"name={payload.get('view_name')} display={payload.get('view_display_id')}",
        )
        if base_view_path:
            print(f"Newsroom: view path base={base_view_path}")
    results: list[dict[str, Any]] = []
    seen_urls: set[str] = set()
    stop = False
    for page in range(max_pages):
        payload["page"] = str(page)
        payload["view_path"] = _apply_page_to_path(base_view_path, page)
        use_page_fallback = page > 0 and (not base_view_path or display_id.startswith("block"))
        if use_page_fallback:
            view_html = _fetch_newsroom_page_html(page, use_playwright)
            if debug:
                print(
                    f"Newsroom: fetched page HTML ({len(view_html)} chars) page={page} fallback=1"
                )
        else:
            view_html = _fetch_view_html(payload)
            if debug:
                print(f"Newsroom: fetched view HTML ({len(view_html)} chars) page={page}")
        links = _filter_links(_extract_release_links(view_html))
        if debug:
            print(f"Newsroom: filtered release links={len(links)} page={page}")
        page_added = 0
        for link in links:
            if link.url in seen_urls:
                continue
            seen_urls.add(link.url)
            if debug:
                print(f"Newsroom: fetch {link.url}")
            text = _fetch_article_text(link.url, use_playwright)
            fields = parse_death_fields(text)
            if debug:
                raw_sentence = fields.get("raw_sentence")
                if raw_sentence and len(raw_sentence) > 200:
                    raw_sentence = raw_sentence[:200] + "..."
                summary = {
                    "person_name": fields.get("person_name"),
                    "date_of_death": fields.get("date_of_death"),
                    "release_date": fields.get("release_date"),
                    "city": fields.get("city"),
                    "state": fields.get("state"),
                    "raw_sentence": raw_sentence,
                }
                print(f"Newsroom: parsed {json.dumps(summary, ensure_ascii=True)}")
            fields["url"] = link.url
            fields["title"] = link.title
            results.append(fields)
            page_added += 1

            date_of_death = fields.get("date_of_death")
            if min_death_year and date_of_death:
                try:
                    year = int(date_of_death[:4])
                except ValueError:
                    year = None
                if year is not None and year < min_death_year:
                    stop = True
            if stop_keys:
                name = fields.get("person_name")
                if name and date_of_death:
                    key = f"{name.lower()}|{date_of_death}"
                    if key in stop_keys:
                        if debug:
                            print(f"Newsroom: stop on existing key={key}")
                        stop = True

            if limit > 0 and len(results) >= limit:
                stop = True

            if stop:
                break
        if stop or page_added == 0:
            break
    return results
