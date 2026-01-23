#!/usr/bin/env python3
"""Fetch small favicon-style logos for sources listed in triplets JSON."""

from __future__ import annotations

import argparse
import json
import re
import sys
import io
from collections import Counter, defaultdict
from pathlib import Path
from typing import Iterable
from urllib.parse import urljoin, urlparse
from urllib.request import Request, urlopen


DEFAULT_TRIPLETS_PATH = Path("frontend/public/data/triplets_all.json")
DEFAULT_OUTPUT_DIR = Path("frontend/public/source-logos")
DEFAULT_TOP_N = 30
AGGREGATOR_PREFIXES = ("newsapi", "rssapp", "html:")
DEFAULT_SKIP = {"test"}


def _slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", value).strip("_").lower()
    return cleaned or "source"


def _iter_triplets(path: Path) -> Iterable[dict]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"Triplets payload must be a JSON array: {path}")
    for item in payload:
        if isinstance(item, dict):
            yield item


def _is_aggregator(source: str) -> bool:
    if source.startswith("rss:"):
        source = source[4:]
    return source in AGGREGATOR_PREFIXES or source.startswith(AGGREGATOR_PREFIXES)


def _discover_hostnames(triplets: Iterable[dict]) -> dict[str, str]:
    hostnames: dict[str, Counter[str]] = defaultdict(Counter)
    for item in triplets:
        source = (item.get("source") or "").strip()
        url = (item.get("url") or "").strip()
        if not source or not url:
            continue
        parsed = urlparse(url)
        if not parsed.hostname:
            continue
        hostnames[source][parsed.hostname] += 1
    resolved: dict[str, str] = {}
    for source, counts in hostnames.items():
        resolved[source] = counts.most_common(1)[0][0]
    return resolved


def _fetch_bytes(url: str, user_agent: str, timeout: int) -> tuple[bytes, str] | None:
    req = Request(url, headers={"User-Agent": user_agent})
    with urlopen(req, timeout=timeout) as resp:
        return resp.read(), resp.headers.get("Content-Type", "")


def _candidate_paths() -> list[str]:
    return [
        "/favicon.ico",
        "/favicon.png",
        "/apple-touch-icon.png",
        "/apple-touch-icon-precomposed.png",
    ]


def _extract_icon_links(html: str) -> list[str]:
    links = re.findall(r"<link[^>]+>", html, flags=re.IGNORECASE)
    hrefs: list[str] = []
    for tag in links:
        rel_match = re.search(r'rel="([^"]+)"', tag, flags=re.IGNORECASE)
        href_match = re.search(r'href="([^"]+)"', tag, flags=re.IGNORECASE)
        if not rel_match or not href_match:
            continue
        rel_val = rel_match.group(1).lower()
        if "icon" in rel_val:
            hrefs.append(href_match.group(1))
    return hrefs


def _download_icon(
    hostname: str,
    user_agent: str,
    timeout: int,
) -> tuple[bytes, str, str] | None:
    base = f"https://{hostname}"
    for path in _candidate_paths():
        url = base + path
        try:
            content, content_type = _fetch_bytes(url, user_agent, timeout)
        except Exception:
            continue
        if content:
            return content, content_type, url

    try:
        html_bytes, _ = _fetch_bytes(base, user_agent, timeout)
    except Exception:
        return None
    html = html_bytes.decode("utf-8", "ignore")
    for href in _extract_icon_links(html):
        url = urljoin(base + "/", href)
        try:
            content, content_type = _fetch_bytes(url, user_agent, timeout)
        except Exception:
            continue
        if content:
            return content, content_type, url
    return None


def _guess_ext(content_type: str, url: str) -> str:
    lowered = content_type.lower()
    if "png" in lowered:
        return ".png"
    if "svg" in lowered:
        return ".svg"
    if "icon" in lowered or url.endswith(".ico"):
        return ".ico"
    if url.endswith(".png"):
        return ".png"
    if url.endswith(".svg"):
        return ".svg"
    return ".ico"


def _maybe_resize(
    content: bytes,
    ext: str,
    size: int | None,
) -> tuple[bytes, str]:
    if not size:
        return content, ext
    try:
        from PIL import Image  # type: ignore
    except Exception:
        print("WARN: PIL not available; skipping resize.")
        return content, ext
    if ext == ".ico":
        return content, ext
    with Image.open(io.BytesIO(content)) as img:
        img = img.convert("RGBA")
        img = img.resize((size, size), Image.LANCZOS)
        out = io.BytesIO()
        img.save(out, format="PNG")
        return out.getvalue(), ".png"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fetch favicon-style logos for sources in triplets JSON."
    )
    parser.add_argument(
        "--triplets",
        type=Path,
        default=DEFAULT_TRIPLETS_PATH,
        help="Path to triplets_all.json.",
    )
    parser.add_argument(
        "--output-dir",
        type=Path,
        default=DEFAULT_OUTPUT_DIR,
        help="Directory to store downloaded icons.",
    )
    parser.add_argument(
        "--all-sources",
        action="store_true",
        help="Process every distinct source in the triplets file.",
    )
    parser.add_argument(
        "--top-n",
        type=int,
        default=DEFAULT_TOP_N,
        help="Number of top sources to process by volume.",
    )
    parser.add_argument(
        "--include-aggregators",
        action="store_true",
        help="Include aggregator sources like newsapi/rssapp.",
    )
    parser.add_argument(
        "--timeout",
        type=int,
        default=20,
        help="Network timeout in seconds.",
    )
    parser.add_argument(
        "--resize",
        type=int,
        default=None,
        help="Resize icons to a square PNG (requires Pillow).",
    )
    parser.add_argument(
        "--save-hostnames",
        action="store_true",
        help="Also fetch icons by article hostnames and save using hostname slugs.",
    )
    args = parser.parse_args()

    if not args.triplets.exists():
        raise SystemExit(f"Triplets file not found: {args.triplets}")

    triplets = list(_iter_triplets(args.triplets))
    source_counts = Counter(
        (item.get("source") or "").strip() for item in triplets if item.get("source")
    )
    if args.all_sources:
        sources = [source for source in source_counts.keys()]
    else:
        sources = [source for source, _ in source_counts.most_common(args.top_n)]
    if not args.include_aggregators:
        sources = [source for source in sources if not _is_aggregator(source)]
    sources = [source for source in sources if source not in DEFAULT_SKIP]

    hostname_map = _discover_hostnames(triplets)
    hostnames = sorted({urlparse(item.get("url", "")).hostname for item in triplets if item.get("url")})
    hostnames = [host for host in hostnames if host]
    args.output_dir.mkdir(parents=True, exist_ok=True)
    user_agent = "Mozilla/5.0 (X11; Linux x86_64)"

    for source in sources:
        hostname = hostname_map.get(source)
        if not hostname:
            print(f"{source}\tSKIP(no hostname)")
            continue
        result = _download_icon(hostname, user_agent, args.timeout)
        if not result:
            print(f"{source}\tFAILED")
            continue
        content, content_type, url = result
        ext = _guess_ext(content_type, url)
        content, ext = _maybe_resize(content, ext, args.resize)
        filename = _slugify(source) + ext
        out_path = args.output_dir / filename
        out_path.write_bytes(content)
        print(f"{source}\t{hostname}\t{url}\t{out_path}")

    if args.save_hostnames:
        for hostname in hostnames:
            result = _download_icon(hostname, user_agent, args.timeout)
            if not result:
                print(f"{hostname}\tFAILED")
                continue
            content, content_type, url = result
            ext = _guess_ext(content_type, url)
            content, ext = _maybe_resize(content, ext, args.resize)
            filename = _slugify(hostname) + ext
            out_path = args.output_dir / filename
            out_path.write_bytes(content)
            print(f"{hostname}\t{url}\t{out_path}")


if __name__ == "__main__":
    raise SystemExit(main())
