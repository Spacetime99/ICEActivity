#!/usr/bin/env python3
"""List hostnames missing SVG/PNG logos in frontend/public/source-logos."""

from __future__ import annotations

import argparse
import json
import re
from collections import Counter
from pathlib import Path
from urllib.parse import urlparse


DEFAULT_TRIPLETS = Path("frontend/public/data/triplets_all.json")
DEFAULT_LOGO_DIR = Path("frontend/public/source-logos")


def _slugify(value: str) -> str:
    cleaned = re.sub(r"[^a-zA-Z0-9]+", "_", value).strip("_").lower()
    return cleaned or "source"


def main() -> None:
    parser = argparse.ArgumentParser(
        description="List hostnames missing SVG/PNG logos."
    )
    parser.add_argument(
        "--triplets",
        type=Path,
        default=DEFAULT_TRIPLETS,
        help="Path to triplets_all.json.",
    )
    parser.add_argument(
        "--logo-dir",
        type=Path,
        default=DEFAULT_LOGO_DIR,
        help="Directory containing source-logos.",
    )
    parser.add_argument(
        "--min-count",
        type=int,
        default=1,
        help="Minimum number of articles required to include a host.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Limit number of rows (0 = no limit).",
    )
    args = parser.parse_args()

    if not args.triplets.exists():
        raise SystemExit(f"Triplets file not found: {args.triplets}")
    if not args.logo_dir.exists():
        raise SystemExit(f"Logo directory not found: {args.logo_dir}")

    payload = json.loads(args.triplets.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise SystemExit("Triplets payload must be a JSON array.")

    counts: Counter[str] = Counter()
    for item in payload:
        if not isinstance(item, dict):
            continue
        url = (item.get("url") or "").strip()
        if not url:
            continue
        try:
            host = urlparse(url).hostname or ""
        except ValueError:
            host = ""
        if not host:
            continue
        host = host.replace("www.", "")
        counts[host] += 1

    rows: list[tuple[str, int]] = []
    for host, count in counts.most_common():
        if count < args.min_count:
            continue
        slug = _slugify(host)
        has_svg = (args.logo_dir / f"{slug}.svg").exists()
        has_png = (args.logo_dir / f"{slug}.png").exists()
        if not (has_svg or has_png):
            rows.append((host, count))

    if args.limit > 0:
        rows = rows[: args.limit]

    for host, count in rows:
        print(f"{host}\t{count}")


if __name__ == "__main__":
    main()
