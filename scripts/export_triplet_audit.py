#!/usr/bin/env python3
"""Export a human-readable audit report for triplet extraction quality."""

from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services import news_triplets  # noqa: E402


@dataclass(frozen=True)
class TripletRow:
    story_id: str
    url: str
    title: str
    published_at: str
    who: str
    what: str
    where_text: str


def _ascii_safe(value: str) -> str:
    return value.encode("ascii", "backslashreplace").decode("ascii")


def _format_summary_line(item: TripletRow) -> str:
    who = item.who.strip()
    what = item.what.strip()
    if who and what:
        return f"{who} {what}"
    if who:
        return who
    if what:
        return what
    return ""


def _filter_incomplete_actions(items: list[TripletRow]) -> list[TripletRow]:
    completed: set[str] = set()
    trailing_prepositions = {"against", "to", "for", "into", "with", "over"}
    for item in items:
        who = item.who.strip().lower()
        what = item.what.strip().lower()
        if not who or not what:
            continue
        words = what.split()
        if len(words) >= 3:
            base = " ".join(words[:2])
            completed.add(f"{who}|{base}")
    filtered: list[TripletRow] = []
    for item in items:
        who = item.who.strip().lower()
        what = item.what.strip().lower()
        if not who or not what:
            filtered.append(item)
            continue
        words = what.split()
        if words and len(words) <= 3:
            last = words[-1]
            if last in trailing_prepositions:
                base = " ".join(words[: min(2, len(words))])
                if f"{who}|{base}" in completed:
                    continue
        filtered.append(item)
    return filtered


def _infer_object(what: str, article_text: str) -> tuple[str, str]:
    cleaned = what.strip()
    if not cleaned:
        return "", ""
    if news_triplets._needs_object_completion(cleaned):
        fallback = news_triplets._fallback_object_from_text(cleaned, article_text)
        fallback = news_triplets._clean_object_candidate(fallback)
        return fallback, "from_text"
    base = news_triplets._action_base(cleaned)
    base_len = len(base.split()) if base else 0
    words = cleaned.split()
    if len(words) <= base_len:
        return "", "from_what"
    return " ".join(words[base_len:]).strip(), "from_what"


def _article_text(article: dict) -> str:
    return news_triplets.combine_article_text(
        article,
        max_chars=None,
        max_sentences=None,
    )


def _text_snippet(text: str, max_bytes: int) -> str:
    if not text:
        return ""
    raw = text.encode("utf-8")[:max_bytes]
    return raw.decode("utf-8", "ignore")


def _load_triplets(path: Path) -> list[TripletRow]:
    payload = json.loads(path.read_text(encoding="utf-8"))
    if not isinstance(payload, list):
        raise ValueError(f"Triplets payload must be a JSON array: {path}")
    rows: list[TripletRow] = []
    for item in payload:
        if not isinstance(item, dict):
            continue
        rows.append(
            TripletRow(
                story_id=str(item.get("story_id") or ""),
                url=str(item.get("url") or ""),
                title=str(item.get("title") or ""),
                published_at=str(item.get("publishedAt") or ""),
                who=str(item.get("who") or ""),
                what=str(item.get("what") or ""),
                where_text=str(item.get("where_text") or ""),
            )
        )
    return rows


def _collect_article_map(articles_dir: Path, urls: set[str]) -> dict[str, dict]:
    article_map: dict[str, dict] = {}
    report_paths = sorted(articles_dir.glob("news_reports_*.jsonl"))
    for path in report_paths:
        with path.open("r", encoding="utf-8") as handle:
            for line in handle:
                try:
                    article = json.loads(line)
                except json.JSONDecodeError:
                    continue
                url = str(article.get("url") or "")
                source_id = str(article.get("source_id") or "")
                candidates = [value for value in (url, source_id) if value]
                if not any(candidate in urls for candidate in candidates):
                    continue
                for candidate in candidates:
                    if candidate in urls and candidate not in article_map:
                        article_map[candidate] = article
    return article_map


def _write_report(
    output_path: Path,
    output_jsonl: Path | None,
    triplets: list[TripletRow],
    article_map: dict[str, dict],
    max_bytes: int,
    limit: int | None,
) -> None:
    grouped: dict[str, list[TripletRow]] = {}
    for row in triplets:
        key = row.url or row.story_id
        grouped.setdefault(key, []).append(row)
    items = list(grouped.items())
    if limit:
        items = items[:limit]

    output_path.parent.mkdir(parents=True, exist_ok=True)
    if output_jsonl:
        output_jsonl.parent.mkdir(parents=True, exist_ok=True)
        jsonl_handle = output_jsonl.open("w", encoding="utf-8")
    else:
        jsonl_handle = None

    with output_path.open("w", encoding="utf-8") as handle:
        handle.write(f"Triplet audit report ({len(items)} articles)\n")
        handle.write("=" * 72 + "\n\n")
        for idx, (key, rows) in enumerate(items, start=1):
            article = article_map.get(key) or {}
            article_text = _article_text(article)
            snippet = _text_snippet(article_text, max_bytes=max_bytes)
            title = rows[0].title or article.get("title") or ""
            published_at = rows[0].published_at or article.get("published_at") or ""
            url = rows[0].url or article.get("url") or key

            filtered = _filter_incomplete_actions(rows)
            filtered_keys = {
                (row.story_id, row.who, row.what, row.where_text) for row in filtered
            }

            handle.write(f"[{idx}] URL: {_ascii_safe(url)}\n")
            if title:
                handle.write(f"Title: {_ascii_safe(str(title))}\n")
            if published_at:
                handle.write(f"Published: {_ascii_safe(str(published_at))}\n")
            handle.write("Text (first 1200 bytes):\n")
            handle.write(_ascii_safe(snippet) + "\n")
            handle.write("Triplets:\n")
            for row in rows:
                summary = _format_summary_line(row)
                included = (row.story_id, row.who, row.what, row.where_text) in filtered_keys
                obj, obj_source = _infer_object(row.what, article_text)
                handle.write(
                    "  - who: {who}\n"
                    "    what: {what}\n"
                    "    object: {obj}\n"
                    "    object_source: {obj_source}\n"
                    "    where: {where}\n"
                    "    summary: {summary}\n"
                    "    summary_included: {included}\n"
                    "    key: {key}\n"
                    "    Score: \n".format(
                        who=_ascii_safe(row.who),
                        what=_ascii_safe(row.what),
                        obj=_ascii_safe(obj),
                        obj_source=obj_source,
                        where=_ascii_safe(row.where_text),
                        summary=_ascii_safe(summary),
                        included="yes" if included else "no",
                        key=_ascii_safe(
                            "|".join(
                                [
                                    row.story_id or "",
                                    row.who or "",
                                    row.what or "",
                                    row.where_text or "",
                                ]
                            )
                        ),
                    )
                )
                if jsonl_handle:
                    jsonl_handle.write(
                        json.dumps(
                            {
                                "story_id": row.story_id,
                                "url": url,
                                "title": title,
                                "published_at": published_at,
                                "who": row.who,
                                "what": row.what,
                                "object": obj,
                                "object_source": obj_source,
                                "where_text": row.where_text,
                                "summary": summary,
                                "summary_included": included,
                                "text_snippet": snippet,
                            },
                            ensure_ascii=False,
                        )
                        + "\n"
                    )
            handle.write("\n" + "-" * 72 + "\n\n")
    if jsonl_handle:
        jsonl_handle.close()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Export an audit report for triplet extraction quality."
    )
    parser.add_argument(
        "--triplets",
        type=Path,
        default=Path("frontend/public/data/triplets_all.json"),
        help="Triplets slice to audit (default: frontend/public/data/triplets_all.json).",
    )
    parser.add_argument(
        "--articles-dir",
        type=Path,
        default=Path("datasets/news_ingest"),
        help="Directory containing news_reports_*.jsonl files.",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=Path("datasets/news_ingest/triplet_audit.txt"),
        help="Path for the human-readable audit report.",
    )
    parser.add_argument(
        "--output-jsonl",
        type=Path,
        default=Path("datasets/news_ingest/triplet_audit.jsonl"),
        help="Path for the machine-readable JSONL audit output.",
    )
    parser.add_argument(
        "--max-bytes",
        type=int,
        default=1200,
        help="Max bytes of article text to include.",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Limit the number of articles in the report.",
    )
    args = parser.parse_args()

    triplets = _load_triplets(args.triplets)
    urls = {row.url or row.story_id for row in triplets if row.url or row.story_id}
    article_map = _collect_article_map(args.articles_dir, urls)
    _write_report(
        output_path=args.output,
        output_jsonl=args.output_jsonl,
        triplets=triplets,
        article_map=article_map,
        max_bytes=args.max_bytes,
        limit=args.limit,
    )


if __name__ == "__main__":
    raise SystemExit(main())
