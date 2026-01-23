import argparse
import json
from pathlib import Path

DEFAULT_DATA_DIR = Path("/var/www/ice-map/data")


def load_items(path: Path) -> list[dict]:
    with path.open("r", encoding="utf-8") as handle:
        return json.load(handle)


def find_matches(items: list[dict], needle: str) -> list[dict]:
    needle_lower = needle.lower()
    matches = []
    for item in items:
        story_id = str(item.get("story_id", ""))
        title = str(item.get("title", ""))
        what = str(item.get("what", ""))
        if (
            needle_lower in story_id.lower()
            or needle_lower in title.lower()
            or needle_lower in what.lower()
        ):
            matches.append(item)
    return matches


def main() -> None:
    parser = argparse.ArgumentParser(description="Inspect Nginx triplet slice JSON files.")
    parser.add_argument("needle", help="Substring to search (story_id/title/what).")
    parser.add_argument(
        "--data-dir",
        default=str(DEFAULT_DATA_DIR),
        help="Directory containing triplets_*.json files.",
    )
    parser.add_argument(
        "--labels",
        default="3d,7d,1mo,3mo,all",
        help="Comma-separated slice labels to check.",
    )
    args = parser.parse_args()

    data_dir = Path(args.data_dir)
    labels = [label.strip() for label in args.labels.split(",") if label.strip()]
    if not labels:
        raise SystemExit("No labels provided.")

    for label in labels:
        path = data_dir / f"triplets_{label}.json"
        if not path.exists():
            print(f"{label}: missing ({path})")
            continue
        items = load_items(path)
        matches = find_matches(items, args.needle)
        print(f"{label}: {len(matches)} match(es) in {path}")
        for match in matches[:3]:
            print(match)
        if len(matches) > 3:
            print(f"... {len(matches) - 3} more")


if __name__ == "__main__":
    main()
