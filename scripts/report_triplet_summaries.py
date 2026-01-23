#!/usr/bin/env python3
from __future__ import annotations

import argparse
import re
from collections import Counter, defaultdict
from pathlib import Path


SUMMARY_RE = re.compile(r"Summary:\s+story_id=(\S+)\s+summary=(.+)")
ACTION_RE = re.compile(r"Completing action for story_id=(\S+)\s+who=(.+?)\s+what=(.+?)\s+object=(.+)")


def classify_summary(summary: str) -> list[str]:
    flags: list[str] = []
    lowered = summary.lower()
    if " said " in lowered or lowered.startswith("said "):
        flags.append("reporting_verb")
    if re.search(r"\b(unknown|none)\b", lowered):
        flags.append("unknown_token")
    if re.search(r"\b(\w+)\s+\\1\\b", summary, flags=re.IGNORECASE):
        flags.append("dup_word")
    if re.search(r"\b(?:was|were|is|are)\\s*$", lowered):
        flags.append("ends_with_aux")
    if re.search(r"\b(?:to|for|with|into|over)$", lowered):
        flags.append("ends_with_prep")
    if len(summary.split()) <= 3:
        flags.append("too_short")
    return flags


def read_report(log_path: Path) -> tuple[list[dict[str, str]], list[dict[str, str]]]:
    summaries: list[dict[str, str]] = []
    actions_by_story: dict[str, dict[str, str]] = {}
    for line in log_path.read_text(errors="ignore").splitlines():
        action_match = ACTION_RE.search(line)
        if action_match:
            story_id, who, what, obj = action_match.groups()
            actions_by_story[story_id] = {"who": who, "what": what, "object": obj}
            continue
        summary_match = SUMMARY_RE.search(line)
        if summary_match:
            story_id, summary = summary_match.groups()
            entry = {"story_id": story_id, "summary": summary}
            entry.update(actions_by_story.get(story_id, {}))
            summaries.append(entry)
    flagged: list[dict[str, str]] = []
    for entry in summaries:
        flags = classify_summary(entry["summary"])
        if flags:
            flagged.append({**entry, "flags": ",".join(flags)})
    return summaries, flagged


def main() -> None:
    parser = argparse.ArgumentParser(description="Summarize triplet summary quality from log output.")
    parser.add_argument("log_path", type=Path, help="Path to rerun_triplets_all.sh log file.")
    parser.add_argument("--out", type=Path, default=Path("tmp/triplet_report.txt"))
    args = parser.parse_args()

    summaries, flagged = read_report(args.log_path)
    total = len(summaries)
    flagged_count = len(flagged)
    flag_counts = Counter()
    for entry in flagged:
        for flag in entry["flags"].split(","):
            flag_counts[flag] += 1

    lines: list[str] = []
    lines.append(f"Total summaries: {total}")
    lines.append(f"Flagged summaries: {flagged_count}")
    if flag_counts:
        lines.append("Flag counts:")
        for flag, count in flag_counts.most_common():
            lines.append(f"  {flag}: {count}")
    lines.append("")
    lines.append("Flagged samples:")
    for entry in flagged[:60]:
        lines.append(f"- {entry['story_id']}")
        lines.append(f"  summary: {entry['summary']}")
        if entry.get("who") or entry.get("what") or entry.get("object"):
            lines.append(
                f"  action: who={entry.get('who','')} what={entry.get('what','')} object={entry.get('object','')}"
            )
        lines.append(f"  flags: {entry['flags']}")
        lines.append("")

    args.out.parent.mkdir(parents=True, exist_ok=True)
    args.out.write_text("\n".join(lines))
    print(f"Wrote {args.out}")


if __name__ == "__main__":
    main()
