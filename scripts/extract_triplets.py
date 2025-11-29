"""CLI entrypoint to extract who/what/where triplets from the latest news dump."""

from pathlib import Path
import sys

# Ensure repository root is on sys.path when invoked as a script.
REPO_ROOT = Path(__file__).resolve().parent.parent
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from src.services.news_triplets import main  # noqa: E402  (import after path tweak)


if __name__ == "__main__":
    raise SystemExit(main())
