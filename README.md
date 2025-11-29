# ICEActivity

News ingestion pipeline for ICE-related activity. It gathers headlines, filters for relevance, enriches with full-text and location signals, geocodes city/facility mentions (favoring a seeded ICE facility catalog), and emits JSONL for downstream mapping plus a run log for quick QA.

## What it does
- Fetches ICE/immigration enforcement news via RSS (Reuters/NBC/ICE press), NewsAPI (optional), and GDELT.
- Applies keyword filtering and basic deduping across sources.
- Optionally downloads full article content to improve city/facility extraction.
- Geocodes locations with a priority order: facility catalog → cache → Nominatim → Google (if `GOOGLE_ACC_KEY` is set), with failure caching and query caps.
- Writes deduped JSONL snapshots, keeps a persistent story index/SQLite index, and appends a per-run CSV log (counts and geocode stats).

## Key components
- `src/services/news_ingestion.py`: Main orchestration (sources, filtering, geocoding, output, logging).
- `src/services/geocoding.py`: Geocoder with SQLite cache, Nominatim + Google fallback, failure TTL, and stats.
- `scripts/ingest_news.py`: CLI entrypoint (same as `python -m src.services.news_ingestion`).
- `assets/ice_facilities.csv`: Seeded ICE/HSI facility catalog with lat/lon, used before external geocoding.
- `scripts/geocode_facilities_google.py`: One-off script to fill missing facility lat/lon via Google Geocoding API using `GOOGLE_ACC_KEY`.
- `src/services/news_triplets.py`: LLM-powered who/what/where extraction from the latest news dump (writes triplet JSONL + SQLite index).
- `scripts/extract_triplets.py`: CLI entrypoint for triplet extraction.

## Prerequisites & setup
1. **System requirements**
   - Python 3.11+ (Linux/macOS recommended; Windows WSL works).
   - Git, curl, and build tools for Python dependencies.
   - CUDA-capable GPU + NVIDIA drivers if you plan to run the Phi-3 extractor locally.

2. **Create a virtual environment**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

3. **Environment variables** (copy `config/.env.example` → `.env` and fill in as needed)
   ```bash
   cp config/.env.example .env
   # edit .env with NEWSAPI_KEY / GOOGLE_ACC_KEY / HF_HOME overrides
   ```
   - `NEWSAPI_KEY` (optional) enables NewsAPI fetches.
   - `GOOGLE_ACC_KEY` (optional) enables Google Geocoding fallback and the facility geocoder script.
   - Hugging Face cache defaults to `~/.cache/huggingface`; override with `export HF_HOME=/path/to/cache` if desired.

4. **LLM weights**
   - No model files live in the repo. Running `scripts/extract_triplets.py` or `scripts/phi_test.py` automatically downloads `microsoft/Phi-3-mini-128k-instruct` via Hugging Face and stores it in your HF cache.
   - Ensure you have enough disk space (tens of GB) and GPU memory (or pass `--load-in-4bit` in `phi_test.py` for lower VRAM usage).

5. **Optional: facility geocoding**
   ```bash
   GOOGLE_ACC_KEY=... python scripts/geocode_facilities_google.py
   ```
   This backfills missing coordinates in `assets/ice_facilities.csv`.

## Outputs
- JSONL snapshots: `datasets/news_ingest/news_reports_<timestamp>.jsonl` (deduped per run).
- Run log: `datasets/news_ingest/run_log.csv` (counts of new articles, facility hits, geocode hits by cache/Nominatim/Google, failures).
- Story index: `datasets/news_ingest/story_index.json` (cross-run dedupe).
- SQLite index: `datasets/news_ingest/news_index.sqlite` (stories, sources, publications, locations).
- Geocode cache: `datasets/news_ingest/geocache.sqlite` (successful and failed lookups; failures cached for 7 days).

## Running the ingestor
```
source .venv/bin/activate
# optional: set env vars (NEWSAPI_KEY, GOOGLE_ACC_KEY)
PYTHONUNBUFFERED=1 python -u -m src.services.news_ingestion \
  --from-date 2025-10-27 --to-date 2025-11-26 \
  --gdelt-max-days 30 \
  --skip-newsapi \
  --fetch-content \
  --fetch-content-limit 10 \
  --geocode-max-queries 50 \
  --log-level INFO \
  --output-dir datasets/news_ingest
```

Useful flags:
- `--skip-newsapi`: avoid NewsAPI (helpful if your key is rate-limited or free-tier).
- `--fetch-content` / `--fetch-content-limit N`: pull article bodies to improve location extraction.
- `--disable-geocoding`: skip geocoding for quick dry runs.
- `--geocode-max-queries N`: cap external geocode lookups per run.
- `--include-all-rss`: pull all RSS entries (no keyword filter) for debugging.

## Triplet extraction (who/what/where)
Use the latest `news_reports_*.jsonl` dump and geocode each `where` with the shared cache:
```
python scripts/extract_triplets.py \
  --input-dir datasets/news_ingest \
  --output-dir datasets/news_ingest \
  --model-id microsoft/Phi-3-mini-128k-instruct \
  --max-new-tokens 160 \
  --repetition-penalty 1.05
```
Outputs:
- JSONL: `datasets/news_ingest/triplets_<timestamp>.jsonl`
- SQLite index: `datasets/news_ingest/triplets_index.sqlite` (one row per triplet, keyed by story_id+who+what+where)

## Environment variables
- `NEWSAPI_KEY`: optional; NewsAPI free tier only covers ~30 days.
- `GOOGLE_ACC_KEY`: optional; used as a fallback geocoder after Nominatim.

## Facility catalog
- `assets/ice_facilities.csv` is loaded first; matches on facility names or city/state resolve immediately and do not hit external geocoders.
- You can regenerate missing coords with: `python scripts/geocode_facilities_google.py` (requires `GOOGLE_ACC_KEY`).

## Caching and throttling
- Geocode failures are cached for 7 days to avoid repeated misses.
- Nominatim calls are throttled (~1.1s between uncached requests).
- Facility matches do not count against the geocode query cap.

## Logging
- Per-run CSV log: `datasets/news_ingest/run_log.csv`
  - Fields: `timestamp_iso,new_articles,facility_hits,geocode_cache_hits,geocode_nominatim_hits,geocode_google_hits,geocode_failures`
- stdout logs show fetch progress, filter counts, content fetches, geocode attempts, and resolution paths.

## Notes
- Respect Google and Nominatim terms; keep API keys out of version control.
- `.gitignore` excludes `.env`, `.venv/`, datasets, and caches.***
