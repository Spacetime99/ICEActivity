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
- `scripts/phi_test.py`: Lightweight developer harness to sanity-check Phi-3 prompts (no shared state).

The pattern is: every runnable CLI under `scripts/` just adds the repo root to `sys.path` and calls into the corresponding module in `src/services/`. That keeps business logic reusable while keeping the developer workflows simple (e.g., `python scripts/ingest_news.py` vs. `python -m src.services.news_ingestion`).

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
Helpful switches:
- `--input-file datasets/news_ingest/news_reports_20260110T030329Z.jsonl`: re-run a specific dump without touching the rest.
- `--process-all-dumps`: walk every `news_reports_*.jsonl` (oldest → newest) so you can rebuild the entire backlog after fixing GPU memory.

Outputs:
- JSONL: `datasets/news_ingest/triplets_<timestamp>.jsonl`
- SQLite index: `datasets/news_ingest/triplets_index.sqlite` (one row per triplet, keyed by story_id+who+what+where)

To backfill historical triplets (e.g., after moving machines), first copy the archived
`triplets_*.jsonl` files into `datasets/news_ingest`, then hydrate them into the SQLite index
without re-running the LLM:
```
python scripts/extract_triplets.py \
  --input-dir datasets/news_ingest \
  --output-dir datasets/news_ingest \
  --hydrate-existing
```

## Scheduling the pipeline
The helper script `scripts/run_ingest_and_extract.sh` chains a full ingest + triplet extraction run, activates the repo virtualenv if needed, and appends logs to `logs/ingest.log`. You can wire it up to cron or systemd:

**cron (hourly example)**
```
0 * * * * cd /home/spacetime/codex && ./scripts/run_ingest_and_extract.sh >> logs/cron.log 2>&1
```

**systemd timer**
1. Create `/etc/systemd/system/icepipeline.service`:
   ```
   [Unit]
   Description=ICEActivity ingest + triplet extraction
   After=network.target

   [Service]
   Type=oneshot
   WorkingDirectory=/home/spacetime/codex
   ExecStart=/home/spacetime/codex/scripts/run_ingest_and_extract.sh
   ```
2. Create `/etc/systemd/system/icepipeline.timer`:
   ```
   [Unit]
   Description=Run ICEActivity pipeline hourly

   [Timer]
   OnCalendar=hourly
   Persistent=true

   [Install]
   WantedBy=timers.target
   ```
3. Enable and start:
   ```
   sudo systemctl enable --now icepipeline.timer
   ```
The script recreates the SQLite caches and JSONL outputs automatically if they’re missing, so new environments can just schedule the timer and let the pipeline bootstrap itself.

## Triplet API & Map Frontend

### FastAPI backend
The FastAPI service lives in `src/api/main.py` and reads directly from `datasets/news_ingest/triplets_index.sqlite`. It exposes:

- `GET /health` → `{"status":"ok"}`
- `GET /api/triplets?since_hours=24&bbox=west,south,east,north` → JSON array of triplets (lat/lon required, ordered by `published_at` desc, max 2000 rows).

Run it locally with:
```bash
source .venv/bin/activate
uvicorn src.api.main:app --reload --host 127.0.0.1 --port 5000
# or use the helper script (honors $PORT/$HOST):
./scripts/run_icemap_api.sh
```

Example request (last 6 hours, no bounding box):
```bash
curl "http://localhost:5000/api/triplets?since_hours=6"
```

### React + Leaflet frontend
A lightweight Vite/React map client lives under `frontend/`.

Setup & run (base path defaults to `/ice/` for production builds):
```bash
cd frontend
npm install
npm run dev        # http://localhost:3000 for local work
npm run build      # outputs to frontend/dist (served at /ice)
```

The frontend fetches map data from the FastAPI base URL (`VITE_API_BASE_URL`, defaults to `http://localhost:5000`). You can override it via:
```bash
VITE_API_BASE_URL=https://api.example.com npm run dev
```

The UI provides 6h/24h/3d/7d filters, groups triplets by rounded coordinates, colors markers by recency, and links back to the source articles.

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
