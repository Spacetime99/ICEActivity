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
- Chat summaries: `docs/chat-YYYYMMDD.md`.

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
- `--memory-percent 0.8`: cap the HF model to 80% of GPU RAM (default 90%) to leave a safety buffer for extra passes.
- `--low-memory`: shorter inputs/tokens and skip the LLM location fallback so the extractor fits comfortably on 32 GB cards.
- `--skip-llm-location`: disable the helper location extraction entirely and rely on text-based cues.
- `--quantize 4bit`: load the extractor in quantized/4-bit mode (Phi-3 or Phi-4) for lower VRAM usage.

Outputs:
- JSONL: `datasets/news_ingest/triplets_<timestamp>.jsonl`
- SQLite index: `datasets/news_ingest/triplets_index.sqlite` (one row per triplet, keyed by story_id+who+what+where)

Event type detection notes:
- The triplet extractor tags protest-related event types by keyword scanning.
- "March" is treated as a calendar month (not a protest) when it is capitalized and
  appears mid-sentence, or when it starts a sentence and is followed immediately by
  a 2- or 4-digit year (1960-2050 or 60-50). Action forms such as "marching" or
  "marchers" still count as protest activity.
- Avoid outlet-specific or story-specific hardcoded checks; prefer generic, reusable rules.

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

## Developer helpers
- `scripts/run_pytest.sh` / `scripts/list_tests.sh`: activate the venv, ensure pytest is installed, and run/collect the requested tests.
- `scripts/find_location_gaps.py`: scan the latest ingest dump for articles with location hints but no coordinates.
- `scripts/run_find_triplet_gaps.sh`: wrap the above to also check `triplets_index.sqlite` for missing or un-geocoded triplets after extraction.
- `scripts/run_low_memory_pipeline.sh`: run the full ingest→extract→export pipeline with the low-memory Phi-3/4 extractor flags (`--low-memory --skip-llm-location --memory-percent 0.8 --quantize 4bit`).

## Static Triplet Slices & Map Frontend

### Static JSON slices (no API)
The map now reads from static JSON slices so it can be deployed as a fully static site.
Slices live under `frontend/public/data` and are generated from the triplets SQLite index:

- `triplets_3d.json`
- `triplets_7d.json`
- `triplets_1mo.json`
- `triplets_3mo.json`
- `triplets_all.json`

Regenerate them with:
```bash
python scripts/export_triplets_static.py
```

By default the frontend fetches from `/data` (resolved via `VITE_STATIC_DATA_BASE_URL`,
falling back to `<origin>/<base>/data`).

### React + Leaflet frontend
A lightweight Vite/React map client lives under `frontend/`.

Setup & run (base path defaults to `/ice/` for production builds):
```bash
cd frontend
npm install
npm run dev        # http://localhost:3000 for local work
npm run build      # outputs to frontend/dist (served at /ice)
```

The frontend fetches map data from the static data base URL (`VITE_STATIC_DATA_BASE_URL`).
You can override it via:
```bash
VITE_STATIC_DATA_BASE_URL=https://cdn.example.com/data npm run dev
```

The UI provides 6h/24h/3d/7d filters, groups triplets by rounded coordinates, colors markers by recency, and links back to the source articles.

## Recent updates
- Stats layout polish (month ticks, y-axis labels, captions, subtitle sizing) in `frontend/src/StatsPage.tsx` and `frontend/src/stats.css`.
- New coverage cards (protest mix, child mentions, U.S. status, red/blue coverage, top states) in `frontend/src/StatsPage.tsx` with copy updates in `frontend/src/i18n.ts`.
- Centralized child/U.S. status detection patterns, expanded to cover naturalization/birthright phrasing, in `frontend/src/mentionPatterns.ts`.
- Added child and U.S. status badges in headlines/stats cards in `frontend/src/HeadlinesPage.tsx` and `frontend/src/StatsPage.tsx`.
- Weekly ratios zero out low-volume weeks (fewer than 5 articles) in `frontend/src/StatsPage.tsx`.

## Environment variables
- `NEWSAPI_KEY`: optional; NewsAPI free tier only covers ~30 days.
- `GOOGLE_ACC_KEY`: optional; used as a fallback geocoder after Nominatim.
- `NOTIFY_EMAIL`: recipient for run summary emails (defaults to `jon.skyclad@gmail.com` in `scripts/run_ingest_and_extract.sh`).
- `NOTIFY_FROM`: sender address for run summary emails (defaults to `SMTP_USER`).
- `SMTP_HOST`: SMTP host for email notifications (Gmail: `smtp.gmail.com`).
- `SMTP_PORT`: SMTP port (Gmail STARTTLS: `587`).
- `SMTP_USER`: SMTP username (Gmail address).
- `SMTP_PASSWORD`: SMTP password (use a Gmail app password).
- `SMTP_PASSWORD_FILE`: optional file path for SMTP password (e.g., `config/gm_app_pw.txt`).
- `SMTP_STARTTLS`: set to `1` to use STARTTLS.
- `SMTP_TLS`: set to `1` to use SMTP over SSL/TLS (usually port `465`).

Email notifications are best-effort: the script uses `sendmail` if available, otherwise SMTP.
The scheduled pipeline email from `scripts/run_ingest_and_extract.sh` includes both:
- step health as `success (exit 0)` / `failure (exit N)` for ingest, extraction, export, deaths update, and upload
- data volumes (for example `ingest_reports_written`, `triplet_articles_processed`, `triplets_extracted`, `triplet_file_rows`, and output file paths)

This avoids confusion between Unix exit codes (`0` = success) and record totals.
To test delivery:
```bash
SMTP_HOST=smtp.gmail.com \
SMTP_PORT=587 \
SMTP_STARTTLS=1 \
SMTP_USER="your_gmail@gmail.com" \
SMTP_PASSWORD_FILE=config/gm_app_pw.txt \
python3 scripts/notify_run.py \
  --to jon.skyclad@gmail.com \
  --from your_gmail@gmail.com \
  --subject "ICEActivity test" \
  --body "Test email from notify_run.py"
```

## Facility catalog
- `assets/ice_facilities.csv` is loaded first; matches on facility names or city/state resolve immediately and do not hit external geocoders.
- You can regenerate missing coords with: `python scripts/geocode_facilities_google.py` (requires `GOOGLE_ACC_KEY`).

## Caching and throttling
- Geocode failures are cached for 7 days to avoid repeated misses.
- Nominatim calls are throttled (~1.1s between uncached requests).
- Facility matches do not count against the geocode query cap.

## Logging
- Per-run CSV log: `datasets/news_ingest/run_log.csv`
  - Fields: `timestamp_iso,new_articles,facility_hits,geocode_cache_hits,geocode_nominatim_hits,geocode_google_hits,geocode_failures,fetch_failures`
- stdout logs show fetch progress, filter counts, content fetches, geocode attempts, and resolution paths.

## Notes
- Respect Google and Nominatim terms; keep API keys out of version control.
- `.gitignore` excludes `.env`, `.venv/`, datasets, and caches.***
- **Workflow note:** Please put runnable commands/scripts in files inside the repo. I can't reliably copy/paste code from chat output.
