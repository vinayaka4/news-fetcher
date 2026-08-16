# India news metadata fetcher

An RSS-first collector for PIB and approved newspaper feeds. It stores headlines,
links, dates, authors and RSS excerpts in SQLite locally or PostgreSQL in
production; it does **not** scrape full
articles, bypass paywalls, or republish publisher content.

The system now also preserves immutable raw JSON envelopes, exposes a paginated
date API, and accepts manual PDF uploads. See `ARCHITECTURE.md` for the JSON
contract, endpoints, scheduler, and production migration path.

Run the complete success and failure-path suite with:

```powershell
.venv\Scripts\python -m pytest -q
```

The suite covers RSS, PIB partial failures, Perplexity API errors, raw-store
idempotency and versioning, API validation, PDF corruption/duplicates, and
scheduler failure-state persistence.

## Quick start

```powershell
python -m venv .venv
.venv\Scripts\pip install -r requirements.txt
.venv\Scripts\python fetch_news.py
```

PIB is enabled by default. List available sources:

```powershell
.venv\Scripts\python fetch_news.py --list-sources
```

## Dashboard

Start the local dashboard after fetching articles:

```powershell
.venv\Scripts\python dashboard.py
```

Open `http://127.0.0.1:5000`. The dashboard supports text search, publisher
and ministry filters, and ascending or descending publication dates.

## Perplexity daily UPSC digest

```powershell
$env:PERPLEXITY_API_KEY='your-key-here'
.venv\Scripts\python perplexity_ingest.py
```

Use `--date 2026-08-02` to backfill an IST date. The command stores structured
summaries, UPSC classifications, dates, and citations—not full articles.
`PERPLEXITY_MODEL` defaults to `sonar` and can be changed to `sonar-pro`.

For personal/non-commercial testing, explicitly opt into newspaper feeds only
after reviewing their current terms:

```powershell
$env:NEWS_ENABLED_SOURCES='pib,indian_express_india,times_of_india_india'
.venv\Scripts\python fetch_news.py
```

Repeated runs are safe: canonical URLs and fuzzy title matching suppress exact
and cross-publisher duplicates. Inspect data with:

```sql
SELECT publisher, title, article_url, published_at
FROM articles ORDER BY published_at DESC;
```

## The Hindu and Deccan Herald

They are intentionally not assigned guessed or scraped endpoints. Add an entry
to `feeds.json` only after obtaining a current publisher-approved RSS/API or a
syndication licence. Deccan Herald's published terms prohibit archiving its feed;
The Hindu should likewise be confirmed directly before automated storage.

For a commercial learning platform, obtain publisher licences and replace SQLite
with durable PostgreSQL storage. Generate your own short summaries from content
you are licensed to process; retain attribution and source links.

## Scheduling

The included GitHub Actions workflow runs at 22:00 IST. Its SQLite file is
ephemeral, so connect durable storage before deployment. On Windows Task
Scheduler, run `python C:\path\to\fetch_news.py` daily from this directory.

For this local dashboard, register the complete RSS + PIB + Perplexity pipeline:

```powershell
powershell -ExecutionPolicy Bypass -File .\register_daily_task.ps1
```

It runs every day at 04:00 IST and writes operational logs under `logs`. The
computer must be on. The scheduled RSS job uses PIB and Times of India feeds. Indian
Express and The Hindu discovery and summaries are handled through Perplexity
citations. Full text is fetched only for official PIB releases.
## Consumer-ready article API

Use the normalized endpoint when another service needs article content:

```text
GET /api/v1/articles?date=2026-08-09&source=pib&content_status=ready&limit=100
```

Each item includes `publisher`, `source_key`, `title`, `article_url`,
`published_at`, `ministry`, `excerpt`, `full_text`, `content_status`, and
`fetched_at`. `content_status` accepts `ready`, `pending`, or `all`. Follow
`next_cursor` until it is `null` to retrieve every page. The existing
`/api/v1/news` endpoint remains the immutable raw-ingestion JSON API.

Perplexity structured summaries are available from:

```text
GET /api/v1/perplexity?date=2026-08-09&limit=100
GET /api/v1/rss?date=2026-08-09&limit=100
```

`/api/v1/digests` remains an alias for the Perplexity endpoint. A single
date-filtered snapshot containing PIB, RSS, Perplexity, and uploaded PDFs is:

```text
GET /api/v1/all?date=2026-08-09&limit=100
```

The combined response keeps each source under `sources`, reports per-source
counts and statuses, and sets top-level `complete` only when all four source
groups are ready. PDF records contain a `structured_url` by default; add
`include_pdf_content=true` only when the caller needs the full newspaper JSON.

## AI consolidation service

Consolidate duplicate coverage from the four-source API without changing any
raw or normalized source records:

```powershell
python consolidate_news.py --date 2026-08-09 --api-base-url http://127.0.0.1:5000
```

The service performs deterministic pre-clustering, asks the configured AI model
to merge semantic duplicates, rejects responses that omit or duplicate an input,
and atomically writes versioned rows to `consolidation_runs` and
`consolidated_stories`. Identical input is idempotent. Read the latest completed
version for a date from:

```text
GET /api/v1/consolidated?date=2026-08-09
```

Each consolidated story contains its source record IDs and compact source
citations. The original source data remains in `raw_events`, `articles`,
`digest_stories`, and the uploaded PDF records.

Pipeline health is available from `GET /api/v1/pipeline/status`. RSS and
Perplexity source jobs survive restarts and retry with exponential backoff.
PIB uses empty `articles.full_text` rows directly as its durable work list.
Run or resume the pipeline manually with:

```powershell
python queue_worker.py --enqueue-daily --hydrate-missing-pib --drain
```

Verify the official PIB count gate for a date with:

```text
GET /api/v1/pib/completeness?date=2026-08-11
```

Consumers should use that date only when `complete` is true, meaning
`expected_count == discovered_count == ready_count`, `missing_count == 0`, and
the latest discovery/listing health checks are verified and fresh.

For production deployment, `render.yaml` provisions the API, PostgreSQL, and
scheduled ingestion/recovery jobs. Follow [RENDER_DEPLOYMENT.md](RENDER_DEPLOYMENT.md).
Once deployed, opening `/api/v1/pib` directly in a browser returns the latest PIB
data as JSON; add `?date=YYYY-MM-DD` for a specific publication date.
