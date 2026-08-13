# News ingestion architecture

## Data flow

```text
PIB official pages ─┐
RSS feeds ──────────┼─> source adapters ─> raw JSON envelope ─> raw_events
Perplexity API ─────┤                              │
PDF uploads ────────┘                              ├─> normalized tables
                                                   └─> date JSON API
```

Raw ingestion is the system of record. The existing `articles` and
`digest_stories` tables are projections used by the dashboard. Reprocessing can
build new projections from raw records without fetching the source again.

## Raw JSON contract

```json
{
  "schema_version": "1.0",
  "source": {
    "type": "rss",
    "key": "times_of_india_india",
    "publisher": "Times of India"
  },
  "identity": {
    "external_id": "https://example.com/article"
  },
  "timestamps": {
    "published_at": "2026-08-09T01:00:00+00:00",
    "fetched_at": "2026-08-09T02:00:00+00:00",
    "event_date": "2026-08-09"
  },
  "metadata": {
    "feed_url": "https://example.com/feed.xml"
  },
  "payload": {
    "title": "Raw source fields are retained here"
  }
}
```

The content hash and `(source_key, external_id, content_hash)` constraint make
repeated ingestion idempotent while preserving a new event when source content
changes.

## HTTP API

Start `dashboard.py`, then query:

```text
GET /api/v1/news?date=2026-08-09
GET /api/v1/news?date=2026-08-09&source=perplexity&limit=100
GET /api/v1/sources
POST /api/v1/uploads/pdf
```

`GET /api/v1/news` accepts a maximum page size of 500 and returns
`next_cursor` when another page is available.

PDF upload example:

```powershell
curl.exe -X POST http://127.0.0.1:5000/api/v1/uploads/pdf `
  -F "file=@C:\path\paper.pdf" `
  -F "date=2026-08-09" `
  -F "source=The Hindu e-paper"
```

PDF uploads are extracted block-by-block with PyMuPDF and stored as a versioned
hierarchy: newspaper issue -> section -> article -> paragraphs. Original
page/block coordinates remain in `source_blocks` for traceability. Retrieve the
structured projection with:

```text
GET /api/v1/uploads/pdf?date=2026-08-13
GET /api/v1/uploads/pdf/{document_id}
```

Add `-F "edition=Final Home"` when the edition is known. Digital-text PDFs are
processed immediately. Image-only or scanned PDFs return `ocr_required: true`;
OCR and AI section classification remain explicit later stages so uncertain
text is not silently invented or assigned to the wrong article.

PDFs are limited to 25 MB. The original file is stored under `uploads/YYYY/MM`,
while extracted page text and metadata are stored as a raw JSON event. Image-only
PDFs produce empty text unless an OCR adapter is added later.

## Scheduling

Windows Task Scheduler starts `scheduled_run.py` at both possible Eastern-time
equivalents of 04:00 IST. The script checks the current IST hour and the
`pipeline_runs` table, so exactly one run proceeds per IST date. Failed runs can
retry at the alternate trigger only when it is still 04:00 IST.

## Production scaling

The local deployment uses SQLite and local files. A production deployment should
replace them behind the same interfaces:

- PostgreSQL for `raw_events`, projections, and pipeline-run coordination.
- JSONB for `raw_json`, with indexes on date, source, and selected payload keys.
- S3/R2-compatible object storage for PDF bytes.
- A queue for independent source jobs and PDF extraction workers.
- Advisory locks or a distributed scheduler to guarantee one daily run.
- API authentication, tenant ownership, rate limits, retention policies, and
  publisher-specific licensing controls before external access.
# Source adapters

Ingestion behavior is separated by responsibility:

```text
news_fetcher/
  core.py                 shared Source model and text/URL helpers
  database.py             normalized database schema initialization
  sources/
    pib.py                 PIB listing, detail parsing, full-text hydration
    rss.py                 RSS download, parsing, metadata normalization
    perplexity.py          prompt, API validation, structured digest storage
    pdf_reader.py          PDF validation, extraction, and upload persistence
  cli.py                   thin source dispatcher and command-line interface
  api.py                   HTTP transport only
  job_queue.py             durable job persistence
queue_worker.py            invokes source adapters from durable jobs
```

Top-level scripts such as `fetch_news.py` and `perplexity_ingest.py` remain
stable compatibility entry points. New source-specific behavior belongs in its
corresponding `news_fetcher/sources/` module rather than in the CLI, API, or
queue worker.
