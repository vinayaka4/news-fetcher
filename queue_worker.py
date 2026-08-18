from __future__ import annotations

import argparse
import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path

import requests

from news_fetcher.core import Source, load_sources
from news_fetcher.database import initialize
from news_fetcher.db import connect, database_target
from news_fetcher.sources import fetch_source
from news_fetcher.sources.pib import hydrate_missing, refresh_existing_audits
from news_fetcher.job_queue import claim_next, complete, enqueue, fail, initialize_jobs
from news_fetcher.raw_store import IST
from news_fetcher.sources.perplexity import load_local_env, request_digest, store_stories
from news_fetcher.sources.rss import SourceAccessDeferred
from news_fetcher.consolidation import (ConsolidationError, fetch_daily_snapshot,
                                        request_ai_groups, store_consolidation)
from news_fetcher.api import build_all_sources_snapshot

ROOT = Path(__file__).resolve().parent


class UpstreamNotReady(ConsolidationError):
    """Retryable wait state that must not fail the ingestion workflow."""


def default_target_date(now: datetime | None = None) -> str:
    return ((now or datetime.now(IST)).astimezone(IST) - timedelta(days=1)).date().isoformat()


def enqueue_daily(connection, target_date: str) -> int:
    sources = load_sources(ROOT / "feeds.json")
    requested = {v.strip() for v in os.getenv(
        "NEWS_ENABLED_SOURCES", "pib,indian_express_india,indian_express_upsc,"
        "times_of_india_top,times_of_india_india").split(",") if v.strip()}
    count = 0
    for source in sources:
        if source.key not in requested:
            continue
        enqueue(connection, job_key=f"source:{source.key}:{target_date}", job_type="fetch_source",
                source_key=source.key, payload=source.__dict__)
        count += 1
    perplexity_job = enqueue(connection, job_key=f"perplexity:{target_date}", job_type="fetch_perplexity",
            source_key="perplexity", payload={"date": target_date,
            "model": os.getenv("PERPLEXITY_MODEL", "sonar")})
    consolidation_job = enqueue(connection, job_key=f"consolidate:{target_date}", job_type="consolidate_news",
            source_key="consolidated", payload={"date": target_date,
            "model": os.getenv("CONSOLIDATION_MODEL", os.getenv("PERPLEXITY_MODEL", "sonar")),
            "api_base_url": os.getenv("NEWS_API_BASE_URL", "http://127.0.0.1:5000")})
    timestamp = datetime.now(timezone.utc).isoformat()
    if not connection.execute("""SELECT 1 FROM digest_stories
      WHERE substr(published_at,1,10)=? LIMIT 1""", (target_date,)).fetchone():
        connection.execute("""UPDATE ingestion_jobs SET status='pending',attempts=0,
          next_attempt_at=?,completed_at=NULL,last_error=NULL,updated_at=?
          WHERE id=? AND status='complete'""", (timestamp, timestamp, perplexity_job))
    if not connection.execute("""SELECT 1 FROM consolidation_runs
      WHERE publication_date=? AND status='complete' LIMIT 1""", (target_date,)).fetchone():
        connection.execute("""UPDATE ingestion_jobs SET status='pending',attempts=0,
          next_attempt_at=?,completed_at=NULL,last_error=NULL,updated_at=?
          WHERE id=? AND status='complete'""", (timestamp, timestamp, consolidation_job))
    connection.commit()
    return count + 2


def hydrate_missing_pib(connection) -> tuple[int, int]:
    saved, failed_count = hydrate_missing(connection)
    refresh_existing_audits(connection)
    return saved, failed_count


def execute(connection, job) -> None:
    payload = json.loads(job["payload_json"])
    if job["job_type"] == "fetch_source":
        source = Source(**payload)
        fetch_source(connection, source, os.getenv("NEWS_USER_AGENT", "UPSCNewsFetcher/1.0"))
    elif job["job_type"] == "fetch_perplexity":
        api_key = os.getenv("PERPLEXITY_API_KEY")
        if not api_key:
            raise RuntimeError("PERPLEXITY_API_KEY is not set")
        stories, _ = request_digest(api_key, payload["date"], payload["model"])
        if not stories:
            raise UpstreamNotReady("Perplexity returned no valid stories for the requested date")
        store_stories(connection, stories)
    elif job["job_type"] == "consolidate_news":
        api_key = os.getenv("PERPLEXITY_API_KEY")
        if not api_key:
            raise RuntimeError("PERPLEXITY_API_KEY is not set")
        try:
            snapshot = fetch_daily_snapshot(payload["api_base_url"], payload["date"])
        except requests.RequestException as api_error:
            try:
                snapshot = build_all_sources_snapshot(connection, payload["date"], limit=200,
                                                      include_pdf_content=True, compact=True)
                print(f"source API unavailable; using database snapshot: {type(api_error).__name__}",
                      flush=True)
            except Exception as database_error:
                raise UpstreamNotReady(
                    "Daily source API and database snapshot are temporarily unavailable") from database_error
        required = {item.strip() for item in os.getenv(
            "CONSOLIDATION_REQUIRED_SOURCES", "pib,rss,perplexity").split(",") if item.strip()}
        unavailable = sorted(source for source in required
                             if snapshot.get("source_statuses", {}).get(source) != "ready")
        if unavailable:
            raise UpstreamNotReady(f"Required source groups are not ready: {', '.join(unavailable)}")
        store_consolidation(connection, payload["date"], snapshot, payload["model"],
            lambda candidates: request_ai_groups(api_key, candidates, payload["model"]))
    else:
        raise RuntimeError(f"Unknown job type: {job['job_type']}")


def drain(database: Path, maximum: int) -> tuple[int, int, int]:
    succeeded = failed_count = deferred_count = 0
    with connect(database) as connection:
        initialize(connection)
        while maximum <= 0 or succeeded + failed_count + deferred_count < maximum:
            job = claim_next(connection)
            if not job:
                break
            try:
                execute(connection, job)
                complete(connection, job["id"]); succeeded += 1
                print(f"complete {job['job_type']} {job['job_key']}", flush=True)
            except (UpstreamNotReady, SourceAccessDeferred) as error:
                connection.rollback()
                fail(connection, job["id"], f"{type(error).__name__}: {error}")
                deferred_count += 1
                print(f"deferred {job['job_type']} {job['job_key']}: {error}", flush=True)
            except Exception as error:
                # PostgreSQL rejects every command after a statement error until
                # the transaction is rolled back. Roll back partial source work
                # before recording the durable retry state.
                connection.rollback()
                fail(connection, job["id"], f"{type(error).__name__}: {error}"); failed_count += 1
                print(f"retry {job['job_type']} {job['job_key']}: {error}", flush=True)
    return succeeded, failed_count, deferred_count


def main() -> int:
    load_local_env()
    parser = argparse.ArgumentParser(description="Durable ingestion and content worker")
    parser.add_argument("--database", default=database_target())
    parser.add_argument("--enqueue-daily", action="store_true")
    parser.add_argument("--date", default=default_target_date(),
                        help="Publication date; defaults to the completed previous IST day")
    parser.add_argument("--hydrate-missing-pib", "--enqueue-missing-pib", action="store_true",
                        help="Fetch PIB rows whose full_text is empty directly from the database")
    parser.add_argument("--drain", action="store_true")
    parser.add_argument("--max-jobs", type=int, default=0)
    args = parser.parse_args()
    hydration_failures = 0
    with connect(args.database) as connection:
        initialize(connection)
        if args.enqueue_daily:
            connection.execute("""INSERT INTO pipeline_runs
              (run_date_ist,started_at,status,message) VALUES (?,?,'running',NULL)
              ON CONFLICT(run_date_ist) DO UPDATE SET started_at=excluded.started_at,
              completed_at=NULL,status='running',message=NULL""",
              (args.date, datetime.now().astimezone().isoformat()))
            connection.commit()
            print(f"queued {enqueue_daily(connection, args.date)} daily jobs")
        if args.hydrate_missing_pib:
            saved, hydration_failures = hydrate_missing_pib(connection)
            print(f"PIB database hydration: saved={saved}, still_missing={hydration_failures}")
    retried = deferred = 0
    if args.drain:
        succeeded, retried, deferred = drain(args.database, args.max_jobs)
        print(f"worker complete: succeeded={succeeded}, scheduled_for_retry={retried}, "
              f"waiting_for_sources={deferred}")
    exit_code = 1 if (retried or hydration_failures) else 0
    if args.enqueue_daily:
        with connect(args.database) as connection:
            initialize(connection)
            connection.execute("""UPDATE pipeline_runs SET completed_at=?,status=?,message=?
              WHERE run_date_ist=?""", (datetime.now().astimezone().isoformat(),
              "failed" if exit_code else ("partial" if deferred else "complete"),
              f"retry_jobs={retried},deferred_jobs={deferred},missing_pib={hydration_failures}", args.date))
            connection.commit()
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
