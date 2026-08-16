from __future__ import annotations

import sqlite3
from datetime import datetime

from news_fetcher.db import backend
from news_fetcher.raw_store import initialize_raw_store
from news_fetcher.raw_store import IST


def initialize(connection: sqlite3.Connection) -> None:
    # PostgreSQL DDL takes relation locks. Serialize schema initialization so a
    # deploy/migration cannot deadlock an ingestion worker starting at the same
    # time. This transaction-scoped lock is automatically released on commit.
    if backend(connection) == "postgres":
        connection.execute("SELECT pg_advisory_xact_lock(731946201)")
    connection.executescript("""
        CREATE TABLE IF NOT EXISTS articles (
            id TEXT PRIMARY KEY, publisher TEXT NOT NULL, source_key TEXT NOT NULL,
            title TEXT NOT NULL, normalized_title TEXT NOT NULL,
            article_url TEXT NOT NULL UNIQUE, published_at TEXT, author TEXT,
            excerpt TEXT, ministry TEXT, full_text TEXT, fetched_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_articles_published ON articles(published_at);
        CREATE INDEX IF NOT EXISTS idx_articles_title ON articles(normalized_title);
        CREATE TABLE IF NOT EXISTS digest_stories (
            id TEXT PRIMARY KEY, headline TEXT NOT NULL, normalized_title TEXT NOT NULL,
            category TEXT NOT NULL, summary_json TEXT NOT NULL,
            upsc_relevance TEXT NOT NULL, source_urls_json TEXT NOT NULL,
            published_at TEXT NOT NULL, details_json TEXT, fetched_at TEXT NOT NULL
        );
        CREATE INDEX IF NOT EXISTS idx_digest_stories_date ON digest_stories(published_at);
        CREATE INDEX IF NOT EXISTS idx_digest_stories_category ON digest_stories(category);
        CREATE INDEX IF NOT EXISTS idx_digest_stories_upsc ON digest_stories(upsc_relevance);
        CREATE TABLE IF NOT EXISTS consolidation_runs (
            id TEXT PRIMARY KEY, publication_date TEXT NOT NULL,
            input_hash TEXT NOT NULL, input_count INTEGER NOT NULL,
            output_count INTEGER, model TEXT NOT NULL, prompt_version TEXT NOT NULL,
            status TEXT NOT NULL, source_snapshot_json TEXT NOT NULL,
            started_at TEXT NOT NULL, completed_at TEXT, error TEXT,
            UNIQUE(publication_date,input_hash,model,prompt_version)
        );
        CREATE INDEX IF NOT EXISTS idx_consolidation_runs_date_status
          ON consolidation_runs(publication_date,status,completed_at);
        CREATE TABLE IF NOT EXISTS consolidated_stories (
            id TEXT PRIMARY KEY, run_id TEXT NOT NULL, publication_date TEXT NOT NULL,
            title TEXT NOT NULL, category TEXT NOT NULL, summary_json TEXT NOT NULL,
            key_facts_json TEXT NOT NULL, source_refs_json TEXT NOT NULL,
            source_count INTEGER NOT NULL, created_at TEXT NOT NULL,
            FOREIGN KEY(run_id) REFERENCES consolidation_runs(id)
        );
        CREATE INDEX IF NOT EXISTS idx_consolidated_stories_date
          ON consolidated_stories(publication_date,run_id);
        CREATE TABLE IF NOT EXISTS pib_ingestion_runs (
            publication_date TEXT PRIMARY KEY,
            expected_count INTEGER NOT NULL,
            discovered_count INTEGER NOT NULL,
            ready_count INTEGER NOT NULL,
            missing_count INTEGER NOT NULL,
            complete INTEGER NOT NULL,
            listing_fetched_at TEXT NOT NULL,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS pib_source_health (
            source_key TEXT PRIMARY KEY,
            discovery_status TEXT NOT NULL,
            last_discovery_attempt_at TEXT,
            last_successful_discovery_at TEXT,
            listing_http_status INTEGER,
            listing_error TEXT,
            consecutive_discovery_failures INTEGER NOT NULL DEFAULT 0,
            listing_parse_healthy INTEGER NOT NULL DEFAULT 0,
            circuit_breaker_state TEXT NOT NULL DEFAULT 'closed',
            consecutive_structural_failures INTEGER NOT NULL DEFAULT 0,
            last_hydration_success_at TEXT,
            updated_at TEXT NOT NULL
        );
        CREATE TABLE IF NOT EXISTS pib_flags (
            id TEXT PRIMARY KEY,
            flag_type TEXT NOT NULL,
            severity TEXT NOT NULL,
            source TEXT NOT NULL,
            publication_date TEXT,
            prid TEXT,
            first_seen_at TEXT NOT NULL,
            last_seen_at TEXT NOT NULL,
            resolved_at TEXT,
            message TEXT NOT NULL,
            metadata_json TEXT NOT NULL,
            UNIQUE(flag_type, source, publication_date, prid)
        );
        CREATE INDEX IF NOT EXISTS idx_pib_flags_active ON pib_flags(resolved_at, severity);
        CREATE UNIQUE INDEX IF NOT EXISTS uq_pib_flags_identity
          ON pib_flags(flag_type, source, COALESCE(publication_date,''), COALESCE(prid,''));
    """)
    if backend(connection) == "postgres":
        columns = {row[0] for row in connection.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='articles'")}
    else:
        columns = {row[1] for row in connection.execute("PRAGMA table_info(articles)")}
    if "ministry" not in columns:
        connection.execute("ALTER TABLE articles ADD COLUMN ministry TEXT")
    if "full_text" not in columns:
        connection.execute("ALTER TABLE articles ADD COLUMN full_text TEXT")
    if "content_attempts" not in columns:
        connection.execute("ALTER TABLE articles ADD COLUMN content_attempts INTEGER NOT NULL DEFAULT 0")
    if "last_content_attempt_at" not in columns:
        connection.execute("ALTER TABLE articles ADD COLUMN last_content_attempt_at TEXT")
    if "content_last_error" not in columns:
        connection.execute("ALTER TABLE articles ADD COLUMN content_last_error TEXT")
    # Versions before 2026-08-16 normalized RSS timestamps to UTC while the
    # scheduler and API dates represent IST publication days. Convert those
    # existing rows once; timestamps already carrying +05:30 are untouched.
    rss_timestamp_rows = connection.execute("""SELECT id,published_at FROM articles
      WHERE source_key!='pib' AND (published_at LIKE ? OR published_at LIKE ?)""",
      ("%+00:00", "%Z")).fetchall()
    for row in rss_timestamp_rows:
        if not str(row[1]).endswith(("+00:00", "Z")):
            continue
        try:
            converted = datetime.fromisoformat(row[1].replace("Z", "+00:00")).astimezone(IST).isoformat()
        except (AttributeError, TypeError, ValueError):
            continue
        connection.execute("UPDATE articles SET published_at=? WHERE id=?", (converted, row[0]))
    audit_columns = ({row[0] for row in connection.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name='pib_ingestion_runs'")}
        if backend(connection) == "postgres" else
        {row[1] for row in connection.execute("PRAGMA table_info(pib_ingestion_runs)")})
    audit_migrated = False
    for name, definition in (
        ("discovery_verified", "INTEGER NOT NULL DEFAULT 0"),
        ("listing_parse_healthy", "INTEGER NOT NULL DEFAULT 0"),
        ("source_fresh", "INTEGER NOT NULL DEFAULT 0"),
        ("flags_json", "TEXT NOT NULL DEFAULT '[]'")):
        if name not in audit_columns:
            connection.execute(f"ALTER TABLE pib_ingestion_runs ADD COLUMN {name} {definition}")
            audit_migrated = True
    if audit_migrated:
        connection.execute("UPDATE pib_ingestion_runs SET complete=0")
    if backend(connection) == "postgres":
        digest_columns = {row[0] for row in connection.execute(
            "SELECT column_name FROM information_schema.columns WHERE table_name='digest_stories'")}
    else:
        digest_columns = {row[1] for row in connection.execute("PRAGMA table_info(digest_stories)")}
    if "details_json" not in digest_columns:
        connection.execute("ALTER TABLE digest_stories ADD COLUMN details_json TEXT")
    connection.execute("CREATE INDEX IF NOT EXISTS idx_articles_ministry ON articles(ministry)")
    initialize_raw_store(connection)
    if backend(connection) == "sqlite":
        connection.execute("PRAGMA optimize")
