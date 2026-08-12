from __future__ import annotations

import hashlib
import json
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

IST = timezone(timedelta(hours=5, minutes=30))
SCHEMA_VERSION = "1.0"


def json_safe(value: Any) -> Any:
    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if isinstance(value, dict):
        return {str(key): json_safe(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [json_safe(item) for item in value]
    return str(value)


def initialize_raw_store(connection: sqlite3.Connection) -> None:
    connection.executescript("""
        CREATE TABLE IF NOT EXISTS raw_events (
            id TEXT PRIMARY KEY,
            schema_version TEXT NOT NULL,
            source_type TEXT NOT NULL,
            source_key TEXT NOT NULL,
            publisher TEXT NOT NULL,
            external_id TEXT NOT NULL,
            event_date TEXT NOT NULL,
            published_at TEXT,
            fetched_at TEXT NOT NULL,
            content_hash TEXT NOT NULL,
            raw_json TEXT NOT NULL,
            UNIQUE(source_key, external_id, content_hash)
        );
        CREATE INDEX IF NOT EXISTS idx_raw_events_date_id ON raw_events(event_date, id);
        CREATE INDEX IF NOT EXISTS idx_raw_events_source_date ON raw_events(source_key, event_date);
        CREATE TABLE IF NOT EXISTS uploaded_documents (
            id TEXT PRIMARY KEY,
            original_filename TEXT NOT NULL,
            stored_path TEXT NOT NULL,
            sha256 TEXT NOT NULL UNIQUE,
            media_type TEXT NOT NULL,
            page_count INTEGER NOT NULL,
            document_date TEXT NOT NULL,
            source_name TEXT,
            uploaded_at TEXT NOT NULL,
            raw_event_id TEXT NOT NULL,
            FOREIGN KEY(raw_event_id) REFERENCES raw_events(id)
        );
        CREATE INDEX IF NOT EXISTS idx_uploaded_documents_date ON uploaded_documents(document_date);
        CREATE TABLE IF NOT EXISTS pipeline_runs (
            run_date_ist TEXT PRIMARY KEY,
            started_at TEXT NOT NULL,
            completed_at TEXT,
            status TEXT NOT NULL,
            message TEXT
        );
    """)
    from news_fetcher.job_queue import initialize_jobs
    initialize_jobs(connection)


def build_envelope(*, source_type: str, source_key: str, publisher: str,
                   external_id: str, payload: Any, published_at: str | None = None,
                   metadata: dict[str, Any] | None = None,
                   fetched_at: str | None = None) -> dict[str, Any]:
    fetched_at = fetched_at or datetime.now(timezone.utc).isoformat()
    event_date = (published_at or fetched_at)[:10]
    return {
        "schema_version": SCHEMA_VERSION,
        "source": {"type": source_type, "key": source_key, "publisher": publisher},
        "identity": {"external_id": external_id},
        "timestamps": {"published_at": published_at, "fetched_at": fetched_at, "event_date": event_date},
        "metadata": json_safe(metadata or {}),
        "payload": json_safe(payload),
    }


def store_raw_event(connection: sqlite3.Connection, envelope: dict[str, Any]) -> tuple[str, bool]:
    serialized_payload = json.dumps(envelope["payload"], ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    content_hash = hashlib.sha256(serialized_payload.encode("utf-8")).hexdigest()
    source = envelope["source"]
    external_id = envelope["identity"]["external_id"]
    event_id = hashlib.sha256(f"{source['key']}:{external_id}:{content_hash}".encode()).hexdigest()
    raw_json = json.dumps(envelope, ensure_ascii=False, separators=(",", ":"))
    cursor = connection.execute(
        """INSERT INTO raw_events
           (id, schema_version, source_type, source_key, publisher, external_id,
            event_date, published_at, fetched_at, content_hash, raw_json)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(source_key, external_id, content_hash) DO NOTHING""",
        (event_id, envelope["schema_version"], source["type"], source["key"], source["publisher"],
         external_id, envelope["timestamps"]["event_date"], envelope["timestamps"]["published_at"],
         envelope["timestamps"]["fetched_at"], content_hash, raw_json),
    )
    return event_id, cursor.rowcount == 1
