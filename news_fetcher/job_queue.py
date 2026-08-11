from __future__ import annotations

import json
import sqlite3
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any


def now() -> str:
    return datetime.now(timezone.utc).isoformat()


def initialize_jobs(connection: sqlite3.Connection) -> None:
    connection.executescript("""
      CREATE TABLE IF NOT EXISTS ingestion_jobs (
        id TEXT PRIMARY KEY,
        job_key TEXT NOT NULL UNIQUE,
        job_type TEXT NOT NULL,
        source_key TEXT NOT NULL,
        payload_json TEXT NOT NULL,
        status TEXT NOT NULL DEFAULT 'pending',
        attempts INTEGER NOT NULL DEFAULT 0,
        max_attempts INTEGER NOT NULL DEFAULT 20,
        next_attempt_at TEXT NOT NULL,
        locked_at TEXT,
        last_error TEXT,
        created_at TEXT NOT NULL,
        updated_at TEXT NOT NULL,
        completed_at TEXT
      );
      CREATE INDEX IF NOT EXISTS idx_jobs_ready
        ON ingestion_jobs(status, next_attempt_at, created_at);
      CREATE INDEX IF NOT EXISTS idx_jobs_source_status
        ON ingestion_jobs(source_key, status);
    """)


def enqueue(connection: sqlite3.Connection, *, job_key: str, job_type: str,
            source_key: str, payload: dict[str, Any], max_attempts: int = 20) -> str:
    initialize_jobs(connection)
    timestamp = now()
    job_id = str(uuid.uuid4())
    connection.execute("""INSERT INTO ingestion_jobs
      (id,job_key,job_type,source_key,payload_json,status,attempts,max_attempts,
       next_attempt_at,created_at,updated_at)
      VALUES (?,?,?,?,?,'pending',0,?,?,?,?)
      ON CONFLICT(job_key) DO UPDATE SET
        payload_json=excluded.payload_json,
        status=CASE WHEN ingestion_jobs.status='complete' THEN 'complete' ELSE 'pending' END,
        next_attempt_at=CASE WHEN ingestion_jobs.status='complete'
                             THEN ingestion_jobs.next_attempt_at ELSE excluded.next_attempt_at END,
        updated_at=excluded.updated_at""",
      (job_id, job_key, job_type, source_key,
       json.dumps(payload, ensure_ascii=False, separators=(",", ":")),
       max_attempts, timestamp, timestamp, timestamp))
    row = connection.execute("SELECT id FROM ingestion_jobs WHERE job_key=?", (job_key,)).fetchone()
    return row[0]


def claim_next(connection: sqlite3.Connection) -> sqlite3.Row | None:
    initialize_jobs(connection)
    connection.row_factory = sqlite3.Row
    timestamp = now()
    # Recover work abandoned by a terminated worker after 15 minutes.
    stale = (datetime.now(timezone.utc) - timedelta(minutes=15)).isoformat()
    connection.execute("""UPDATE ingestion_jobs SET status='retry', locked_at=NULL,
      next_attempt_at=?, updated_at=? WHERE status='running' AND locked_at < ?""",
      (timestamp, timestamp, stale))
    row = connection.execute("""SELECT * FROM ingestion_jobs
      WHERE status IN ('pending','retry') AND next_attempt_at <= ? AND attempts < max_attempts
      ORDER BY next_attempt_at, created_at LIMIT 1""", (timestamp,)).fetchone()
    if not row:
        connection.commit()
        return None
    updated = connection.execute("""UPDATE ingestion_jobs SET status='running',
      attempts=attempts+1, locked_at=?, updated_at=?
      WHERE id=? AND status IN ('pending','retry')""", (timestamp, timestamp, row["id"])).rowcount
    connection.commit()
    return connection.execute("SELECT * FROM ingestion_jobs WHERE id=?", (row["id"],)).fetchone() if updated else None


def complete(connection: sqlite3.Connection, job_id: str) -> None:
    timestamp = now()
    connection.execute("""UPDATE ingestion_jobs SET status='complete', completed_at=?,
      updated_at=?, locked_at=NULL, last_error=NULL WHERE id=?""", (timestamp, timestamp, job_id))
    connection.commit()


def fail(connection: sqlite3.Connection, job_id: str, error: str) -> None:
    row = connection.execute("SELECT attempts,max_attempts FROM ingestion_jobs WHERE id=?", (job_id,)).fetchone()
    attempts, maximum = row
    terminal = attempts >= maximum
    delay_minutes = min(2 ** max(attempts - 1, 0), 360)
    retry_at = (datetime.now(timezone.utc) + timedelta(minutes=delay_minutes)).isoformat()
    connection.execute("""UPDATE ingestion_jobs SET status=?, next_attempt_at=?, last_error=?,
      updated_at=?, locked_at=NULL WHERE id=?""",
      ("failed" if terminal else "retry", retry_at, error[:2000], now(), job_id))
    connection.commit()
