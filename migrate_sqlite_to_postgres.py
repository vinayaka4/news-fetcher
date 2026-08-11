from __future__ import annotations

import argparse
import os
import sqlite3
from pathlib import Path

from news_fetcher.database import initialize
from news_fetcher.db import connect

TABLES = [
    "raw_events", "articles", "digest_stories", "pipeline_runs",
    "pib_ingestion_runs", "pib_source_health", "pib_flags", "ingestion_jobs",
    "uploaded_documents",
]


def sqlite_columns(connection, table: str) -> list[str]:
    return [row[1] for row in connection.execute(f"PRAGMA table_info({table})")]


def postgres_columns(connection, table: str) -> set[str]:
    return {row[0] for row in connection.execute(
        "SELECT column_name FROM information_schema.columns WHERE table_name=?", (table,))}


def migrate(source_path: Path, target_url: str) -> dict[str, int]:
    if not target_url.startswith(("postgres://", "postgresql://")):
        raise ValueError("Target must be a PostgreSQL connection URL")
    counts: dict[str, int] = {}
    with sqlite3.connect(source_path) as source, connect(target_url) as target:
        source.row_factory = sqlite3.Row
        initialize(target)
        for table in TABLES:
            if not source.execute(
                    "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (table,)).fetchone():
                continue
            columns = [name for name in sqlite_columns(source, table)
                       if name in postgres_columns(target, table)]
            if not columns:
                continue
            placeholders = ",".join("?" for _ in columns)
            sql = (f"INSERT INTO {table} ({','.join(columns)}) VALUES ({placeholders}) "
                   "ON CONFLICT DO NOTHING")
            inserted = 0
            for row in source.execute(f"SELECT {','.join(columns)} FROM {table}"):
                inserted += max(target.execute(sql, tuple(row[name] for name in columns)).rowcount, 0)
            target.commit()
            counts[table] = inserted
            print(f"{table}: inserted {inserted}", flush=True)
    return counts


def main() -> int:
    parser = argparse.ArgumentParser(description="Copy the local SQLite dataset to PostgreSQL")
    parser.add_argument("--source", type=Path, default=Path("news.db"))
    parser.add_argument("--target", default=os.getenv("DATABASE_URL"))
    args = parser.parse_args()
    if not args.target:
        parser.error("--target or DATABASE_URL is required")
    migrate(args.source, args.target)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
