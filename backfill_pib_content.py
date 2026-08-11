from __future__ import annotations

import argparse
from pathlib import Path

from news_fetcher.database import initialize
from news_fetcher.db import connect, database_target
from news_fetcher.sources.pib import hydrate_missing, refresh_existing_audits


def backfill(database: Path, retries: int, delay: float, limit: int) -> tuple[int, int]:
    with connect(database) as connection:
        initialize(connection)
        completed, failed = hydrate_missing(
            connection, retries=retries, delay=delay, limit=limit)
        refresh_existing_audits(connection)
        return completed, failed


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill PIB rows whose full_text is empty")
    parser.add_argument("--database", default=database_target())
    parser.add_argument("--retries", type=int, default=5)
    parser.add_argument("--delay", type=float, default=0.5)
    parser.add_argument("--limit", type=int, default=0, help="0 means every missing PIB article")
    args = parser.parse_args()
    completed, failed = backfill(args.database, args.retries, args.delay, args.limit)
    print(f"Backfill complete: saved={completed}, still_missing={failed}")
    return 0 if failed == 0 else 1


if __name__ == "__main__":
    raise SystemExit(main())
