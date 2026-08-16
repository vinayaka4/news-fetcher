from __future__ import annotations

import argparse
import os
import sys
from datetime import datetime

import requests

from news_fetcher.consolidation import (ConsolidationError, fetch_daily_snapshot,
                                        request_ai_groups, store_consolidation)
from news_fetcher.database import initialize
from news_fetcher.db import connect, database_target
from news_fetcher.raw_store import IST
from news_fetcher.sources.perplexity import load_local_env


def main(argv: list[str] | None = None) -> int:
    load_local_env()
    parser = argparse.ArgumentParser(description="Consolidate one daily four-source news snapshot")
    parser.add_argument("--date", default=datetime.now(IST).date().isoformat())
    parser.add_argument("--database", default=database_target())
    parser.add_argument("--api-base-url", default=os.getenv("NEWS_API_BASE_URL", "http://127.0.0.1:5000"))
    parser.add_argument("--model", default=os.getenv("CONSOLIDATION_MODEL",
                                                     os.getenv("PERPLEXITY_MODEL", "sonar")))
    args = parser.parse_args(argv)
    try:
        datetime.strptime(args.date, "%Y-%m-%d")
    except ValueError:
        print("date must be YYYY-MM-DD", file=sys.stderr); return 2
    api_key = os.getenv("PERPLEXITY_API_KEY")
    if not api_key:
        print("PERPLEXITY_API_KEY is not set", file=sys.stderr); return 2
    try:
        snapshot = fetch_daily_snapshot(args.api_base_url, args.date)
        with connect(args.database) as connection:
            initialize(connection)
            result = store_consolidation(connection, args.date, snapshot, args.model,
                lambda candidates: request_ai_groups(api_key, candidates, args.model))
        print(f"Consolidation complete: input={result['input_count']}, "
              f"output={result['output_count']}, duplicate={result['duplicate']}")
        return 0
    except (requests.RequestException, ConsolidationError, ValueError) as error:
        print(f"Consolidation failed: {error}", file=sys.stderr); return 1


if __name__ == "__main__":
    raise SystemExit(main())
