from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime

import requests

from news_fetcher.raw_store import IST
from news_fetcher.db import connect, database_target
from news_fetcher.sources.perplexity import *  # compatibility for existing callers/tests


def main(argv: list[str] | None = None) -> int:
    load_local_env()
    parser = argparse.ArgumentParser(description="Fetch a structured UPSC digest from Perplexity")
    parser.add_argument("--date", default=datetime.now(IST).date().isoformat())
    parser.add_argument("--database", default=database_target())
    parser.add_argument("--model", default=os.getenv("PERPLEXITY_MODEL", "sonar"))
    args = parser.parse_args(argv)
    api_key = os.getenv("PERPLEXITY_API_KEY")
    if not api_key:
        print("PERPLEXITY_API_KEY is not set.", file=sys.stderr)
        return 2
    try:
        stories, usage = request_digest(api_key, args.date, args.model)
        with connect(args.database) as connection:
            saved = store_stories(connection, stories)
        print(f"Perplexity returned {len(stories)} valid stories; saved {saved} new stories.")
        if usage:
            print(f"Usage: {json.dumps(usage, separators=(',', ':'))}")
        return 0
    except (requests.RequestException, ValueError) as error:
        print(f"Perplexity ingestion failed: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
