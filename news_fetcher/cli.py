from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import requests

from news_fetcher.core import (Source, canonical_url, clean_text, load_sources,
                               normalize_title, selected_sources)
from news_fetcher.database import initialize
from news_fetcher.db import connect, database_target
from news_fetcher.sources import fetch_source
from news_fetcher.sources.pib import (detail_url as pib_detail_url,
                                      fetch_content as fetch_pib_content,
                                      fetch_releases as fetch_pib_releases,
                                      parse_content as parse_pib_content,
                                      parse_releases as parse_pib_releases,
                                      release_id as pib_prid)
from news_fetcher.sources.rss import (extract_ministry, is_duplicate,
                                      parse_date)

ROOT = Path(__file__).resolve().parent.parent


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Fetch configured news sources")
    parser.add_argument("--database", default=database_target())
    parser.add_argument("--feeds", type=Path, default=ROOT / "feeds.json")
    parser.add_argument("--sources", default=os.getenv("NEWS_ENABLED_SOURCES"))
    parser.add_argument("--list-sources", action="store_true")
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    sources = load_sources(args.feeds)
    if args.list_sources:
        for source in sources:
            print(f"{source.key:24} {source.usage:30} {source.url}")
        return 0
    try:
        chosen = selected_sources(sources, args.sources)
    except ValueError as error:
        print(error, file=sys.stderr)
        return 2
    user_agent = os.getenv("NEWS_USER_AGENT", "CurrentAffairsReader/1.0 (contact: you@example.com)")
    with connect(args.database) as connection:
        initialize(connection)
        total = 0
        for source in chosen:
            try:
                count = fetch_source(connection, source, user_agent)
                total += count
                print(f"{source.publisher} ({source.key}): {count} new")
            except (requests.RequestException, RuntimeError) as error:
                print(f"{source.publisher} ({source.key}): failed: {error}", file=sys.stderr)
        print(f"Total saved: {total}")
    return 0
