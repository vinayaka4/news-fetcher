"""Source-specific ingestion adapters and their shared dispatcher."""

from __future__ import annotations

import sqlite3

from news_fetcher.core import Source
from news_fetcher.sources.pib import fetch_releases
from news_fetcher.sources.rss import fetch_rss


def fetch_source(connection: sqlite3.Connection, source: Source, user_agent: str) -> int:
    """Route a source configuration to the correct ingestion adapter."""
    if source.key == "pib":
        return fetch_releases(connection, source, user_agent)
    return fetch_rss(connection, source, user_agent)
