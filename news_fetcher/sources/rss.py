from __future__ import annotations

import hashlib
import sqlite3
from datetime import timezone, datetime
from difflib import SequenceMatcher
from typing import Any

import feedparser
import requests
from dateutil import parser as date_parser

from news_fetcher.core import Source, canonical_url, clean_text, normalize_title
from news_fetcher.raw_store import build_envelope, store_raw_event
from news_fetcher.raw_store import IST


def parse_date(entry: dict[str, Any]) -> str | None:
    raw = entry.get("published") or entry.get("updated") or entry.get("created")
    if not raw:
        return None
    try:
        value = date_parser.parse(raw)
        if value.tzinfo is None:
            value = value.replace(tzinfo=timezone.utc)
        # Daily ingestion and all public date APIs use the Indian publication
        # day. Retain the offset so midnight-boundary stories are not filed
        # under the previous UTC date.
        return value.astimezone(IST).isoformat()
    except (TypeError, ValueError, OverflowError):
        return None


def extract_ministry(entry: dict[str, Any]) -> str | None:
    explicit = clean_text(entry.get("ministry") or entry.get("department"))
    if explicit:
        return explicit
    for tag in entry.get("tags", []):
        term = clean_text(tag.get("term"))
        if term and term.lower() not in {"press release", "pib", "government"}:
            return term
    return None


def is_duplicate(connection: sqlite3.Connection, url: str, title: str,
                 threshold: float = 0.88, source_key: str | None = None) -> bool:
    if connection.execute("SELECT 1 FROM articles WHERE article_url=?", (url,)).fetchone():
        return True
    normalized = normalize_title(title)
    if source_key:
        rows = connection.execute("""SELECT normalized_title FROM articles
          WHERE source_key=? ORDER BY fetched_at DESC LIMIT 500""", (source_key,)).fetchall()
    else:
        rows = connection.execute(
            "SELECT normalized_title FROM articles ORDER BY fetched_at DESC LIMIT 500").fetchall()
    return any(SequenceMatcher(None, normalized, row[0]).ratio() >= threshold for row in rows)


def fetch_rss(connection: sqlite3.Connection, source: Source, user_agent: str) -> int:
    response = requests.get(source.url, timeout=30, headers={"User-Agent": user_agent})
    response.raise_for_status()
    feed = feedparser.parse(response.content)
    if feed.bozo and not feed.entries:
        raise RuntimeError(str(feed.bozo_exception))
    saved = 0
    for entry in feed.entries:
        title, raw_url = clean_text(entry.get("title")), entry.get("link")
        if not title or not raw_url:
            continue
        url, published_at = canonical_url(raw_url), parse_date(entry)
        store_raw_event(connection, build_envelope(
            source_type="rss", source_key=source.key, publisher=source.publisher,
            external_id=url, published_at=published_at, payload=dict(entry),
            metadata={"feed_url": source.url}))
        if is_duplicate(connection, url, title, source_key=source.key):
            continue
        article_id = hashlib.sha256(url.encode()).hexdigest()
        connection.execute("""INSERT INTO articles
          (id,publisher,source_key,title,normalized_title,article_url,published_at,
           author,excerpt,ministry,fetched_at) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
          (article_id, source.publisher, source.key, title, normalize_title(title), url,
           published_at, clean_text(entry.get("author")) or None,
           clean_text(entry.get("summary") or entry.get("description")),
           extract_ministry(entry), datetime.now(timezone.utc).isoformat()))
        saved += 1
    connection.commit()
    return saved
