from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from datetime import datetime, timezone
from urllib.parse import urlsplit

import requests

from news_fetcher.core import normalize_title
from news_fetcher.database import initialize
from news_fetcher.raw_store import build_envelope, store_raw_event

API_URL = "https://api.perplexity.ai/v1/sonar"
CATEGORIES = ["Polity", "Economy", "International Relations", "Environment",
              "Science & Technology", "Government Schemes", "Society", "Other"]
GS_PAPERS = ["GS1", "GS2", "GS3", "GS4", "Prelims", "Essay"]
STORY_SCHEMA = {
    "type": "object", "properties": {"stories": {"type": "array", "maxItems": 20,
    "items": {"type": "object", "properties": {
        "headline": {"type": "string"}, "category": {"type": "string", "enum": CATEGORIES},
        "summary": {"type": "array", "minItems": 3, "maxItems": 5, "items": {"type": "string"}},
        "upsc_relevance": {"type": "string", "enum": GS_PAPERS},
        "sources": {"type": "array", "minItems": 1, "items": {"type": "string"}},
        "publication_date": {"type": "string", "format": "date"}, "background": {"type": "string"},
        "why_it_matters": {"type": "string"},
        "prelims_facts": {"type": "array", "minItems": 2, "maxItems": 5, "items": {"type": "string"}},
        "mains_angle": {"type": "string"}},
        "required": ["headline", "category", "summary", "upsc_relevance", "sources",
                     "publication_date", "background", "why_it_matters", "prelims_facts", "mains_angle"],
        "additionalProperties": False}}}, "required": ["stories"], "additionalProperties": False}


def load_local_env(path: str = ".env") -> None:
    try:
        lines = open(path, encoding="utf-8").read().splitlines()
    except FileNotFoundError:
        return
    for line in lines:
        stripped = line.strip()
        if stripped and not stripped.startswith("#") and "=" in stripped:
            key, value = stripped.split("=", 1)
            if key.strip() and value.strip():
                os.environ.setdefault(key.strip(), value.strip().strip('"').strip("'"))


def build_prompt(target_date: str) -> str:
    return f"""Find the most important Indian current-affairs stories published on {target_date}.
Prioritize The Hindu and The Indian Express, then Times of India, Deccan Herald, and PIB.
Merge reports about the same event. Include only UPSC-useful stories. Use real source URLs from
search results and never invent URLs. Provide 3-5 concise factual points, one allowed category,
and one allowed UPSC paper. For every story add concise background context, why it matters for
UPSC, 2-5 factual Prelims points, and one analytical Mains angle. Distinguish verified facts from
analysis and do not speculate. Return at most 20 stories."""


def valid_url(value: str) -> bool:
    parts = urlsplit(value)
    return parts.scheme in {"http", "https"} and bool(parts.netloc)


def validate_stories(payload: dict, target_date: str) -> list[dict]:
    if not isinstance(payload.get("stories"), list):
        raise ValueError("Response does not contain a stories array")
    valid = []
    for item in payload["stories"]:
        if not isinstance(item, dict):
            continue
        summary, prelims = item.get("summary"), item.get("prelims_facts")
        sources = list(dict.fromkeys(str(url).strip() for url in item.get("sources", [])
                                     if valid_url(str(url))))
        points = [str(point).strip() for point in summary] if isinstance(summary, list) else []
        facts = [str(point).strip() for point in prelims] if isinstance(prelims, list) else []
        if (str(item.get("headline", "")).strip() and item.get("category") in CATEGORIES
                and item.get("upsc_relevance") in GS_PAPERS and 3 <= len(points) <= 5
                and 2 <= len(facts) <= 5 and sources and item.get("publication_date") == target_date
                and all(str(item.get(field, "")).strip()
                        for field in ("background", "why_it_matters", "mains_angle"))):
            item["headline"] = str(item["headline"]).strip()
            item["summary"], item["sources"], item["prelims_facts"] = points, sources, facts
            valid.append(item)
    return valid


def request_digest(api_key: str, target_date: str, model: str = "sonar") -> tuple[list[dict], dict]:
    response = requests.post(API_URL, timeout=120,
        headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
        json={"model": model, "messages": [
            {"role": "system", "content": "You are an accurate UPSC current-affairs editor."},
            {"role": "user", "content": build_prompt(target_date)}],
            "response_format": {"type": "json_schema", "json_schema": {
                "name": "daily_upsc_digest", "schema": STORY_SCHEMA}},
            "temperature": 0.1, "max_tokens": 6000})
    response.raise_for_status()
    body = response.json()
    try:
        content = body["choices"][0]["message"]["content"]
        payload = json.loads(content) if isinstance(content, str) else content
    except (KeyError, IndexError, TypeError, json.JSONDecodeError) as error:
        raise ValueError("Perplexity returned an invalid structured response") from error
    return validate_stories(payload, target_date), body.get("usage", {})


def store_stories(connection: sqlite3.Connection, stories: list[dict]) -> int:
    initialize(connection)
    saved = 0
    for story in stories:
        normalized = normalize_title(story["headline"])
        story_id = hashlib.sha256(f"{story['publication_date']}:{normalized}".encode()).hexdigest()
        store_raw_event(connection, build_envelope(
            source_type="ai_search", source_key="perplexity", publisher="Perplexity Digest",
            external_id=story_id, published_at=f"{story['publication_date']}T00:00:00+05:30",
            payload=story, metadata={"model": os.getenv("PERPLEXITY_MODEL", "sonar")}))
        details = {key: story[key] for key in ("background", "why_it_matters", "prelims_facts", "mains_angle")}
        existing = connection.execute("SELECT 1 FROM digest_stories WHERE id=?", (story_id,)).fetchone()
        if existing:
            connection.execute("""UPDATE digest_stories SET headline=?,category=?,summary_json=?,
              upsc_relevance=?,source_urls_json=?,details_json=?,fetched_at=? WHERE id=?""",
              (story["headline"], story["category"], json.dumps(story["summary"], ensure_ascii=False),
               story["upsc_relevance"], json.dumps(story["sources"], ensure_ascii=False),
               json.dumps(details, ensure_ascii=False), datetime.now(timezone.utc).isoformat(), story_id))
            continue
        connection.execute("""INSERT INTO digest_stories
          (id,headline,normalized_title,category,summary_json,upsc_relevance,
           source_urls_json,published_at,details_json,fetched_at) VALUES (?,?,?,?,?,?,?,?,?,?)""",
          (story_id, story["headline"], normalized, story["category"],
           json.dumps(story["summary"], ensure_ascii=False), story["upsc_relevance"],
           json.dumps(story["sources"], ensure_ascii=False), story["publication_date"],
           json.dumps(details, ensure_ascii=False), datetime.now(timezone.utc).isoformat()))
        saved += 1
    connection.commit()
    return saved
