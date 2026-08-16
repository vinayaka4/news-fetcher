from __future__ import annotations

import hashlib
import json
import os
import re
import uuid
from datetime import datetime, timezone
from difflib import SequenceMatcher
from typing import Any, Callable

import requests

from news_fetcher.core import normalize_title

PROMPT_VERSION = "1.0"
API_URL = "https://api.perplexity.ai/chat/completions"
CATEGORIES = ["Polity", "Economy", "International Relations", "Environment",
              "Science & Technology", "Government Schemes", "Society", "Other"]


class ConsolidationError(RuntimeError):
    pass


def fetch_daily_snapshot(base_url: str, target_date: str) -> dict:
    response = requests.get(f"{base_url.rstrip('/')}/api/v1/all", timeout=180,
                            params={"date": target_date, "limit": 200,
                                    "include_pdf_content": "true"})
    response.raise_for_status()
    payload = response.json()
    if payload.get("date") != target_date or not isinstance(payload.get("sources"), dict):
        raise ConsolidationError("Daily source API returned an invalid snapshot")
    return payload


def _text(value: Any, maximum: int = 1600) -> str:
    if isinstance(value, list):
        value = " ".join(str(item) for item in value)
    return re.sub(r"\s+", " ", str(value or "")).strip()[:maximum]


def normalize_snapshot(snapshot: dict) -> list[dict]:
    records: list[dict] = []
    sources = snapshot.get("sources", {})
    for item in sources.get("pib", {}).get("items", []):
        records.append({"record_id": f"pib:{item['id']}", "source_type": "pib",
          "publisher": "Press Information Bureau", "title": _text(item.get("title"), 300),
          "url": item.get("article_url"), "content": _text(item.get("full_text"))})
    for item in sources.get("rss", {}).get("items", []):
        records.append({"record_id": f"rss:{item['id']}", "source_type": "rss",
          "publisher": item.get("publisher"), "title": _text(item.get("title"), 300),
          "url": item.get("article_url"), "content": _text(item.get("content_text"))})
    for item in sources.get("perplexity", {}).get("items", []):
        records.append({"record_id": f"perplexity:{item['id']}", "source_type": "perplexity",
          "publisher": "Perplexity Digest", "title": _text(item.get("headline"), 300),
          "url": (item.get("source_urls") or [None])[0],
          "content": _text(item.get("summary"))})
    for document in sources.get("pdf", {}).get("items", []):
        newspaper = document.get("newspaper") or {}
        for section in newspaper.get("sections", []):
            for article in section.get("articles", []):
                title = _text(article.get("title") or article.get("deck"), 300)
                if not title:
                    continue
                records.append({"record_id": f"pdf:{document['id']}:{article['id']}",
                  "source_type": "pdf", "publisher": document.get("source_name"),
                  "title": title, "url": document.get("structured_url"),
                  "content": _text(article.get("content"))})
    unique = {record["record_id"]: record for record in records if record["title"]}
    return [unique[key] for key in sorted(unique)]


def _similar(left: str, right: str) -> bool:
    a, b = normalize_title(left), normalize_title(right)
    if not a or not b:
        return False
    a_tokens, b_tokens = set(a.split()), set(b.split())
    overlap = len(a_tokens & b_tokens) / max(min(len(a_tokens), len(b_tokens)), 1)
    return overlap >= .72 or SequenceMatcher(None, a, b).ratio() >= .78


def precluster(records: list[dict]) -> list[dict]:
    clusters: list[list[dict]] = []
    for record in sorted(records, key=lambda item: normalize_title(item["title"])):
        cluster = next((group for group in clusters
                        if any(_similar(record["title"], member["title"]) for member in group)), None)
        if cluster is None:
            clusters.append([record])
        else:
            cluster.append(record)
    candidates = []
    for number, group in enumerate(clusters, start=1):
        candidates.append({"candidate_id": f"candidate_{number:04d}",
          "titles": [item["title"] for item in group[:12]],
          "source_record_ids": [item["record_id"] for item in group],
          "evidence": [{"publisher": item.get("publisher"), "content": item["content"][:350]}
                       for item in group[:8]]})
    return candidates


def request_ai_groups(api_key: str, candidates: list[dict], model: str = "sonar") -> list[dict]:
    if not candidates:
        return []
    schema = {"type": "object", "properties": {"stories": {"type": "array", "items": {
      "type": "object", "properties": {
        "title": {"type": "string"}, "category": {"type": "string", "enum": CATEGORIES},
        "summary": {"type": "array", "minItems": 2, "maxItems": 8, "items": {"type": "string"}},
        "key_facts": {"type": "array", "minItems": 1, "maxItems": 10, "items": {"type": "string"}},
        "candidate_ids": {"type": "array", "minItems": 1, "items": {"type": "string"}}},
      "required": ["title", "category", "summary", "key_facts", "candidate_ids"],
      "additionalProperties": False}}}, "required": ["stories"], "additionalProperties": False}
    response = requests.post(os.getenv("CONSOLIDATION_API_URL", API_URL), timeout=180,
      headers={"Authorization": f"Bearer {api_key}", "Content-Type": "application/json"},
      json={"model": model, "messages": [
        {"role": "system", "content": "You consolidate supplied news only. Never add outside facts."},
        {"role": "user", "content": "Merge candidates describing the same event. Use every candidate_id exactly once. Preserve disagreements as qualified facts. Return comprehensive factual summaries. INPUT:\n" + json.dumps(candidates, ensure_ascii=False)}],
        "response_format": {"type": "json_schema", "json_schema": {
          "name": "consolidated_daily_news", "schema": schema}}, "temperature": 0.0,
        "max_tokens": 12000})
    response.raise_for_status()
    try:
        content = response.json()["choices"][0]["message"]["content"]
        return json.loads(content)["stories"] if isinstance(content, str) else content["stories"]
    except (KeyError, IndexError, TypeError, json.JSONDecodeError) as error:
        raise ConsolidationError("AI returned invalid consolidation JSON") from error


def validate_groups(groups: list[dict], candidates: list[dict]) -> list[dict]:
    candidate_map = {item["candidate_id"]: item for item in candidates}
    seen, validated = [], []
    for group in groups:
        ids = group.get("candidate_ids")
        if (not isinstance(ids, list) or not ids or any(item not in candidate_map for item in ids)
                or group.get("category") not in CATEGORIES):
            raise ConsolidationError("AI output contains invalid candidate references")
        summary, facts = group.get("summary"), group.get("key_facts")
        clean_summary = [_text(item, 1000) for item in summary] if isinstance(summary, list) else []
        clean_facts = [_text(item, 1000) for item in facts] if isinstance(facts, list) else []
        clean_summary, clean_facts = [item for item in clean_summary if item], [item for item in clean_facts if item]
        if not _text(group.get("title"), 300) or len(clean_summary) < 2 or not clean_facts:
            raise ConsolidationError("AI output does not match the consolidated story schema")
        seen.extend(ids)
        refs = [record_id for candidate_id in ids
                for record_id in candidate_map[candidate_id]["source_record_ids"]]
        validated.append({"title": _text(group["title"], 300), "category": group["category"],
                          "summary": clean_summary, "key_facts": clean_facts,
                          "source_record_ids": sorted(set(refs))})
    expected = sorted(candidate_map)
    if sorted(seen) != expected or len(seen) != len(set(seen)):
        raise ConsolidationError("AI output omitted or duplicated input candidates")
    return validated


def consolidate_snapshot(snapshot: dict, ai_function: Callable[[list[dict]], list[dict]]) -> tuple[list[dict], list[dict]]:
    records = normalize_snapshot(snapshot)
    if not records:
        raise ConsolidationError("No source records are available for this date")
    candidates = precluster(records)
    maximum = int(os.getenv("CONSOLIDATION_MAX_CANDIDATES", "250"))
    if len(candidates) > maximum:
        raise ConsolidationError(f"Candidate count {len(candidates)} exceeds configured maximum {maximum}")
    return records, validate_groups(ai_function(candidates), candidates)


def store_consolidation(connection, target_date: str, snapshot: dict, model: str,
                        ai_function: Callable[[list[dict]], list[dict]]) -> dict:
    records = normalize_snapshot(snapshot)
    serialized = json.dumps(records, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
    input_hash = hashlib.sha256(serialized.encode()).hexdigest()
    existing = connection.execute("""SELECT id,output_count FROM consolidation_runs
      WHERE publication_date=? AND input_hash=? AND model=? AND prompt_version=? AND status='complete'""",
      (target_date, input_hash, model, PROMPT_VERSION)).fetchone()
    if existing:
        return {"run_id": existing[0], "input_count": len(records),
                "output_count": existing[1], "duplicate": True}
    run_id, now = str(uuid.uuid4()), datetime.now(timezone.utc).isoformat()
    connection.execute("""INSERT INTO consolidation_runs
      (id,publication_date,input_hash,input_count,model,prompt_version,status,
       source_snapshot_json,started_at) VALUES (?,?,?,?,?,?,'running',?,?)""",
      (run_id, target_date, input_hash, len(records), model, PROMPT_VERSION, serialized, now))
    connection.commit()
    try:
        records, stories = consolidate_snapshot(snapshot, ai_function)
        for story in stories:
            story_id = hashlib.sha256((run_id + ":" + ":".join(story["source_record_ids"])).encode()).hexdigest()
            connection.execute("""INSERT INTO consolidated_stories
              (id,run_id,publication_date,title,category,summary_json,key_facts_json,
               source_refs_json,source_count,created_at) VALUES (?,?,?,?,?,?,?,?,?,?)""",
              (story_id, run_id, target_date, story["title"], story["category"],
               json.dumps(story["summary"], ensure_ascii=False),
               json.dumps(story["key_facts"], ensure_ascii=False),
               json.dumps(story["source_record_ids"], ensure_ascii=False),
               len(story["source_record_ids"]), now))
        connection.execute("""UPDATE consolidation_runs SET status='complete',output_count=?,
          completed_at=?,error=NULL WHERE id=?""", (len(stories), datetime.now(timezone.utc).isoformat(), run_id))
        connection.commit()
        return {"run_id": run_id, "input_count": len(records), "output_count": len(stories),
                "duplicate": False}
    except Exception as error:
        connection.rollback()
        connection.execute("""UPDATE consolidation_runs SET status='failed',completed_at=?,error=?
          WHERE id=?""", (datetime.now(timezone.utc).isoformat(), f"{type(error).__name__}: {error}"[:2000], run_id))
        connection.commit()
        raise
