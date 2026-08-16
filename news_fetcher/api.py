from __future__ import annotations

import json
import os
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

from flask import Blueprint, current_app, jsonify, request

from news_fetcher.db import connect as db_connect
from news_fetcher.consolidation import newspaper_from_url
from news_fetcher.sources.pdf_reader import MAX_UPLOAD_BYTES, PdfIngestionError, ingest_upload

api = Blueprint("api", __name__, url_prefix="/api/v1")


@api.get("")
@api.get("/")
def api_index():
    return jsonify({"service": "UPSC News API", "version": "1",
      "endpoints": {
        "latest_pib_data": "/api/v1/pib",
        "pib_by_date": "/api/v1/pib?date=YYYY-MM-DD",
        "pib_completeness": "/api/v1/pib/completeness?date=YYYY-MM-DD",
        "pib_health": "/api/v1/pib/health",
        "rss_by_date": "/api/v1/rss?date=YYYY-MM-DD",
        "perplexity_by_date": "/api/v1/perplexity?date=YYYY-MM-DD",
        "all_sources_by_date": "/api/v1/all?date=YYYY-MM-DD",
        "consolidated_by_date": "/api/v1/consolidated?date=YYYY-MM-DD",
        "articles": "/api/v1/articles?date=YYYY-MM-DD&source=pib&content_status=ready",
        "raw_events": "/api/v1/news?date=YYYY-MM-DD",
        "pdf_newspapers": "/api/v1/uploads/pdf?date=YYYY-MM-DD",
        "health_check": "/api/v1/healthz"}})


def configured_database() -> str:
    return current_app.config.get("DATABASE_URL") or current_app.config["NEWS_DATABASE"]


def connect():
    # Schema initialization belongs to deployment/worker startup, not the HTTP
    # request path. Running DDL for every request can deadlock active ingestion.
    return db_connect(configured_database())


def parse_iso_date(value: str | None) -> str:
    if not value:
        return datetime.now(timezone.utc).date().isoformat()
    return date.fromisoformat(value).isoformat()


def _request_date_and_limit(default_limit: int = 100, maximum: int = 500) -> tuple[str, int]:
    return (parse_iso_date(request.args.get("date")),
            min(max(int(request.args.get("limit", str(default_limit))), 1), maximum))


def _article_item(row) -> dict:
    item = dict(row)
    full_text = (item.get("full_text") or "").strip()
    excerpt = (item.get("excerpt") or "").strip()
    item["content_kind"] = "full_text" if full_text else ("rss_excerpt" if excerpt else "metadata_only")
    item["content_text"] = full_text or excerpt or None
    item["content_status"] = "ready" if (full_text or item["source_key"] != "pib") else "pending"
    return item


def _digest_item(row) -> dict:
    item = dict(row)
    errors = []
    for column, output, fallback in (("summary_json", "summary", []),
                                      ("source_urls_json", "source_urls", []),
                                      ("details_json", "details", None)):
        raw = item.pop(column, None)
        try:
            item[output] = json.loads(raw) if raw else fallback
        except (TypeError, json.JSONDecodeError):
            item[output] = fallback
            errors.append(f"invalid_{column}")
    item["content_status"] = "ready" if not errors else "partial"
    item["content_kind"] = "structured_summary"
    item["data_quality_flags"] = errors
    return item


def _query_pib(connection, event_date: str, limit: int) -> dict:
    rows = connection.execute("""SELECT id,title,article_url,published_at,ministry,
      full_text,fetched_at FROM articles WHERE source_key='pib'
      AND substr(published_at,1,10)=? ORDER BY published_at DESC,id LIMIT ?""",
      (event_date, limit)).fetchall()
    audit = connection.execute("""SELECT expected_count,discovered_count,ready_count,
      missing_count,complete FROM pib_ingestion_runs WHERE publication_date=?""",
      (event_date,)).fetchone()
    complete = bool(audit["complete"]) if audit else False
    return {"source": "pib", "date": event_date, "count": len(rows),
            "status": "ready" if complete else ("partial" if rows else "no_data"),
            "complete": complete, "completeness": dict(audit) if audit else None,
            "items": [dict(row) | {"content_status": "ready" if row["full_text"] else "pending"}
                      for row in rows]}


def _query_rss(connection, event_date: str, limit: int, cursor: str = "") -> dict:
    cursor_sql, parameters = ("AND id>?", (event_date, cursor, limit + 1)) if cursor \
        else ("", (event_date, limit + 1))
    rows = connection.execute(f"""SELECT id,publisher,source_key,title,article_url,published_at,
      author,excerpt,ministry,full_text,fetched_at FROM articles
      WHERE source_key!='pib' AND substr(published_at,1,10)=?
      {cursor_sql} ORDER BY id LIMIT ?""", parameters).fetchall()
    has_more = len(rows) > limit; rows = rows[:limit]
    items = [_article_item(row) for row in rows]
    return {"source": "rss", "date": event_date, "count": len(items),
            "status": "ready" if items else "no_data", "complete": None,
            "coverage": "best_effort_feed_coverage",
            "next_cursor": rows[-1]["id"] if has_more and rows else None, "items": items}


def _query_perplexity(connection, event_date: str, limit: int, cursor: str = "") -> dict:
    cursor_sql, parameters = ("AND id>?", (event_date, cursor, limit + 1)) if cursor \
        else ("", (event_date, limit + 1))
    rows = connection.execute(f"""SELECT * FROM digest_stories
      WHERE substr(published_at,1,10)=? {cursor_sql} ORDER BY id LIMIT ?""", parameters).fetchall()
    has_more = len(rows) > limit; rows = rows[:limit]
    items = [_digest_item(row) for row in rows]
    status = ("partial" if any(item["content_status"] == "partial" for item in items)
              else "ready" if items else "no_data")
    return {"source": "perplexity", "date": event_date, "count": len(items),
            "status": status, "complete": None, "coverage": "best_effort_search_summary",
            "next_cursor": rows[-1]["id"] if has_more and rows else None, "items": items}


def _query_pdfs(connection, event_date: str, limit: int, include_content: bool) -> dict:
    rows = connection.execute("""SELECT d.id,d.original_filename,d.document_date,d.source_name,
      d.page_count,d.uploaded_at,r.raw_json FROM uploaded_documents d
      JOIN raw_events r ON r.id=d.raw_event_id WHERE d.document_date=?
      ORDER BY d.uploaded_at DESC,d.id LIMIT ?""", (event_date, limit)).fetchall()
    items = []
    for row in rows:
        item = dict(row); raw_json = item.pop("raw_json")
        item["structured_url"] = f"/api/v1/uploads/pdf/{item['id']}"
        try:
            newspaper = json.loads(raw_json).get("payload", {}).get("newspaper")
        except (TypeError, json.JSONDecodeError):
            newspaper = None
        item["content_status"] = "ready" if newspaper else "partial"
        if include_content:
            item["newspaper"] = newspaper
        items.append(item)
    status = ("partial" if any(item["content_status"] == "partial" for item in items)
              else "ready" if items else "no_data")
    return {"source": "pdf", "date": event_date, "count": len(items), "status": status,
            "complete": None, "coverage": "manual_uploads", "items": items}


@api.get("/articles")
def get_articles():
    """Return normalized, consumer-ready articles including hydrated content."""
    try:
        event_date = parse_iso_date(request.args.get("date"))
        limit = min(max(int(request.args.get("limit", "100")), 1), 500)
    except (ValueError, TypeError):
        return jsonify({"error": "date must be YYYY-MM-DD and limit must be an integer"}), 400
    source = request.args.get("source", "").strip()
    status = request.args.get("content_status", "all").strip().lower()
    cursor = request.args.get("cursor", "").strip()
    if status not in {"all", "ready", "pending"}:
        return jsonify({"error": "content_status must be all, ready, or pending"}), 400
    conditions = ["substr(published_at, 1, 10) = ?"]
    values: list[object] = [event_date]
    if source:
        conditions.append("source_key = ?"); values.append(source)
    if status == "ready":
        conditions.append("""((source_key='pib' AND LENGTH(TRIM(COALESCE(full_text,'')))>0)
          OR source_key!='pib')""")
    elif status == "pending":
        conditions.append("""((source_key='pib' AND LENGTH(TRIM(COALESCE(full_text,'')))=0)
          )""")
    if cursor:
        conditions.append("id > ?"); values.append(cursor)
    with connect() as connection:
        rows = connection.execute(
            f"""SELECT id, publisher, source_key, title, article_url, published_at,
                       author, excerpt, ministry, full_text, fetched_at
                FROM articles WHERE {' AND '.join(conditions)}
                ORDER BY id LIMIT ?""",
            (*values, limit + 1),
        ).fetchall()
    has_more = len(rows) > limit
    rows = rows[:limit]
    items = []
    for row in rows:
        item = dict(row)
        full_text = (item["full_text"] or "").strip()
        excerpt = (item["excerpt"] or "").strip()
        item["content_kind"] = "full_text" if full_text else ("rss_excerpt" if excerpt else "metadata_only")
        item["content_text"] = full_text or excerpt or None
        item["content_status"] = "ready" if (full_text or item["source_key"] != "pib") else "pending"
        items.append(item)
    return jsonify({
        "date": event_date, "source": source or None, "content_status": status,
        "count": len(items), "next_cursor": rows[-1]["id"] if has_more and rows else None,
        "items": items,
    })


@api.get("/pib")
def get_pib():
    """Browser-friendly PIB JSON; defaults to the latest ingested date."""
    requested_date = request.args.get("date")
    try:
        limit = min(max(int(request.args.get("limit", "100")), 1), 500)
    except (ValueError, TypeError):
        return jsonify({"error": "limit must be an integer"}), 400
    with connect() as connection:
        if requested_date:
            try:
                publication_date = parse_iso_date(requested_date)
            except ValueError:
                return jsonify({"error": "date must be YYYY-MM-DD"}), 400
        else:
            latest = connection.execute(
                "SELECT MAX(substr(published_at,1,10)) FROM articles WHERE source_key='pib'").fetchone()
            publication_date = latest[0] if latest and latest[0] else None
        if not publication_date:
            return jsonify({"source": "pib", "count": 0, "items": [],
                            "message": "No PIB data has been ingested yet"})
        rows = connection.execute("""SELECT id,title,article_url,published_at,ministry,
          full_text,fetched_at FROM articles WHERE source_key='pib'
          AND substr(published_at,1,10)=? ORDER BY published_at DESC,id LIMIT ?""",
          (publication_date, limit)).fetchall()
        audit = connection.execute("""SELECT expected_count,discovered_count,ready_count,
          missing_count,complete FROM pib_ingestion_runs WHERE publication_date=?""",
          (publication_date,)).fetchone()
    return jsonify({"source": "pib", "date": publication_date, "count": len(rows),
                    "complete": bool(audit[4]) if audit else False,
                    "completeness": dict(audit) if audit else None,
                    "items": [dict(row) | {"content_status": "ready" if row["full_text"] else "pending"}
                              for row in rows]})


@api.get("/healthz")
def healthz():
    try:
        with connect() as connection:
            connection.execute("SELECT 1").fetchone()
        return jsonify({"status": "ok", "database": "connected"})
    except Exception as error:
        return jsonify({"status": "error", "database": "unavailable",
                        "error": type(error).__name__}), 503


@api.get("/digests")
@api.get("/perplexity")
def get_digests():
    try:
        event_date, limit = _request_date_and_limit()
    except (ValueError, TypeError):
        return jsonify({"error": "date must be YYYY-MM-DD and limit must be an integer"}), 400
    cursor = request.args.get("cursor", "").strip()
    with connect() as connection:
        result = _query_perplexity(connection, event_date, limit, cursor)
    return jsonify(result)


@api.get("/rss")
def get_rss():
    try:
        event_date, limit = _request_date_and_limit()
    except (ValueError, TypeError):
        return jsonify({"error": "date must be YYYY-MM-DD and limit must be an integer"}), 400
    cursor = request.args.get("cursor", "").strip()
    with connect() as connection:
        result = _query_rss(connection, event_date, limit, cursor)
    return jsonify(result)


@api.get("/all")
def get_all_sources():
    """Return a consistent date snapshot for PIB, RSS, Perplexity, and PDFs."""
    try:
        event_date, limit = _request_date_and_limit(default_limit=100, maximum=200)
    except (ValueError, TypeError):
        return jsonify({"error": "date must be YYYY-MM-DD and limit must be an integer"}), 400
    include_pdf_content = request.args.get("include_pdf_content", "false").strip().lower() in {
        "1", "true", "yes"}
    with connect() as connection:
        sources = {
            "pib": _query_pib(connection, event_date, limit),
            "rss": _query_rss(connection, event_date, limit),
            "perplexity": _query_perplexity(connection, event_date, limit),
            "pdf": _query_pdfs(connection, event_date, min(limit, 25), include_pdf_content),
        }
    statuses = {name: value["status"] for name, value in sources.items()}
    total = sum(value["count"] for value in sources.values())
    overall = ("ready" if all(status == "ready" for status in statuses.values())
               else "partial" if total else "no_data")
    return jsonify({"date": event_date, "status": overall,
                    "complete": overall == "ready", "total_count": total,
                    "source_counts": {name: value["count"] for name, value in sources.items()},
                    "source_statuses": statuses, "sources": sources})


@api.get("/consolidated")
def get_consolidated():
    try:
        event_date, limit = _request_date_and_limit(default_limit=100, maximum=500)
    except (ValueError, TypeError):
        return jsonify({"error": "date must be YYYY-MM-DD and limit must be an integer"}), 400
    with connect() as connection:
        run = connection.execute("""SELECT id,input_hash,input_count,output_count,model,
          prompt_version,started_at,completed_at,source_snapshot_json FROM consolidation_runs
          WHERE publication_date=? AND status='complete'
          ORDER BY completed_at DESC,id DESC LIMIT 1""", (event_date,)).fetchone()
        if not run:
            return jsonify({"date": event_date, "source": "consolidated",
                            "status": "no_data", "count": 0, "items": []})
        rows = connection.execute("""SELECT id,title,category,summary_json,key_facts_json,
          source_refs_json,source_count,created_at FROM consolidated_stories
          WHERE run_id=? ORDER BY id LIMIT ?""", (run["id"], limit)).fetchall()
    run_item = dict(run)
    try:
        source_records = {item["record_id"]: item for item in
                          json.loads(run_item.pop("source_snapshot_json"))}
    except (TypeError, KeyError, json.JSONDecodeError):
        source_records = {}; run_item.pop("source_snapshot_json", None)
    items = []
    for row in rows:
        item = dict(row)
        item["summary"] = json.loads(item.pop("summary_json"))
        item["key_facts"] = json.loads(item.pop("key_facts_json"))
        item["source_record_ids"] = json.loads(item.pop("source_refs_json"))
        item["sources"] = [{key: source_records[record_id].get(key)
                            for key in ("record_id", "source_type", "source_tag", "publisher",
                                        "newspaper", "newspapers", "title", "url", "urls")}
                           for record_id in item["source_record_ids"] if record_id in source_records]
        tag_counts = {}
        for source in item["sources"]:
            source["source_tag"] = source.get("source_tag") or source.get("source_type")
            newspapers = source.get("newspapers") or []
            if not newspapers and source.get("newspaper"):
                newspapers = [source["newspaper"]]
            if not newspapers and source["source_tag"] in {"rss", "pdf"} and source.get("publisher"):
                newspapers = [source["publisher"]]
            if not newspapers and source["source_tag"] == "perplexity":
                inferred = [newspaper_from_url(url) for url in
                            (source.get("urls") or [source.get("url")]) if url]
                newspapers = list(dict.fromkeys(name for name in inferred if name))
            source["newspapers"] = newspapers
            source["newspaper"] = newspapers[0] if len(newspapers) == 1 else source.get("newspaper")
            names = source.get("newspapers") or [source.get("newspaper")]
            names = [name for name in names if name] or [None]
            for newspaper in names:
                key = (source.get("source_tag") or source.get("source_type"), newspaper)
                tag_counts[key] = tag_counts.get(key, 0) + 1
        item["source_tags"] = [{"source_tag": tag, "newspaper": newspaper, "count": count}
                               for (tag, newspaper), count in sorted(
                                   tag_counts.items(), key=lambda value: (value[0][0], value[0][1] or ""))]
        items.append(item)
    return jsonify({"date": event_date, "source": "consolidated", "status": "ready",
                    "count": len(items), "run": run_item, "items": items})


@api.get("/news")
def get_news():
    try:
        event_date = parse_iso_date(request.args.get("date"))
        limit = min(max(int(request.args.get("limit", "100")), 1), 500)
    except (ValueError, TypeError):
        return jsonify({"error": "date must be YYYY-MM-DD and limit must be an integer"}), 400
    source = request.args.get("source", "").strip()
    cursor = request.args.get("cursor", "").strip()
    conditions, values = ["event_date = ?"], [event_date]
    if source:
        conditions.append("source_key = ?"); values.append(source)
    if cursor:
        conditions.append("id > ?"); values.append(cursor)
    where = " AND ".join(conditions)
    with connect() as connection:
        rows = connection.execute(
            f"SELECT id, raw_json FROM raw_events WHERE {where} ORDER BY id LIMIT ?",
            (*values, limit + 1),
        ).fetchall()
    has_more = len(rows) > limit
    rows = rows[:limit]
    items = [json.loads(row["raw_json"]) | {"event_id": row["id"]} for row in rows]
    return jsonify({
        "date": event_date, "source": source or None, "count": len(items),
        "next_cursor": rows[-1]["id"] if has_more and rows else None, "items": items,
    })


@api.get("/sources")
def get_sources():
    with connect() as connection:
        rows = connection.execute(
            "SELECT source_key, publisher, source_type, COUNT(1) count FROM raw_events GROUP BY 1,2,3 ORDER BY 1"
        ).fetchall()
    return jsonify({"sources": [dict(row) for row in rows]})


@api.get("/pipeline/status")
def pipeline_status():
    with connect() as connection:
        jobs = connection.execute("""SELECT source_key,status,COUNT(*) count,
          MIN(created_at) oldest_created_at,MAX(updated_at) last_updated_at
          FROM ingestion_jobs GROUP BY source_key,status ORDER BY source_key,status""").fetchall()
        missing = connection.execute("""SELECT source_key,COUNT(*) count FROM articles
          WHERE source_key='pib' AND LENGTH(TRIM(COALESCE(full_text,'')))=0
          GROUP BY source_key""").fetchall()
        latest_run = connection.execute("""SELECT run_date_ist,started_at,completed_at,status,message
          FROM pipeline_runs ORDER BY run_date_ist DESC LIMIT 1""").fetchone()
        pib_dates = connection.execute("""SELECT publication_date,expected_count,
          discovered_count,ready_count,missing_count,complete,listing_fetched_at,updated_at
          FROM pib_ingestion_runs ORDER BY publication_date DESC LIMIT 31""").fetchall()
    healthy = (not any(row["status"] == "failed" for row in jobs)
               and not any(not row["complete"] for row in pib_dates))
    return jsonify({"healthy": healthy,
                    "jobs": [dict(row) for row in jobs],
                    "missing_content": [dict(row) for row in missing],
                    "pib_completeness": [dict(row) for row in pib_dates],
                    "latest_pipeline_run": dict(latest_run) if latest_run else None})


@api.get("/pib/completeness")
def pib_completeness():
    try:
        publication_date = parse_iso_date(request.args.get("date"))
    except ValueError:
        return jsonify({"error": "date must be YYYY-MM-DD"}), 400
    with connect() as connection:
        row = connection.execute("""SELECT publication_date,expected_count,discovered_count,
          ready_count,missing_count,complete,listing_fetched_at,updated_at,
          discovery_verified,listing_parse_healthy,source_fresh,flags_json
          FROM pib_ingestion_runs WHERE publication_date=?""", (publication_date,)).fetchone()
        health = connection.execute("""SELECT discovery_status,last_successful_discovery_at
          FROM pib_source_health WHERE source_key='pib'""").fetchone()
    if not row:
        return jsonify({"date": publication_date, "status": "not_ingested", "complete": False}), 404
    result = dict(row)
    result["complete"] = bool(result["complete"])
    for key in ("discovery_verified", "listing_parse_healthy", "source_fresh"):
        result[key] = bool(result[key])
    result["flags"] = json.loads(result.pop("flags_json") or "[]")
    freshness_minutes = int(os.getenv("PIB_SOURCE_FRESHNESS_MINUTES", "180"))
    dynamically_fresh = False
    if health and health[1]:
        dynamically_fresh = (datetime.now(timezone.utc) - datetime.fromisoformat(health[1])
                             <= timedelta(minutes=freshness_minutes))
    result["source_fresh"] = bool(result["source_fresh"] and dynamically_fresh)
    if not result["source_fresh"] and "SOURCE_STALE" not in result["flags"]:
        result["flags"].append("SOURCE_STALE")
    result["complete"] = bool(result["complete"] and result["source_fresh"]
                              and health and health[0] == "healthy")
    result["status"] = "complete" if result["complete"] else "incomplete"
    return jsonify(result)


@api.get("/pib/health")
def pib_health():
    with connect() as connection:
        health = connection.execute("SELECT * FROM pib_source_health WHERE source_key='pib'").fetchone()
        missing = connection.execute("""SELECT COUNT(*) FROM articles WHERE source_key='pib'
          AND LENGTH(TRIM(COALESCE(full_text,'')))=0""").fetchone()[0]
        metrics = connection.execute("""SELECT COUNT(*) total,
          SUM(CASE WHEN content_attempts>0 THEN 1 ELSE 0 END) attempted,
          SUM(CASE WHEN content_attempts>1 THEN content_attempts-1 ELSE 0 END) retry_attempts,
          SUM(CASE WHEN content_attempts>1 AND LENGTH(TRIM(COALESCE(full_text,'')))>0 THEN 1 ELSE 0 END) retry_successes
          FROM articles WHERE source_key='pib'""").fetchone()
        flags = connection.execute("""SELECT flag_type,severity,publication_date,prid,
          first_seen_at,last_seen_at,message,metadata_json FROM pib_flags
          WHERE resolved_at IS NULL ORDER BY severity DESC,last_seen_at DESC""").fetchall()
        latest = connection.execute("""SELECT publication_date,complete FROM pib_ingestion_runs
          ORDER BY publication_date DESC LIMIT 1""").fetchone()
    if not health:
        return jsonify({"source": "pib", "status": "not_initialized",
                        "dataset_health": "not_final", "flags": []}), 503
    health_dict = dict(health)
    last_success = health_dict.get("last_successful_discovery_at")
    age_minutes = None
    if last_success:
        age_minutes = max(0, int((datetime.now(timezone.utc) - datetime.fromisoformat(last_success)).total_seconds()/60))
    flag_items = []
    for row in flags:
        item = dict(row); item["metadata"] = json.loads(item.pop("metadata_json") or "{}")
        flag_items.append(item)
    total, attempted, retry_attempts, retry_successes = [value or 0 for value in metrics]
    hydration_status = "healthy" if missing == 0 else "degraded"
    discovery_status = health_dict["discovery_status"]
    dataset_health = "final" if latest and latest[1] and discovery_status == "healthy" else "not_final"
    return jsonify({"source": "pib", "status": "healthy" if dataset_health == "final" else "degraded",
      "discovery": {"status": discovery_status,
        "last_attempt_at": health_dict["last_discovery_attempt_at"],
        "last_success_at": last_success,
        "source_age_minutes": age_minutes,
        "listing_http_status": health_dict["listing_http_status"],
        "listing_error": health_dict["listing_error"],
        "consecutive_failures": health_dict["consecutive_discovery_failures"]},
      "hydration": {"status": hydration_status, "missing": missing,
        "last_success_at": health_dict["last_hydration_success_at"]},
      "circuit_breaker": {"state": health_dict["circuit_breaker_state"],
        "consecutive_structural_failures": health_dict["consecutive_structural_failures"]},
      "metrics": {"article_total": total, "attempted": attempted,
        "retry_attempts": retry_attempts, "retry_successes": retry_successes,
        "terminal_missing": missing}, "dataset_health": dataset_health, "flags": flag_items})


@api.post("/uploads/pdf")
def upload_pdf():
    upload = request.files.get("file")
    if not upload or not upload.filename:
        return jsonify({"error": "multipart field 'file' is required"}), 400
    data = upload.read(MAX_UPLOAD_BYTES + 1)
    if len(data) > MAX_UPLOAD_BYTES:
        return jsonify({"error": "PDF exceeds 25 MB limit"}), 413
    document_date = request.form.get("date")
    try:
        document_date = parse_iso_date(document_date)
    except ValueError:
        return jsonify({"error": "date must be YYYY-MM-DD"}), 400
    upload_root = Path(current_app.config["UPLOAD_DIRECTORY"])
    source_name = request.form.get("source", "Manual PDF upload").strip() or "Manual PDF upload"
    edition = request.form.get("edition", "").strip() or None
    reprocess = request.form.get("reprocess", "").strip().lower() in {"1", "true", "yes"}
    try:
        with connect() as connection:
            result = ingest_upload(connection, upload_root, data=data, filename=upload.filename,
                                   document_date=document_date, source_name=source_name,
                                   edition=edition, reprocess=reprocess)
    except PdfIngestionError as error:
        return jsonify({"error": str(error)}), 400
    if result.pop("duplicate"):
        return jsonify({"error": "PDF already uploaded", **result}), 409
    return jsonify(result), 201


@api.get("/uploads/pdf")
def list_pdf_uploads():
    requested_date = request.args.get("date")
    try:
        document_date = parse_iso_date(requested_date) if requested_date else None
    except ValueError:
        return jsonify({"error": "date must be YYYY-MM-DD"}), 400
    conditions, values = [], []
    if document_date:
        conditions.append("document_date=?"); values.append(document_date)
    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    with connect() as connection:
        rows = connection.execute(f"""SELECT id,original_filename,page_count,document_date,
          source_name,uploaded_at FROM uploaded_documents {where}
          ORDER BY document_date DESC,uploaded_at DESC LIMIT 500""", values).fetchall()
    items = [dict(row) | {"structured_url": f"/api/v1/uploads/pdf/{row['id']}"} for row in rows]
    return jsonify({"date": document_date, "count": len(items), "items": items})


@api.get("/uploads/pdf/<document_id>")
def get_pdf_upload(document_id: str):
    with connect() as connection:
        row = connection.execute("""SELECT d.id,d.original_filename,d.document_date,
          d.source_name,d.page_count,d.uploaded_at,r.raw_json
          FROM uploaded_documents d JOIN raw_events r ON r.id=d.raw_event_id
          WHERE d.id=?""", (document_id,)).fetchone()
    if not row:
        return jsonify({"error": "PDF document not found"}), 404
    envelope = json.loads(row["raw_json"])
    newspaper = envelope.get("payload", {}).get("newspaper")
    if not newspaper:
        return jsonify({"error": "This legacy PDF has no structured newspaper projection",
                        "document_id": document_id}), 409
    return jsonify({"document_id": row["id"], "original_filename": row["original_filename"],
                    "uploaded_at": row["uploaded_at"], "newspaper": newspaper})
