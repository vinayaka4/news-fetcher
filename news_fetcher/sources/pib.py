from __future__ import annotations

import hashlib
import json
import os
import random
import re
import sqlite3
import time
import uuid
from collections import Counter
from datetime import datetime, timedelta, timezone
from urllib.parse import urljoin

import requests
from bs4 import BeautifulSoup
from dateutil import parser as date_parser

from news_fetcher.core import Source, canonical_url, clean_text, normalize_title
from news_fetcher.raw_store import IST, build_envelope, store_raw_event

ALL_RELEASES_URL = "https://www.pib.gov.in/AllRelease.aspx?MenuId=3&lang=1&reg=3"
HEADERS = {
    "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
                  "(KHTML, like Gecko) Chrome/126.0 Safari/537.36",
    "Accept-Language": "en-IN,en;q=0.9", "Referer": "https://www.pib.gov.in/",
}
BOT_MARKERS = ("access denied", "captcha", "bot challenge", "cloudflare", "temporarily unavailable")


class PibContentError(RuntimeError):
    def __init__(self, code: str, message: str):
        super().__init__(message); self.code = code


def record_flag(connection, flag_type: str, severity: str, message: str,
                publication_date: str | None = None, prid: str | None = None,
                metadata: dict | None = None) -> None:
    timestamp = datetime.now(timezone.utc).isoformat()
    existing = connection.execute("""SELECT id FROM pib_flags WHERE flag_type=? AND source='pib'
      AND COALESCE(publication_date,'')=COALESCE(?,'') AND COALESCE(prid,'')=COALESCE(?,'')""",
      (flag_type, publication_date, prid)).fetchone()
    payload = json.dumps(metadata or {}, ensure_ascii=False, separators=(",", ":"))
    if existing:
        connection.execute("""UPDATE pib_flags SET severity=?,last_seen_at=?,resolved_at=NULL,
          message=?,metadata_json=? WHERE id=?""",
          (severity, timestamp, message, payload, existing[0]))
    else:
        connection.execute("""INSERT INTO pib_flags
          (id,flag_type,severity,source,publication_date,prid,first_seen_at,last_seen_at,
           resolved_at,message,metadata_json) VALUES (?,?,?,'pib',?,?,?,?,NULL,?,?)""",
          (str(uuid.uuid4()), flag_type, severity, publication_date, prid,
           timestamp, timestamp, message, payload))
    connection.commit()


def resolve_flag(connection, flag_type: str, publication_date: str | None = None,
                 prid: str | None = None) -> None:
    connection.execute("""UPDATE pib_flags SET resolved_at=?,last_seen_at=? WHERE flag_type=?
      AND source='pib' AND COALESCE(publication_date,'')=COALESCE(?,'')
      AND COALESCE(prid,'')=COALESCE(?,'') AND resolved_at IS NULL""",
      (datetime.now(timezone.utc).isoformat(), datetime.now(timezone.utc).isoformat(),
       flag_type, publication_date, prid))
    connection.commit()


def update_discovery_health(connection, *, status: str, http_status: int | None = None,
                            error: str | None = None, parse_healthy: bool = False,
                            success: bool = False) -> None:
    timestamp = datetime.now(timezone.utc).isoformat()
    connection.execute("""INSERT INTO pib_source_health
      (source_key,discovery_status,last_discovery_attempt_at,last_successful_discovery_at,
       listing_http_status,listing_error,consecutive_discovery_failures,
       listing_parse_healthy,updated_at) VALUES ('pib',?,?,?,?,?,?,?,?)
      ON CONFLICT(source_key) DO UPDATE SET discovery_status=excluded.discovery_status,
       last_discovery_attempt_at=excluded.last_discovery_attempt_at,
       last_successful_discovery_at=CASE WHEN ? THEN excluded.last_successful_discovery_at
         ELSE pib_source_health.last_successful_discovery_at END,
       listing_http_status=excluded.listing_http_status,listing_error=excluded.listing_error,
       consecutive_discovery_failures=CASE WHEN ? THEN 0
         ELSE pib_source_health.consecutive_discovery_failures+1 END,
       listing_parse_healthy=excluded.listing_parse_healthy,updated_at=excluded.updated_at""",
      (status, timestamp, timestamp if success else None, http_status, error,
       0 if success else 1, int(parse_healthy), timestamp, success, success))
    connection.commit()


def validate_listing(html: str, releases: list[dict[str, str | None]]) -> list[str]:
    soup = BeautifulSoup(html, "html.parser")
    links = soup.select("a[href*='PressReleseDetail'], a[href*='PressReleasePage']")
    flags = []
    if not links:
        flags.append("LISTING_EMPTY")
    if links and not releases:
        flags.append("LISTING_PARSE_ANOMALY")
    source_prids = [release_id(link.get("href", "")) for link in links]
    valid_source = [value for value in source_prids if value]
    if links and len(valid_source) / len(links) < 0.95:
        flags.append("LISTING_PARSE_ANOMALY")
    if len({release_id(str(item["url"])) for item in releases}) != len(releases):
        flags.append("LISTING_PARSE_ANOMALY")
    if len(releases) >= 10:
        dated = sum(bool(item.get("published_at")) for item in releases)
        if dated / len(releases) < 0.80:
            flags.append("LISTING_PARSE_ANOMALY")
    return sorted(set(flags))


def detail_url(value: str) -> str:
    absolute = urljoin("https://www.pib.gov.in/", value)
    match = re.search(r"[?&]PRID=(\d+)", absolute, re.I)
    return (f"https://www.pib.gov.in/PressReleasePage.aspx?PRID={match.group(1)}&lang=1&reg=3"
            if match else absolute)


def release_id(value: str) -> str | None:
    match = re.search(r"[?&]PRID=(\d+)", value, re.I)
    return match.group(1) if match else None


def parse_releases(html: str) -> list[dict[str, str | None]]:
    soup, releases, seen = BeautifulSoup(html, "html.parser"), [], set()
    for link in soup.select("a[href*='PressReleseDetail'], a[href*='PressReleasePage']"):
        container = link.find_parent("li")
        if not container:
            continue
        url, prid = detail_url(link.get("href", "")), release_id(detail_url(link.get("href", "")))
        if not prid or prid in seen:
            continue
        seen.add(prid)
        title = clean_text(link.get_text(" ", strip=True))
        if not title:
            continue
        release_list, ministry = container.find_parent("ul"), None
        if release_list:
            heading = container.find_previous("h3")
            if heading and heading.find_parent("ul") is release_list:
                ministry = clean_text(heading.get_text(" ", strip=True)) or None
        match = re.search(r"Posted\s+on:\s*(\d{1,2}\s+[A-Za-z]{3}\s+\d{4})",
                          container.get_text(" ", strip=True), re.I)
        published_at = (date_parser.parse(match.group(1)).replace(tzinfo=timezone.utc).isoformat()
                        if match else None)
        releases.append({"title": title, "url": url, "published_at": published_at,
                         "ministry": ministry})
    return releases


def parse_content(html: str) -> str:
    soup = BeautifulSoup(html, "html.parser")
    content = (soup.select_one(".innner-page-main-about-us-content-right-part")
               or soup.select_one(".content-area") or soup.select_one("#content")
               or soup.select_one("main") or soup.select_one("article"))
    if not content:
        return ""
    for node in content.select("script, style, nav, form, .share, .social-media"):
        node.decompose()
    return clean_text(content.get_text("\n", strip=True))


def fetch_content(url: str) -> str:
    response = requests.get(url, timeout=20, headers=HEADERS)
    response.raise_for_status()
    lowered = response.text.lower()
    if any(marker in lowered for marker in BOT_MARKERS):
        raise PibContentError("BOT_CHALLENGE_DETECTED", "PIB returned an access/challenge page")
    content = parse_content(response.text)
    if not content:
        raise PibContentError("DETAIL_PARSE_FAILED", "PIB content container was not recognized")
    minimum = int(os.getenv("PIB_MIN_CONTENT_CHARS", "120"))
    if len(content) < minimum:
        raise PibContentError("CONTENT_VALIDATION_FAILED",
                              f"PIB content was shorter than {minimum} characters")
    prid = release_id(url)
    if prid and prid not in response.url and f"PRID={prid}" not in response.text:
        raise PibContentError("CONTENT_VALIDATION_FAILED", "PIB response did not match requested PRID")
    return content


def retry_delay(error: Exception, attempt: int, base: float) -> float:
    if isinstance(error, requests.HTTPError) and error.response is not None:
        status = error.response.status_code
        if status == 429:
            try:
                return min(float(error.response.headers.get("Retry-After", 0)), 900) or 60
            except ValueError:
                return 60
        if status == 403:
            return min(300 * attempt, 1800)
        if status == 404:
            return min(120 * attempt, 900)
    return min(base * (2 ** max(attempt - 1, 0)) + random.uniform(0, max(base, 0.1)), 900)


def hydrate_missing(connection: sqlite3.Connection, publication_dates: set[str] | None = None,
                    retries: int | None = None, delay: float | None = None,
                    limit: int = 0) -> tuple[int, int]:
    """Use empty full_text rows as the durable PIB work list."""
    retries = retries or int(os.getenv("PIB_CONTENT_RETRIES", "5"))
    delay = float(os.getenv("PIB_RETRY_DELAY", "0.25")) if delay is None else delay
    conditions = ["source_key='pib'", "LENGTH(TRIM(COALESCE(full_text,'')))=0"]
    values: list[str] = []
    if publication_dates:
        placeholders = ",".join("?" for _ in publication_dates)
        conditions.append(f"substr(published_at,1,10) IN ({placeholders})")
        values.extend(sorted(publication_dates))
    query = f"SELECT id,article_url FROM articles WHERE {' AND '.join(conditions)} ORDER BY published_at DESC"
    if limit > 0:
        query += " LIMIT ?"
        values.append(limit)
    rows = connection.execute(query, values).fetchall()
    saved = failed = 0
    structural_failures = 0
    circuit_threshold = int(os.getenv("PIB_CIRCUIT_BREAKER_THRESHOLD", "5"))
    for article_id, url in rows:
        text, last_error = "", "empty content"
        for attempt in range(1, retries + 1):
            attempted_at = datetime.now(timezone.utc).isoformat()
            caught: Exception | None = None
            try:
                text = fetch_content(url)
                if text:
                    break
                last_error = "page contained no recognizable article text"
            except (requests.RequestException, PibContentError) as error:
                caught = error
                last_error = f"{type(error).__name__}: {error}"
                if isinstance(error, PibContentError):
                    structural_failures += 1
                    record_flag(connection, error.code, "critical" if error.code == "BOT_CHALLENGE_DETECTED" else "warning",
                                str(error), str(connection.execute(
                                    "SELECT substr(published_at,1,10) FROM articles WHERE id=?", (article_id,)).fetchone()[0]),
                                release_id(url), {"url": url, "attempt": attempt})
                else:
                    record_flag(connection, "DETAIL_FETCH_FAILED", "warning", str(error),
                                prid=release_id(url), metadata={"url": url, "attempt": attempt})
            connection.execute("""UPDATE articles SET content_attempts=content_attempts+1,
              last_content_attempt_at=?,content_last_error=? WHERE id=?""",
              (attempted_at, last_error[:2000], article_id))
            connection.commit()
            if structural_failures >= circuit_threshold:
                connection.execute("""UPDATE pib_source_health SET circuit_breaker_state='open',
                  consecutive_structural_failures=?,updated_at=? WHERE source_key='pib'""",
                  (structural_failures, datetime.now(timezone.utc).isoformat()))
                record_flag(connection, "CONTENT_SCHEMA_CHANGED", "critical",
                            "Detail parser circuit breaker opened after repeated structural failures",
                            metadata={"threshold": circuit_threshold})
                return saved, len(rows) - saved
            if attempt < retries and delay:
                time.sleep(retry_delay(caught or RuntimeError(last_error), attempt, delay))
        if text:
            connection.execute("""UPDATE articles SET full_text=?,content_attempts=content_attempts+1,
              last_content_attempt_at=?,content_last_error=NULL WHERE id=?""",
              (text, datetime.now(timezone.utc).isoformat(), article_id))
            connection.commit()
            saved += 1
            structural_failures = 0
            connection.execute("""INSERT INTO pib_source_health
              (source_key,discovery_status,listing_parse_healthy,circuit_breaker_state,
               consecutive_structural_failures,last_hydration_success_at,updated_at)
              VALUES ('pib','degraded',0,'closed',0,?,?)
              ON CONFLICT(source_key) DO UPDATE SET circuit_breaker_state='closed',
               consecutive_structural_failures=0,last_hydration_success_at=excluded.last_hydration_success_at,
               updated_at=excluded.updated_at""",
              (datetime.now(timezone.utc).isoformat(), datetime.now(timezone.utc).isoformat()))
            connection.commit()
        else:
            failed += 1
    return saved, failed


def refresh_audits(connection: sqlite3.Connection, expected: dict[str, int]) -> None:
    timestamp = datetime.now(timezone.utc).isoformat()
    health = connection.execute("""SELECT discovery_status,last_successful_discovery_at,
      listing_parse_healthy FROM pib_source_health WHERE source_key='pib'""").fetchone()
    freshness_minutes = int(os.getenv("PIB_SOURCE_FRESHNESS_MINUTES", "180"))
    last_success = datetime.fromisoformat(health[1]) if health and health[1] else None
    source_fresh = bool(last_success and datetime.now(timezone.utc) - last_success <= timedelta(minutes=freshness_minutes))
    discovery_verified = bool(health and health[0] == "healthy")
    parse_healthy = bool(health and health[2])
    for publication_date, expected_count in expected.items():
        discovered, ready = connection.execute("""SELECT COUNT(*),
          SUM(CASE WHEN LENGTH(TRIM(COALESCE(full_text,'')))>0 THEN 1 ELSE 0 END)
          FROM articles WHERE source_key='pib' AND substr(published_at,1,10)=?""",
          (publication_date,)).fetchone()
        ready = ready or 0
        # PIB can remove or re-date a PRID after we have archived it. Preserve
        # that article, and treat the listing count as a minimum completeness
        # requirement instead of making the date permanently impossible to
        # finalize because the durable archive contains an extra ready row.
        missing = max(expected_count - ready, 0)
        active_flags = [row[0] for row in connection.execute("""SELECT flag_type FROM pib_flags
          WHERE resolved_at IS NULL AND (publication_date=? OR publication_date IS NULL)
          AND severity IN ('warning','critical') ORDER BY flag_type""", (publication_date,)).fetchall()]
        complete = int(discovered >= expected_count and ready >= expected_count and missing == 0
                       and discovery_verified and parse_healthy and source_fresh and not active_flags)
        connection.execute("""INSERT INTO pib_ingestion_runs
          (publication_date,expected_count,discovered_count,ready_count,missing_count,
           complete,listing_fetched_at,updated_at,discovery_verified,listing_parse_healthy,
           source_fresh,flags_json) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)
          ON CONFLICT(publication_date) DO UPDATE SET expected_count=excluded.expected_count,
          discovered_count=excluded.discovered_count,ready_count=excluded.ready_count,
          missing_count=excluded.missing_count,complete=excluded.complete,
          listing_fetched_at=excluded.listing_fetched_at,updated_at=excluded.updated_at,
          discovery_verified=excluded.discovery_verified,
          listing_parse_healthy=excluded.listing_parse_healthy,source_fresh=excluded.source_fresh,
          flags_json=excluded.flags_json""",
          (publication_date, expected_count, discovered, ready, missing, complete, timestamp, timestamp,
           int(discovery_verified), int(parse_healthy), int(source_fresh), json.dumps(active_flags)))
    connection.commit()


def refresh_existing_audits(connection: sqlite3.Connection) -> None:
    expected = dict(connection.execute(
        "SELECT publication_date,expected_count FROM pib_ingestion_runs").fetchall())
    if expected:
        refresh_audits(connection, expected)


def mark_discovery_failure(connection, flag_type: str, error: Exception | str,
                           http_status: int | None = None) -> None:
    message = str(error)
    update_discovery_health(connection, status="failed", http_status=http_status,
                            error=message, parse_healthy=False, success=False)
    record_flag(connection, flag_type, "critical", message)
    connection.execute("""UPDATE pib_ingestion_runs SET complete=0,discovery_verified=0,
      source_fresh=0,updated_at=? WHERE publication_date=(SELECT MAX(publication_date)
      FROM pib_ingestion_runs)""", (datetime.now(timezone.utc).isoformat(),))
    connection.commit()


def count_anomalies(connection, expected: dict[str, int]) -> list[str]:
    threshold = float(os.getenv("PIB_COUNT_ANOMALY_PERCENT", "70")) / 100
    anomalies = []
    for publication_date, count in expected.items():
        recent = [row[0] for row in connection.execute("""SELECT expected_count
          FROM pib_ingestion_runs WHERE publication_date<? ORDER BY publication_date DESC LIMIT 7""",
          (publication_date,)).fetchall()]
        if len(recent) < 3:
            continue
        ordered = sorted(recent); median = ordered[len(ordered)//2]
        if median and abs(count - median) / median > threshold:
            anomalies.append(publication_date)
    return anomalies


def fetch_releases(connection: sqlite3.Connection, source: Source, user_agent: str) -> int:
    attempt_time = datetime.now(timezone.utc).isoformat()
    connection.execute("""INSERT INTO pib_source_health
      (source_key,discovery_status,last_discovery_attempt_at,listing_parse_healthy,updated_at)
      VALUES ('pib','degraded',?,0,?) ON CONFLICT(source_key) DO UPDATE SET
      discovery_status='degraded',last_discovery_attempt_at=excluded.last_discovery_attempt_at,
      updated_at=excluded.updated_at""", (attempt_time, attempt_time))
    connection.commit()
    try:
        response = requests.get(ALL_RELEASES_URL, timeout=30, headers=HEADERS)
        response.raise_for_status()
    except requests.RequestException as error:
        flag = "DNS_UNAVAILABLE" if isinstance(error, requests.ConnectionError) else "LISTING_FETCH_FAILED"
        mark_discovery_failure(connection, flag, error,
                               getattr(getattr(error, "response", None), "status_code", None))
        raise
    releases = parse_releases(response.text)
    listing_status = getattr(response, "status_code", getattr(response, "status", 200))
    structural_flags = validate_listing(response.text, releases)
    if structural_flags:
        flag = "LISTING_EMPTY" if "LISTING_EMPTY" in structural_flags else "LISTING_PARSE_ANOMALY"
        error = RuntimeError("PIB listing structural validation failed: " + ", ".join(structural_flags))
        mark_discovery_failure(connection, flag, error, listing_status)
        raise error
    releases.sort(key=lambda item: item.get("published_at") or "", reverse=True)
    expected = Counter(str(item["published_at"] or "")[:10] for item in releases if item["published_at"])
    anomalies = count_anomalies(connection, dict(expected))
    # A structurally valid HTTP 200 listing proves that transient transport and
    # parse failures have recovered, even when a date-specific volume warning
    # remains for operator review.
    resolve_flag(connection, "DNS_UNAVAILABLE"); resolve_flag(connection, "LISTING_FETCH_FAILED")
    resolve_flag(connection, "LISTING_EMPTY"); resolve_flag(connection, "LISTING_PARSE_ANOMALY")
    if anomalies:
        for publication_date in anomalies:
            record_flag(connection, "DISCOVERY_COUNT_ANOMALY", "warning",
                        "Discovery count deviates from the recent median", publication_date,
                        metadata={"count": expected[publication_date]})
        update_discovery_health(connection, status="healthy", http_status=listing_status,
                                parse_healthy=True, success=True)
    else:
        update_discovery_health(connection, status="healthy", http_status=listing_status,
                                parse_healthy=True, success=True)
    for publication_date in expected:
        if publication_date not in anomalies:
            resolve_flag(connection, "DISCOVERY_COUNT_ANOMALY", publication_date)
    saved = 0
    for release in releases:
        title, url = str(release["title"]), canonical_url(str(release["url"]))
        store_raw_event(connection, build_envelope(
            source_type="official_web", source_key=source.key, publisher=source.publisher,
            external_id=url, published_at=release["published_at"], payload=release,
            metadata={"index_url": ALL_RELEASES_URL}))
        prid = release_id(url)
        existing = connection.execute("""SELECT id FROM articles
          WHERE article_url=? OR (source_key='pib' AND article_url LIKE ?)""",
          (url, f"%PRID={prid}%" if prid else url)).fetchone()
        if existing:
            connection.execute("""UPDATE articles SET article_url=?,title=?,normalized_title=?,
              published_at=?,ministry=? WHERE id=?""", (url, title, normalize_title(title),
              release["published_at"], release["ministry"], existing[0]))
            continue
        if not title or not url:
            continue
        publication_date = str(release["published_at"] or "")[:10]
        previous_audit = connection.execute(
            "SELECT expected_count FROM pib_ingestion_runs WHERE publication_date=?",
            (publication_date,)).fetchone()
        if previous_audit and expected.get(publication_date, 0) > previous_audit[0]:
            record_flag(connection, "LATE_RELEASE_DETECTED", "info",
                        "PIB listing added one or more PRIDs after the prior audit",
                        publication_date, prid,
                        {"previous_expected": previous_audit[0], "new_expected": expected[publication_date]})
        if previous_audit and publication_date < datetime.now(IST).date().isoformat():
            record_flag(connection, "BACKDATED_RELEASE", "info",
                        "Newly discovered PRID belongs to an earlier publication date",
                        publication_date, prid)
        article_id = hashlib.sha256(url.encode()).hexdigest()
        collision = connection.execute("SELECT article_url FROM articles WHERE id=?", (article_id,)).fetchone()
        if collision and canonical_url(collision[0]) != url:
            repaired = hashlib.sha256(canonical_url(collision[0]).encode()).hexdigest()
            if connection.execute("SELECT 1 FROM articles WHERE id=?", (repaired,)).fetchone():
                repaired = hashlib.sha256((collision[0] + "#legacy").encode()).hexdigest()
            connection.execute("UPDATE articles SET id=? WHERE id=?", (repaired, article_id))
        connection.execute("""INSERT INTO articles
          (id,publisher,source_key,title,normalized_title,article_url,published_at,
           author,excerpt,ministry,full_text,fetched_at) VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
          (article_id, source.publisher, source.key, title, normalize_title(title), url,
           release["published_at"], None, "", release["ministry"], "",
           datetime.now(timezone.utc).isoformat()))
        saved += 1
    connection.commit()
    hydrate_missing(connection, set(expected))
    refresh_audits(connection, dict(expected))
    return saved
