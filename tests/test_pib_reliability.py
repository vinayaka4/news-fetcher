import sqlite3
from datetime import datetime, timezone

import pytest
import requests

from news_fetcher.core import Source
from news_fetcher.database import initialize
from news_fetcher.sources import pib


class Response:
    def __init__(self, text="", status=200, url="https://www.pib.gov.in/"):
        self.text, self.status_code, self.url, self.headers = text, status, url, {}

    def raise_for_status(self):
        if self.status_code >= 400:
            error = requests.HTTPError(f"HTTP {self.status_code}")
            error.response = self
            raise error


def source():
    return Source("pib", "Press Information Bureau", "unused")


def test_pib_dns_failure_preserves_count_and_marks_source_failed(monkeypatch):
    connection = sqlite3.connect(":memory:"); initialize(connection)
    connection.execute("""INSERT INTO pib_ingestion_runs
      (publication_date,expected_count,discovered_count,ready_count,missing_count,
       complete,listing_fetched_at,updated_at) VALUES ('2026-08-11',97,97,97,0,1,'x','x')""")
    monkeypatch.setattr(pib.requests, "get", lambda *a, **k: (_ for _ in ()).throw(
        requests.ConnectionError("DNS unavailable")))
    with pytest.raises(requests.ConnectionError):
        pib.fetch_releases(connection, source(), "agent")
    assert connection.execute(
        "SELECT expected_count,complete FROM pib_ingestion_runs").fetchone() == (97, 0)
    assert connection.execute(
        "SELECT discovery_status,consecutive_discovery_failures FROM pib_source_health").fetchone() == ("failed", 1)
    assert connection.execute(
        "SELECT flag_type FROM pib_flags WHERE resolved_at IS NULL").fetchone()[0] == "DNS_UNAVAILABLE"


def test_empty_listing_is_not_success(monkeypatch):
    connection = sqlite3.connect(":memory:"); initialize(connection)
    monkeypatch.setattr(pib.requests, "get", lambda *a, **k: Response("<html><body>PIB</body></html>"))
    with pytest.raises(RuntimeError):
        pib.fetch_releases(connection, source(), "agent")
    assert connection.execute("SELECT discovery_status FROM pib_source_health").fetchone()[0] == "failed"
    assert connection.execute("SELECT flag_type FROM pib_flags").fetchone()[0] == "LISTING_EMPTY"


def test_partial_listing_structure_is_rejected(monkeypatch):
    links = "".join(
        f'<li><a href="/PressReleasePage.aspx?PRID={100+i if i < 5 else "bad"}">Release {i}</a> Posted on: 11 Aug 2026</li>'
        for i in range(10))
    html = f'<ul class="num"><h3>Ministry</h3>{links}</ul>'
    connection = sqlite3.connect(":memory:"); initialize(connection)
    monkeypatch.setattr(pib.requests, "get", lambda *a, **k: Response(html))
    with pytest.raises(RuntimeError):
        pib.fetch_releases(connection, source(), "agent")
    assert connection.execute("SELECT flag_type FROM pib_flags").fetchone()[0] == "LISTING_PARSE_ANOMALY"


def test_http_200_access_denied_is_not_full_text(monkeypatch):
    monkeypatch.setattr(pib.requests, "get", lambda *a, **k: Response(
        "<html><main>Access Denied - CAPTCHA challenge</main></html>",
        url="https://www.pib.gov.in/PressReleasePage.aspx?PRID=123"))
    with pytest.raises(pib.PibContentError) as caught:
        pib.fetch_content("https://www.pib.gov.in/PressReleasePage.aspx?PRID=123")
    assert caught.value.code == "BOT_CHALLENGE_DETECTED"


def test_structural_failures_open_detail_circuit(monkeypatch):
    connection = sqlite3.connect(":memory:"); initialize(connection)
    connection.execute("""INSERT INTO pib_source_health
      (source_key,discovery_status,listing_parse_healthy,updated_at)
      VALUES ('pib','healthy',1,'now')""")
    for number in range(5):
        connection.execute("""INSERT INTO articles
          (id,publisher,source_key,title,normalized_title,article_url,published_at,full_text,fetched_at)
          VALUES (?,?,?,?,?,?,?,?,?)""", (str(number),"PIB","pib",f"Title {number}",f"title {number}",
          f"https://www.pib.gov.in/PressReleasePage.aspx?PRID={number}","2026-08-11","","now"))
    connection.commit()
    monkeypatch.setattr(pib, "fetch_content", lambda url: (_ for _ in ()).throw(
        pib.PibContentError("DETAIL_PARSE_FAILED", "selector missing")))
    saved, failed = pib.hydrate_missing(connection, retries=1, delay=0)
    assert (saved, failed) == (0, 5)
    assert connection.execute(
        "SELECT circuit_breaker_state FROM pib_source_health").fetchone()[0] == "open"
    assert connection.execute(
        "SELECT COUNT(*) FROM pib_flags WHERE flag_type='CONTENT_SCHEMA_CHANGED'").fetchone()[0] == 1


def test_date_anomaly_does_not_block_other_complete_dates():
    connection = sqlite3.connect(":memory:"); initialize(connection)
    now = datetime.now(timezone.utc).isoformat()
    connection.execute("""INSERT INTO pib_source_health
      (source_key,discovery_status,last_successful_discovery_at,listing_parse_healthy,updated_at)
      VALUES ('pib','healthy',?,1,?)""", (now, now))
    for date, count in (("2026-08-11", 1), ("2026-08-12", 1)):
        connection.execute("""INSERT INTO articles
          (id,publisher,source_key,title,normalized_title,article_url,published_at,full_text,fetched_at)
          VALUES (?,?,?,?,?,?,?,?,?)""", (date,"PIB","pib",date,date,
          f"https://www.pib.gov.in/PressReleasePage.aspx?PRID={date[-2:]}",date,"full text",now))
    pib.record_flag(connection, "DISCOVERY_COUNT_ANOMALY", "warning", "unusual count", "2026-08-11")
    pib.refresh_audits(connection, {"2026-08-11": 1, "2026-08-12": 1})
    assert connection.execute("SELECT complete FROM pib_ingestion_runs WHERE publication_date='2026-08-11'").fetchone()[0] == 0
    assert connection.execute("SELECT complete FROM pib_ingestion_runs WHERE publication_date='2026-08-12'").fetchone()[0] == 1


def test_preserved_ready_release_above_current_listing_count_can_finalize():
    connection = sqlite3.connect(":memory:"); initialize(connection)
    now = datetime.now(timezone.utc).isoformat()
    connection.execute("""INSERT INTO pib_source_health
      (source_key,discovery_status,last_successful_discovery_at,listing_parse_healthy,updated_at)
      VALUES ('pib','healthy',?,1,?)""", (now, now))
    for number in range(2):
        connection.execute("""INSERT INTO articles
          (id,publisher,source_key,title,normalized_title,article_url,published_at,full_text,fetched_at)
          VALUES (?,?,?,?,?,?,?,?,?)""", (str(number),"PIB","pib",str(number),str(number),
          f"https://www.pib.gov.in/PressReleasePage.aspx?PRID={number}","2026-08-11","full text",now))
    pib.refresh_audits(connection, {"2026-08-11": 1})
    assert connection.execute("""SELECT expected_count,discovered_count,ready_count,complete
      FROM pib_ingestion_runs""").fetchone() == (1, 2, 2, 1)


def test_valid_listing_resolves_transient_dns_flag_even_with_count_warning(monkeypatch):
    connection = sqlite3.connect(":memory:"); initialize(connection)
    pib.record_flag(connection, "DNS_UNAVAILABLE", "critical", "old DNS failure")
    html = """<ul class='num'><h3>Ministry</h3><li>
      <a href='/PressReleasePage.aspx?PRID=501'>Release</a>
      Posted on: 12 Aug 2026</li></ul>"""
    monkeypatch.setattr(pib.requests, "get", lambda *a, **k: Response(html))
    monkeypatch.setattr(pib, "count_anomalies", lambda *a, **k: ["2026-08-12"])
    monkeypatch.setattr(pib, "fetch_content", lambda url: "complete official release text")
    assert pib.fetch_releases(connection, source(), "agent") == 1
    assert connection.execute("""SELECT COUNT(*) FROM pib_flags
      WHERE flag_type='DNS_UNAVAILABLE' AND resolved_at IS NULL""").fetchone()[0] == 0
    assert connection.execute(
        "SELECT discovery_status FROM pib_source_health WHERE source_key='pib'").fetchone()[0] == "healthy"
