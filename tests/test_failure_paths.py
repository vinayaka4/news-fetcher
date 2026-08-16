import io
import json
import sqlite3
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest
import requests
from pypdf import PdfWriter

import dashboard
import perplexity_ingest
import scheduled_run
import queue_worker
from news_fetcher.cli import Source, fetch_pib_releases, fetch_source, initialize
from news_fetcher.raw_store import build_envelope, initialize_raw_store, store_raw_event
from news_fetcher.job_queue import claim_next, complete, enqueue, fail, initialize_jobs


class Response:
    def __init__(self, content=b"", text="", status=200):
        self.content = content
        self.text = text or content.decode("utf-8", errors="ignore")
        self.status = status

    def raise_for_status(self):
        if self.status >= 400:
            raise requests.HTTPError(f"HTTP {self.status}")

    def json(self):
        return json.loads(self.text)


RSS_XML = b"""<?xml version="1.0"?><rss version="2.0"><channel>
<title>Test feed</title><item><title>Policy reform announced</title>
<link>https://example.com/policy?utm_source=test</link>
<pubDate>Sun, 09 Aug 2026 04:00:00 GMT</pubDate><description>Feed excerpt</description>
</item></channel></rss>"""


def test_rss_success_dumps_raw_and_normalized_rows(monkeypatch):
    monkeypatch.setattr("news_fetcher.sources.rss.requests.get", lambda *args, **kwargs: Response(RSS_XML))
    source = Source("test_rss", "Test Publisher", "https://example.com/feed")
    connection = sqlite3.connect(":memory:")
    initialize(connection)
    assert fetch_source(connection, source, "test-agent") == 1
    assert connection.execute("SELECT COUNT(1) FROM articles").fetchone()[0] == 1
    raw = connection.execute("SELECT raw_json FROM raw_events").fetchone()[0]
    assert json.loads(raw)["payload"]["title"] == "Policy reform announced"
    assert connection.execute("SELECT article_url FROM articles").fetchone()[0] == "https://example.com/policy"


def test_rss_network_failure_writes_no_partial_rows(monkeypatch):
    monkeypatch.setattr("news_fetcher.sources.rss.requests.get",
                        lambda *args, **kwargs: (_ for _ in ()).throw(requests.ConnectionError("offline")))
    connection = sqlite3.connect(":memory:")
    initialize(connection)
    with pytest.raises(requests.ConnectionError):
        fetch_source(connection, Source("test", "Test", "https://example.com/feed"), "agent")
    assert connection.execute("SELECT COUNT(1) FROM articles").fetchone()[0] == 0
    assert connection.execute("SELECT COUNT(1) FROM raw_events").fetchone()[0] == 0


def test_malformed_rss_is_rejected_without_database_pollution(monkeypatch):
    monkeypatch.setattr("news_fetcher.sources.rss.requests.get", lambda *args, **kwargs: Response(b"not xml"))
    connection = sqlite3.connect(":memory:")
    initialize(connection)
    with pytest.raises(RuntimeError):
        fetch_source(connection, Source("bad", "Bad feed", "https://example.com/bad"), "agent")
    assert connection.execute("SELECT COUNT(1) FROM raw_events").fetchone()[0] == 0


def test_pib_detail_failure_keeps_listing_metadata_and_raw_event(monkeypatch):
    html = """<ul class="num"><h3>Ministry of Finance</h3><li>
      <a href="/PressReleseDetail.aspx?PRID=555">Official budget release</a>
      Posted on: 09 Aug 2026</li></ul>"""
    monkeypatch.setattr("news_fetcher.sources.pib.requests.get",
                        lambda *args, **kwargs: Response(text=html))
    monkeypatch.setattr("news_fetcher.sources.pib.fetch_content",
                        lambda url: (_ for _ in ()).throw(requests.ConnectionError("detail offline")))
    connection = sqlite3.connect(":memory:")
    initialize(connection)
    saved = fetch_pib_releases(connection, Source("pib", "PIB", "unused"), "agent")
    assert saved == 1
    row = connection.execute("SELECT title, ministry, full_text FROM articles").fetchone()
    assert row == ("Official budget release", "Ministry of Finance", "")
    assert connection.execute("SELECT COUNT(1) FROM raw_events WHERE source_key='pib'").fetchone()[0] == 1
    assert connection.execute("""SELECT expected_count,discovered_count,ready_count,
      missing_count,complete FROM pib_ingestion_runs""").fetchone() == (1, 1, 0, 1, 0)


def test_pib_parser_handles_nested_ministry_and_all_release_links(monkeypatch):
    html = """<div><ul class="alternate-layout"><div><h3>Ministry of Education</h3></div>
      <li><span><a href="/PressReleasePage.aspx?PRID=601">Education release</a></span>
      Posted on: 08 Aug 2026</li></ul></div>"""
    monkeypatch.setattr("news_fetcher.sources.pib.requests.get", lambda *args, **kwargs: Response(text=html))
    monkeypatch.setattr("news_fetcher.sources.pib.fetch_content", lambda url: "Complete official text")
    connection = sqlite3.connect(":memory:")
    initialize(connection)
    assert fetch_pib_releases(connection, Source("pib", "PIB", "unused"), "agent") == 1
    assert connection.execute("SELECT ministry, full_text FROM articles").fetchone() == (
        "Ministry of Education", "Complete official text")


def test_pib_detail_failure_does_not_prevent_later_content(monkeypatch):
    html = """<ul class="num"><h3>Ministry of Education</h3>
      <li><a href="/PressReleasePage.aspx?PRID=701">First education policy</a> Posted on: 08 Aug 2026</li>
      <li><a href="/PressReleasePage.aspx?PRID=702">Second education policy</a> Posted on: 08 Aug 2026</li>
      </ul>"""
    monkeypatch.setattr("news_fetcher.sources.pib.requests.get", lambda *args, **kwargs: Response(text=html))
    calls = []
    def detail(url):
        calls.append(url)
        if "701" in url:
            raise requests.ConnectionError("one page failed")
        return "Second release full text"
    monkeypatch.setattr("news_fetcher.sources.pib.fetch_content", detail)
    monkeypatch.setenv("PIB_CONTENT_RETRIES", "1")
    connection = sqlite3.connect(":memory:")
    initialize(connection)
    assert fetch_pib_releases(connection, Source("pib", "PIB", "unused"), "agent") == 2
    assert len(calls) == 2
    assert connection.execute("SELECT full_text FROM articles WHERE article_url LIKE '%702%'").fetchone()[0] == "Second release full text"


def test_pib_similar_titles_with_different_prids_are_not_deduplicated(monkeypatch):
    html = """<ul class="num"><h3>Ministry of Education</h3>
      <li><a href="/PressReleasePage.aspx?PRID=801">National education programme launched</a></li>
      <li><a href="/PressReleasePage.aspx?PRID=802">National education program launched</a></li>
      </ul>"""
    monkeypatch.setattr("news_fetcher.sources.pib.requests.get", lambda *args, **kwargs: Response(text=html))
    monkeypatch.setattr("news_fetcher.sources.pib.fetch_content", lambda url: "text")
    connection = sqlite3.connect(":memory:")
    initialize(connection)
    assert fetch_pib_releases(connection, Source("pib", "PIB", "unused"), "agent") == 2
    assert connection.execute("SELECT COUNT(*) FROM articles").fetchone()[0] == 2


def test_pib_exact_same_title_and_date_preserves_every_prid(monkeypatch):
    html = """<ul class="num"><h3>Prime Minister's Office</h3>
      <li><a href="/PressReleasePage.aspx?PRID=811">Same official address</a> Posted on: 08 Aug 2026</li>
      <li><a href="/PressReleasePage.aspx?PRID=812">Same official address</a> Posted on: 08 Aug 2026</li>
      </ul>"""
    monkeypatch.setattr("news_fetcher.sources.pib.requests.get", lambda *args, **kwargs: Response(text=html))
    monkeypatch.setattr("news_fetcher.sources.pib.fetch_content", lambda url: "text")
    connection = sqlite3.connect(":memory:")
    initialize(connection)
    assert fetch_pib_releases(connection, Source("pib", "PIB", "unused"), "agent") == 2
    assert connection.execute("SELECT COUNT(*) FROM articles").fetchone()[0] == 2
    assert connection.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0] == 2
    assert connection.execute("""SELECT expected_count,discovered_count,ready_count,
      missing_count,complete FROM pib_ingestion_runs""").fetchone() == (2, 2, 2, 0, 1)


def test_pib_repairs_legacy_url_derived_id_collision(monkeypatch):
    html = """<ul class="num"><h3>Ministry of Education</h3><li>
      <a href="/PressReleasePage.aspx?PRID=901">Missing education release</a></li></ul>"""
    monkeypatch.setattr("news_fetcher.sources.pib.requests.get", lambda *args, **kwargs: Response(text=html))
    monkeypatch.setattr("news_fetcher.sources.pib.fetch_content", lambda url: "text")
    connection = sqlite3.connect(":memory:")
    initialize(connection)
    import hashlib
    old_url = "https://www.pib.gov.in/PressReleasePage.aspx?PRID=901&lang=1&reg=3"
    moved_url = "https://www.pib.gov.in/PressReleasePage.aspx?PRID=999&lang=1&reg=3"
    stale_id = hashlib.sha256(old_url.encode()).hexdigest()
    connection.execute("""INSERT INTO articles
      (id,publisher,source_key,title,normalized_title,article_url,fetched_at)
      VALUES (?,?,?,?,?,?,?)""", (stale_id,"PIB","pib","Other release","other release",moved_url,"now"))
    assert fetch_pib_releases(connection, Source("pib", "PIB", "unused"), "agent") == 1
    assert connection.execute("SELECT COUNT(*) FROM articles").fetchone()[0] == 2


def test_raw_store_is_idempotent_but_preserves_changed_payload():
    connection = sqlite3.connect(":memory:")
    initialize_raw_store(connection)
    base = dict(source_type="rss", source_key="feed", publisher="Paper",
                external_id="item-1", published_at="2026-08-09T00:00:00+00:00")
    _, first = store_raw_event(connection, build_envelope(payload={"title": "Version 1"}, **base))
    _, duplicate = store_raw_event(connection, build_envelope(payload={"title": "Version 1"}, **base))
    _, changed = store_raw_event(connection, build_envelope(payload={"title": "Version 2"}, **base))
    assert (first, duplicate, changed) == (True, False, True)
    assert connection.execute("SELECT COUNT(1) FROM raw_events").fetchone()[0] == 2


def test_perplexity_http_failure_is_reported(monkeypatch):
    monkeypatch.setattr(perplexity_ingest.requests, "post",
                        lambda *args, **kwargs: Response(status=503))
    with pytest.raises(requests.HTTPError):
        perplexity_ingest.request_digest("key", "2026-08-09")


def make_pdf() -> bytes:
    stream = io.BytesIO()
    writer = PdfWriter()
    writer.add_blank_page(width=612, height=792)
    writer.write(stream)
    return stream.getvalue()


def test_pdf_rejects_corrupt_file_and_duplicate(tmp_path):
    database = tmp_path / "pdf.db"
    connection = sqlite3.connect(database); initialize(connection); connection.close()
    dashboard.app.config.update(NEWS_DATABASE=str(database), UPLOAD_DIRECTORY=str(tmp_path / "uploads"))
    client = dashboard.app.test_client()
    bad = client.post("/api/v1/uploads/pdf",
                      data={"file": (io.BytesIO(b"not a pdf"), "fake.pdf")},
                      content_type="multipart/form-data")
    assert bad.status_code == 400
    pdf = make_pdf()
    first = client.post("/api/v1/uploads/pdf",
                        data={"file": (io.BytesIO(pdf), "paper.pdf"), "date": "2026-08-09"},
                        content_type="multipart/form-data")
    second = client.post("/api/v1/uploads/pdf",
                         data={"file": (io.BytesIO(pdf), "paper-again.pdf"), "date": "2026-08-09"},
                         content_type="multipart/form-data")
    assert first.status_code == 201
    assert second.status_code == 409
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(1) FROM uploaded_documents").fetchone()[0] == 1


def test_news_api_rejects_bad_parameters(tmp_path):
    dashboard.app.config["NEWS_DATABASE"] = str(tmp_path / "api.db")
    response = dashboard.app.test_client().get("/api/v1/news?date=not-a-date&limit=nope")
    assert response.status_code == 400


def test_articles_api_exposes_full_text_and_status(tmp_path):
    database = tmp_path / "articles.db"
    connection = sqlite3.connect(database)
    initialize(connection)
    connection.execute("""INSERT INTO articles
      (id,publisher,source_key,title,normalized_title,article_url,published_at,
       full_text,fetched_at) VALUES (?,?,?,?,?,?,?,?,?)""",
      ("article-1", "Press Information Bureau", "pib", "Education policy",
       "education policy", "https://pib.gov.in/?PRID=1", "2026-08-09T00:00:00+00:00",
       "Complete official release text", "2026-08-09T01:00:00+00:00"))
    connection.commit(); connection.close()
    dashboard.app.config["NEWS_DATABASE"] = str(database)
    response = dashboard.app.test_client().get(
        "/api/v1/articles?date=2026-08-09&source=pib&content_status=ready")
    assert response.status_code == 200
    payload = response.get_json()
    assert payload["count"] == 1
    assert payload["items"][0]["full_text"] == "Complete official release text"
    assert payload["items"][0]["content_status"] == "ready"


def test_articles_api_rejects_invalid_content_status(tmp_path):
    database = tmp_path / "articles-invalid.db"
    connection = sqlite3.connect(database); initialize(connection); connection.close()
    dashboard.app.config["NEWS_DATABASE"] = str(database)
    response = dashboard.app.test_client().get(
        "/api/v1/articles?date=2026-08-09&content_status=broken")
    assert response.status_code == 400


def test_pib_completeness_api_exposes_count_gate(tmp_path):
    database = tmp_path / "pib-completeness.db"
    connection = sqlite3.connect(database); initialize(connection)
    now = datetime.now(timezone.utc).isoformat()
    connection.execute("""INSERT INTO pib_source_health
      (source_key,last_discovery_attempt_at,last_successful_discovery_at,
       discovery_status,consecutive_discovery_failures,circuit_breaker_state,
       consecutive_structural_failures,updated_at)
      VALUES (?,?,?,?,?,?,?,?)""",
      ("pib", now, now, "healthy", 0, "closed", 0, now))
    connection.execute("""INSERT INTO pib_ingestion_runs
      (publication_date,expected_count,discovered_count,ready_count,missing_count,
       complete,listing_fetched_at,updated_at,discovery_verified,
       listing_parse_healthy,source_fresh) VALUES (?,?,?,?,?,?,?,?,?,?,?)""",
      ("2026-08-11", 31, 31, 31, 0, 1, now, now, 1, 1, 1))
    connection.commit(); connection.close()
    dashboard.app.config["NEWS_DATABASE"] = str(database)
    payload = dashboard.app.test_client().get(
        "/api/v1/pib/completeness?date=2026-08-11").get_json()
    assert payload["expected_count"] == payload["ready_count"] == 31
    assert payload["complete"] is True and payload["status"] == "complete"


def test_browser_pib_api_returns_full_text_json(tmp_path):
    database = tmp_path / "browser-pib.db"
    connection = sqlite3.connect(database); initialize(connection)
    connection.execute("""INSERT INTO articles
      (id,publisher,source_key,title,normalized_title,article_url,published_at,
       full_text,fetched_at) VALUES (?,?,?,?,?,?,?,?,?)""",
      ("pib-1","Press Information Bureau","pib","Official release","official release",
       "https://pib.gov.in/?PRID=1","2026-08-11T00:00:00+00:00","Complete PIB text","now"))
    connection.execute("""INSERT INTO pib_ingestion_runs
      (publication_date,expected_count,discovered_count,ready_count,missing_count,
       complete,listing_fetched_at,updated_at) VALUES ('2026-08-11',1,1,1,0,1,'now','now')""")
    connection.commit(); connection.close()
    dashboard.app.config.update(NEWS_DATABASE=str(database), DATABASE_URL=None)
    payload = dashboard.app.test_client().get("/api/v1/pib?date=2026-08-11").get_json()
    assert payload["complete"] is True and payload["count"] == 1
    assert payload["items"][0]["full_text"] == "Complete PIB text"
    assert payload["items"][0]["content_status"] == "ready"


def test_pib_health_api_separates_discovery_and_hydration(tmp_path):
    database = tmp_path / "pib-health.db"
    connection = sqlite3.connect(database); initialize(connection)
    connection.execute("""INSERT INTO pib_source_health
      (source_key,discovery_status,last_discovery_attempt_at,last_successful_discovery_at,
       listing_parse_healthy,circuit_breaker_state,updated_at)
      VALUES ('pib','healthy','2026-08-11T00:00:00+00:00','2026-08-11T00:00:00+00:00',
      1,'closed','2026-08-11T00:00:00+00:00')""")
    connection.execute("""INSERT INTO pib_ingestion_runs
      (publication_date,expected_count,discovered_count,ready_count,missing_count,
       complete,listing_fetched_at,updated_at) VALUES ('2026-08-11',1,1,1,0,1,'now','now')""")
    connection.commit(); connection.close()
    dashboard.app.config.update(NEWS_DATABASE=str(database), DATABASE_URL=None)
    payload = dashboard.app.test_client().get("/api/v1/pib/health").get_json()
    assert payload["discovery"]["status"] == "healthy"
    assert payload["hydration"]["missing"] == 0
    assert payload["circuit_breaker"]["state"] == "closed"


def test_rss_excerpt_is_consumer_ready_content(tmp_path):
    database = tmp_path / "rss-api.db"
    connection = sqlite3.connect(database); initialize(connection)
    connection.execute("""INSERT INTO articles
      (id,publisher,source_key,title,normalized_title,article_url,published_at,excerpt,fetched_at)
      VALUES (?,?,?,?,?,?,?,?,?)""", ("rss-1","Paper","feed","Headline","headline",
      "https://example.com/1","2026-08-09T00:00:00+00:00","Licensed feed excerpt","now"))
    connection.commit(); connection.close()
    dashboard.app.config["NEWS_DATABASE"] = str(database)
    item = dashboard.app.test_client().get(
      "/api/v1/articles?date=2026-08-09&source=feed&content_status=ready").get_json()["items"][0]
    assert item["content_status"] == "ready"
    assert item["content_kind"] == "rss_excerpt"
    assert item["content_text"] == "Licensed feed excerpt"


def test_perplexity_digest_api_returns_structured_json(tmp_path):
    database = tmp_path / "digest-api.db"
    connection = sqlite3.connect(database); initialize(connection)
    connection.execute("""INSERT INTO digest_stories
      (id,headline,normalized_title,category,summary_json,upsc_relevance,
       source_urls_json,published_at,details_json,fetched_at)
      VALUES (?,?,?,?,?,?,?,?,?,?)""", ("d1","Policy","policy","Polity",'["One","Two","Three"]',
      "GS2",'["https://example.com"]',"2026-08-09",'{"background":"Context"}',"now"))
    connection.commit(); connection.close()
    dashboard.app.config["NEWS_DATABASE"] = str(database)
    item = dashboard.app.test_client().get("/api/v1/digests?date=2026-08-09").get_json()["items"][0]
    assert item["content_status"] == "ready"
    assert item["content_kind"] == "structured_summary"
    assert item["summary"] == ["One", "Two", "Three"]


def test_scheduler_records_failed_pipeline(monkeypatch, tmp_path):
    database = tmp_path / "runs.db"
    monkeypatch.setattr(scheduled_run.subprocess, "run", lambda *args, **kwargs: SimpleNamespace(returncode=7))
    assert scheduled_run.main(["--force", "--database", str(database)]) == 7
    with sqlite3.connect(database) as connection:
        status, message = connection.execute("SELECT status, message FROM pipeline_runs").fetchone()
    assert status == "failed"
    assert message == "exit_code=7"


def test_job_queue_is_idempotent_and_completes():
    connection = sqlite3.connect(":memory:")
    initialize_jobs(connection)
    first = enqueue(connection, job_key="source:pib:2026-08-09", job_type="fetch_source",
                    source_key="pib", payload={"date": "2026-08-09"})
    second = enqueue(connection, job_key="source:pib:2026-08-09", job_type="fetch_source",
                     source_key="pib", payload={"date": "2026-08-09"})
    connection.commit()
    assert first == second
    assert connection.execute("SELECT COUNT(*) FROM ingestion_jobs").fetchone()[0] == 1
    job = claim_next(connection)
    assert job["status"] == "running" and job["attempts"] == 1
    complete(connection, job["id"])
    assert connection.execute("SELECT status FROM ingestion_jobs").fetchone()[0] == "complete"


def test_failed_job_is_persisted_for_retry():
    connection = sqlite3.connect(":memory:")
    initialize_jobs(connection)
    enqueue(connection, job_key="perplexity:2026-08-09", job_type="fetch_perplexity",
            source_key="perplexity", payload={"date": "2026-08-09"})
    connection.commit()
    job = claim_next(connection)
    fail(connection, job["id"], "temporary upstream outage")
    status, attempts, error, next_attempt = connection.execute(
        "SELECT status,attempts,last_error,next_attempt_at FROM ingestion_jobs").fetchone()
    assert status == "retry" and attempts == 1
    assert error == "temporary upstream outage" and next_attempt


def test_worker_rolls_back_source_transaction_before_recording_retry(tmp_path, monkeypatch):
    database = tmp_path / "worker-rollback.db"
    with sqlite3.connect(database) as connection:
        initialize(connection)
        enqueue(connection, job_key="source:test:2026-08-12", job_type="fetch_source",
                source_key="test", payload={})
        connection.commit()

    def partially_write_then_fail(connection, job):
        connection.execute("""INSERT INTO articles
          (id,publisher,source_key,title,normalized_title,article_url,fetched_at)
          VALUES ('partial','Test','test','Partial','partial','https://example.com/partial','now')""")
        raise RuntimeError("source failed")

    monkeypatch.setattr(queue_worker, "execute", partially_write_then_fail)
    assert queue_worker.drain(database, 1) == (0, 1, 0)
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(*) FROM articles WHERE id='partial'").fetchone()[0] == 0
        status, error = connection.execute(
            "SELECT status,last_error FROM ingestion_jobs WHERE job_key='source:test:2026-08-12'").fetchone()
    assert status == "retry"
    assert "source failed" in error
