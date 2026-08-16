import sqlite3

import dashboard
from news_fetcher.database import initialize
from news_fetcher.raw_store import build_envelope, store_raw_event


DATE = "2026-08-15"


def client_for(database):
    dashboard.app.config.update(NEWS_DATABASE=str(database), DATABASE_URL=None)
    return dashboard.app.test_client()


def seed_all_sources(database):
    with sqlite3.connect(database) as connection:
        initialize(connection)
        connection.execute("""INSERT INTO articles
          (id,publisher,source_key,title,normalized_title,article_url,published_at,
           excerpt,full_text,fetched_at) VALUES (?,?,?,?,?,?,?,?,?,?)""",
          ("pib-1", "PIB", "pib", "Official release", "official release",
           "https://pib.gov.in/1", f"{DATE}T10:00:00+05:30", None, "Complete text", "now"))
        connection.execute("""INSERT INTO articles
          (id,publisher,source_key,title,normalized_title,article_url,published_at,
           excerpt,full_text,fetched_at) VALUES (?,?,?,?,?,?,?,?,?,?)""",
          ("rss-1", "The Hindu", "the_hindu", "Feed report", "feed report",
           "https://example.com/rss/1", f"{DATE}T09:00:00+05:30", "Feed excerpt", None, "now"))
        connection.execute("""INSERT INTO pib_ingestion_runs
          (publication_date,expected_count,discovered_count,ready_count,missing_count,
           complete,listing_fetched_at,updated_at) VALUES (?,?,?,?,?,?,?,?)""",
          (DATE, 1, 1, 1, 0, 1, "now", "now"))
        connection.execute("""INSERT INTO digest_stories
          (id,headline,normalized_title,category,summary_json,upsc_relevance,
           source_urls_json,published_at,details_json,fetched_at)
          VALUES (?,?,?,?,?,?,?,?,?,?)""",
          ("digest-1", "Policy update", "policy update", "Polity",
           '["One","Two","Three"]', "GS2", '["https://example.com/story"]', DATE,
           '{"background":"Context"}', "now"))
        envelope = build_envelope(source_type="pdf_upload", source_key="manual_pdf",
          publisher="The Hindu", external_id="pdf-sha", published_at=f"{DATE}T00:00:00+00:00",
          payload={"newspaper": {"schema_version": "1.1", "sections": [{"section_name": "News",
            "articles": [{"id": "a1", "title": "PDF story", "content": ["x" * 3000],
                          "source_blocks": [{"text": "large layout metadata"}]}]}]}})
        raw_id, _ = store_raw_event(connection, envelope)
        connection.execute("""INSERT INTO uploaded_documents
          (id,original_filename,stored_path,sha256,media_type,page_count,document_date,
           source_name,uploaded_at,raw_event_id) VALUES (?,?,?,?,?,?,?,?,?,?)""",
          ("pdf-1", "paper.pdf", "2026/08/pdf-1.pdf", "pdf-sha", "application/pdf", 10,
           DATE, "The Hindu", "now", raw_id))
        connection.commit()


def test_dedicated_rss_and_perplexity_apis_are_date_filtered(tmp_path):
    database = tmp_path / "sources.db"; seed_all_sources(database)
    client = client_for(database)
    rss = client.get(f"/api/v1/rss?date={DATE}").get_json()
    perplexity = client.get(f"/api/v1/perplexity?date={DATE}").get_json()
    assert rss["count"] == 1 and rss["items"][0]["source_key"] == "the_hindu"
    assert rss["items"][0]["content_text"] == "Feed excerpt"
    assert perplexity["count"] == 1 and perplexity["items"][0]["summary"] == ["One", "Two", "Three"]
    assert client.get("/api/v1/rss?date=2026-02-30").status_code == 400
    assert client.get("/api/v1/perplexity?limit=nope").status_code == 400


def test_unified_api_exposes_four_source_groups_from_one_snapshot(tmp_path):
    database = tmp_path / "all.db"; seed_all_sources(database)
    payload = client_for(database).get(
        f"/api/v1/all?date={DATE}&include_pdf_content=true").get_json()
    assert payload["status"] == "ready" and payload["complete"] is True
    assert payload["source_counts"] == {"pdf": 1, "perplexity": 1, "pib": 1, "rss": 1}
    assert set(payload["sources"]) == {"pib", "rss", "perplexity", "pdf"}
    assert payload["sources"]["pib"]["completeness"]["missing_count"] == 0
    assert payload["sources"]["pdf"]["items"][0]["newspaper"]["schema_version"] == "1.1"


def test_unified_api_reports_empty_and_partial_data_without_claiming_completeness(tmp_path):
    database = tmp_path / "empty.db"
    with sqlite3.connect(database) as connection:
        initialize(connection); connection.commit()
    client = client_for(database)
    empty = client.get(f"/api/v1/all?date={DATE}").get_json()
    assert empty["status"] == "no_data" and empty["complete"] is False
    assert all(status == "no_data" for status in empty["source_statuses"].values())

    with sqlite3.connect(database) as connection:
        connection.execute("""INSERT INTO digest_stories
          (id,headline,normalized_title,category,summary_json,upsc_relevance,
           source_urls_json,published_at,details_json,fetched_at)
          VALUES ('bad','Bad JSON','bad json','Other','not-json','GS2','[]',?,NULL,'now')""", (DATE,))
        connection.commit()
    partial = client.get(f"/api/v1/all?date={DATE}").get_json()
    story = partial["sources"]["perplexity"]["items"][0]
    assert partial["status"] == "partial" and partial["complete"] is False
    assert story["content_status"] == "partial"
    assert "invalid_summary_json" in story["data_quality_flags"]


def test_pdf_content_is_linked_by_default_to_control_response_size(tmp_path):
    database = tmp_path / "pdf-size.db"; seed_all_sources(database)
    item = client_for(database).get(f"/api/v1/all?date={DATE}").get_json()["sources"]["pdf"]["items"][0]
    assert item["structured_url"] == "/api/v1/uploads/pdf/pdf-1"
    assert "newspaper" not in item


def test_compact_all_source_snapshot_bounds_ai_transfer_size(tmp_path):
    database = tmp_path / "compact.db"; seed_all_sources(database)
    payload = client_for(database).get(
        f"/api/v1/all?date={DATE}&include_pdf_content=true&compact=true").get_json()
    article = payload["sources"]["pdf"]["items"][0]["newspaper"]["sections"][0]["articles"][0]
    assert len(article["content"][0]) == 2000
    assert "source_blocks" not in article
