import sqlite3

import pytest

import dashboard
import queue_worker
from news_fetcher.consolidation import (ConsolidationError, normalize_snapshot,
                                        store_consolidation)
from news_fetcher.database import initialize
from news_fetcher.raw_store import build_envelope, store_raw_event


DATE = "2026-08-15"


def snapshot():
    return {"date": DATE, "sources": {
      "pib": {"items": [{"id": "p1", "title": "Cabinet approves education mission",
        "article_url": "https://pib.gov.in/p1", "full_text": "The Cabinet approved the mission."}]},
      "rss": {"items": [{"id": "r1", "publisher": "Paper",
        "title": "Cabinet approves new education mission", "article_url": "https://paper/r1",
        "content_text": "The national programme received Cabinet approval."}]},
      "perplexity": {"items": [{"id": "d1", "headline": "Education mission approved",
        "summary": ["Cabinet approval was announced."], "source_urls": ["https://paper/r1"]}]},
      "pdf": {"items": [{"id": "pdf1", "source_name": "Paper",
        "structured_url": "/api/v1/uploads/pdf/pdf1", "newspaper": {"sections": [{"articles": [{
          "id": "a1", "title": "Sports result", "content": ["The home team won."]}]}]}}]}}}


def merge_everything(candidates):
    return [{"title": "Daily consolidated report", "category": "Government Schemes",
             "summary": ["The supplied reports were consolidated.", "Duplicate coverage was merged."],
             "key_facts": ["All source records remain traceable."],
             "candidate_ids": [item["candidate_id"] for item in candidates]}]


def test_normalization_preserves_traceable_ids_for_all_four_sources():
    records = normalize_snapshot(snapshot())
    assert {item["source_type"] for item in records} == {"pib", "rss", "perplexity", "pdf"}
    assert {item["record_id"] for item in records} == {
        "pib:p1", "rss:r1", "perplexity:d1", "pdf:pdf1:a1"}


def test_consolidation_is_atomic_idempotent_and_preserves_raw_events(tmp_path):
    database = tmp_path / "consolidated.db"
    with sqlite3.connect(database) as connection:
        initialize(connection)
        store_raw_event(connection, build_envelope(source_type="rss", source_key="paper",
          publisher="Paper", external_id="raw-1", published_at=f"{DATE}T00:00:00+00:00",
          payload={"title": "Original raw record"}))
        connection.commit()
        first = store_consolidation(connection, DATE, snapshot(), "test-model", merge_everything)
        second = store_consolidation(connection, DATE, snapshot(), "test-model", merge_everything)
        assert first["duplicate"] is False and second["duplicate"] is True
        assert connection.execute("SELECT COUNT(*) FROM consolidated_stories").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(*) FROM raw_events").fetchone()[0] == 1
        refs = connection.execute("SELECT source_refs_json FROM consolidated_stories").fetchone()[0]
        assert all(record_id in refs for record_id in ("pib:p1", "rss:r1", "perplexity:d1", "pdf:pdf1:a1"))


def test_invalid_ai_coverage_publishes_no_partial_stories(tmp_path):
    database = tmp_path / "failed.db"
    with sqlite3.connect(database) as connection:
        initialize(connection); connection.commit()
        with pytest.raises(ConsolidationError, match="omitted or duplicated"):
            store_consolidation(connection, DATE, snapshot(), "test-model", lambda candidates: [{
              "title": "Incomplete", "category": "Other", "summary": ["One", "Two"],
              "key_facts": ["Fact"], "candidate_ids": [candidates[0]["candidate_id"]]}])
        assert connection.execute("SELECT COUNT(*) FROM consolidated_stories").fetchone()[0] == 0
        assert connection.execute("SELECT status FROM consolidation_runs").fetchone()[0] == "failed"


def test_consolidated_api_returns_latest_completed_run(tmp_path):
    database = tmp_path / "api.db"
    with sqlite3.connect(database) as connection:
        initialize(connection); connection.commit()
        result = store_consolidation(connection, DATE, snapshot(), "test-model", merge_everything)
    dashboard.app.config.update(NEWS_DATABASE=str(database), DATABASE_URL=None)
    payload = dashboard.app.test_client().get(f"/api/v1/consolidated?date={DATE}").get_json()
    assert payload["status"] == "ready" and payload["run"]["id"] == result["run_id"]
    assert payload["items"][0]["source_count"] == 4
    assert payload["items"][0]["summary"][0].startswith("The supplied")
    assert {source["source_type"] for source in payload["items"][0]["sources"]} == {
        "pib", "rss", "perplexity", "pdf"}


def test_consolidated_api_validates_date_and_returns_no_data(tmp_path):
    database = tmp_path / "empty.db"
    with sqlite3.connect(database) as connection:
        initialize(connection); connection.commit()
    dashboard.app.config.update(NEWS_DATABASE=str(database), DATABASE_URL=None)
    client = dashboard.app.test_client()
    assert client.get("/api/v1/consolidated?date=bad").status_code == 400
    assert client.get(f"/api/v1/consolidated?date={DATE}").get_json()["status"] == "no_data"


def test_daily_queue_includes_durable_consolidation_after_source_jobs(monkeypatch):
    monkeypatch.setenv("NEWS_ENABLED_SOURCES", "pib")
    connection = sqlite3.connect(":memory:"); initialize(connection)
    assert queue_worker.enqueue_daily(connection, DATE) == 3
    jobs = connection.execute("SELECT job_type FROM ingestion_jobs ORDER BY created_at,id").fetchall()
    assert {row[0] for row in jobs} == {"fetch_source", "fetch_perplexity", "consolidate_news"}


def test_worker_retries_consolidation_until_required_sources_are_ready(monkeypatch):
    monkeypatch.setenv("PERPLEXITY_API_KEY", "test-key")
    monkeypatch.setattr(queue_worker, "fetch_daily_snapshot", lambda *args: {
      "source_statuses": {"pib": "partial", "rss": "ready", "perplexity": "ready"}})
    job = {"job_type": "consolidate_news", "payload_json": (
      '{"date":"2026-08-15","model":"sonar","api_base_url":"https://api.example"}')}
    with pytest.raises(ConsolidationError, match="pib"):
        queue_worker.execute(sqlite3.connect(":memory:"), job)
