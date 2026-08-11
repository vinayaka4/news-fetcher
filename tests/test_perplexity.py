import json
import sqlite3

import perplexity_ingest as ingest


class FakeResponse:
    def raise_for_status(self):
        return None

    def json(self):
        content = {"stories": [{
            "headline": "Cabinet approves education scheme",
            "category": "Government Schemes",
            "summary": ["Point one", "Point two", "Point three"],
            "upsc_relevance": "GS2",
            "sources": ["https://pib.gov.in/example", "not-a-url"],
            "publication_date": "2026-08-02",
            "background": "The cabinet considered a national education proposal.",
            "why_it_matters": "It affects education governance under GS2.",
            "prelims_facts": ["Education is in the Concurrent List", "The cabinet is part of the executive"],
            "mains_angle": "Assess cooperative federalism in education policy.",
        }]}
        return {"choices": [{"message": {"content": json.dumps(content)}}], "usage": {"total_tokens": 10}}


def test_request_and_store_structured_digest(monkeypatch):
    monkeypatch.setattr(ingest.requests, "post", lambda *args, **kwargs: FakeResponse())
    stories, usage = ingest.request_digest("test-key", "2026-08-02")
    assert len(stories) == 1
    assert stories[0]["sources"] == ["https://pib.gov.in/example"]
    connection = sqlite3.connect(":memory:")
    assert ingest.store_stories(connection, stories) == 1
    assert ingest.store_stories(connection, stories) == 0
    assert usage["total_tokens"] == 10


def test_missing_key_exits_cleanly(monkeypatch):
    monkeypatch.delenv("PERPLEXITY_API_KEY", raising=False)
    monkeypatch.setattr(ingest, "load_local_env", lambda *args, **kwargs: None)
    assert ingest.main(["--date", "2026-08-02", "--database", ":memory:"]) == 2
