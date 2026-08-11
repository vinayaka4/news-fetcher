import sqlite3

import dashboard
import json
from news_fetcher.cli import initialize


def test_dashboard_filters_and_sorts(tmp_path):
    database = tmp_path / "test.db"
    with sqlite3.connect(database) as connection:
        initialize(connection)
        connection.executemany(
            """INSERT INTO articles
               (id, publisher, source_key, title, normalized_title, article_url,
                published_at, author, excerpt, ministry, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [
                ("1", "PIB", "pib", "Earlier release", "earlier release", "https://a/1",
                 "2026-01-01T00:00:00+00:00", None, "Alpha", "Finance", "2026-01-01"),
                ("2", "PIB", "pib", "Later release", "later release", "https://a/2",
                 "2026-02-01T00:00:00+00:00", None, "Beta", "Defence", "2026-02-01"),
            ],
        )
    dashboard.DATABASE = database
    response = dashboard.app.test_client().get("/?order=asc&ministry=Finance")
    assert response.status_code == 200
    assert b"Earlier release" in response.data
    assert b"Later release" not in response.data


def test_hindu_citation_appears_in_publisher_filter(tmp_path):
    database = tmp_path / "digest.db"
    with sqlite3.connect(database) as connection:
        initialize(connection)
        connection.execute(
            """INSERT INTO digest_stories
               (id, headline, normalized_title, category, summary_json, upsc_relevance,
                source_urls_json, published_at, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("d1", "Court ruling explained", "court ruling explained", "Polity",
             json.dumps(["One", "Two", "Three"]), "GS2",
             json.dumps(["https://www.thehindu.com/news/example"]),
             "2026-08-02", "2026-08-02"),
        )
    dashboard.DATABASE = database
    response = dashboard.app.test_client().get(
        "/?publisher=The+Hindu+%28via+Perplexity%29"
    )
    assert response.status_code == 200
    assert b"Court ruling explained" in response.data
    assert b"The Hindu (via Perplexity)" in response.data


def test_pib_card_stays_in_dashboard_when_content_pending(tmp_path):
    database = tmp_path / "pib.db"
    with sqlite3.connect(database) as connection:
        initialize(connection)
        connection.execute(
            """INSERT INTO articles
               (id, publisher, source_key, title, normalized_title, article_url,
                published_at, excerpt, ministry, full_text, fetched_at)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            ("p1", "Press Information Bureau", "pib", "Official cabinet release",
             "official cabinet release", "https://www.pib.gov.in/release", "2026-08-02",
             "", "Cabinet", "", "2026-08-02"),
        )
    dashboard.DATABASE = database
    response = dashboard.app.test_client().get("/?publisher=Press+Information+Bureau")
    assert b"Official release text is pending ingestion" in response.data
    assert b"Read original" not in response.data
