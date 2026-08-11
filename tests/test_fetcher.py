import sqlite3

from news_fetcher.cli import canonical_url, initialize, is_duplicate, normalize_title, parse_pib_content, parse_pib_releases


def test_canonical_url_removes_tracking_and_fragment():
    assert canonical_url("HTTPS://Example.com/a/?utm_source=x&id=2#top") == "https://example.com/a?id=2"


def test_normalize_title():
    assert normalize_title("India's  Big—Story!") == "india s big story"


def test_duplicate_by_similar_title():
    connection = sqlite3.connect(":memory:")
    initialize(connection)
    connection.execute(
        """INSERT INTO articles
           (id, publisher, source_key, title, normalized_title, article_url,
            published_at, author, excerpt, fetched_at)
           VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        ("1", "A", "a", "India launches new lunar mission", "india launches new lunar mission",
         "https://example.com/1", None, None, "", "2026-01-01T00:00:00+00:00"),
    )
    assert is_duplicate(connection, "https://other.example/2", "India launches new lunar mission")


def test_parse_pib_releases_with_ministry_and_date():
    releases = parse_pib_releases("""
      <ul class="num"><h3>Ministry of Finance</h3><li>
      <a href="/PressReleseDetail.aspx?PRID=123">Budget update</a>
      Posted on: 02 Aug 2026</li></ul>
    """)
    assert releases == [{
        "title": "Budget update",
        "url": "https://www.pib.gov.in/PressReleasePage.aspx?PRID=123&lang=1&reg=3",
        "published_at": "2026-08-02T00:00:00+00:00",
        "ministry": "Ministry of Finance",
    }]


def test_parse_pib_content_removes_scripts():
    html = '<div class="content-area"><script>bad()</script><p>Official release text.</p></div>'
    assert parse_pib_content(html) == "Official release text."
