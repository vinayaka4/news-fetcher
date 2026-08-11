import io
import sqlite3

from pypdf import PdfWriter

import dashboard
from news_fetcher.raw_store import build_envelope, initialize_raw_store, store_raw_event


def test_raw_event_and_date_api(tmp_path):
    database = tmp_path / "raw.db"
    with sqlite3.connect(database) as connection:
        initialize_raw_store(connection)
        store_raw_event(connection, build_envelope(
            source_type="rss", source_key="test_feed", publisher="Test Paper",
            external_id="https://example.com/a", published_at="2026-08-09T01:00:00+00:00",
            payload={"title": "A test story", "raw_field": [1, 2]},
        ))
        connection.commit()
    dashboard.app.config["NEWS_DATABASE"] = str(database)
    response = dashboard.app.test_client().get("/api/v1/news?date=2026-08-09&source=test_feed")
    assert response.status_code == 200
    body = response.get_json()
    assert body["count"] == 1
    assert body["items"][0]["payload"]["raw_field"] == [1, 2]
    assert body["items"][0]["schema_version"] == "1.0"


def test_pdf_upload_creates_document_and_raw_event(tmp_path):
    database = tmp_path / "uploads.db"
    upload_directory = tmp_path / "uploads"
    dashboard.app.config.update(NEWS_DATABASE=str(database), UPLOAD_DIRECTORY=str(upload_directory))
    stream = io.BytesIO()
    writer = PdfWriter()
    writer.add_blank_page(width=612, height=792)
    writer.write(stream)
    stream.seek(0)
    response = dashboard.app.test_client().post(
        "/api/v1/uploads/pdf",
        data={"file": (stream, "daily-paper.pdf"), "date": "2026-08-09", "source": "Test E-paper"},
        content_type="multipart/form-data",
    )
    assert response.status_code == 201
    assert response.get_json()["page_count"] == 1
    with sqlite3.connect(database) as connection:
        assert connection.execute("SELECT COUNT(1) FROM uploaded_documents").fetchone()[0] == 1
        assert connection.execute("SELECT COUNT(1) FROM raw_events WHERE source_key='manual_pdf'").fetchone()[0] == 1

