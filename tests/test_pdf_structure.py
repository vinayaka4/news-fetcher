import io
import sqlite3

from pypdf import PdfWriter

import dashboard
from news_fetcher.database import initialize
from news_fetcher.sources.pdf_reader import structure_newspaper


def test_structure_newspaper_preserves_blocks_and_builds_articles():
    pages = [{"page_number": 1, "width": 600, "height": 800, "text": "",
              "blocks": [
                  {"order": 1, "text": "WORLD NEWS", "bbox": [1, 1, 10, 10]},
                  {"order": 2, "text": "Delegates finalized a treaty.", "bbox": [1, 20, 10, 30]},
                  {"order": 3, "text": "TECHNOLOGY", "bbox": [1, 40, 10, 50]},
                  {"order": 4, "text": "Engineers released a model.", "bbox": [1, 60, 10, 70]}]}]
    result = structure_newspaper("Daily", "2026-08-13", pages, "Home")
    articles = result["sections"][0]["articles"]
    assert result["schema_version"] == "1.0" and result["edition"] == "Home"
    assert [article["title"] for article in articles] == ["WORLD NEWS", "TECHNOLOGY"]
    assert articles[0]["content"] == ["Delegates finalized a treaty."]
    assert articles[1]["source_blocks"][0]["bbox"] == [1, 60, 10, 70]


def test_structure_newspaper_marks_image_only_document_for_ocr():
    result = structure_newspaper("Scanned Daily", "2026-08-13",
                                 [{"page_number": 1, "text": "", "blocks": []}])
    assert result["extraction"]["ocr_required"] is True
    assert result["sections"][0]["articles"] == []


def test_structured_pdf_can_be_retrieved_by_document_id(tmp_path):
    database = tmp_path / "pdf-structured.db"
    with sqlite3.connect(database) as connection:
        initialize(connection)
    dashboard.app.config.update(NEWS_DATABASE=str(database), DATABASE_URL=None,
                                UPLOAD_DIRECTORY=str(tmp_path / "uploads"))
    stream = io.BytesIO(); writer = PdfWriter(); writer.add_blank_page(width=612, height=792)
    writer.write(stream); stream.seek(0)
    client = dashboard.app.test_client()
    response = client.post("/api/v1/uploads/pdf", data={
        "file": (stream, "daily.pdf"), "date": "2026-08-13",
        "source": "The Daily", "edition": "Final Home"}, content_type="multipart/form-data")
    assert response.status_code == 201
    uploaded = response.get_json()
    assert uploaded["ocr_required"] is True and uploaded["structured_url"]
    payload = client.get(uploaded["structured_url"]).get_json()
    assert payload["newspaper"]["newspaper_name"] == "The Daily"
    assert payload["newspaper"]["edition"] == "Final Home"
    listing = client.get("/api/v1/uploads/pdf?date=2026-08-13").get_json()
    assert listing["count"] == 1 and listing["items"][0]["id"] == uploaded["document_id"]
