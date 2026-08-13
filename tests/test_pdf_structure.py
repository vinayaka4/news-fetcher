import io
import sqlite3

from pypdf import PdfWriter

import dashboard
from news_fetcher.database import initialize
from news_fetcher.sources.pdf_reader import _clean, structure_newspaper


def test_structure_newspaper_preserves_blocks_and_builds_articles():
    pages = [{"page_number": 1, "width": 600, "height": 800, "text": "",
              "blocks": [
                  {"order": 1, "text": "WORLD NEWS", "bbox": [1, 100, 100, 110]},
                  {"order": 2, "text": "Delegates finalized a treaty.", "bbox": [1, 120, 100, 130]},
                  {"order": 3, "text": "TECHNOLOGY", "bbox": [200, 100, 300, 110]},
                  {"order": 4, "text": "Engineers released a model.", "bbox": [200, 120, 300, 130]}]}]
    result = structure_newspaper("Daily", "2026-08-13", pages, "Home")
    articles = result["sections"][0]["articles"]
    assert result["schema_version"] == "1.1" and result["edition"] == "Home"
    assert [article["title"] for article in articles] == ["WORLD NEWS", "TECHNOLOGY"]
    assert articles[0]["content"] == ["Delegates finalized a treaty."]
    assert articles[1]["source_blocks"][0]["bbox"] == [200, 120, 300, 130]


def test_structure_newspaper_uses_geometry_to_keep_columns_separate():
    pages = [{"page_number": 2, "width": 600, "height": 800, "text": "x" * 600,
              "image_count": 0, "blocks": [
                  {"order": 1, "text": "Left headline", "bbox": [20, 100, 260, 130],
                   "font_size": 18, "bold_ratio": 1, "fonts": ["PublicoBanner"]},
                  {"order": 2, "text": "Right headline", "bbox": [320, 100, 560, 130],
                   "font_size": 18, "bold_ratio": 1, "fonts": ["PublicoBanner"]},
                  {"order": 3, "text": "Right column body", "bbox": [320, 150, 560, 190],
                   "font_size": 9, "bold_ratio": 0, "fonts": ["PublicoText"]},
                  {"order": 4, "text": "Left column body", "bbox": [20, 150, 260, 190],
                   "font_size": 9, "bold_ratio": 0, "fonts": ["PublicoText"]}]}]
    articles = structure_newspaper("Daily", "2026-08-13", pages)["sections"][0]["articles"]
    assert articles[0]["content"] == ["Left column body"]
    assert articles[1]["content"] == ["Right column body"]


def test_missing_vector_headline_is_preserved_and_flagged_for_ocr():
    pages = [{"page_number": 15, "width": 600, "height": 800, "text": "x" * 600,
              "image_count": 1, "blocks": [
                  {"order": 1, "text": "A descriptive deck for the article", "bbox": [20, 200, 580, 230],
                   "font_size": 11.9, "bold_ratio": 1, "fonts": ["PublicoBanner"]},
                  {"order": 2, "text": "The article body remains available.", "bbox": [20, 250, 280, 500],
                   "font_size": 9, "bold_ratio": 0, "fonts": ["PublicoText"]}]}]
    result = structure_newspaper("Daily", "2026-08-13", pages)
    article = result["sections"][0]["articles"][0]
    assert article["title"] is None and article["deck"].startswith("A descriptive")
    assert article["quality"]["status"] == "ocr_required"
    assert result["extraction"]["ocr_recommended_pages"][0]["page_number"] == 15


def test_ligatures_are_normalized_for_searchable_json():
    assert _clean("green\ufb01eld and e\ufb00orts") == "greenfield and efforts"


def test_structure_newspaper_marks_image_only_document_for_ocr():
    result = structure_newspaper("Scanned Daily", "2026-08-13",
                                 [{"page_number": 1, "text": "", "blocks": []}])
    assert result["extraction"]["ocr_required"] is True
    assert result["sections"][0]["articles"] == []


def test_structured_pdf_can_be_retrieved_by_document_id(tmp_path):
    database = tmp_path / "pdf-structured.db"
    with sqlite3.connect(database) as connection:
        initialize(connection)
    dashboard.DATABASE = database
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
    dashboard_page = client.get("/pdfs?date=2026-08-13")
    assert dashboard_page.status_code == 200
    assert b"daily.pdf" in dashboard_page.data and b"View structured JSON" in dashboard_page.data
