from __future__ import annotations

import hashlib
import re
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

from werkzeug.utils import secure_filename

from news_fetcher.raw_store import build_envelope, store_raw_event

MAX_UPLOAD_BYTES = 25 * 1024 * 1024
NEWSPAPER_SCHEMA_VERSION = "1.0"
DEFAULT_SECTION = "Unsorted"


class PdfIngestionError(ValueError):
    pass


def _clean(value: str) -> str:
    return re.sub(r"\s+", " ", value).strip()


def extract_pages(path: Path) -> list[dict[str, Any]]:
    """Extract page text blocks in reading order, preserving page coordinates."""
    try:
        import fitz

        document = fitz.open(path)
        pages: list[dict[str, Any]] = []
        for page_number, page in enumerate(document, start=1):
            blocks = []
            for sequence, block in enumerate(page.get_text("blocks", sort=True), start=1):
                x0, y0, x1, y1, text = block[:5]
                cleaned = _clean(text)
                if cleaned:
                    blocks.append({"order": sequence, "text": cleaned,
                                   "bbox": [round(float(v), 2) for v in (x0, y0, x1, y1)]})
            pages.append({"page_number": page_number, "width": round(page.rect.width, 2),
                          "height": round(page.rect.height, 2), "blocks": blocks,
                          "text": "\n\n".join(item["text"] for item in blocks)})
        document.close()
        return pages
    except Exception as error:
        raise PdfIngestionError("PDF could not be parsed") from error


def _looks_like_heading(text: str) -> bool:
    words = text.split()
    if not 1 <= len(words) <= 12 or len(text) > 120:
        return False
    letters = [char for char in text if char.isalpha()]
    uppercase_ratio = (sum(char.isupper() for char in letters) / len(letters)) if letters else 0
    return uppercase_ratio >= 0.75 or (len(words) <= 8 and not text.endswith((".", "?", "!")))


def structure_newspaper(newspaper_name: str, document_date: str,
                        pages: list[dict[str, Any]], edition: str | None = None) -> dict[str, Any]:
    """Create a lossless baseline hierarchy without inventing article metadata.

    Layout blocks are preserved verbatim in `source_blocks`. Conservative heading
    heuristics create article boundaries; uncertain blocks remain in `Unsorted`.
    """
    articles: list[dict[str, Any]] = []
    current: dict[str, Any] | None = None
    ordinal = 0
    for page in pages:
        for block in page["blocks"]:
            text = block["text"]
            if _looks_like_heading(text):
                ordinal += 1
                current = {"id": f"art_{document_date.replace('-', '')}_{ordinal:04d}",
                           "title": text, "author": None, "page_number": page["page_number"],
                           "page_numbers": [page["page_number"]], "keywords": [], "summary": None,
                           "content": [], "source_blocks": []}
                articles.append(current)
                continue
            if current is None:
                ordinal += 1
                current = {"id": f"art_{document_date.replace('-', '')}_{ordinal:04d}",
                           "title": "Untitled extracted content", "author": None,
                           "page_number": page["page_number"], "page_numbers": [page["page_number"]],
                           "keywords": [], "summary": None, "content": [], "source_blocks": []}
                articles.append(current)
            if page["page_number"] not in current["page_numbers"]:
                current["page_numbers"].append(page["page_number"])
            current["content"].append(text)
            current["source_blocks"].append({"page_number": page["page_number"],
                                              "order": block["order"], "bbox": block["bbox"]})

    text_characters = sum(len(page["text"]) for page in pages)
    return {"schema_version": NEWSPAPER_SCHEMA_VERSION, "newspaper_name": newspaper_name,
            "date": document_date, "edition": edition, "extraction": {
                "engine": "pymupdf", "mode": "digital_text", "page_count": len(pages),
                "text_characters": text_characters,
                "ocr_required": text_characters < max(50, len(pages) * 20),
                "classification_status": "heuristic_unreviewed"},
            "sections": [{"section_name": DEFAULT_SECTION, "articles": articles}]}


def ingest_upload(connection: sqlite3.Connection, upload_root: Path, *, data: bytes,
                  filename: str, document_date: str, source_name: str,
                  edition: str | None = None) -> dict:
    if len(data) > MAX_UPLOAD_BYTES:
        raise PdfIngestionError("PDF exceeds 25 MB limit")
    if not data.startswith(b"%PDF-"):
        raise PdfIngestionError("uploaded file is not a PDF")
    digest = hashlib.sha256(data).hexdigest()
    existing = connection.execute(
        "SELECT id,raw_event_id FROM uploaded_documents WHERE sha256=?", (digest,)).fetchone()
    if existing:
        return {"duplicate": True, "document_id": existing[0], "raw_event_id": existing[1]}
    document_id = str(uuid.uuid4())
    relative = Path(document_date[:4]) / document_date[5:7] / f"{document_id}.pdf"
    destination = upload_root / relative
    destination.parent.mkdir(parents=True, exist_ok=True)
    destination.write_bytes(data)
    try:
        pages = extract_pages(destination)
    except PdfIngestionError:
        destination.unlink(missing_ok=True)
        raise
    newspaper = structure_newspaper(source_name, document_date, pages, edition)
    envelope = build_envelope(
        source_type="pdf_upload", source_key="manual_pdf", publisher=source_name,
        external_id=digest, published_at=f"{document_date}T00:00:00+00:00",
        metadata={"filename": secure_filename(filename), "sha256": digest,
                  "page_count": len(pages), "stored_path": relative.as_posix(),
                  "newspaper_schema_version": NEWSPAPER_SCHEMA_VERSION},
        payload={"newspaper": newspaper, "pages": pages})
    raw_event_id, _ = store_raw_event(connection, envelope)
    connection.execute("""INSERT INTO uploaded_documents
      (id,original_filename,stored_path,sha256,media_type,page_count,
       document_date,source_name,uploaded_at,raw_event_id)
      VALUES (?,?,?,?, 'application/pdf',?,?,?,?,?)""",
      (document_id, secure_filename(filename), relative.as_posix(), digest, len(pages),
       document_date, source_name, datetime.now(timezone.utc).isoformat(), raw_event_id))
    connection.commit()
    return {"duplicate": False, "document_id": document_id, "raw_event_id": raw_event_id,
            "date": document_date, "page_count": len(pages),
            "article_count": sum(len(section["articles"]) for section in newspaper["sections"]),
            "ocr_required": newspaper["extraction"]["ocr_required"],
            "structured_url": f"/api/v1/uploads/pdf/{document_id}"}
