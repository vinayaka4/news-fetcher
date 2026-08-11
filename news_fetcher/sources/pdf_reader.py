from __future__ import annotations

import hashlib
import sqlite3
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pypdf import PdfReader
from werkzeug.utils import secure_filename

from news_fetcher.raw_store import build_envelope, store_raw_event

MAX_UPLOAD_BYTES = 25 * 1024 * 1024


class PdfIngestionError(ValueError):
    pass


def extract_pages(path: Path) -> list[dict[str, int | str]]:
    try:
        reader = PdfReader(str(path))
        return [{"page": number, "text": page.extract_text() or ""}
                for number, page in enumerate(reader.pages, start=1)]
    except Exception as error:
        raise PdfIngestionError("PDF could not be parsed") from error


def ingest_upload(connection: sqlite3.Connection, upload_root: Path, *, data: bytes,
                  filename: str, document_date: str, source_name: str) -> dict:
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
    envelope = build_envelope(
        source_type="pdf_upload", source_key="manual_pdf", publisher=source_name,
        external_id=digest, published_at=f"{document_date}T00:00:00+00:00",
        metadata={"filename": secure_filename(filename), "sha256": digest,
                  "page_count": len(pages), "stored_path": relative.as_posix()},
        payload={"pages": pages})
    raw_event_id, _ = store_raw_event(connection, envelope)
    connection.execute("""INSERT INTO uploaded_documents
      (id,original_filename,stored_path,sha256,media_type,page_count,
       document_date,source_name,uploaded_at,raw_event_id)
      VALUES (?,?,?,?, 'application/pdf',?,?,?,?,?)""",
      (document_id, secure_filename(filename), relative.as_posix(), digest, len(pages),
       document_date, source_name, datetime.now(timezone.utc).isoformat(), raw_event_id))
    connection.commit()
    return {"duplicate": False, "document_id": document_id, "raw_event_id": raw_event_id,
            "date": document_date, "page_count": len(pages)}
