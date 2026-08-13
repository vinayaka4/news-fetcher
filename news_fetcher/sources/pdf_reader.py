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
NEWSPAPER_SCHEMA_VERSION = "1.1"
DEFAULT_SECTION = "Unsorted"
LIGATURES = str.maketrans({"\ufb00": "ff", "\ufb01": "fi", "\ufb02": "fl",
                          "\ufb03": "ffi", "\ufb04": "ffl"})


class PdfIngestionError(ValueError):
    pass


def _clean(value: str) -> str:
    value = value.translate(LIGATURES)
    value = "".join(character if character in "\n\t" or ord(character) >= 32 else " "
                    for character in value)
    return re.sub(r"\s+", " ", value).strip()


def _weighted_font_size(spans: list[dict[str, Any]]) -> float:
    weighted = sorted((float(span["size"]), max(len(span.get("text", "").strip()), 1))
                      for span in spans)
    midpoint, running = sum(weight for _, weight in weighted) / 2, 0
    for size, weight in weighted:
        running += weight
        if running >= midpoint:
            return round(size, 2)
    return 0


def extract_pages(path: Path) -> list[dict[str, Any]]:
    """Extract page text blocks in reading order, preserving page coordinates."""
    try:
        import fitz

        document = fitz.open(path)
        pages: list[dict[str, Any]] = []
        for page_number, page in enumerate(document, start=1):
            blocks = []
            page_dict = page.get_text("dict", sort=True)
            for sequence, block in enumerate(page_dict["blocks"], start=1):
                if block.get("type") != 0:
                    continue
                spans = [span for line in block.get("lines", []) for span in line.get("spans", [])]
                cleaned = _clean(" ".join(span.get("text", "") for span in spans))
                if cleaned:
                    fonts = sorted({span.get("font", "") for span in spans})
                    characters = sum(max(len(span.get("text", "").strip()), 1) for span in spans)
                    bold_characters = sum(max(len(span.get("text", "").strip()), 1) for span in spans
                                          if span.get("flags", 0) & 16)
                    blocks.append({"order": sequence, "text": cleaned,
                      "bbox": [round(float(v), 2) for v in block["bbox"]],
                      "font_size": _weighted_font_size(spans),
                      "max_font_size": round(max(float(span["size"]) for span in spans), 2),
                      "bold_ratio": round(bold_characters / characters, 3), "fonts": fonts})
            pages.append({"page_number": page_number, "width": round(page.rect.width, 2),
                          "height": round(page.rect.height, 2), "blocks": blocks,
                          "image_count": len(page.get_images(full=True)),
                          "text": "\n\n".join(item["text"] for item in blocks)})
        document.close()
        return pages
    except Exception as error:
        raise PdfIngestionError("PDF could not be parsed") from error


def _is_advertisement_page(page: dict[str, Any]) -> bool:
    fonts = {font for block in page["blocks"] for font in block.get("fonts", [])}
    return ((len(page["blocks"]) <= 6 and len(page["text"]) < 600 and page.get("image_count", 0))
            or any(font.startswith(("Canva", "Anton")) for font in fonts))


def _section_name(page: dict[str, Any]) -> str:
    if page["page_number"] == 1:
        return "Front Page"
    top = " ".join(block["text"] for block in page["blocks"]
                   if block["bbox"][1] < page["height"] * .07)
    match = re.search(r"\bChennai\s+(.+)$", top, re.I)
    return _clean(match.group(1)).title() if match else DEFAULT_SECTION


def _block_role(block: dict[str, Any], page: dict[str, Any]) -> str:
    text, fonts = block["text"], block.get("fonts", [])
    words = text.split()
    y0, y1 = block["bbox"][1], block["bbox"][3]
    if y0 < page["height"] * .07 or y1 > page["height"] * .985:
        return "page_furniture"
    if any(font.startswith("THMasthead") for font in fonts) or re.fullmatch(r"e\d{5,}", text):
        return "page_furniture"
    if re.search(r"(?:»|>>)\s*PAGE\s*\d+", text, re.I):
        return "continuation_reference"
    if len(text) == 1 and text.isalpha():
        return "drop_cap"
    if re.fullmatch(r"(?:IN\s*BRIEF|INSIDE|NEWS ANALYSIS)", text, re.I):
        return "section_label"
    if (any(font.startswith("SourceSansPro") for font in fonts)
            and block.get("font_size", 0) >= 20 and y0 < page["height"] * .5
            and len(text) > 180 and page.get("image_count", 0)):
        return "caption"
    if any("BoldItal" in font for font in fonts) and len(text) < 100:
        return "caption"
    if len(words) <= 8 and (re.search(r"\b(?:The Hindu Bureau|PTI|Reuters|AFP|AP)\b", text, re.I)
            or (re.search(r"[a-z]", text) and re.search(r"\b[A-Z][A-Z ]{2,}$", text))):
        return "byline"
    banner_font = any("PublicoBanner" in font for font in fonts)
    if banner_font and block.get("font_size", 0) >= 12 and len(text) <= 180:
        return "headline"
    if block.get("font_size", 0) >= 16 and len(text) <= 180 and block.get("bold_ratio", 0) >= .5:
        return "headline"
    if len(text) <= 140 and block.get("bold_ratio", 0) >= .65:
        return "subheading"
    if not fonts:
        words = text.split()
        letters = [character for character in text if character.isalpha()]
        uppercase = (sum(character.isupper() for character in letters) / len(letters)) if letters else 0
        if 1 <= len(words) <= 12 and len(text) <= 120 and uppercase >= .75:
            return "headline"
    return "body"


def structure_newspaper(newspaper_name: str, document_date: str,
                        pages: list[dict[str, Any]], edition: str | None = None) -> dict[str, Any]:
    """Create a lossless baseline hierarchy without inventing article metadata.

    Layout blocks are preserved verbatim in `source_blocks`. Conservative heading
    heuristics create article boundaries; uncertain blocks remain in `Unsorted`.
    """
    sections: dict[str, list[dict[str, Any]]] = {}
    ordinal, skipped_pages, ocr_recommended_pages = 0, [], []
    role_counts, unassigned_blocks = {}, 0
    for page in pages:
        if _is_advertisement_page(page):
            skipped_pages.append({"page_number": page["page_number"], "reason": "advertisement"})
            continue
        section_name = _section_name(page)
        articles = sections.setdefault(section_name, [])
        classified = [(block, _block_role(block, page)) for block in page["blocks"]]
        page_articles: list[tuple[dict[str, Any], dict[str, Any]]] = []
        for block, role in classified:
            role_counts[role] = role_counts.get(role, 0) + 1
            if role != "headline":
                continue
            ordinal += 1
            article = {"id": f"art_{document_date.replace('-', '')}_{ordinal:04d}",
                       "title": block["text"], "author": None, "page_number": page["page_number"],
                       "page_numbers": [page["page_number"]], "keywords": [], "summary": None,
                       "deck": None, "content": [], "source_blocks": [],
                       "quality": {"status": "candidate"}}
            articles.append(article)
            page_articles.append((article, block))

        # Some newspaper display headlines are vector outlines and therefore absent
        # from the PDF text layer. Preserve the article under a null title and mark
        # the page for OCR instead of silently dropping all of its body text.
        if not page_articles and len(page["text"]) >= 500:
            deck_candidates = [(block, role) for block, role in classified
                               if role == "subheading"
                               and any("PublicoBanner" in font for font in block.get("fonts", []))]
            anchor = max(deck_candidates, key=lambda item: item[0].get("font_size", 0))[0] \
                if deck_candidates else next((block for block, role in classified if role == "body"), None)
            if anchor:
                ordinal += 1
                article = {"id": f"art_{document_date.replace('-', '')}_{ordinal:04d}",
                           "title": None, "author": None, "page_number": page["page_number"],
                           "page_numbers": [page["page_number"]], "keywords": [], "summary": None,
                           "deck": anchor["text"] if deck_candidates else None, "content": [],
                           "source_blocks": [], "quality": {"status": "ocr_required"}}
                articles.append(article)
                page_articles.append((article, anchor))
                ocr_recommended_pages.append({"page_number": page["page_number"],
                                              "reason": "headline_missing_from_text_layer"})

        for block, role in classified:
            if role in {"headline", "page_furniture", "caption", "continuation_reference",
                        "drop_cap", "section_label"}:
                continue
            if any(block is anchor for _, anchor in page_articles):
                continue
            bx0, by0, bx1, _ = block["bbox"]
            candidates = []
            for article, headline in page_articles:
                hx0, _, hx1, hy1 = headline["bbox"]
                vertical_distance = by0 - hy1
                if vertical_distance < -4:
                    continue
                overlap = max(0, min(bx1, hx1) - max(bx0, hx0))
                minimum_width = max(min(bx1 - bx0, hx1 - hx0), 1)
                overlap_ratio = overlap / minimum_width
                horizontal_distance = 0 if overlap else min(abs(bx0 - hx1), abs(hx0 - bx1))
                score = vertical_distance + horizontal_distance * 1.5
                if overlap_ratio < .12:
                    score += 250
                candidates.append((score, article))
            if not candidates:
                unassigned_blocks += 1
                continue
            article = min(candidates, key=lambda item: item[0])[1]
            if role == "byline" and not article["author"]:
                article["author"] = block["text"]
            else:
                article["content"].append(block["text"])
            article["source_blocks"].append({"page_number": page["page_number"],
              "order": block["order"], "bbox": block["bbox"], "role": role})

    for articles in sections.values():
        for article in articles:
            status = ("ocr_required" if article["title"] is None else
                      "candidate" if article["content"] else "incomplete")
            article["quality"] = {"status": status,
                                  "paragraph_count": len(article["content"])}

    text_characters = sum(len(page["text"]) for page in pages)
    return {"schema_version": NEWSPAPER_SCHEMA_VERSION, "newspaper_name": newspaper_name,
            "date": document_date, "edition": edition, "extraction": {
                "engine": "pymupdf", "mode": "digital_text", "page_count": len(pages),
                "text_characters": text_characters,
                "ocr_required": text_characters < max(50, len(pages) * 20),
                "classification_status": "layout_candidate_unreviewed",
                "skipped_pages": skipped_pages, "role_counts": role_counts,
                "ocr_recommended_pages": ocr_recommended_pages,
                "unassigned_blocks": unassigned_blocks},
            "sections": [{"section_name": name, "articles": articles}
                         for name, articles in sections.items()]}


def ingest_upload(connection: sqlite3.Connection, upload_root: Path, *, data: bytes,
                  filename: str, document_date: str, source_name: str,
                  edition: str | None = None, reprocess: bool = False) -> dict:
    if len(data) > MAX_UPLOAD_BYTES:
        raise PdfIngestionError("PDF exceeds 25 MB limit")
    if not data.startswith(b"%PDF-"):
        raise PdfIngestionError("uploaded file is not a PDF")
    digest = hashlib.sha256(data).hexdigest()
    existing = connection.execute("SELECT id,raw_event_id,stored_path FROM uploaded_documents WHERE sha256=?",
                                  (digest,)).fetchone()
    if existing and not reprocess:
        return {"duplicate": True, "document_id": existing[0], "raw_event_id": existing[1]}
    document_id = existing[0] if existing else str(uuid.uuid4())
    relative = Path(existing[2]) if existing else Path(document_date[:4]) / document_date[5:7] / f"{document_id}.pdf"
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
    if existing:
        connection.execute("""UPDATE uploaded_documents SET original_filename=?,page_count=?,
          document_date=?,source_name=?,uploaded_at=?,raw_event_id=? WHERE id=?""",
          (secure_filename(filename), len(pages), document_date, source_name,
           datetime.now(timezone.utc).isoformat(), raw_event_id, document_id))
    else:
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
