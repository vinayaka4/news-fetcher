from __future__ import annotations

import json
import re
from dataclasses import dataclass
from pathlib import Path
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

from bs4 import BeautifulSoup


@dataclass(frozen=True)
class Source:
    key: str
    publisher: str
    url: str
    default_enabled: bool = False
    usage: str = "check-publisher-terms"


def clean_text(value: str | None) -> str:
    if not value:
        return ""
    return " ".join(BeautifulSoup(value, "html.parser").get_text(" ", strip=True).split())


def canonical_url(url: str) -> str:
    parts = urlsplit(url.strip())
    query = [(key, value) for key, value in parse_qsl(parts.query)
             if not key.lower().startswith("utm_")]
    return urlunsplit((parts.scheme.lower(), parts.netloc.lower(), parts.path.rstrip("/"),
                       urlencode(query), ""))


def normalize_title(title: str) -> str:
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9\s]", " ", title.lower())).strip()


def load_sources(path: Path) -> list[Source]:
    return [Source(**item) for item in json.loads(path.read_text(encoding="utf-8"))]


def selected_sources(sources: list[Source], requested: str | None) -> list[Source]:
    if requested:
        keys = {value.strip() for value in requested.split(",") if value.strip()}
        unknown = keys - {source.key for source in sources}
        if unknown:
            raise ValueError(f"Unknown source(s): {', '.join(sorted(unknown))}")
        return [source for source in sources if source.key in keys]
    return [source for source in sources if source.default_enabled]
