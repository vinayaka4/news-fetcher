from __future__ import annotations

import re
from difflib import SequenceMatcher

LOW_VALUE_PATTERNS = [
    r"\b(actor|actress|bollywood|celebrity|film review|box office|web series|reality show)\b",
    r"\b(cricket|ipl|football|tennis|badminton|boxing|wrestling|medal|tournament|match score)\b",
    r"\b(horoscope|astrology|numerology|viral video|fashion|recipe)\b",
    r"\b(murder|robbery|theft|assault|accident|suicide|kidnap)\b",
    r"\b(party rally|election rally|campaign trail|joins bjp|joins congress|slams bjp|slams congress)\b",
    r"\b(bjp says|congress says|aap says|political war of words|seat sharing)\b",
]

POLITICAL_ACTORS = re.compile(
    r"\b(bjp|congress|aap|tmc|samajwadi party|rahul gandhi|narendra modi|amit shah|"
    r"priyanka gandhi|mallikarjun kharge|arvind kejriwal|mamata banerjee|"
    r"shehzad poonawalla|opposition leader|party spokesperson)\b", re.I
)
POLITICAL_THEATRE = re.compile(
    r"\b(mock|mocks|mocked|slam|slams|slammed|attack|attacks|attacked|taunt|taunts|"
    r"jibe|dig at|hits back|lashes out|war of words|social media post|tweet|"
    r"challenges|accuses|alleges|calls .* liar|controversial remark)\b", re.I
)

HIGH_VALUE_TERMS = {
    "constitution", "supreme court", "high court", "parliament", "bill", "act",
    "policy", "scheme", "cabinet", "ministry", "government", "economy", "gdp",
    "inflation", "budget", "rbi", "environment", "climate", "science", "technology",
    "international", "treaty", "diplomacy", "security", "defence", "report", "index",
    "commission", "rights", "governance", "education", "health", "agriculture",
}

STOP_WORDS = {"the", "a", "an", "to", "of", "in", "on", "for", "and", "as", "at", "with", "from", "by"}


def is_upsc_relevant(item: dict) -> bool:
    if item.get("publisher") == "Press Information Bureau":
        return True
    if item.get("publisher") == "Perplexity Digest":
        return item.get("category") != "Other"
    text = f"{item.get('title', '')} {item.get('excerpt', '')}".lower()
    if POLITICAL_ACTORS.search(text) and POLITICAL_THEATRE.search(text):
        return any(term in text for term in HIGH_VALUE_TERMS)
    if any(re.search(pattern, text) for pattern in LOW_VALUE_PATTERNS):
        return any(term in text for term in HIGH_VALUE_TERMS)
    return True


def title_tokens(title: str) -> set[str]:
    return {word for word in re.findall(r"[a-z0-9]+", title.lower()) if word not in STOP_WORDS and len(word) > 2}


def same_story(left: dict, right: dict) -> bool:
    a, b = title_tokens(left.get("title", "")), title_tokens(right.get("title", ""))
    if not a or not b:
        return False
    overlap = len(a & b) / min(len(a), len(b))
    if overlap < 0.45:
        return False
    sequence = SequenceMatcher(None, " ".join(sorted(a)), " ".join(sorted(b))).ratio()
    return overlap >= 0.72 or sequence >= 0.82


def deduplicate(items: list[dict]) -> list[dict]:
    priority = {"Perplexity Digest": 3, "Press Information Bureau": 2}
    ranked = sorted(items, key=lambda item: priority.get(item.get("publisher", ""), 1), reverse=True)
    unique: list[dict] = []
    for item in ranked:
        duplicate = next((existing for existing in unique if same_story(item, existing)), None)
        if duplicate:
            source = item.get("article_url")
            if source:
                duplicate["sources"] = duplicate.get("sources") or []
                if source not in duplicate["sources"]:
                    duplicate["sources"].append(source)
            continue
        unique.append(item)
    return unique
