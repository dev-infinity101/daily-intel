"""Keyword filter for the news pipeline.

All 3 news modules (RSS, custom sites, Twitter) pass items through this
before writing to raw_items. Items that match ZERO keywords are discarded.

Short tokens like "EV" and "EVSE" use word-boundary regex to avoid false
positives ("event", "every", "evse" inside a longer token). Multi-word
phrases use plain case-insensitive substring matching.
"""
import re

from app.schemas.ingest import IngestItem

# (keyword, requires_word_boundary)
KEYWORDS: list[tuple[str, bool]] = [
    ("EV", True),
    ("Emobility", False),
    ("E-mobility", False),
    ("Charging", False),
]

LOCATION_KEYWORDS: list[str] = [
    "india",
    "indian",
]

_BOUNDARY_PATTERNS: dict[str, re.Pattern[str]] = {
    kw: re.compile(rf"\b{re.escape(kw)}\b", re.IGNORECASE)
    for kw, wb in KEYWORDS
    if wb
}
_SUBSTRING_KEYWORDS: list[str] = [kw for kw, wb in KEYWORDS if not wb]


def matched_keywords(text: str) -> list[str]:
    found: list[str] = []
    for kw, pat in _BOUNDARY_PATTERNS.items():
        if pat.search(text):
            found.append(kw)
    text_lower = text.lower()
    for kw in _SUBSTRING_KEYWORDS:
        if kw.lower() in text_lower:
            found.append(kw)
    return found


def passes_filter(text: str | None, url: str | None = None) -> tuple[bool, list[str]]:
    combined = f"{text or ''} {url or ''}".strip()
    
    # Check industry keywords
    hits = matched_keywords(combined)
    return bool(hits), hits


def filter_items(items: list[IngestItem]) -> tuple[list[IngestItem], int]:
    """Return (kept_items, dropped_count). Dropped items matched no keyword."""
    kept: list[IngestItem] = []
    dropped = 0
    for item in items:
        ok, _ = passes_filter(item.text, item.url)
        if ok:
            kept.append(item)
        else:
            dropped += 1
    return kept, dropped
