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
    # General Broad Keywords
    ("EV", True),
    ("EVs", True),
    ("Emobility", False),
    ("E-mobility", False),
    ("Charging", False),
    ("Electric Vehicle", False),
    ("Electric Scooter", False),
    ("Electric Car", False),
    ("Electric Bus", False),
    ("Battery Swapping", False),
    ("Li-ion", False),
    ("Lithium-ion", False),
    ("Giga Factory", False),
    
    # Location keywords have been moved to LOCATION_KEYWORDS below
    
    # Major Indian EV Companies / Startups
    ("Tata Motors", False),
    ("Tata Passenger Electric", False),
    ("Mahindra", False),
    ("Ola Electric", False),
    ("Ather Energy", False),
    ("TVS Motor", False),
    ("TVS iQube", False),
    ("Bajaj Auto", False),
    ("Bajaj Chetak", False),
    ("Hero Electric", False),
    ("Vida", True),
    ("Greaves", False),
    ("Ampere", False),
    ("Kinetic Green", False),
    ("Revolt Motors", False),
    ("Oben Electric", False),
    ("Simple Energy", False),
    ("Tork Motors", False),
    ("Ultraviolette", False),
    ("Yulu", False),
    ("MG Motor", False),
    ("BYD India", False),
    ("Log9", False),
    ("Exponent Energy", False),
    ("Gogoro", False),
    ("Sun Mobility", False),
    ("Bounce Infinity", False),
    ("Eicher Motors", False),
    ("Ashok Leyland", False),
    ("Olectra", False),
    ("JBM Auto", False),
    
    # News Sources to always pass
    ("Economic Times", False),
    ("economictimes", False),
    ("autopunditz", False),
]

LOCATION_KEYWORDS: list[str] = [
    "india",
    "indian",
    "maharashtra",
    "delhi",
    "karnataka",
    "tamil nadu",
    "gujarat",
    "telangana",
    "haryana",
    "uttar pradesh",
    "kerala",
    "punjab",
    "rajasthan",
    "west bengal",
    "madhya pradesh",
    "andhra pradesh",
    "odisha",
    "pune",
    "bengaluru",
    "bangalore",
    "chennai",
    "hyderabad",
    "ncr",
    "gurgaon",
    "gurugram",
    "noida",
]

# Set of company names that inherently mean it's related to India
INDIAN_COMPANIES: set[str] = {
    "Tata Motors", "Tata Passenger Electric", "Mahindra", "Ola Electric",
    "Ather Energy", "TVS Motor", "TVS iQube", "Bajaj Auto", "Bajaj Chetak",
    "Hero Electric", "Vida", "Greaves", "Ampere", "Kinetic Green",
    "Revolt Motors", "Oben Electric", "Simple Energy", "Tork Motors",
    "Ultraviolette", "Yulu", "MG Motor", "BYD India", "Log9",
    "Exponent Energy", "Gogoro", "Sun Mobility", "Bounce Infinity",
    "Eicher Motors", "Ashok Leyland", "Olectra", "JBM Auto",
    "Economic Times", "economictimes", "autopunditz"
}

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
    if not hits:
        return False, []

    # If it matched an explicitly Indian company or news source, pass it.
    for hit in hits:
        if hit in INDIAN_COMPANIES:
            return True, hits

    # Otherwise, it must explicitly mention an Indian location/state
    combined_lower = combined.lower()
    for loc in LOCATION_KEYWORDS:
        if re.search(rf"\b{re.escape(loc)}\b", combined_lower):
            return True, hits + [loc]

    return False, []


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
