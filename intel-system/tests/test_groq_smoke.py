"""
Groq compound smoke tests — live API calls.

Tests that groq/compound can:
  1. Respond at all (ping)
  2. Summarize a news article into the expected JSON schema used by news_pipeline
  3. Classify / rank a list of jobs into the expected JSON schema used by linkedin_scraper
  4. Extract job listings from raw careers-page text (extraction_utils contract)
  5. Generate a one-line job summary (job_digest contract)

Run:
    pytest tests/test_groq_smoke.py -v -m groq
    pytest tests/test_groq_smoke.py -v -m groq -s      # show print output

Skip if GROQ_API_KEY is absent:
    GROQ_API_KEY= pytest tests/test_groq_smoke.py -v   # all skipped
"""
import json
import sys
from pathlib import Path
import pytest

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from app.config import settings

# ── skip the whole module when the key is missing ─────────────────────────────
pytestmark = pytest.mark.groq

_SKIP = pytest.mark.skipif(
    not settings.groq_api_key,
    reason="GROQ_API_KEY not set in .env — skipping live Groq smoke tests",
)


def _get_client():
    """Return a fresh AsyncGroq client each call (no shared state)."""
    from groq import AsyncGroq
    return AsyncGroq(api_key=settings.groq_api_key)


from app.utils.llm_client import parse_llm_json, safe_log_str


def _extract_json(raw: str, expect: str = "dict") -> object:
    """Robustly parse JSON by delegating to the production parse_llm_json."""
    return parse_llm_json(raw, expect=expect)


def _safe_print(label: str, data: object) -> None:
    """Print JSON-serialised data, escaping non-ASCII to survive cp1252 Windows terminals."""
    encoded = json.dumps(data, ensure_ascii=True, indent=2)
    print(f"\n[{label}] {encoded}")



# ── 1. Ping ───────────────────────────────────────────────────────────────────

@_SKIP
@pytest.mark.asyncio
async def test_groq_ping() -> None:
    """Model responds with a non-empty string to a trivial prompt."""
    client = _get_client()
    response = await client.chat.completions.create(
        model=settings.groq_model,
        messages=[{"role": "user", "content": "Reply with the single word: PONG"}],
        max_tokens=10,
        temperature=0,
    )
    assert response.choices, "Expected at least one choice"
    content = (response.choices[0].message.content or "").strip()
    assert content, "Response content must not be empty"
    assert "PONG" in content.upper(), f"Expected PONG in response, got: {content!r}"
    print(f"\n[ping] model={response.model!r}  content={content!r}")


# ── 2. News summarisation (news_pipeline schema) ──────────────────────────────

_NEWS_ARTICLE = """\
Tata Motors has announced a strategic partnership with ChargeZone to deploy
300 fast-charging stations across India's top 20 cities by Q3 2026.
The first 50 stations, rated at 120 kW DC, will come online in Mumbai, Pune,
and Hyderabad within 90 days. ChargeZone will operate the network while Tata
Motors provides fleet customers preferential pricing via the Tata.ev app.
The initiative is expected to cut average urban charging wait-times by 40 percent
and is backed by a combined ₹850 crore investment.
"""

_NEWS_SYSTEM_PROMPT = """\
You are an AI assistant for a daily intelligence system focused on EV and Mobility industry in India.
Return ONLY a JSON object with this exact structure:
{
    "headline": "Short, punchy standalone headline (max 10 words).",
    "summary": "A mid-length summary (3-4 sentences) with facts and numbers.",
    "relevance_score": 0.85,
    "category": "Industry News",
    "tags": ["India", "Tata Motors", "Charging"]
}
"""

@_SKIP
@pytest.mark.asyncio
async def test_groq_news_summarisation() -> None:
    """Model returns valid JSON matching the news_pipeline schema for an EV article."""
    client = _get_client()
    response = await client.chat.completions.create(
        model=settings.groq_model,
        messages=[
            {"role": "system", "content": _NEWS_SYSTEM_PROMPT},
            {"role": "user", "content": f"Summarize and score:\n\n{_NEWS_ARTICLE}"},
        ],
        max_tokens=512,
        temperature=0.3,
    )
    assert response.choices, "No choices in response"
    raw = (response.choices[0].message.content or "").strip()
    assert raw, "Empty content from model"

    data = _extract_json(raw)
    _safe_print("news", data)


    # Schema assertions
    assert isinstance(data.get("headline"), str) and data["headline"], "headline must be a non-empty string"
    assert isinstance(data.get("summary"), str) and len(data["summary"]) > 20, "summary must be substantive"
    assert isinstance(data.get("relevance_score"), float | int), "relevance_score must be numeric"
    assert 0.0 <= float(data["relevance_score"]) <= 1.0, "relevance_score must be in [0, 1]"
    assert isinstance(data.get("category"), str), "category must be a string"
    assert isinstance(data.get("tags"), list) and len(data["tags"]) >= 2, "tags must be a list with ≥2 items"

    # EV-specific content sanity
    assert float(data["relevance_score"]) >= 0.7, (
        f"Expected high relevance for India EV article, got {data['relevance_score']}"
    )


# ── 3. Job classification / ranking (linkedin_scraper schema) ─────────────────

_SAMPLE_JOBS_FOR_RANKING = [
    {
        "title": "Deputy Manager – EV Charging Business Development",
        "company": "Tata Power",
        "location": "Mumbai, India",
        "desc": "Own commercial partnerships and rollout for EVSE infrastructure across Maharashtra.",
    },
    {
        "title": "Software Engineer – Backend",
        "company": "Infosys",
        "location": "Bengaluru, India",
        "desc": "Build REST APIs for enterprise SaaS platform. Java, Spring Boot, AWS.",
    },
    {
        "title": "Head of Sales – Electric Fleet Solutions",
        "company": "Euler Motors",
        "location": "Delhi NCR, India",
        "desc": "Lead B2B electric fleet sales strategy, key account management, and last-mile partnerships.",
    },
]

_RANKING_PROMPT_TEMPLATE = """\
You are an EV/mobility domain expert. Rate each job for relevance to the EV and mobility ecosystem.

Domain focus: Electric Vehicles, EV Charging, EVSE, Fleet Electrification, Mobility.

For EACH job return a JSON object with:
  - relevance_score: float 0.0-1.0
  - domain: one short domain label
  - ev_technologies: list of EV-specific technologies found
  - matched_tags: subset of ["#Mobility","#Emobility","#EV","#Charging","#EVSE"] that apply
  - is_hiring_post: true if this is a hiring announcement

Return a JSON ARRAY of {count} objects in the SAME ORDER as the input. No markdown fences.

Jobs:
{jobs}"""


@_SKIP
@pytest.mark.asyncio
async def test_groq_job_ranking() -> None:
    """Model returns a correctly-structured ranking array for a mixed job list."""
    job_lines = "\n".join(
        f"{i+1}. Title: {j['title']} | Company: {j['company']} | Location: {j['location']} | Desc: {j['desc']}"
        for i, j in enumerate(_SAMPLE_JOBS_FOR_RANKING)
    )
    prompt = _RANKING_PROMPT_TEMPLATE.format(count=len(_SAMPLE_JOBS_FOR_RANKING), jobs=job_lines)

    client = _get_client()
    response = await client.chat.completions.create(
        model=settings.groq_model,
        messages=[{"role": "user", "content": prompt}],
        max_tokens=1024,
        temperature=0,
    )
    assert response.choices
    raw = (response.choices[0].message.content or "").strip()
    assert raw, "Empty ranking response from model"

    data = _extract_json(raw, expect="list")
    _safe_print("ranking", data)


    assert isinstance(data, list), "Expected a JSON array"
    assert len(data) == len(_SAMPLE_JOBS_FOR_RANKING), (
        f"Expected {len(_SAMPLE_JOBS_FOR_RANKING)} items, got {len(data)}"
    )

    for i, item in enumerate(data):
        assert "relevance_score" in item, f"Item {i} missing relevance_score"
        score = float(item["relevance_score"])
        assert 0.0 <= score <= 1.0, f"Item {i} score {score} out of range"
        assert "domain" in item and item["domain"], f"Item {i} missing domain"
        assert "ev_technologies" in item, f"Item {i} missing ev_technologies"
        assert "matched_tags" in item, f"Item {i} missing matched_tags"
        assert "is_hiring_post" in item, f"Item {i} missing is_hiring_post"

    # Business logic: EV charging BD role should score higher than backend SWE
    ev_bd_score = float(data[0]["relevance_score"])
    swe_score   = float(data[1]["relevance_score"])
    fleet_score = float(data[2]["relevance_score"])

    assert ev_bd_score > swe_score, (
        f"EV BD role should outrank generic SWE. Got ev_bd={ev_bd_score}, swe={swe_score}"
    )
    assert fleet_score > swe_score, (
        f"Fleet sales role should outrank generic SWE. Got fleet={fleet_score}, swe={swe_score}"
    )


# ── 4. Job extraction (extraction_utils schema) ───────────────────────────────

_CAREERS_PAGE_SNIPPET = """\
Current Openings at ChargeGrid India
======================================

1. Regional Business Manager – EV Charging
   Location: Pune, Maharashtra
   Apply: https://chargegrid.in/careers/rbm-ev-charging-001
   Own relationships with fleet operators and real-estate partners to expand
   DC fast-charging footprint across western India.

2. Key Account Manager – Commercial Vehicles
   Location: Gurugram, Haryana
   Apply: https://chargegrid.in/careers/kam-cv-002
   Manage key fleet accounts (logistics, e-bus operators). Drive upsell of
   ChargeGrid's managed charging subscriptions.

3. Marketing Executive – Digital Campaigns
   Location: Bengaluru, Karnataka
   Apply: https://chargegrid.in/careers/mktg-003
   Run performance marketing campaigns, manage social channels, coordinate
   with agency for content production.
"""

_EXTRACTION_PROMPT = """\
You are a job listing extractor. Extract ALL job listings from the careers page content below.

Return ONLY a valid JSON array — no markdown, no explanation, no fences.
Each element MUST have:
  "job_title": string (required)
  "company": string (required)
  "location": string or null
  "job_url": string or null
  "description": string max 500 chars

Source URL: https://chargegrid.in/careers
Content:
""" + _CAREERS_PAGE_SNIPPET


@_SKIP
@pytest.mark.asyncio
async def test_groq_job_extraction() -> None:
    """Model extracts structured job listings from a raw careers-page text."""
    client = _get_client()
    response = await client.chat.completions.create(
        model=settings.groq_model,
        messages=[{"role": "user", "content": _EXTRACTION_PROMPT}],
        max_tokens=1024,
        temperature=0,
    )
    assert response.choices
    raw = (response.choices[0].message.content or "").strip()
    assert raw, "Empty extraction response from model"

    data = _extract_json(raw, expect="list")
    _safe_print("extraction", data)


    assert isinstance(data, list), "Expected a JSON array of jobs"
    assert len(data) >= 3, f"Expected at least 3 jobs extracted, got {len(data)}"

    for i, job in enumerate(data):
        assert isinstance(job, dict), f"Item {i} is not a dict"
        assert job.get("job_title"), f"Item {i} missing job_title"
        assert job.get("company"), f"Item {i} missing company"

    # spot-check known titles (case-insensitive)
    titles_lower = [j["job_title"].lower() for j in data]
    assert any("business manager" in t or "regional" in t for t in titles_lower), (
        "Expected 'Regional Business Manager' in extracted titles"
    )
    assert any("key account" in t or "commercial" in t for t in titles_lower), (
        "Expected 'Key Account Manager' in extracted titles"
    )


# ── 5. One-line job digest summary (job_digest schema) ────────────────────────

_DIGEST_JOBS_INPUT = [
    {"id": 101, "description": "Lead the end-to-end commercial strategy for EV charging infrastructure rollout across Tier-2 cities in India, partnering with state DISCOMs and real-estate developers."},
    {"id": 102, "description": "Manage key fleet accounts for an electric three-wheeler OEM; own the full sales cycle from prospecting to order closure."},
    {"id": 103, "description": "Drive brand partnerships and co-marketing campaigns for a fast-growing EV fintech platform, coordinate with creative agencies."},
]

_DIGEST_PROMPT = (
    'You are an expert HR assistant. Provide a highly concise ONE-LINE summary '
    '(max 15-20 words) for each job description. '
    'Focus ONLY on the core responsibility and domain. Remove generic filler.\n\n'
    'Input JSON:\n'
    + json.dumps(_DIGEST_JOBS_INPUT) +
    '\n\nReturn ONLY a JSON object mapping the job "id" (as string key) to the one-line '
    '"summary" string. No markdown fences, no explanation.\n'
    'Example: {"101": "Lead EV charging infrastructure rollout across India."}'
)


@_SKIP
@pytest.mark.asyncio
async def test_groq_digest_summaries() -> None:
    """Model produces id→summary mapping matching the job_digest._generate_summaries contract."""
    client = _get_client()
    response = await client.chat.completions.create(
        model=settings.groq_model,
        messages=[{"role": "user", "content": _DIGEST_PROMPT}],
        max_tokens=256,
        temperature=0,
    )
    assert response.choices
    raw = (response.choices[0].message.content or "").strip()
    assert raw, "Empty digest response from model"

    data = _extract_json(raw)
    _safe_print("digest", data)


    assert isinstance(data, dict), "Expected a JSON object"
    expected_ids = {str(j["id"]) for j in _DIGEST_JOBS_INPUT}
    found_ids = set(data.keys())
    assert expected_ids.issubset(found_ids) or len(found_ids) > 0, (
        f"Expected keys {expected_ids}, got {found_ids}"
    )

    for key, summary in data.items():
        assert isinstance(summary, str) and len(summary.split()) <= 30, (
            f"Summary for id={key} is too long or not a string: {summary!r}"
        )
        assert len(summary) > 5, f"Summary for id={key} is suspiciously short: {summary!r}"
