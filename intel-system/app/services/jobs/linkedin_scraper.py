"""LinkedIn Jobs + Hiring Posts — EV/Mobility domain focused.

Retrieval strategies (tried in order per keyword):
  1. LinkedIn Guest Jobs API — paginated (start=0,25,50,...), direct HTML
     parsing via _parse_job_cards_from_html() first; Gemini fallback only when
     parse yields nothing (malformed / structurally changed HTML).
  2. Apify fallback — when APIFY_TOKEN is set and guest API yields nothing.

Hiring posts: any retrieved card whose description or title contains a hiring
  signal ("we're hiring", "hiring now", etc.) is tagged
  source_type="linkedin_hiring_post".

Post-retrieval pipeline:
  - _rank_jobs_with_llm() — single batched OpenRouter/Nemotron-3-Ultra call (up to 20 jobs) for:
      relevance_score, domain, ev_technologies, matched_tags, is_hiring_post
  - Result exposed through JobRank / search_linkedin_ev_jobs_ranked().

Layer separation:
  retrieval  → _fetch_via_guest_api_paginated / _fetch_via_apify
  parsing    → _parse_job_cards_from_html
  filtering  → classifier.is_ev_domain_relevant / is_hiring_post
  ranking    → _rank_jobs_with_llm
"""
from __future__ import annotations

import asyncio
import json
import random
import re
from dataclasses import dataclass, field
from datetime import datetime

import httpx
import structlog
from app.config import settings

from app.schemas.job import JobIn
from app.services.jobs.classifier import (
    EV_SEARCH_TERMS,
    classify_job_domain,
    extract_ev_keywords,
    infer_experience_level,
    is_ev_domain_relevant,
    is_hiring_post,
    match_target_tags,
    score_ev_relevance,
)
from app.utils.llm_client import call_llm_with_rate_limit

log = structlog.get_logger()

_GUEST_API = "https://www.linkedin.com/jobs-guest/jobs/api/seeMoreJobPostings/search"
_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.7",
    "Referer": "https://www.linkedin.com/",
}
_PAGE_SIZE = 25
_APIFY_LINKEDIN_ACTOR = "curious_coder/linkedin-jobs-scraper"

# ── Regex patterns for direct HTML card parsing ───────────────────────────────
_LI_SPLIT = re.compile(r"(?=<li\b)", re.I)
_CARD_URL = re.compile(
    r'href="(https://(?:www\.)?linkedin\.com/jobs/view/(\d+)[^"]*)"', re.I
)
_CARD_TITLE = re.compile(
    r'class="base-search-card__title"[^>]*>\s*([^\n<]{2,120}?)\s*(?:\n|</)', re.I
)
_CARD_COMPANY = re.compile(
    r'class="hidden-nested-link"[^>]*>\s*([^\n<]{1,120}?)\s*(?:\n|</)', re.I
)
_CARD_LOCATION = re.compile(
    r'class="job-search-card__location"[^>]*>\s*([^\n<]{2,120}?)\s*(?:\n|</)', re.I
)
_CARD_DATE = re.compile(
    r'class="job-search-card__listdate[^"]*"[^>]*datetime="(\d{4}-\d{2}-\d{2})"', re.I
)
_CARD_SR_ONLY = re.compile(r'<span class="sr-only">\s*([^<]+?)\s*</span>', re.I)


# ── Rich result type ──────────────────────────────────────────────────────────

@dataclass
class JobRank:
    job: JobIn
    relevance_score: float = 0.0
    domain: str = ""
    ev_technologies: list[str] = field(default_factory=list)
    matched_tags: list[str] = field(default_factory=list)
    is_hiring_post_flag: bool = False
    source_page: int = 1


# ── Direct HTML parser ────────────────────────────────────────────────────────

def _parse_job_cards_from_html(
    html: str,
    default_location: str = "India",
    source_page: int = 1,
) -> list[tuple[JobIn, int]]:
    """Extract structured job cards from LinkedIn guest-API HTML without Gemini.

    Returns list of (JobIn, page_number) tuples.
    Falls back gracefully when CSS class names are unexpected.
    """
    results: list[tuple[JobIn, int]] = []
    seen_ids: set[str] = set()

    # Split the HTML blob into individual <li> card fragments.
    fragments = _LI_SPLIT.split(html)

    for frag in fragments:
        if "base-search-card" not in frag and "job-search-card" not in frag:
            continue

        url_m = _CARD_URL.search(frag)
        if not url_m:
            continue
        full_url = url_m.group(1)
        job_id = url_m.group(2)
        # Canonicalise URL — strip tracking params
        clean_url = f"https://www.linkedin.com/jobs/view/{job_id}/"

        if job_id in seen_ids:
            continue
        seen_ids.add(job_id)

        title_m = _CARD_TITLE.search(frag)
        if not title_m:
            # Try sr-only aria label as fallback: "Title at Company"
            sr_m = _CARD_SR_ONLY.search(frag)
            if sr_m:
                parts = sr_m.group(1).split(" at ", 1)
                title_raw = parts[0].strip()
                company_raw = parts[1].strip() if len(parts) > 1 else "Unknown"
            else:
                continue
        else:
            title_raw = title_m.group(1).strip()
            company_m = _CARD_COMPANY.search(frag)
            company_raw = company_m.group(1).strip() if company_m else "Unknown"

        loc_m = _CARD_LOCATION.search(frag)
        location = loc_m.group(1).strip() if loc_m else default_location

        date_m = _CARD_DATE.search(frag)
        posted_at: datetime | None = None
        if date_m:
            try:
                posted_at = datetime.fromisoformat(date_m.group(1))
            except ValueError:
                pass

        # Hiring post detection from the title/aria text
        combined_text = f"{title_raw} {company_raw}"
        src_type = "linkedin_hiring_post" if is_hiring_post(combined_text) else "linkedin"

        results.append((
            JobIn(
                company=company_raw,
                job_title=title_raw,
                location=location,
                job_url=clean_url,
                external_job_id=job_id,
                source_type=src_type,
                posted_at=posted_at,
            ),
            source_page,
        ))

    return results


# ── Rate-limited page fetcher ─────────────────────────────────────────────────

async def _fetch_page(
    client: httpx.AsyncClient,
    keyword: str,
    location: str,
    start: int,
    retries: int = 3,
) -> str:
    """Fetch one LinkedIn guest-API page with retry+backoff. Returns raw HTML."""
    params = {
        "keywords": keyword,
        "location": location,
        "start": start,
        "count": _PAGE_SIZE,
        "f_TPR": "r2592000",  # last 30 days
    }
    for attempt in range(1, retries + 1):
        try:
            r = await client.get(_GUEST_API, params=params)
            if r.status_code == 429:
                wait = 10 * attempt
                log.warning("linkedin.rate_limited", start=start, wait=wait)
                await asyncio.sleep(wait)
                continue
            r.raise_for_status()
            return r.text
        except httpx.HTTPStatusError as exc:
            if attempt == retries:
                log.warning("linkedin.http_error", start=start, status=exc.response.status_code)
                return ""
            await asyncio.sleep(5 * attempt)
        except Exception as exc:
            if attempt == retries:
                log.warning("linkedin.fetch_error", start=start, error=str(exc))
                return ""
            await asyncio.sleep(5 * attempt)
    return ""


# ── Paginated guest-API retrieval ─────────────────────────────────────────────

async def _fetch_via_guest_api_paginated(
    keyword: str,
    location: str = "India",
    max_pages: int = 3,
) -> list[tuple[JobIn, int]]:
    """Paginate through LinkedIn job search results for one keyword.

    Uses _parse_job_cards_from_html() primarily; falls back to Gemini extraction
    only when direct parse yields 0 results on the first page.
    """
    all_results: list[tuple[JobIn, int]] = []
    seen_job_ids: set[str] = set()

    async with httpx.AsyncClient(
        timeout=30, headers=_HEADERS, follow_redirects=True
    ) as client:
        for page_idx in range(max_pages):
            start = page_idx * _PAGE_SIZE

            # Rate limiting between pages
            if page_idx > 0:
                await asyncio.sleep(random.uniform(1.5, 3.0))

            html = await _fetch_page(client, keyword, location, start)

            if not html or len(html.strip()) < 50:
                log.info("linkedin.empty_page", keyword=keyword, page=page_idx + 1)
                break

            cards = _parse_job_cards_from_html(html, location, source_page=page_idx + 1)

            if not cards and page_idx == 0:
                # HTML structure may have changed — Gemini fallback for page 1 only
                log.info("linkedin.direct_parse_empty_fallback_llm", keyword=keyword)
                from app.services.jobs.extraction_utils import extract_jobs_from_diff
                source_url = (
                    f"https://www.linkedin.com/jobs/search/"
                    f"?keywords={keyword}&location={location}"
                )
                llm_jobs = await extract_jobs_from_diff(html[:8000], source_url)
                for j in llm_jobs:
                    j.source_type = "linkedin"
                    cards = [(j, 1)]

            new_on_page = 0
            for job, pg in cards:
                ext_id = job.external_job_id or ""
                if ext_id and ext_id in seen_job_ids:
                    continue
                if ext_id:
                    seen_job_ids.add(ext_id)
                all_results.append((job, pg))
                new_on_page += 1

            log.info(
                "linkedin.page_fetched",
                keyword=keyword,
                page=page_idx + 1,
                new_cards=new_on_page,
            )

            # Stop early if page returned fewer cards than PAGE_SIZE (last page)
            if new_on_page < _PAGE_SIZE // 2:
                break

    return all_results


# ── Apify fallback ────────────────────────────────────────────────────────────

def _parse_apify_linkedin_item(item: dict) -> JobIn | None:
    title = (
        item.get("title") or item.get("jobTitle") or item.get("positionName") or ""
    ).strip()
    if not title:
        return None

    desc = (
        item.get("description") or item.get("descriptionText") or item.get("jobDescription") or ""
    )[:500]

    company = (
        item.get("company") or item.get("companyName") or item.get("employerName") or "Unknown"
    )
    location = item.get("location") or item.get("locationText") or item.get("place")
    url = (
        item.get("url") or item.get("jobUrl") or item.get("applyUrl")
        or item.get("linkedInUrl") or ""
    )
    ext_id = str(item.get("id") or item.get("jobId") or item.get("linkedinId") or "")

    posted_raw = item.get("postedAt") or item.get("datePosted") or item.get("date")
    posted_at: datetime | None = None
    if posted_raw:
        try:
            posted_at = datetime.fromisoformat(str(posted_raw).replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass

    src = "linkedin_hiring_post" if is_hiring_post(f"{title} {desc}") else "linkedin"

    return JobIn(
        company=company,
        job_title=title,
        location=location,
        description=desc,
        job_url=url,
        external_job_id=ext_id,
        source_type=src,
        posted_at=posted_at,
    )


async def _fetch_via_apify(keyword: str, location: str = "India") -> list[JobIn]:
    if not settings.apify_token and not settings.apify_token_secondary:
        return []

    from app.services.jobs.apify_adapter import _fetch_dataset, _trigger_actor, _wait_for_run

    run_input = {
        "searchUrl": (
            f"https://www.linkedin.com/jobs/search/"
            f"?keywords={keyword.replace(' ', '%20')}"
            f"&location={location.replace(' ', '%20')}"
            f"&f_TPR=r604800"
        ),
        "maxJobs": _PAGE_SIZE,
    }
    try:
        run_id, used_token = await _trigger_actor(_APIFY_LINKEDIN_ACTOR, run_input)
        dataset_id = await _wait_for_run(run_id, used_token)
        raw_items = await _fetch_dataset(dataset_id, used_token)
    except Exception as exc:
        log.warning("linkedin.apify_failed", keyword=keyword, error=str(exc))
        return []

    return [j for item in raw_items if (j := _parse_apify_linkedin_item(item)) is not None]


# ── Gemini semantic ranking (single batched call) ─────────────────────────────

async def _rank_jobs_with_llm(jobs: list[JobIn]) -> list[dict]:
    """Score a batch of jobs semantically.  Returns one dict per job (same order).

    Dict keys: relevance_score (float), domain (str), ev_technologies (list[str]),
    matched_tags (list[str]), is_hiring_post (bool).

    Falls back gracefully to heuristic scoring when Gemini is unavailable.
    """
    if not jobs:
        return []

    if not settings.tensormux_api_key:
        return [_heuristic_rank(j) for j in jobs]

    job_list_text = "\n".join(
        f"{i + 1}. Title: {j.job_title} | Company: {j.company} "
        f"| Location: {j.location or 'n/a'} "
        f"| Desc: {(j.description or '')[:200]}"
        for i, j in enumerate(jobs)
    )

    prompt = f"""You are an EV/mobility domain expert. Rate each job for relevance to the
EV and mobility ecosystem.

Domain focus: Electric Vehicles (#EV), E-Mobility (#Emobility), EV Charging (#Charging),
EVSE (#EVSE), Charging Infrastructure, Battery Electric Vehicles, Fleet Charging,
Grid Integration, Smart Charging, V2G, OCPP, Mobility (#Mobility).

For EACH job return a JSON object with:
  - relevance_score: float 0.0-1.0 (0=completely irrelevant, 1.0=perfect fit)
  - domain: one short domain label (e.g. "EV Charging Infrastructure", "Fleet & Mobility",
    "Battery & Powertrain", "Grid & Energy", "Software / Platform", "Hardware / Electronics")
  - ev_technologies: list of EV-specific technologies found (OCPP, CCS, CHAdeMO,
    ISO 15118, J1772, V2G, V2X, DCFC, BMS, EVMS, smart charging, etc.)
  - matched_tags: subset of ["#Mobility","#Emobility","#EV","#Charging","#EVSE"] that apply
  - is_hiring_post: true if this is a hiring announcement (we're hiring, job opening, etc.)

Return a JSON ARRAY of {len(jobs)} objects in the SAME ORDER as the input. No markdown fences.

Jobs:
{job_list_text}"""

    from openai import AsyncOpenAI

    or_client = AsyncOpenAI(
        api_key=settings.tensormux_api_key,
        base_url="https://api.tensormux.com/v1",
        default_headers={
            "HTTP-Referer": "https://daily-intel.app",
            "X-Title": "Daily Intel",
        },
    )
    last_exc: Exception | None = None

    for attempt in range(1, 4):
        try:
            response = await call_llm_with_rate_limit(
                client=or_client,
                model=settings.tensormux_model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0.01,
            )
            if not getattr(response, "choices", None):
                raise ValueError("Empty choices in response")
            text = response.choices[0].message.content
            if not text:
                raise ValueError("Empty content in response")
            text = text.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
            ranked = json.loads(text)
            if isinstance(ranked, list) and len(ranked) == len(jobs):
                return [_sanitise_rank(r) for r in ranked]
            break
        except Exception as exc:
            last_exc = exc
            if attempt < 3:
                await asyncio.sleep(5 * attempt)

    log.warning("linkedin.openrouter_ranking_failed", error=str(last_exc))
    return [_heuristic_rank(j) for j in jobs]


def _sanitise_rank(raw: object) -> dict:
    if not isinstance(raw, dict):
        return {"relevance_score": 0.0, "domain": "Other", "ev_technologies": [], "matched_tags": [], "is_hiring_post": False}
    return {
        "relevance_score": float(raw.get("relevance_score") or 0.0),
        "domain":          str(raw.get("domain") or "Other"),
        "ev_technologies": list(raw.get("ev_technologies") or []),
        "matched_tags":    list(raw.get("matched_tags") or []),
        "is_hiring_post":  bool(raw.get("is_hiring_post") or False),
    }


def _heuristic_rank(job: JobIn) -> dict:
    """Deterministic heuristic fallback when Gemini is unavailable."""
    return {
        "relevance_score":  score_ev_relevance(job.job_title, job.description or "", job.location),
        "domain":           classify_job_domain(job.job_title, job.description or ""),
        "ev_technologies":  extract_ev_keywords(job.job_title, job.description or ""),
        "matched_tags":     match_target_tags(job.job_title, job.description or ""),
        "is_hiring_post":   job.source_type == "linkedin_hiring_post",
    }


# ── Public search API ─────────────────────────────────────────────────────────

async def search_linkedin_ev_jobs(
    search_terms: list[str] | None = None,
    location: str = "India",
    max_pages: int = 3,
) -> list[JobIn]:
    """Search LinkedIn for EV/Mobility jobs. Returns list[JobIn] (scheduler-safe)."""
    ranked = await search_linkedin_ev_jobs_ranked(
        search_terms=search_terms,
        location=location,
        max_pages=max_pages,
        llm_rank=False,
    )
    return [r.job for r in ranked]


async def search_linkedin_ev_jobs_ranked(
    search_terms: list[str] | None = None,
    location: str = "India",
    max_pages: int = 3,
    llm_rank: bool = True,
    keywords_filter: list[str] | None = None,
    company_filter: str | None = None,
) -> list[JobRank]:
    """Full pipeline: retrieve → parse → EV-filter → (optionally) Gemini rank.

    Args:
        search_terms: LinkedIn search keywords (default: EV_SEARCH_TERMS)
        location: LinkedIn location filter
        max_pages: pagination depth per search term
        llm_rank: whether to call OpenRouter/Nemotron-3-Ultra for semantic scoring
        keywords_filter: if set, only keep jobs matching at least one keyword
        company_filter: if set, only keep jobs from this company (substring match)
    """
    terms = search_terms or EV_SEARCH_TERMS
    seen_urls: set[str] = set()
    raw_pairs: list[tuple[JobIn, int]] = []  # (job, page_number)

    for i, term in enumerate(terms):
        if i > 0:
            await asyncio.sleep(random.uniform(2.0, 4.0))

        pairs = await _fetch_via_guest_api_paginated(term, location, max_pages)

        if not pairs and (settings.apify_token or settings.apify_token_secondary):
            log.info("linkedin.falling_back_to_apify", term=term)
            apify_jobs = await _fetch_via_apify(term, location)
            pairs = [(j, 1) for j in apify_jobs]

        for job, pg in pairs:
            if job.job_url and job.job_url in seen_urls:
                continue
            if job.job_url:
                seen_urls.add(job.job_url)
            raw_pairs.append((job, pg))

    # EV domain filter
    ev_pairs = [
        (j, pg) for j, pg in raw_pairs
        if is_ev_domain_relevant(j.job_title, j.description or "", location=j.location)
    ]

    # Optional company filter
    if company_filter:
        cf = company_filter.lower()
        ev_pairs = [(j, pg) for j, pg in ev_pairs if cf in j.company.lower()]

    # Optional keyword filter
    if keywords_filter:
        kf = [k.lower() for k in keywords_filter]
        ev_pairs = [
            (j, pg) for j, pg in ev_pairs
            if any(k in (j.job_title + " " + (j.description or "")).lower() for k in kf)
        ]

    jobs_only = [j for j, _ in ev_pairs]
    page_map = {i: pg for i, (_, pg) in enumerate(ev_pairs)}

    log.info("linkedin.total_ev_jobs", count=len(jobs_only))

    # Gemini ranking (batched in groups of 20)
    all_ranks: list[dict] = []
    if llm_rank:
        batch_size = 20
        for start in range(0, len(jobs_only), batch_size):
            batch = jobs_only[start : start + batch_size]
            batch_ranks = await _rank_jobs_with_llm(batch)
            all_ranks.extend(batch_ranks)
    else:
        all_ranks = [_heuristic_rank(j) for j in jobs_only]

    results: list[JobRank] = []
    for i, (job, rank) in enumerate(zip(jobs_only, all_ranks)):
        results.append(
            JobRank(
                job=job,
                relevance_score=rank["relevance_score"],
                domain=rank["domain"],
                ev_technologies=rank["ev_technologies"],
                matched_tags=rank["matched_tags"],
                is_hiring_post_flag=rank["is_hiring_post"] or job.source_type == "linkedin_hiring_post",
                source_page=page_map.get(i, 1),
            )
        )

    # Sort by relevance descending
    results.sort(key=lambda r: r.relevance_score, reverse=True)
    return results


# ── Scheduler entry point ─────────────────────────────────────────────────────

async def poll_linkedin() -> None:
    """APScheduler entry point — filter and persist LinkedIn EV business jobs."""
    jobs = await search_linkedin_ev_jobs()
    if not jobs:
        return

    from app.database import SessionLocal
    from app.services.jobs.job_pipeline import persist_filtered_jobs

    db = SessionLocal()
    try:
        kept = await persist_filtered_jobs(jobs, db)
        log.info("linkedin.pipeline_complete", kept=kept, total=len(jobs))
    finally:
        await db.close()
