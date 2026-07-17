"""Apify adapter — triggers actor runs and ingests results via the /ingest endpoint.

Each target company has an Apify actor configured. The actor is triggered via the
Apify REST API; on completion the dataset is fetched and normalized into RawItems
POSTed to the internal /ingest endpoint.

Webhook flow (production): Apify → POST /ingest/jobs/apify-complete
Poll flow (local/dev): APScheduler triggers poll_all_target_companies() directly.
"""
from dataclasses import dataclass, field
from datetime import datetime, timezone

import httpx
import structlog

from app.config import settings
from app.schemas.job import JobIn
from app.services.jobs.classifier import extract_skills, infer_experience_level, infer_remote
from app.services.jobs.normalizer import compute_job_dedup_hash

log = structlog.get_logger()

_APIFY_BASE = "https://api.apify.com/v2"


class ApifyQuotaError(Exception):
    """Raised when Apify returns 402 (concurrent-run limit) or 429 (rate limit).

    The orchestrator catches this to stop all further T1 runs in the current
    scrape session rather than hammering the API with more requests.
    """

# ── Scrape result tracking ────────────────────────────────────────────────────

NO_CAREER_URLS        = "NO_CAREER_URLS"
APIFY_RETURNED_ZERO   = "APIFY_RETURNED_ZERO_JOBS"
EXTRACTION_FAILED     = "EXTRACTION_FAILED"
NO_RELEVANT_JOBS      = "NO_RELEVANT_JOBS_FOUND"
SCRAPE_FAILED         = "SCRAPE_FAILED"
SUCCESS               = "SUCCESS"


@dataclass
class CompanyScrapeResult:
    slug: str
    display_name: str
    urls: list[str]
    jobs_extracted: int = 0
    jobs_inserted: int = 0
    status: str = SUCCESS
    error: str | None = None


_WEB_SCRAPER_PAGE_FN = """
async function pageFunction(context) {
    const { $, request } = context;
    const jobs = [];
    $('a[href]').each((_, el) => {
        const href = $(el).attr('href') || '';
        const text = $(el).text().trim();
        if (text && href && (href.includes('job') || href.includes('career') || href.includes('position') || href.includes('role'))) {
            jobs.push({ jobTitle: text, jobUrl: href.startsWith('http') ? href : request.url });
        }
    });
    if (jobs.length === 0) {
        return [{ text: $('body').text().replace(/\\s+/g, ' ').trim().slice(0, 8000), url: request.url }];
    }
    return jobs;
}
"""


def _build_run_input(actor_id: str, run_input: dict) -> dict:
    """Inject actor-specific defaults when missing."""
    if "web-scraper" in actor_id and "pageFunction" not in run_input:
        return {**run_input, "pageFunction": _WEB_SCRAPER_PAGE_FN, "maxPagesPerCrawl": 3}
    if "website-content-crawler" in actor_id:
        return {
            **run_input,
            "crawlerType": "playwright:chrome",
            "maxCrawlDepth": 1,            # follow pagination links from the listing page
            "maxCrawlPages": 3,            # process up to 3 pages per company run
            "waitForNetworkIdleSecs": 7,  # V3: down from 12 — more companies per CU
            "dynamicContentWaitSecs": 5,
            "htmlTransformer": "readableText",
            "readableTextCharThreshold": 50,
            "requestTimeoutSecs": 60,
            "navigationTimeoutSecs": 45,
            "ignoreSslErrors": True,
        }
    return run_input


async def _trigger_actor(actor_id: str, run_input: dict) -> tuple[str, str]:
    """Start an actor run and return the run ID and token used.

    Raises ApifyQuotaError on 402 (concurrent-run limit) or 429 (rate limit)
    so the orchestrator can stop all further T1 runs cleanly rather than
    letting them queue up and fail one-by-one.
    """
    actor_path = actor_id.replace("/", "~")
    run_input = _build_run_input(actor_id, run_input)
    async with httpx.AsyncClient(timeout=30) as client:
        tokens_to_try = [settings.apify_token]
        if settings.apify_token_secondary:
            tokens_to_try.append(settings.apify_token_secondary)

        for attempt, token in enumerate(tokens_to_try):
            if not token:
                continue
            r = await client.post(
                f"{_APIFY_BASE}/acts/{actor_path}/runs",
                params={"token": token},
                json=run_input,
            )
            try:
                r.raise_for_status()
                return r.json()["data"]["id"], token
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                if status in (402, 429):
                    if attempt < len(tokens_to_try) - 1:
                        log.warning("apify.primary_quota_hit_retrying_secondary", status=status)
                        continue
                    raise ApifyQuotaError(
                        f"Apify limit hit ({status}) — concurrent-run or rate limit exceeded"
                    ) from exc
                raise
        raise RuntimeError("No valid apify tokens available")


async def _abort_apify_run(run_id: str, token: str) -> None:
    """Abort a dangling Apify run to stop it consuming free-tier CU."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.post(
                f"{_APIFY_BASE}/actor-runs/{run_id}/abort",
                params={"token": token},
            )
        log.info("apify.run_aborted", run_id=run_id)
    except Exception as exc:
        log.warning("apify.abort_failed", run_id=run_id, error=str(exc))


async def _wait_for_run(run_id: str, token: str, timeout: int = 600) -> str:
    """Poll until the run finishes; return dataset ID.

    Retries transient HTTP errors (429, 503, network blips) up to 3 times
    before giving up. On overall timeout, aborts the Apify run to prevent
    the dangling actor from burning free-tier CU.
    """
    import asyncio

    polls = timeout // 10
    consecutive_http_errors = 0

    async with httpx.AsyncClient(timeout=30) as client:
        for _ in range(polls):
            try:
                r = await client.get(
                    f"{_APIFY_BASE}/actor-runs/{run_id}",
                    params={"token": token},
                )
                r.raise_for_status()
                consecutive_http_errors = 0
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code in (429, 503):
                    consecutive_http_errors += 1
                    if consecutive_http_errors >= 3:
                        raise
                    await asyncio.sleep(15)
                    continue
                raise
            except (httpx.ConnectError, httpx.ReadTimeout):
                consecutive_http_errors += 1
                if consecutive_http_errors >= 3:
                    raise
                await asyncio.sleep(15)
                continue

            data = r.json()["data"]
            if data["status"] in ("SUCCEEDED", "FAILED", "ABORTED", "TIMED-OUT"):
                if data["status"] != "SUCCEEDED":
                    raise RuntimeError(f"Apify run {run_id} ended with {data['status']}")
                return data["defaultDatasetId"]
            await asyncio.sleep(10)

    # Abort the dangling run so it stops consuming CU
    await _abort_apify_run(run_id, token)
    raise TimeoutError(f"Apify run {run_id} did not finish within {timeout}s (aborted)")


async def _fetch_dataset(dataset_id: str, token: str) -> list[dict]:
    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(
            f"{_APIFY_BASE}/datasets/{dataset_id}/items",
            params={"token": token, "format": "json"},
        )
        r.raise_for_status()
        return r.json()  # type: ignore[return-value]


def _normalize_apify_item(item: dict, company_slug: str) -> JobIn:
    title = item.get("title") or item.get("jobTitle") or "Unknown Role"
    company = item.get("company") or company_slug
    location = item.get("location") or item.get("locationName")
    url = item.get("url") or item.get("applyUrl") or item.get("jobUrl") or ""
    posted_raw = item.get("postedDate") or item.get("datePosted")
    posted_at: datetime | None = None
    if posted_raw:
        try:
            posted_at = datetime.fromisoformat(posted_raw.replace("Z", "+00:00"))
        except (ValueError, AttributeError):
            pass

    desc = (item.get("description") or "")[:500]

    return JobIn(
        company=company,
        job_title=title,
        location=location,
        remote=infer_remote(location, desc),
        description=desc,
        job_url=url,
        external_job_id=str(item.get("id") or ""),
        source_type=f"apify_{company_slug}",
        posted_at=posted_at,
    )


def _is_crawler_output(items: list[dict]) -> bool:
    """True when items are raw page blobs (no structured job fields)."""
    if not items:
        return False
    first = items[0]
    has_text = bool(first.get("text") or first.get("markdown"))
    lacks_title = not (first.get("title") or first.get("jobTitle"))
    return has_text and lacks_title


async def _extract_jobs_from_crawler_items(items: list[dict], company_slug: str, career_url: str, token: str) -> list[JobIn]:
    """Extract structured jobs from Apify crawler page blobs.

    Uses the V3 extraction pipeline (extraction_utils) which:
      1. Mines embedded JSON before LLM
      2. Isolates job-dense HTML region
      3. Chunks large content across multiple LLM calls
    Replaces the old html[:12000] hard truncation.
    """
    import re as _re

    from app.services.jobs.extraction_utils import _is_eightfold_url, extract_jobs_from_html

    # For Eightfold portals: drop /settings and /themes pages before extraction.
    # These pages contain only theme/CSS config JSON (180-220 K chars) which
    # overwhelms every chunk the LLM sees.  The generic pipeline then gets 0
    # job data and the Eightfold fallback also gets a poisoned combined blob.
    if _is_eightfold_url(career_url):
        _EF_SKIP = ("/settings", "/themes", "/privacy", "/cookie")
        filtered = [
            item for item in items
            if not any(skip in (item.get("url") or "") for skip in _EF_SKIP)
        ]
        if filtered:
            log.info(
                "apify.eightfold_page_filter",
                company=company_slug,
                before=len(items),
                after=len(filtered),
            )
            items = filtered

    async def _best_content(item: dict) -> str:
        md  = item.get("markdown") or ""
        txt = item.get("text") or ""
        inline_html = item.get("html") or ""

        # prefer longest of markdown / plain text
        content = md if len(md) >= len(txt) else txt

        # upgrade to inline HTML if text/markdown are thin
        if len(content) < 1000 and inline_html:
            stripped = _re.sub(r"<[^>]+>", " ", inline_html)
            stripped = _re.sub(r"\s{2,}", " ", stripped).strip()
            if len(stripped) > len(content):
                content = stripped

        # fetch raw HTML from htmlUrl when content is still short
        html_url = item.get("htmlUrl") or ""
        if html_url and len(content) < 2000:
            try:
                async with httpx.AsyncClient(timeout=30) as client:
                    r = await client.get(html_url, params={"token": token})
                    r.raise_for_status()
                    raw_html = r.text
                    stripped = _re.sub(r"<script[^>]*>.*?</script>", " ", raw_html, flags=_re.S | _re.I)
                    stripped = _re.sub(r"<style[^>]*>.*?</style>", " ", stripped, flags=_re.S | _re.I)
                    stripped = _re.sub(r"<[^>]+>", " ", stripped)
                    stripped = _re.sub(r"\s{2,}", " ", stripped).strip()
                    if len(stripped) > len(content):
                        content = stripped
            except Exception as exc:
                log.debug("apify.htmlurl_fetch_failed", company=company_slug, error=str(exc))

        return content  # V3: no truncation here — extraction_utils handles chunking

    contents = [await _best_content(item) for item in items]
    combined = "\n\n".join(c for c in contents if c)

    log.info(
        "apify.crawler_content",
        company=company_slug,
        pages=len(items),
        combined_chars=len(combined),
    )

    if len(combined) < 2000:
        log.warning("apify.too_few_characters_escalating_to_t2", company=company_slug, chars=len(combined))
        return []

    # V3 extraction pipeline — no 12K cap
    jobs = await extract_jobs_from_html(combined, career_url, company_slug)
    for j in jobs:
        if j.source_type in ("changedetection", "unknown"):
            j.source_type = f"apify_{company_slug}"
    return jobs


async def poll_actor(actor_id: str, company_slug: str, run_input: dict) -> list[JobIn]:
    if not settings.apify_token and not settings.apify_token_secondary:
        log.warning("apify.token_missing", company=company_slug)
        return []
    run_id, used_token = await _trigger_actor(actor_id, run_input)
    dataset_id = await _wait_for_run(run_id, used_token)
    _MAX_PAGES = 3
    raw_items_all = await _fetch_dataset(dataset_id, used_token)
    raw_items     = raw_items_all[:_MAX_PAGES]

    log.info(
        "apify.pages_received",
        company=company_slug,
        pages_discovered=len(raw_items_all),
        pages_processed=len(raw_items),
        actor=actor_id,
    )

    from app.services.jobs.changedetection import ExtractionError

    if _is_crawler_output(raw_items):
        career_url = (run_input.get("startUrls") or [{}])[0].get("url", "")
        log.info("apify.crawler_output_detected", company=company_slug, pages=len(raw_items))
        try:
            jobs = await _extract_jobs_from_crawler_items(raw_items, company_slug, career_url, used_token)
        except ExtractionError as exc:
            log.error(
                "apify.extraction_error",
                company=company_slug,
                reason=str(exc),
            )
            print(f"\n  [ERROR] LLM extraction failed — NOT 'no jobs found': {exc}\n")
            return []
    else:
        jobs = [_normalize_apify_item(item, company_slug) for item in raw_items]

    log.info("apify.fetched", company=company_slug, count=len(jobs))
    return jobs


async def run_t1_apify(company) -> list[JobIn]:  # type: ignore[no-untyped-def]
    """T1-only Apify run — no HTTP fallback.

    Returns extracted JobIn list (pre-filter). Empty list on any failure so
    the orchestrator can classify the outcome and decide whether to escalate
    to T2 Browserbase.
    """
    career_urls: list[str] = company.career_urls or []
    if not career_urls:
        log.warning("apify.no_career_urls", company=company.slug)
        return []

    if not settings.apify_token and not settings.apify_token_secondary:
        log.warning("apify.no_token", company=company.slug)
        return []

    actor_id  = company.apify_actor_id or "apify/website-content-crawler"
    # Cap at 2 URLs per run to conserve Apify credits (listing page only)
    run_input = {"startUrls": [{"url": u} for u in career_urls[:2]]}

    log.info("apify.t1_run", company=company.slug, actor=actor_id, urls=len(career_urls[:2]))
    return await poll_actor(actor_id, company.slug, run_input)


# ── Legacy shim — kept for backward-compat with existing callers ──────────────

async def _fetch_company_jobs_by_ats(company) -> list[JobIn]:  # type: ignore[no-untyped-def]
    """Backward-compat shim. New code should use run_t1_apify() directly."""
    return await run_t1_apify(company)


async def poll_all_target_companies(slug: str | None = None) -> dict[str, object]:
    """Backward-compat entry point. Delegates to the V3 orchestrator."""
    from app.services.jobs.scrape_orchestrator import orchestrate_scrape
    return await orchestrate_scrape(slug=slug)


async def _ingest_jobs(jobs: list[JobIn], company_slug: str, target_company_id: int | None = None) -> int:
    """Filter and persist jobs for a target company.

    Returns the number of new rows inserted (0 on empty input or pipeline error).
    """
    if not jobs:
        log.info("apify.no_jobs_fetched", company=company_slug)
        return 0

    from app.database import SessionLocal
    from app.services.jobs.job_pipeline import persist_filtered_jobs

    log.info("apify.ingesting", company=company_slug, count=len(jobs))
    db = SessionLocal()
    try:
        kept = await persist_filtered_jobs(jobs, db, target_company_id=target_company_id)
        log.info("apify.pipeline_complete", company=company_slug, kept=kept, total=len(jobs))
        return kept
    except Exception:
        log.exception("apify.pipeline_error", company=company_slug)
        return 0
    finally:
        await db.close()
