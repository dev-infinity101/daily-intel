"""LinkedIn Hiring Posts adapter — T4 scraper for the V4 job pipeline.

Uses the same Apify actor as the news LinkedIn module
(harvestapi/linkedin-post-search) but with a single targeted query:

    '"hiring" "EV" "india"'

Fetches at most MAX_POSTS (20) posts, restricted to the past week.
Converts raw LinkedIn posts into JobIn objects and routes them through
the standard persist_filtered_jobs() pipeline.

This is intentionally lightweight — 20 posts max, no T1 budget guard,
same httpx pattern used by app/services/news/linkedin.py.
source_type is 'linkedin_posts_hiring' to distinguish from:
  - 'linkedin_news'       (news table, hashtag posts)
  - 'linkedin_jobs_apify' (job listing actor)
"""
import asyncio
from datetime import UTC, datetime, timedelta
from typing import Any

import httpx
import structlog

from app.config import settings
from app.database import SessionLocal
from app.schemas.job import JobIn
from app.services.jobs.job_pipeline import persist_filtered_jobs

log = structlog.get_logger(__name__)

_APIFY_BASE = "https://api.apify.com/v2"

# Actor shared with the news LinkedIn module
_ACTOR_ID = "harvestapi/linkedin-post-search"

# Single query — must be one string (not split into multiple)
HIRING_QUERY = '"hiring" "EV" "india"'
MAX_POSTS = 50
WEEK_SECONDS = 7 * 24 * 3600


# ── Actor lifecycle ───────────────────────────────────────────────────────────

async def _trigger_hiring_actor() -> tuple[str, str]:
    """Fire the harvestapi/linkedin-post-search actor for EV hiring posts.

    Uses primary → secondary token fallback, matching news/linkedin.py pattern.
    Returns (run_id, token_used).
    """
    actor_path = _ACTOR_ID.replace("/", "~")
    run_input: dict[str, Any] = {
        "searchQueries": [HIRING_QUERY],  # single query string, not split
        "maxPosts": MAX_POSTS,
        # dateRange is a supported field on harvestapi actor; local post-filter
        # below acts as the safety net if the actor ignores this field.
        "dateRange": "pastWeek",
    }

    tokens_to_try = [settings.apify_token]
    if settings.apify_token_secondary:
        tokens_to_try.append(settings.apify_token_secondary)

    async with httpx.AsyncClient(timeout=30) as client:
        for attempt, token in enumerate(tokens_to_try):
            if not token:
                continue
            try:
                r = await client.post(
                    f"{_APIFY_BASE}/acts/{actor_path}/runs",
                    params={"token": token},
                    json=run_input,
                )
                r.raise_for_status()
                run_id: str = r.json()["data"]["id"]
                log.info(
                    "linkedin_posts_hiring.actor_started",
                    run_id=run_id,
                    query=HIRING_QUERY,
                    max_posts=MAX_POSTS,
                )
                return run_id, token
            except httpx.HTTPStatusError as exc:
                status = exc.response.status_code
                if status in (401, 402, 403, 429) and attempt < len(tokens_to_try) - 1:
                    log.warning(
                        "linkedin_posts_hiring.primary_token_failed",
                        status=status,
                    )
                    continue
                raise
    raise RuntimeError("linkedin_posts_hiring: No valid Apify tokens available")


async def _wait_for_dataset(run_id: str, token: str, timeout: int = 300) -> list[dict]:
    """Poll until the run finishes and return raw items from its dataset."""
    dataset_id: str | None = None
    polls = timeout // 10

    async with httpx.AsyncClient(timeout=30) as client:
        for _ in range(polls):
            await asyncio.sleep(10)
            r = await client.get(
                f"{_APIFY_BASE}/actor-runs/{run_id}",
                params={"token": token},
            )
            r.raise_for_status()
            data = r.json()["data"]
            status = data["status"]
            if status == "SUCCEEDED":
                dataset_id = data["defaultDatasetId"]
                break
            if status in ("FAILED", "ABORTED", "TIMED-OUT"):
                raise RuntimeError(
                    f"linkedin_posts_hiring: Apify run {run_id} ended with {status}"
                )

    if not dataset_id:
        raise TimeoutError(
            f"linkedin_posts_hiring: Apify run {run_id} did not complete within {timeout}s"
        )

    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(
            f"{_APIFY_BASE}/datasets/{dataset_id}/items",
            params={"token": token, "format": "json"},
        )
        r.raise_for_status()
        return r.json()  # type: ignore[return-value]


# ── Normalisation ─────────────────────────────────────────────────────────────

def _parse_posted_at(post: dict[str, Any]) -> datetime | None:
    """Best-effort parse of postedAt.date or postedAt.timestamp."""
    posted_at_block: dict = post.get("postedAt") or {}
    date_raw: str = posted_at_block.get("date") or ""
    try:
        return datetime.fromisoformat(date_raw.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        pass
    ts = posted_at_block.get("timestamp")
    if ts:
        try:
            return datetime.fromtimestamp(ts / 1000, tz=UTC)
        except (OSError, OverflowError, ValueError):
            pass
    return None


def _is_within_week(posted_at: datetime | None) -> bool:
    """Local safety net: reject posts older than 7 days regardless of actor filter."""
    if posted_at is None:
        # No date → keep; don't silently discard posts the actor thought were recent
        return True
    cutoff = datetime.now(UTC) - timedelta(seconds=WEEK_SECONDS)
    # Make naive datetimes UTC-aware for comparison
    if posted_at.tzinfo is None:
        posted_at = posted_at.replace(tzinfo=UTC)
    return posted_at >= cutoff


def normalize_hiring_post(post: dict[str, Any]) -> JobIn | None:
    """Convert a raw harvestapi post dict into a JobIn for the jobs pipeline.

    Field mapping:
      - job_title   : first non-empty line of content (≤120 chars), or "EV Hiring Post"
      - company     : author.name  (person/company who posted)
      - description : full post content (≤500 chars) with author info prepended
      - job_url     : linkedinUrl of the post
      - location    : "India" (inferred from the query; posts rarely embed location)
      - source_type : "linkedin_posts_hiring"
    """
    content: str = (post.get("content") or "").strip()
    if not content:
        return None

    post_id = str(post.get("id") or "")
    url = post.get("linkedinUrl") or None
    if not url:
        return None  # no clickable link → not useful in digest

    author: dict = post.get("author") or {}
    author_name = (author.get("name") or "Unknown").strip()
    author_info = (author.get("info") or "").strip()

    posted_at = _parse_posted_at(post)

    # Use the first non-empty line as the job title, capped at 120 chars
    first_line = next((ln.strip() for ln in content.splitlines() if ln.strip()), "")
    job_title = (first_line[:120] or "EV Hiring Post")

    author_line = f"{author_name} ({author_info})" if author_info else author_name
    description = f"[LinkedIn post by {author_line}] {content[:460]}"

    return JobIn(
        company=author_name[:128],
        job_title=job_title,
        location="India",
        remote=False,
        department=None,
        description=description[:500],
        job_url=url,
        external_job_id=post_id or None,
        source_type="linkedin_posts_hiring",
        posted_at=posted_at,
    )


# ── Main entry point ──────────────────────────────────────────────────────────

async def fetch_and_persist_linkedin_posts_hiring() -> tuple[list[JobIn], dict]:
    """T4: Fetch EV hiring posts from LinkedIn and persist through the job pipeline.

    Single query: '"hiring" "EV" "india"', max 20 posts, last 7 days only.

    Returns:
        (normalized_jobs, stats_dict)
        normalized_jobs — pre-filter list for gap analysis bookkeeping
        stats_dict      — insertion metrics with status/error keys
    """
    if not settings.apify_token and not settings.apify_token_secondary:
        log.warning("linkedin_posts_hiring.disabled", reason="no_apify_token")
        return [], {
            "status": "skipped",
            "source": "linkedin_posts_hiring",
            "reason": "no_apify_token",
            "jobs_fetched": 0,
            "jobs_inserted": 0,
        }

    log.info("linkedin_posts_hiring.start", query=HIRING_QUERY, max_posts=MAX_POSTS)

    # ── 1. Trigger actor and wait ─────────────────────────────────────────────
    try:
        run_id, used_token = await _trigger_hiring_actor()
        raw_posts = await _wait_for_dataset(run_id, used_token)
    except httpx.HTTPStatusError as exc:
        log.error(
            "linkedin_posts_hiring.apify_http_error",
            status_code=exc.response.status_code,
            error=str(exc),
        )
        return [], {
            "status": "error",
            "source": "linkedin_posts_hiring",
            "reason": f"apify_http_error_{exc.response.status_code}",
            "jobs_fetched": 0,
            "jobs_inserted": 0,
        }
    except Exception as exc:
        log.error("linkedin_posts_hiring.actor_failed", error=str(exc))
        return [], {
            "status": "error",
            "source": "linkedin_posts_hiring",
            "reason": "actor_failed",
            "error": str(exc),
            "jobs_fetched": 0,
            "jobs_inserted": 0,
        }

    log.info("linkedin_posts_hiring.fetched", raw_count=len(raw_posts))

    # ── 2. Date-filter (safety net for actors that ignore dateRange) ──────────
    recent_posts = [p for p in raw_posts if _is_within_week(_parse_posted_at(p))]
    dropped_old = len(raw_posts) - len(recent_posts)
    if dropped_old:
        log.info("linkedin_posts_hiring.date_filtered", dropped=dropped_old, kept=len(recent_posts))

    # ── 3. Hard cap at MAX_POSTS after date filter ────────────────────────────
    capped_posts = recent_posts[:MAX_POSTS]

    # ── 4. Normalise — deduplicate in-memory by post ID ──────────────────────
    seen_ids: set[str] = set()
    normalized: list[JobIn] = []
    for raw in capped_posts:
        job = normalize_hiring_post(raw)
        if not job:
            continue
        ext_id = job.external_job_id or ""
        if ext_id and ext_id in seen_ids:
            continue
        if ext_id:
            seen_ids.add(ext_id)
        normalized.append(job)

    log.info("linkedin_posts_hiring.normalized", total=len(normalized))

    if not normalized:
        return [], {
            "status": "completed",
            "source": "linkedin_posts_hiring",
            "jobs_fetched": 0,
            "jobs_inserted": 0,
        }

    # ── 5. Persist through standard EV filter pipeline ───────────────────────
    db = SessionLocal()
    try:
        inserted = await persist_filtered_jobs(normalized, db)
        stats = {
            "status": "completed",
            "source": "linkedin_posts_hiring",
            "jobs_fetched": len(normalized),
            "jobs_inserted": inserted,
        }
        log.info("linkedin_posts_hiring.persisted", **stats)
        return normalized, stats
    except Exception as exc:
        log.exception("linkedin_posts_hiring.persist_failed")
        return normalized, {
            "status": "error",
            "source": "linkedin_posts_hiring",
            "jobs_fetched": len(normalized),
            "jobs_inserted": 0,
            "error": str(exc),
        }
    finally:
        await db.close()
