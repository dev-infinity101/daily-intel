"""Module 3c — LinkedIn Community Posts scraper via Apify.

Dedicated scraper for EV-domain LinkedIn community posts using the same
Apify actor as Module 3b (harvestapi/linkedin-post-search), but with
its own search queries and a no-rejection policy.

All scraped posts are ingested as source_type='linkedin_community' and
later summarised/ranked by the LLM pipeline without any filtering —
every post is kept and included in the digest.
"""
import asyncio
from datetime import datetime, timezone
from typing import Any

import httpx
import structlog

from app.config import settings
from app.database import SessionLocal
from app.schemas.ingest import IngestItem, IngestRequest
from app.services.dedup import ingest_items

log = structlog.get_logger()

_APIFY_BASE = "https://api.apify.com/v2"

# Combined EV + India query exactly as configured in the Apify console.
SEARCH_QUERIES: list[str] = ['"#EV" "India"']
MAX_POSTS = 50


async def _trigger_actor() -> tuple[str, str]:
    actor_id = settings.apify_linkedin_actor_id
    actor_path = actor_id.replace("/", "~")
    run_input = {
        "searchQueries": SEARCH_QUERIES,
        "maxPosts": MAX_POSTS,
    }
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
                run_id: str = r.json()["data"]["id"]
                log.info("linkedin_community.actor_started", run_id=run_id, queries=SEARCH_QUERIES)
                return run_id, token
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code in (401, 402, 403, 429) and attempt < len(tokens_to_try) - 1:
                    log.warning("linkedin_community.primary_token_failed_retrying_secondary", status=exc.response.status_code)
                    continue
                raise
        raise RuntimeError("No valid apify tokens available")


async def _wait_for_dataset(run_id: str, token: str, timeout: int = 300) -> list[dict]:
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
                raise RuntimeError(f"LinkedIn Community Apify run {run_id} ended with {status}")

    if not dataset_id:
        raise TimeoutError(f"LinkedIn Community Apify run {run_id} did not complete within {timeout}s")

    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(
            f"{_APIFY_BASE}/datasets/{dataset_id}/items",
            params={"token": token, "format": "json"},
        )
        r.raise_for_status()
        return r.json()  # type: ignore[return-value]


def _normalize_post(post: dict[str, Any]) -> IngestItem | None:
    text: str = post.get("content") or ""
    if not text:
        return None

    post_id = str(post.get("id") or "")
    url = post.get("linkedinUrl") or None
    author: dict = post.get("author") or {}
    name = author.get("name") or "unknown"
    info = author.get("info") or ""

    posted_at: dict = post.get("postedAt") or {}
    date_raw: str = posted_at.get("date") or ""
    try:
        occurred_at = datetime.fromisoformat(date_raw.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        ts = posted_at.get("timestamp")
        occurred_at = (
            datetime.fromtimestamp(ts / 1000, tz=timezone.utc) if ts else datetime.now(timezone.utc)
        )

    # Include author info for richer context in LLM summarization.
    author_line = f"{name} ({info})" if info else name

    return IngestItem(
        external_id=post_id or None,
        occurred_at=occurred_at,
        url=url,
        text=f"{author_line}: {text}",
        payload=post,
    )


async def poll_linkedin_community() -> dict[str, Any]:
    """Scrape LinkedIn community posts and ingest all of them (no keyword filtering)."""
    if not settings.apify_token and not settings.apify_token_secondary:
        log.warning("linkedin_community.apify_token_missing")
        return {"status": "skipped", "reason": "no_apify_token"}

    log.info("linkedin_community.poll_start", queries=SEARCH_QUERIES)

    try:
        run_id, used_token = await _trigger_actor()
        raw_posts = await _wait_for_dataset(run_id, used_token)
    except httpx.HTTPStatusError as e:
        log.error("linkedin_community.apify_http_error", status_code=e.response.status_code, error=str(e))
        return {
            "status": "error",
            "reason": f"apify_http_error_{e.response.status_code}",
            "message": "Apify API rejected the request. You may be out of free credits, or the actor is no longer available to your account.",
        }
    except Exception as e:
        log.error("linkedin_community.apify_unknown_error", error=str(e))
        return {"status": "error", "reason": "apify_unknown_error", "message": str(e)}

    log.info("linkedin_community.fetched", count=len(raw_posts))

    # Normalize — no keyword filtering; accept everything.
    items = [n for p in raw_posts if (n := _normalize_post(p)) is not None]
    log.info("linkedin_community.normalized", kept=len(items), total=len(raw_posts))

    if not items:
        return {
            "status": "ok",
            "fetched": len(raw_posts),
            "kept": 0,
            "accepted": 0,
            "duplicates": 0,
        }

    db = SessionLocal()
    try:
        result = await ingest_items(
            db,
            source_type="linkedin_community",
            request=IngestRequest(source_identifier="linkedin_community_posts", items=items),
        )
        log.info("linkedin_community.ingested", accepted=result.accepted, duplicates=result.duplicates)
        return {
            "status": "ok",
            "fetched": len(raw_posts),
            "kept": len(items),
            "accepted": result.accepted,
            "duplicates": result.duplicates,
        }
    finally:
        await db.close()
