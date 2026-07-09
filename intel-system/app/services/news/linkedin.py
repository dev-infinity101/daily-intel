"""Module 3b — LinkedIn news scraper via Apify (hashtag/keyword search).

Searches public LinkedIn posts tagged with domain hashtags rather than a
specific account. Hashtag search can still surface off-topic posts that
merely co-occur, so the shared keyword filter runs as a safety net before
anything reaches raw_items.

Actor: harvestapi/linkedin-post-search (no-cookie public search;
configurable via settings.apify_linkedin_actor_id). Verify input field
names in the Apify console before first run — actor schemas can change
between versions.
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
from app.services.news.keyword_filter import filter_items

log = structlog.get_logger()

_APIFY_BASE = "https://api.apify.com/v2"

# Each hashtag is run as its own search query by the actor.
SEARCH_QUERIES: list[str] = ["#EV", "#charging", "#Mobility"]
MAX_POSTS = 60


async def _trigger_actor() -> str:
    actor_id = settings.apify_linkedin_actor_id
    actor_path = actor_id.replace("/", "~")
    run_input = {
        "searchQueries": SEARCH_QUERIES,
        "maxPosts": MAX_POSTS,
    }
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.post(
            f"{_APIFY_BASE}/acts/{actor_path}/runs",
            params={"token": settings.apify_token},
            json=run_input,
        )
        r.raise_for_status()
        run_id: str = r.json()["data"]["id"]
    log.info("linkedin.actor_started", run_id=run_id, queries=SEARCH_QUERIES)
    return run_id


async def _wait_for_dataset(run_id: str, timeout: int = 300) -> list[dict]:
    dataset_id: str | None = None
    polls = timeout // 10

    async with httpx.AsyncClient(timeout=30) as client:
        for _ in range(polls):
            await asyncio.sleep(10)
            r = await client.get(
                f"{_APIFY_BASE}/actor-runs/{run_id}",
                params={"token": settings.apify_token},
            )
            r.raise_for_status()
            data = r.json()["data"]
            status = data["status"]
            if status == "SUCCEEDED":
                dataset_id = data["defaultDatasetId"]
                break
            if status in ("FAILED", "ABORTED", "TIMED-OUT"):
                raise RuntimeError(f"LinkedIn Apify run {run_id} ended with {status}")

    if not dataset_id:
        raise TimeoutError(f"LinkedIn Apify run {run_id} did not complete within {timeout}s")

    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(
            f"{_APIFY_BASE}/datasets/{dataset_id}/items",
            params={"token": settings.apify_token, "format": "json"},
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

    posted_at: dict = post.get("postedAt") or {}
    date_raw: str = posted_at.get("date") or ""
    try:
        occurred_at = datetime.fromisoformat(date_raw.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        ts = posted_at.get("timestamp")
        occurred_at = (
            datetime.fromtimestamp(ts / 1000, tz=timezone.utc) if ts else datetime.now(timezone.utc)
        )

    return IngestItem(
        external_id=post_id or None,
        occurred_at=occurred_at,
        url=url,
        text=f"{name}: {text}",
        payload=post,
    )


async def poll_linkedin() -> dict[str, Any]:
    if not settings.apify_token:
        log.warning("linkedin.apify_token_missing")
        return {"status": "skipped", "reason": "no_apify_token"}

    log.info("linkedin.poll_start", queries=SEARCH_QUERIES)

    run_id = await _trigger_actor()
    raw_posts = await _wait_for_dataset(run_id)
    log.info("linkedin.fetched", count=len(raw_posts))

    items = [n for p in raw_posts if (n := _normalize_post(p)) is not None]
    filtered, dropped = filter_items(items)
    log.info("linkedin.keyword_filter", kept=len(filtered), dropped=dropped)

    if not filtered:
        return {
            "status": "ok",
            "fetched": len(raw_posts),
            "kept": 0,
            "dropped": dropped,
            "accepted": 0,
            "duplicates": 0,
        }

    db = SessionLocal()
    try:
        result = await ingest_items(
            db,
            source_type="linkedin_news",
            request=IngestRequest(source_identifier="linkedin_hashtag_search", items=filtered),
        )
        log.info("linkedin.ingested", accepted=result.accepted, duplicates=result.duplicates)
        return {
            "status": "ok",
            "fetched": len(raw_posts),
            "kept": len(filtered),
            "dropped": dropped,
            "accepted": result.accepted,
            "duplicates": result.duplicates,
        }
    finally:
        await db.close()
