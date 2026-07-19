"""Module 3 — Twitter/X scraper via Apify (account-targeted).

Pulls recent tweets from specific handles (TWITTER_HANDLES) rather than a
broad keyword search — narrower, cheaper, and matches the source the user
wants tracked. Tweets still pass through the shared keyword filter since
accounts like @TheStreet post general financial news, not EV-only content.

Actor: apidojo/twitter-user-scraper (handle-based; configurable via
settings.apify_twitter_actor_id). Verify input field names in the Apify
console before first run — actor schemas can change between versions.
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

# Twitter/X accounts to pull recent posts from. Add more handles here.
TWITTER_HANDLES: list[str] = ["TheStreet"]
MAX_TWEETS_PER_HANDLE = 30


async def _trigger_actor() -> tuple[str, str]:
    actor_id = settings.apify_twitter_actor_id
    actor_path = actor_id.replace("/", "~")
    run_input = {
        "twitterHandles": TWITTER_HANDLES,
        "maxItems": MAX_TWEETS_PER_HANDLE * len(TWITTER_HANDLES),
        "sort": "Latest",
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
                log.info("twitter.actor_started", run_id=run_id, handles=TWITTER_HANDLES)
                return run_id, token
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code in (401, 402, 403, 429) and attempt < len(tokens_to_try) - 1:
                    log.warning("twitter.primary_token_failed_retrying_secondary", status=exc.response.status_code)
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
                raise RuntimeError(f"Twitter Apify run {run_id} ended with {status}")

    if not dataset_id:
        raise TimeoutError(f"Twitter Apify run {run_id} did not complete within {timeout}s")

    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(
            f"{_APIFY_BASE}/datasets/{dataset_id}/items",
            params={"token": token, "format": "json"},
        )
        r.raise_for_status()
        return r.json()  # type: ignore[return-value]


def _normalize_tweet(tweet: dict[str, Any]) -> IngestItem | None:
    text: str = tweet.get("text") or tweet.get("full_text") or ""
    if not text:
        return None

    tweet_id = str(tweet.get("id") or tweet.get("id_str") or "")
    url = tweet.get("url") or (
        f"https://twitter.com/i/web/status/{tweet_id}" if tweet_id else None
    )
    author: dict = tweet.get("author") or {}
    handle = author.get("userName") or author.get("screen_name") or "unknown"

    created_raw: str = tweet.get("createdAt") or tweet.get("created_at") or ""
    try:
        occurred_at = datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        occurred_at = datetime.now(timezone.utc)

    return IngestItem(
        external_id=tweet_id or None,
        occurred_at=occurred_at,
        url=url,
        text=f"@{handle}: {text}",
        payload=tweet,
    )


async def poll_twitter() -> dict[str, Any]:
    if not settings.apify_token and not settings.apify_token_secondary:
        log.warning("twitter.apify_token_missing")
        return {"status": "skipped", "reason": "no_apify_token"}

    log.info("twitter.poll_start", handles=TWITTER_HANDLES)

    try:
        run_id, used_token = await _trigger_actor()
        raw_tweets = await _wait_for_dataset(run_id, used_token)
    except httpx.HTTPStatusError as e:
        log.error("twitter.apify_http_error", status_code=e.response.status_code, error=str(e))
        return {
            "status": "error", 
            "reason": f"apify_http_error_{e.response.status_code}",
            "message": "Apify API rejected the request. You may be out of free credits, or the actor is no longer available to your account."
        }
    except Exception as e:
        log.error("twitter.apify_unknown_error", error=str(e))
        return {"status": "error", "reason": "apify_unknown_error", "message": str(e)}

    log.info("twitter.fetched", count=len(raw_tweets))

    items = [n for t in raw_tweets if (n := _normalize_tweet(t)) is not None]
    filtered, dropped = filter_items(items)
    log.info("twitter.keyword_filter", kept=len(filtered), dropped=dropped)

    if not filtered:
        return {
            "status": "ok",
            "fetched": len(raw_tweets),
            "kept": 0,
            "dropped": dropped,
            "accepted": 0,
            "duplicates": 0,
        }

    db = SessionLocal()
    try:
        result = await ingest_items(
            db,
            source_type="twitter",
            request=IngestRequest(source_identifier="twitter_handles", items=filtered),
        )
        log.info("twitter.ingested", accepted=result.accepted, duplicates=result.duplicates)
        return {
            "status": "ok",
            "fetched": len(raw_tweets),
            "kept": len(filtered),
            "dropped": dropped,
            "accepted": result.accepted,
            "duplicates": result.duplicates,
        }
    finally:
        await db.close()
