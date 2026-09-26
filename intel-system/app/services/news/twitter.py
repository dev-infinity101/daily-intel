"""Module 3 — Twitter/X scraper via Apify (account-targeted).

Pulls recent tweets from specific handles (TWITTER_HANDLES) using
the Apify actor apidojo/twitter-profile-scraper (actor ID dy7gIgPRMhrOrfW0f).

Extracts tweet text, engagement metrics, author metadata, and dates,
normalized into IngestItem objects for ingestion and downstream LLM filtering.
"""
import asyncio
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import httpx
import structlog

from app.config import settings
from app.database import SessionLocal
from app.schemas.ingest import IngestItem, IngestRequest
from app.services.dedup import ingest_items

log = structlog.get_logger()

_APIFY_BASE = "https://api.apify.com/v2"

# Twitter/X accounts to pull recent posts from
TWITTER_HANDLES: list[str] = ["teslaclubin", "xroaders_001"]
DEFAULT_MAX_ITEMS = 10


def _parse_tweet_date(created_raw: str) -> datetime:
    """Parse Twitter timestamps in ISO, RFC 2822, or Twitter format."""
    if not created_raw:
        return datetime.now(timezone.utc)

    # 1. ISO format (e.g. 2026-09-05T12:00:00.000Z)
    try:
        return datetime.fromisoformat(created_raw.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        pass

    # 2. Twitter standard format (e.g. "Wed Sep 24 18:06:27 +0000 2025")
    try:
        return datetime.strptime(created_raw, "%a %b %d %H:%M:%S %z %Y")
    except (ValueError, TypeError):
        pass

    # 3. RFC 2822 / parsedate format
    try:
        dt = parsedate_to_datetime(created_raw)
        if dt:
            return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    except Exception:
        pass

    return datetime.now(timezone.utc)


async def _trigger_actor(
    handles: list[str] | None = None,
    max_items: int = DEFAULT_MAX_ITEMS,
    start_date: str | None = None,
    end_date: str | None = None,
) -> tuple[str, str]:
    actor_id = settings.apify_twitter_actor_id
    actor_path = actor_id.replace("/", "~")
    target_handles = handles or TWITTER_HANDLES

    now = datetime.now(timezone.utc)
    effective_start = start_date or (now - timedelta(days=1)).strftime("%Y-%m-%d")
    effective_end = end_date or now.strftime("%Y-%m-%d")

    run_input = {
        "customMapFunction": "(object) => { return {...object} }",
        "end": effective_end,
        "getAboutData": False,
        "getReplies": False,
        "includeNativeRetweets": True,
        "maxItems": max_items,
        "onlyImages": False,
        "start": effective_start,
        "startUrls": [
            "https://x.com/teslaclubin/"
        ],
        "twitterHandles": target_handles,
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
                log.info(
                    "twitter.actor_started",
                    run_id=run_id,
                    actor=actor_id,
                    handles=target_handles,
                    max_items=max_items,
                    start=effective_start,
                    end=effective_end,
                )
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
    # Filter out non-tweet dataset items (e.g., account about data if enabled)
    item_type = tweet.get("type")
    if item_type and item_type != "tweet":
        return None

    # Tweet text: support fullText, text, full_text, or nested tweet object
    text: str = (
        tweet.get("fullText")
        or tweet.get("text")
        or tweet.get("full_text")
        or (tweet.get("tweet", {}).get("text") if isinstance(tweet.get("tweet"), dict) else "")
        or ""
    ).strip()
    if not text:
        return None

    tweet_id = str(tweet.get("id") or tweet.get("id_str") or "")
    url = tweet.get("url") or tweet.get("twitterUrl") or (
        f"https://x.com/i/web/status/{tweet_id}" if tweet_id else None
    )
    author: dict = tweet.get("author") or tweet.get("user") or {}
    handle = author.get("userName") or author.get("screen_name") or "unknown"

    created_raw: str = tweet.get("createdAt") or tweet.get("created_at") or ""
    occurred_at = _parse_tweet_date(created_raw)

    return IngestItem(
        external_id=tweet_id or None,
        occurred_at=occurred_at,
        url=url,
        text=f"@{handle}: {text}",
        payload=tweet,
    )


async def poll_twitter(
    handles: list[str] | None = None,
    max_items: int = DEFAULT_MAX_ITEMS,
    start_date: str | None = None,
    end_date: str | None = None,
) -> dict[str, Any]:
    """Scrapes recent tweets from target Twitter handles via Apify and ingests them into raw_items."""
    if not settings.apify_token and not settings.apify_token_secondary:
        log.warning("twitter.apify_token_missing")
        return {"status": "skipped", "reason": "no_apify_token"}

    target_handles = handles or TWITTER_HANDLES
    log.info("twitter.poll_start", handles=target_handles, max_items=max_items)

    try:
        run_id, used_token = await _trigger_actor(
            handles=target_handles,
            max_items=max_items,
            start_date=start_date,
            end_date=end_date,
        )
        raw_tweets = await _wait_for_dataset(run_id, used_token)
    except httpx.HTTPStatusError as e:
        log.error("twitter.apify_http_error", status_code=e.response.status_code, error=str(e))
        return {
            "status": "error",
            "reason": f"apify_http_error_{e.response.status_code}",
            "message": "Apify API rejected the request. You may be out of free credits, or the actor is no longer available to your account.",
        }
    except Exception as e:
        log.error("twitter.apify_unknown_error", error=str(e))
        return {"status": "error", "reason": "apify_unknown_error", "message": str(e)}

    items = [n for t in raw_tweets if (n := _normalize_tweet(t)) is not None]
    log.info("twitter.normalized", fetched=len(raw_tweets), kept=len(items))

    if not items:
        return {
            "status": "ok",
            "fetched": len(raw_tweets),
            "kept": 0,
            "dropped": len(raw_tweets),
            "accepted": 0,
            "duplicates": 0,
        }

    db = SessionLocal()
    try:
        result = await ingest_items(
            db,
            source_type="twitter",
            request=IngestRequest(source_identifier="twitter_handles", items=items),
        )
        log.info("twitter.ingested", accepted=result.accepted, duplicates=result.duplicates)
        return {
            "status": "ok",
            "fetched": len(raw_tweets),
            "kept": len(items),
            "dropped": len(raw_tweets) - len(items),
            "accepted": result.accepted,
            "duplicates": result.duplicates,
        }
    finally:
        await db.close()
