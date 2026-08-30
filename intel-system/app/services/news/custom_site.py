"""Module 2 — Custom site scraper via Apify.

Uses the apify/website-content-crawler to fetch the full markdown text of
hardcoded custom sites. This avoids managing webhooks and provides clean
text directly to the LLM.
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
from app.services.news.keyword_filter import passes_filter

log = structlog.get_logger()

_APIFY_BASE = "https://api.apify.com/v2"

TARGET_URL = "https://www.autopunditz.com/latest-posts"
ACTOR_ID = "apify/website-content-crawler"


async def _trigger_actor(url: str) -> tuple[str, str]:
    actor_path = ACTOR_ID.replace("/", "~")
    run_input = {
        "startUrls": [{"url": url}],
        "maxCrawlPages": 3,
        "crawlerType": "playwright:firefox",
        "saveHtml": False,
        "saveMarkdown": True
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
                log.info("custom_site.actor_started", run_id=run_id, url=url)
                return run_id, token
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code in (401, 402, 403, 429) and attempt < len(tokens_to_try) - 1:
                    log.warning("custom_site.primary_token_failed", status=exc.response.status_code)
                    continue
                raise
        raise RuntimeError("No valid apify tokens available")


async def _wait_for_dataset(run_id: str, token: str, timeout: int = 600) -> list[dict]:
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
                raise RuntimeError(f"Apify run {run_id} ended with {status}")

    if not dataset_id:
        raise TimeoutError(f"Apify run {run_id} did not complete within {timeout}s")

    async with httpx.AsyncClient(timeout=60) as client:
        r = await client.get(
            f"{_APIFY_BASE}/datasets/{dataset_id}/items",
            params={"token": token, "format": "json"},
        )
        r.raise_for_status()
        return r.json()  # type: ignore[return-value]


async def poll_custom_sites() -> dict[str, Any]:
    if not settings.apify_token and not settings.apify_token_secondary:
        log.warning("custom_site.apify_token_missing")
        return {"status": "skipped", "reason": "no_apify_token"}

    log.info("custom_site.poll_start", target=TARGET_URL)

    try:
        run_id, used_token = await _trigger_actor(TARGET_URL)
        raw_items = await _wait_for_dataset(run_id, used_token)
    except httpx.HTTPStatusError as e:
        log.error("custom_site.apify_http_error", status_code=e.response.status_code, error=str(e))
        return {"status": "error", "reason": f"apify_http_error_{e.response.status_code}"}
    except Exception as e:
        log.error("custom_site.apify_unknown_error", error=str(e))
        return {"status": "error", "reason": "apify_unknown_error", "message": str(e)}

    if not raw_items:
        return {"status": "ok", "kept": 0}

    items_to_ingest = []
    total_hits = []

    for item_data in raw_items:
        text = item_data.get("markdown") or item_data.get("text") or ""
        url = item_data.get("url") or TARGET_URL
        
        if not text:
            continue

        # Keyword check: Make sure this site output mentions EV in India
        ok, hits = passes_filter(text, url)
        if not ok:
            log.info("custom_site.keyword_miss", url=url)
            continue
            
        total_hits.extend(hits)

        # Format as an IngestItem (Truncate to 12000 chars to avoid blowing up the DB / LLM Context)
        items_to_ingest.append(
            IngestItem(
                external_id=url, # Use URL as external_id so exact content deduplicates correctly across days
                occurred_at=datetime.now(timezone.utc),
                url=url,
                text=text[:12000],
                payload={"source": "apify_website_crawler", "run_id": run_id, "crawled_url": url}
            )
        )

    if not items_to_ingest:
        return {"status": "ok", "kept": 0, "reason": "no_items_passed_filter"}

    db = SessionLocal()
    try:
        result = await ingest_items(
            db,
            source_type="custom_site",
            request=IngestRequest(source_identifier="custom_site_scraper", items=items_to_ingest),
        )
        log.info("custom_site.ingested", accepted=result.accepted, duplicates=result.duplicates, hits=total_hits)
        return {
            "status": "ok",
            "accepted": result.accepted,
            "duplicates": result.duplicates,
            "matched_keywords": total_hits,
        }
    finally:
        await db.close()
