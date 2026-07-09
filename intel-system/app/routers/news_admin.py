"""News module router.

Two prefix groups:
  /ingest/news/*   — webhook receivers (changedetection.io)
  /admin/news/*    — management endpoints (custom sites, keywords, manual triggers)
"""
import re
from datetime import datetime, timezone

import structlog
from fastapi import APIRouter
from pydantic import BaseModel

from app.database import SessionLocal
from app.schemas.ingest import IngestItem, IngestRequest
from app.services.dedup import ingest_items
from app.services.news.keyword_filter import KEYWORDS, filter_items, passes_filter

log = structlog.get_logger()
router = APIRouter(tags=["news"])


# ── Helpers ───────────────────────────────────────────────────────────────────

def _strip_html(html: str) -> str:
    text = re.sub(r"<[^>]+>", " ", html)
    return re.sub(r"\s{2,}", " ", text).strip()


# ── Module 2: changedetection.io webhook ──────────────────────────────────────

class CDWebhookPayload(BaseModel):
    watch_url: str
    title: str | None = None
    diff: str | None = None
    uuid: str | None = None


@router.post("/ingest/news/changedetection")
async def changedetection_webhook(payload: CDWebhookPayload) -> dict:
    """Receive change notifications from changedetection.io for custom sites."""
    raw_text = _strip_html(payload.diff or "")
    if not raw_text.strip():
        return {"status": "ignored", "reason": "empty_diff"}

    ok, hits = passes_filter(raw_text, payload.watch_url)
    if not ok:
        log.info("news.cd_keyword_miss", url=payload.watch_url)
        return {"status": "filtered", "reason": "no_keyword_match"}

    item = IngestItem(
        external_id=payload.uuid,
        occurred_at=datetime.now(timezone.utc),
        url=payload.watch_url,
        text=raw_text[:4000],
        payload=payload.model_dump(),
    )
    db = SessionLocal()
    try:
        result = await ingest_items(
            db,
            source_type="custom_site",
            request=IngestRequest(
                source_identifier=payload.watch_url,
                items=[item],
            ),
        )
        log.info("news.cd_ingested", url=payload.watch_url,
                 accepted=result.accepted, matched=hits)
        return {
            "status": "ok",
            "accepted": result.accepted,
            "duplicates": result.duplicates,
            "matched_keywords": hits,
        }
    finally:
        await db.close()


# ── Module 2: custom site management ─────────────────────────────────────────

class CustomSiteIn(BaseModel):
    url: str
    display_name: str


@router.post("/admin/news/custom-sites")
async def add_custom_site(body: CustomSiteIn) -> dict:
    """Register a URL in changedetection.io. Webhooks back on page change."""
    from app.services.news.custom_site import register_watch
    result = await register_watch(body.url, body.display_name)
    return {"status": "registered", "result": result}


@router.get("/admin/news/custom-sites")
async def list_custom_sites() -> dict:
    from app.services.news.custom_site import list_watches
    watches = await list_watches()
    return {"count": len(watches), "watches": watches}


@router.delete("/admin/news/custom-sites/{uuid}")
async def remove_custom_site(uuid: str) -> dict:
    from app.services.news.custom_site import delete_watch
    await delete_watch(uuid)
    return {"status": "deleted", "uuid": uuid}


# ── Keyword inspection ────────────────────────────────────────────────────────

@router.get("/admin/news/keywords")
async def get_keywords() -> dict:
    return {
        "keywords": [{"keyword": kw, "word_boundary": wb} for kw, wb in KEYWORDS]
    }


class KeywordTestIn(BaseModel):
    text: str


@router.post("/admin/news/keywords/test")
async def test_keyword_filter(body: KeywordTestIn) -> dict:
    """Test whether a piece of text would pass the keyword filter."""
    ok, hits = passes_filter(body.text)
    return {"passes": ok, "matched_keywords": hits, "text": body.text}


# ── Manual triggers ───────────────────────────────────────────────────────────

@router.post("/admin/news/rss/trigger-now")
async def trigger_rss_now() -> dict:
    """Immediately poll all RSS feeds (Module 1)."""
    from app.services.news.rss import poll_rss_feeds
    return await poll_rss_feeds()


@router.post("/admin/news/twitter/trigger-now")
async def trigger_twitter_now() -> dict:
    """Immediately trigger the Apify Twitter handle scrape (Module 3)."""
    from app.services.news.twitter import poll_twitter
    return await poll_twitter()


@router.post("/admin/news/linkedin/trigger-now")
async def trigger_linkedin_now() -> dict:
    """Immediately trigger the Apify LinkedIn hashtag search (Module 3b)."""
    from app.services.news.linkedin import poll_linkedin
    return await poll_linkedin()


# ── Status / preview ─────────────────────────────────────────────────────────

@router.get("/admin/news/status")
async def news_status() -> dict:
    """Count raw_items ingested per news source type in the last 24 hours."""
    from sqlalchemy import text
    db = SessionLocal()
    try:
        rows = (await db.execute(text("""
            SELECT s.type, COUNT(r.id) AS item_count
            FROM raw_items r
            JOIN sources s ON s.id = r.source_id
            WHERE s.type IN ('rss_global', 'custom_site', 'twitter', 'linkedin_news')
              AND r.ingested_at >= NOW() - INTERVAL '24 hours'
            GROUP BY s.type
            ORDER BY s.type
        """))).fetchall()
        return {
            "last_24h": {row[0]: row[1] for row in rows},
            "modules": {
                "rss_global": "Module 1 — global RSS feeds",
                "custom_site": "Module 2 — changedetection.io custom sites",
                "twitter": "Module 3 — Apify Twitter handle scrape (@TheStreet)",
                "linkedin_news": "Module 3b — Apify LinkedIn hashtag search (#EV #charging #Mobility)",
            },
        }
    finally:
        await db.close()
