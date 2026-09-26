"""News module router.

Endpoints:
  /ingest/news/*   — webhook receivers (n8n RSS)
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


# ── Module 2: Apify Custom Sites Scraper (Webhook removed, see tasks.py) ─────


# ── Module 1: n8n RSS webhook ─────────────────────────────────────────────────

class N8nWebhookPayload(BaseModel):
    title: str | None = None
    link: str | None = None
    content: str | None = None
    source: str | None = "n8n_rss"


@router.post("/ingest/news/n8n-webhook")
async def n8n_webhook(payload: N8nWebhookPayload) -> dict:
    """Receive RSS items processed and forwarded by n8n."""
    text = f"{payload.title or ''}. {payload.content or ''}".strip(". ")
    if not text:
        return {"status": "ignored", "reason": "empty_content"}

    ok, hits = passes_filter(text, payload.link)
    if not ok:
        log.info("news.n8n_keyword_miss", url=payload.link)
        return {"status": "filtered", "reason": "no_keyword_match"}

    item = IngestItem(
        external_id=payload.link,
        occurred_at=datetime.now(timezone.utc),
        url=payload.link,
        text=text[:4000],
        payload=payload.model_dump(),
    )
    db = SessionLocal()
    try:
        result = await ingest_items(
            db,
            source_type="rss_global",
            request=IngestRequest(
                source_identifier=payload.source or "n8n",
                items=[item],
            ),
        )
        return {
            "status": "ok",
            "accepted": result.accepted,
            "duplicates": result.duplicates,
            "matched_keywords": hits,
        }
    finally:
        await db.close()


# (Dynamic custom site management endpoints have been removed. Sites are currently hardcoded in custom_site.py)


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

@router.post("/admin/news/custom-sites/trigger-now")
async def trigger_custom_sites_now() -> dict:
    """Immediately trigger the Apify custom sites scrape (Module 2)."""
    from app.services.news.custom_site import poll_custom_sites
    return await poll_custom_sites()

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


@router.post("/admin/news/process-now")
async def process_news_now() -> dict:
    """Immediately process unprocessed raw news items with the LLM pipeline."""
    from app.services.news.news_pipeline import process_unprocessed_news
    return await process_unprocessed_news()


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
                "custom_site": "Module 2 — Apify website-content-crawler (Autopunditz)",
                "twitter": "Module 3 — Apify Twitter handle scrape (@TheStreet)",
                "linkedin_news": "Module 3b — Apify LinkedIn hashtag search (#EV #EVcharging #Emobility) — LLM classifies into LinkedIn Updates or Community Updates",
            },
        }
    finally:
        await db.close()

