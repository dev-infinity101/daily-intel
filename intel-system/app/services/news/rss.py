"""Module 1 — Global RSS feed poller.

APScheduler triggers poll_rss_feeds() every 6 hours. Each feed is fetched
with httpx, parsed with feedparser (sync, run in a thread executor), then
the keyword filter discards off-topic articles before writing to raw_items.

Add or remove feeds from RSS_FEEDS without touching any other file.
"""
import asyncio
from datetime import datetime, timezone
from typing import Any

import feedparser
import httpx
import structlog

from app.database import SessionLocal
from app.schemas.ingest import IngestItem, IngestRequest
from app.services.dedup import ingest_items
from app.services.news.keyword_filter import filter_items

log = structlog.get_logger()

RSS_FEEDS: list[dict[str, str]] = [
    {"name": "electrek",       "url": "https://electrek.co/feed/"},
    {"name": "et_auto",        "url": "https://auto.economictimes.indiatimes.com/rss/topstories"},
    {"name": "evadoption",     "url": "https://evadoption.com/feed/"},
    {"name": "ev_magazine",    "url": "https://evmagazine.com/feed/"},
    {"name": "techcrunch",     "url": "https://techcrunch.com/feed/"},
    {"name": "cnbctv18_auto",  "url": "https://www.cnbctv18.com/commonfeeds/v1/cne/rss/auto.xml"},
]


async def _fetch_entries(url: str) -> list[Any]:
    try:
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            r = await client.get(url)
            r.raise_for_status()
            content = r.text
    except Exception as exc:
        log.warning("rss.fetch_failed", url=url, error=str(exc))
        return []

    loop = asyncio.get_event_loop()
    parsed = await loop.run_in_executor(None, feedparser.parse, content)
    return list(parsed.entries)


def _entry_to_item(entry: Any, source_name: str) -> IngestItem | None:
    title: str = getattr(entry, "title", "") or ""
    summary: str = getattr(entry, "summary", "") or ""
    link: str = getattr(entry, "link", "") or ""
    entry_id: str = getattr(entry, "id", "") or link

    text = f"{title}. {summary}".strip(". ")
    if not text:
        return None

    pub = getattr(entry, "published_parsed", None)
    occurred_at = datetime(*pub[:6], tzinfo=timezone.utc) if pub else datetime.now(timezone.utc)

    return IngestItem(
        external_id=entry_id or None,
        occurred_at=occurred_at,
        url=link or None,
        text=text,
        payload={"title": title, "summary": summary, "link": link, "source": source_name},
    )


async def poll_rss_feeds() -> dict[str, Any]:
    log.info("rss.poll_start", feeds=len(RSS_FEEDS))

    total_fetched = total_kept = total_dropped = total_accepted = total_dupes = 0

    for feed in RSS_FEEDS:
        name, url = feed["name"], feed["url"]
        entries = await _fetch_entries(url)
        total_fetched += len(entries)

        items = [i for e in entries if (i := _entry_to_item(e, name)) is not None]
        filtered, dropped = filter_items(items)
        total_kept += len(filtered)
        total_dropped += dropped

        if not filtered:
            log.debug("rss.all_filtered", source=name, entries=len(entries), dropped=dropped)
            continue

        db = SessionLocal()
        try:
            result = await ingest_items(
                db,
                source_type="rss_global",
                request=IngestRequest(source_identifier=f"rss_{name}", items=filtered),
            )
            total_accepted += result.accepted
            total_dupes += result.duplicates
            log.info("rss.ingested", source=name, kept=len(filtered),
                     accepted=result.accepted, dupes=result.duplicates)
        except Exception:
            log.exception("rss.ingest_error", source=name)
        finally:
            await db.close()

    summary = {
        "status": "ok",
        "feeds": len(RSS_FEEDS),
        "fetched": total_fetched,
        "kept": total_kept,
        "dropped": total_dropped,
        "accepted": total_accepted,
        "duplicates": total_dupes,
    }
    log.info("rss.poll_done", **summary)
    return summary
