from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from jinja2 import Environment, FileSystemLoader
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.digest import Digest
from app.models.processed_item import ProcessedItem
from app.models.raw_item import RawItem

IST = timezone(timedelta(hours=5, minutes=30))
_TEMPLATE_DIR = Path(__file__).parent / "templates"
_jinja = Environment(loader=FileSystemLoader(str(_TEMPLATE_DIR)), autoescape=True)


NEWS_BATCH_SIZE = 50


async def fetch_url_map(db: AsyncSession, items: list[ProcessedItem]) -> dict[int, str]:
    raw_ids = [i.raw_item_id for i in items if i.raw_item_id is not None]
    if not raw_ids:
        return {}
    result = await db.execute(
        select(RawItem.id, RawItem.url).where(RawItem.id.in_(raw_ids))
    )
    return {row.id: row.url for row in result if row.url}


async def fetch_news_items(
    db: AsyncSession,
    hours: int = 168,
    section: str | None = None,
) -> list[ProcessedItem]:
    """Fetch relevant news items processed within the given lookback window (default 168h / 7 days)."""
    cutoff = (datetime.now(IST) - timedelta(hours=hours)).astimezone(timezone.utc)
    
    conditions = [
        ProcessedItem.is_relevant == True,  # noqa: E712
        ProcessedItem.processed_at >= cutoff,
        ProcessedItem.section.in_(["news", "telegram", "whatsapp", "linkedin", "linkedin_community"]),
    ]
    if section:
        conditions.append(ProcessedItem.section == section)
        
    result = await db.execute(
        select(ProcessedItem)
        .where(and_(*conditions))
        .order_by(ProcessedItem.section, ProcessedItem.rank_score.desc())
    )
    return list(result.scalars().all())


# Alias for backward compatibility
fetch_today_items = fetch_news_items


def assemble_html(
    items: list[ProcessedItem],
    subject: str,
    url_map: dict[int, str] | None = None,
    template_name: str = "daily.html.j2"
) -> str:
    sections: dict[str, list[ProcessedItem]] = {}
    for item in items:
        sections.setdefault(item.section, []).append(item)
    template = _jinja.get_template(template_name)
    return template.render(
        sections=sections,
        subject=subject,
        generated_at=datetime.now(IST).strftime("%d %b %Y, %I:%M %p IST"),
        url_map=url_map or {},
    )


async def record_digest(
    db: AsyncSession,
    items: list[ProcessedItem],
    html: str,
    subject: str,
    provider_id: str | None = None,
) -> Digest:
    digest = Digest(
        item_ids=[i.id for i in items],
        html=html,
        subject=subject,
        provider_id=provider_id,
        status="sent" if provider_id else "pending",
    )
    db.add(digest)
    await db.commit()
    await db.refresh(digest)
    return digest


async def run_news_digest(db: AsyncSession, hours: int = 168) -> dict[str, Any]:
    """Assemble and send news digest in batches of max 20 articles.
    
    Defaults to hours=168 (7 days) for weekly Wednesday evening delivery.
    Splits articles into batches of NEWS_BATCH_SIZE (20) so no single email
    becomes excessively large.
    """
    from app.services.email.sender import send_email
    import asyncio
    import structlog
    log = structlog.get_logger()
    
    items = await fetch_news_items(db, hours=hours)
    if not items:
        log.info("digest.news.skipped_empty", hours=hours)
        return {"status": "no_news", "count": 0}
        
    url_map = await fetch_url_map(db, items)

    # ── Split into batches of at most 20 articles ────────────────────────────
    batches = [items[i:i + NEWS_BATCH_SIZE] for i in range(0, len(items), NEWS_BATCH_SIZE)]
    total_batches = len(batches)
    total_items = len(items)
    provider_ids: list[str | None] = []
    sent_count = 0

    log.info(
        "digest.news.batching",
        total_items=total_items,
        total_batches=total_batches,
        batch_size=NEWS_BATCH_SIZE,
    )

    for batch_num, batch_items in enumerate(batches, 1):
        n = len(batch_items)
        if total_batches > 1:
            subject = f"Your Weekly Intel: News — Batch {batch_num}/{total_batches} ({n} article{'s' if n != 1 else ''})"
        else:
            subject = f"Your Weekly Intel: News — {n} article{'s' if n != 1 else ''}"

        html = assemble_html(batch_items, subject, url_map=url_map, template_name="news.html.j2")
        try:
            provider_id = await send_email(subject, html)
            provider_ids.append(provider_id)
            await record_digest(db, batch_items, html, subject, provider_id)
            sent_count += n
            log.info(
                "digest.news.batch_sent",
                batch=batch_num,
                total_batches=total_batches,
                count=n,
                provider_id=provider_id,
            )
        except Exception as exc:
            log.error("digest.news.batch_failed", batch=batch_num, error=str(exc))
            return {
                "status": "error",
                "count": sent_count,
                "batches_sent": batch_num - 1,
                "total_batches": total_batches,
                "error": str(exc),
            }

        if batch_num < total_batches:
            await asyncio.sleep(2)

    log.info("digest.news.done", total_items=total_items, total_batches=total_batches)
    return {
        "status": "sent",
        "count": total_items,
        "batches": total_batches,
        "provider_ids": provider_ids,
    }


