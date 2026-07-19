from datetime import datetime, timedelta, timezone
from pathlib import Path

from jinja2 import Environment, FileSystemLoader
from sqlalchemy import and_, select
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.digest import Digest
from app.models.processed_item import ProcessedItem
from app.models.raw_item import RawItem

IST = timezone(timedelta(hours=5, minutes=30))
_TEMPLATE_DIR = Path(__file__).parent / "templates"
_jinja = Environment(loader=FileSystemLoader(str(_TEMPLATE_DIR)), autoescape=True)


async def fetch_url_map(db: AsyncSession, items: list[ProcessedItem]) -> dict[int, str]:
    raw_ids = [i.raw_item_id for i in items if i.raw_item_id is not None]
    if not raw_ids:
        return {}
    result = await db.execute(
        select(RawItem.id, RawItem.url).where(RawItem.id.in_(raw_ids))
    )
    return {row.id: row.url for row in result if row.url}


async def fetch_today_items(db: AsyncSession, section: str | None = None) -> list[ProcessedItem]:
    cutoff = (datetime.now(IST) - timedelta(hours=24)).astimezone(timezone.utc)
    
    conditions = [
        ProcessedItem.is_relevant == True,  # noqa: E712
        ProcessedItem.processed_at >= cutoff,
    ]
    if section:
        conditions.append(ProcessedItem.section == section)
        
    result = await db.execute(
        select(ProcessedItem)
        .where(and_(*conditions))
        .order_by(ProcessedItem.section, ProcessedItem.rank_score.desc())
    )
    return list(result.scalars().all())


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


async def run_news_digest(db: AsyncSession) -> None:
    from app.services.email.sender import send_email
    import structlog
    log = structlog.get_logger()
    
    # fetch ALL items in the processed_items table (news, linkedin, custom) since Jobs are in a separate table.
    items = await fetch_today_items(db)
    if not items:
        log.info("digest.news.skipped_empty")
        return
        
    url_map = await fetch_url_map(db, items)
    subject = "Your Daily Intel: News"
    html = assemble_html(items, subject, url_map=url_map, template_name="news.html.j2")
    provider_id = await send_email(subject, html)
    await record_digest(db, items, html, subject, provider_id)
    log.info("digest.news.sent", item_count=len(items))

