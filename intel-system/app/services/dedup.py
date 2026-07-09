import hashlib

from sqlalchemy import select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.raw_item import RawItem
from app.models.source import Source
from app.schemas.ingest import IngestRequest, IngestResponse


def compute_content_hash(text: str | None, url: str | None, external_id: str | None) -> str:
    normalized = (text or "") + "|" + (url or "") + "|" + (external_id or "")
    return hashlib.sha256(normalized.strip().encode()).hexdigest()


async def ingest_items(
    db: AsyncSession, source_type: str, request: IngestRequest
) -> IngestResponse:
    result = await db.execute(
        select(Source).where(
            Source.type == source_type,
            Source.identifier == request.source_identifier,
        )
    )
    source = result.scalar_one_or_none()

    if source is None:
        source = Source(
            type=source_type,
            identifier=request.source_identifier,
            display_name=request.source_identifier,
        )
        db.add(source)
        await db.flush()

    accepted = 0
    duplicates = 0

    for item in request.items:
        content_hash = compute_content_hash(item.text, item.url, item.external_id)
        stmt = (
            pg_insert(RawItem)
            .values(
                source_id=source.id,
                external_id=item.external_id,
                content_hash=content_hash,
                payload=item.payload,
                text=item.text,
                url=item.url,
                occurred_at=item.occurred_at,
            )
            .on_conflict_do_nothing(index_elements=["source_id", "content_hash"])
        )
        res = await db.execute(stmt)
        if res.rowcount and res.rowcount > 0:
            accepted += 1
        else:
            duplicates += 1

    await db.commit()
    return IngestResponse(accepted=accepted, duplicates=duplicates)
