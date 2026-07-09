import structlog
from fastapi import APIRouter, Depends, Header, HTTPException
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings
from app.database import get_db
from app.schemas.ingest import IngestRequest, IngestResponse
from app.services.dedup import ingest_items

log = structlog.get_logger()

router = APIRouter(prefix="/ingest", tags=["ingest"])

# Source types that must pass the news keyword filter before DB write.
# Covers n8n-originated RSS items as well as any external poster using these types.
_NEWS_SOURCE_TYPES = {"rss_global", "custom_site", "twitter", "linkedin_news"}


def _verify_token(x_ingest_token: str = Header(...)) -> None:
    if x_ingest_token != settings.ingest_token:
        raise HTTPException(status_code=401, detail="Invalid ingest token")


@router.post("", response_model=IngestResponse, dependencies=[Depends(_verify_token)])
async def ingest(
    body: IngestRequest,
    x_source_type: str = Header(...),
    db: AsyncSession = Depends(get_db),
) -> IngestResponse:
    if x_source_type in _NEWS_SOURCE_TYPES:
        from app.services.news.keyword_filter import filter_items
        filtered, dropped = filter_items(body.items)
        if dropped:
            log.info("ingest.keyword_filter", source_type=x_source_type,
                     dropped=dropped, kept=len(filtered))
        body = IngestRequest(source_identifier=body.source_identifier, items=filtered)

    return await ingest_items(db, source_type=x_source_type, request=body)
