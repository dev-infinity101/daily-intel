"""Job-specific dedup helpers.

Layer 1 (hard): dedup_hash unique constraint in the DB — handled at insert time.
Layer 2 (fuzzy): pg_trgm similarity scan for near-duplicates that survived Layer 1.
"""
import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import SessionLocal

log = structlog.get_logger()

_FUZZY_QUERY = text("""
    SELECT a.id AS id_a, b.id AS id_b,
           similarity(a.job_title, b.job_title) AS sim
    FROM jobs a
    JOIN jobs b ON a.company = b.company AND a.id < b.id
    WHERE a.first_seen_at > NOW() - INTERVAL '7 days'
      AND similarity(a.job_title, b.job_title) > 0.85
""")


async def run_fuzzy_dedup(db: AsyncSession | None = None) -> int:
    """Find near-duplicate jobs from the last 7 days and merge them.
    Returns the number of duplicates collapsed."""
    close_db = db is None
    if db is None:
        db = SessionLocal()

    merged = 0
    try:
        result = await db.execute(_FUZZY_QUERY)
        pairs = result.fetchall()
        for row in pairs:
            # Keep the earlier first_seen_at; mark the newer as closed
            await db.execute(
                text("UPDATE jobs SET is_closed = true WHERE id = :id"),
                {"id": row.id_b},
            )
            merged += 1
        if merged:
            await db.commit()
            log.info("fuzzy_dedup.merged", count=merged)
    finally:
        if close_db:
            await db.close()

    return merged
