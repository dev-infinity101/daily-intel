"""Job lifecycle — stale-job detection and closure.

A job is considered stale when its last_seen_at is older than
settings.job_stale_days. last_seen_at is refreshed by job_pipeline
on every re-scrape (dedup hit), so it accurately reflects the last
time we confirmed the job was present on the company site.

If a company's scrape yields zero jobs for 21 days (default), those
jobs are assumed to have disappeared and are marked is_closed=True
so they no longer appear in the digest.
"""
import structlog
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession

from app.config import settings

log = structlog.get_logger()


async def run_job_staleness_check(db: AsyncSession) -> dict:
    """Mark jobs is_closed=True when not re-seen for settings.job_stale_days.

    Returns a summary dict with the count of newly closed jobs.
    """
    result = await db.execute(
        text("""
            UPDATE jobs
            SET is_closed = true
            WHERE is_closed = false
              AND last_seen_at < now() - make_interval(days => :stale_days)
            RETURNING id
        """),
        {"stale_days": settings.job_stale_days},
    )
    closed_ids = [row[0] for row in result.fetchall()]
    await db.commit()

    count = len(closed_ids)
    log.info(
        "job_lifecycle.staleness_check",
        closed=count,
        stale_days=settings.job_stale_days,
    )
    return {"closed": count, "stale_days": settings.job_stale_days}
