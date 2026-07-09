"""Lever public postings API — https://api.lever.co/v0/postings/{slug}"""
from datetime import datetime, timezone

import httpx
import structlog

from app.schemas.job import JobIn

log = structlog.get_logger()

_BASE = "https://api.lever.co/v0/postings"


def _parse(job: dict, slug: str) -> JobIn:
    categories = job.get("categories") or {}
    return JobIn(
        company=slug,
        job_title=job.get("text") or "Role",
        location=categories.get("location"),
        department=categories.get("team"),
        description=(job.get("descriptionPlain") or "")[:500],
        job_url=job.get("hostedUrl") or job.get("applyUrl") or "",
        external_job_id=str(job.get("id", "")),
        source_type="ats_lever",
        posted_at=datetime.fromtimestamp(job["createdAt"] / 1000, tz=timezone.utc) if job.get("createdAt") else None,
    )


async def fetch_lever(slug: str) -> list[JobIn]:
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{_BASE}/{slug}", params={"mode": "json"})
        r.raise_for_status()
        jobs_raw = r.json()
    jobs = [_parse(j, slug) for j in jobs_raw]
    log.info("lever.fetched", slug=slug, count=len(jobs))
    return jobs
