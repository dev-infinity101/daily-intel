"""Workable public jobs widget API — https://{slug}.workable.com/api/v3/jobs"""
from datetime import datetime, timezone

import httpx
import structlog

from app.schemas.job import JobIn

log = structlog.get_logger()


def _parse(job: dict, slug: str) -> JobIn:
    return JobIn(
        company=job.get("department") or slug,
        job_title=job.get("title") or "Role",
        location=job.get("location", {}).get("city"),
        department=job.get("department"),
        description=(job.get("description") or "")[:500],
        job_url=job.get("url") or "",
        external_job_id=str(job.get("shortcode", "")),
        source_type="ats_workable",
        posted_at=datetime.fromisoformat(job["created_at"]) if job.get("created_at") else None,
    )


async def fetch_workable(slug: str) -> list[JobIn]:
    url = f"https://{slug}.workable.com/api/v3/jobs"
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(url, params={"limit": 100, "details": "true"})
        r.raise_for_status()
        jobs_raw = r.json().get("results", [])
    jobs = [_parse(j, slug) for j in jobs_raw]
    log.info("workable.fetched", slug=slug, count=len(jobs))
    return jobs
