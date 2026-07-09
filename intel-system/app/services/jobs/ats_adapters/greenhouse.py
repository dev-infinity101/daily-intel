"""Greenhouse public job board API — https://boards-api.greenhouse.io/v1/boards/{slug}/jobs"""
from datetime import datetime, timezone

import httpx
import structlog

from app.schemas.job import JobIn

log = structlog.get_logger()

_BASE = "https://boards-api.greenhouse.io/v1/boards"


def _parse(job: dict, slug: str) -> JobIn:
    return JobIn(
        company=slug,
        job_title=job.get("title") or "Role",
        location=job.get("location", {}).get("name"),
        description=(job.get("content") or "")[:500],
        job_url=job.get("absolute_url") or "",
        external_job_id=str(job.get("id", "")),
        source_type="ats_greenhouse",
        posted_at=datetime.fromisoformat(job["updated_at"]) if job.get("updated_at") else None,
    )


async def fetch_greenhouse(slug: str) -> list[JobIn]:
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(f"{_BASE}/{slug}/jobs", params={"content": "true"})
        r.raise_for_status()
        jobs_raw = r.json().get("jobs", [])
    jobs = [_parse(j, slug) for j in jobs_raw]
    log.info("greenhouse.fetched", slug=slug, count=len(jobs))
    return jobs
