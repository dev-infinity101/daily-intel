"""Ashby public job board API — https://jobs.ashbyhq.com/api/non-user-facing/job-board/jobs?organizationHostedJobsPageName={slug}"""
from datetime import datetime

import httpx
import structlog

from app.schemas.job import JobIn

log = structlog.get_logger()

_BASE = "https://jobs.ashbyhq.com/api/non-user-facing/job-board/jobs"


def _parse(job: dict, slug: str) -> JobIn:
    return JobIn(
        company=slug,
        job_title=job.get("title") or "Role",
        location=(job.get("locationName") or job.get("location") or {}).get("name") if isinstance(job.get("locationName") or job.get("location"), dict) else job.get("locationName"),
        department=job.get("departmentName"),
        description=(job.get("descriptionHtml") or "")[:500],
        job_url=f"https://jobs.ashbyhq.com/{slug}/{job.get('id', '')}",
        external_job_id=str(job.get("id", "")),
        source_type="ats_ashby",
        posted_at=datetime.fromisoformat(job["publishedAt"]) if job.get("publishedAt") else None,
    )


async def fetch_ashby(slug: str) -> list[JobIn]:
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(_BASE, params={"organizationHostedJobsPageName": slug})
        r.raise_for_status()
        jobs_raw = r.json().get("jobs", [])
    jobs = [_parse(j, slug) for j in jobs_raw]
    log.info("ashby.fetched", slug=slug, count=len(jobs))
    return jobs
