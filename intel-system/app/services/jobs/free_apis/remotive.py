"""Remotive public API — free, no key required."""
from datetime import datetime

import httpx
import structlog

from app.schemas.job import JobIn
from app.services.jobs.classifier import is_ev_domain_relevant

log = structlog.get_logger()

_URL = "https://remotive.com/api/remote-jobs"
_RELEVANT_CATEGORIES = {
    "Software Development",
    "DevOps / Sysadmin",
    "Data",
    "Product",
    "Design",
}


def _parse(job: dict) -> JobIn:
    return JobIn(
        company=job.get("company_name") or "Unknown",
        job_title=job.get("title") or "Role",
        location=job.get("candidate_required_location"),
        remote=True,
        description=(job.get("description") or "")[:500],
        job_url=job.get("url") or "",
        external_job_id=str(job.get("id", "")),
        source_type="remotive",
        posted_at=datetime.fromisoformat(job["publication_date"]) if job.get("publication_date") else None,
    )


async def poll_remotive() -> None:
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(_URL, params={"limit": 50})
        r.raise_for_status()
        jobs_raw = r.json().get("jobs", [])

    all_jobs = [_parse(j) for j in jobs_raw]
    jobs = [j for j in all_jobs if is_ev_domain_relevant(j.job_title, j.description or "")]
    log.info("remotive.fetched", total=len(all_jobs), ev_relevant=len(jobs))
    await _ingest(jobs)


async def _ingest(jobs: list[JobIn]) -> None:
    if not jobs:
        return
    from app.database import SessionLocal
    from app.services.jobs.job_pipeline import persist_filtered_jobs

    db = SessionLocal()
    try:
        kept = await persist_filtered_jobs(jobs, db)
        log.info("remotive.pipeline_complete", kept=kept, total=len(jobs))
    finally:
        await db.close()
