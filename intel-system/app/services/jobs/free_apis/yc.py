"""YC Work at a Startup — public API, no key required."""
import httpx
import structlog

from app.schemas.job import JobIn
from app.services.jobs.classifier import is_ev_domain_relevant

log = structlog.get_logger()

_URL = "https://www.workatastartup.com/jobs.json"


def _parse(job: dict) -> JobIn:
    company = (job.get("company") or {}).get("name") or "Unknown"
    return JobIn(
        company=company,
        job_title=job.get("title") or "Role",
        location=job.get("locations", [""])[0] if job.get("locations") else None,
        remote=bool(job.get("remote")),
        description=(job.get("description") or "")[:500],
        job_url=f"https://www.workatastartup.com/jobs/{job.get('id', '')}",
        external_job_id=str(job.get("id", "")),
        source_type="yc",
        posted_at=None,
    )


async def poll_yc() -> None:
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(_URL)
        r.raise_for_status()
        jobs_raw = r.json().get("jobs", [])

    all_jobs = [_parse(j) for j in jobs_raw]
    jobs = [j for j in all_jobs if is_ev_domain_relevant(j.job_title, j.description or "")]
    log.info("yc.fetched", total=len(all_jobs), ev_relevant=len(jobs))
    await _ingest(jobs)


async def _ingest(jobs: list[JobIn]) -> None:
    if not jobs:
        return
    from app.database import SessionLocal
    from app.services.jobs.job_pipeline import persist_filtered_jobs

    db = SessionLocal()
    try:
        kept = await persist_filtered_jobs(jobs, db)
        log.info("yc.pipeline_complete", kept=kept, total=len(jobs))
    finally:
        await db.close()
