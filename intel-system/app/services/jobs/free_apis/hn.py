"""HN 'Who is Hiring' — monthly thread via Algolia API."""
from datetime import UTC, datetime

import httpx
import structlog

from app.schemas.job import JobIn
from app.services.jobs.classifier import infer_remote, is_ev_domain_relevant

log = structlog.get_logger()

_ALGOLIA = "https://hn.algolia.com/api/v1/search"
_WHO_IS_HIRING = 'author:whoishiring "who is hiring"'


def _parse_comment(comment: dict) -> JobIn | None:
    text: str = comment.get("comment_text") or ""
    if not text or len(text) < 50:
        return None

    lines = [l.strip() for l in text.split("\n") if l.strip()]
    title = lines[0][:120] if lines else "Software Engineer"
    company = "Unknown"

    # Simple heuristic: "Company Name | Role | Location"
    if " | " in title:
        parts = title.split(" | ")
        company = parts[0].strip()[:128]
        title = parts[1].strip()[:200] if len(parts) > 1 else title

    url = comment.get("url") or ""
    posted_at = datetime.fromtimestamp(comment.get("created_at_i", 0), tz=UTC)

    return JobIn(
        company=company,
        job_title=title,
        location=None,
        remote=infer_remote(None, text),
        description=text[:500],
        job_url=url or f"https://news.ycombinator.com/item?id={comment.get('objectID')}",
        external_job_id=str(comment.get("objectID", "")),
        source_type="hn",
        posted_at=posted_at,
    )


async def poll_hn() -> None:
    async with httpx.AsyncClient(timeout=30) as client:
        r = await client.get(_ALGOLIA, params={"query": _WHO_IS_HIRING, "tags": "story", "hitsPerPage": 1})
        r.raise_for_status()
        hits = r.json().get("hits", [])
        if not hits:
            log.warning("hn.no_hiring_thread")
            return
        thread_id = hits[0]["objectID"]

        cr = await client.get(
            _ALGOLIA,
            params={"tags": f"comment,story_{thread_id}", "hitsPerPage": 200},
        )
        cr.raise_for_status()
        comments = cr.json().get("hits", [])

    all_jobs = [j for c in comments if (j := _parse_comment(c)) is not None]
    jobs = [j for j in all_jobs if is_ev_domain_relevant(j.job_title, j.description or "")]
    log.info("hn.fetched", total=len(all_jobs), ev_relevant=len(jobs))
    await _ingest(jobs)


async def _ingest(jobs: list[JobIn]) -> None:
    if not jobs:
        return
    from app.database import SessionLocal
    from app.services.jobs.job_pipeline import persist_filtered_jobs

    db = SessionLocal()
    try:
        kept = await persist_filtered_jobs(jobs, db)
        log.info("hn.pipeline_complete", kept=kept, total=len(jobs))
    finally:
        await db.close()
