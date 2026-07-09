"""Workday public careers JSON API adapter.

Workday exposes a REST endpoint for each tenant:
  POST https://{tenant}.{wdN}.myworkdayjobs.com/wday/cxs/{tenant}/{board}/jobs
  Body: {"limit": 20, "offset": 0, "searchText": ""}

The tenant subdomain, Workday domain number (wd1/wd3/...) and board name
are all encoded in the career URL already stored in target_companies.career_urls.
This adapter parses the URL to build the API endpoint automatically — no
extra config required.

Usage:
  from app.services.jobs.ats_adapters.workday import fetch_workday
  jobs = await fetch_workday(slug="abb", career_url="https://abb.wd3.myworkdayjobs.com/ABBcareers")
"""
import re
from datetime import datetime

import httpx
import structlog

from app.schemas.job import JobIn

log = structlog.get_logger()

_URL_RE = re.compile(
    r"https?://(?P<tenant>[^.]+)\.(?P<wdN>wd\d+)\.myworkdayjobs\.com/(?P<board>[^/?#]+)",
    re.I,
)


def _parse_workday_url(career_url: str) -> tuple[str, str, str] | None:
    """Return (tenant, wdN, board) from a Workday career URL, or None."""
    m = _URL_RE.match(career_url)
    if not m:
        return None
    return m.group("tenant"), m.group("wdN"), m.group("board")


def _coerce_str(val: object) -> str:
    """Workday returns some text fields as {instances: [{text: "..."}]} dicts."""
    if isinstance(val, dict):
        instances = val.get("instances") or []
        if instances and isinstance(instances[0], dict):
            return instances[0].get("text") or ""
    return str(val) if val else ""


def _parse_job(item: dict, slug: str, base_url: str) -> JobIn | None:
    title = _coerce_str(item.get("title"))
    if not title:
        return None

    external_path = item.get("externalPath") or ""
    job_url = (
        f"{base_url.rstrip('/')}{external_path}"
        if external_path.startswith("/")
        else external_path or base_url
    )

    location = _coerce_str(item.get("locationsText"))

    posted_raw = item.get("postedOn") or ""
    posted_at: datetime | None = None
    try:
        posted_at = datetime.fromisoformat(posted_raw.replace("Z", "+00:00"))
    except (ValueError, AttributeError):
        pass

    return JobIn(
        company=slug,
        job_title=title,
        location=location or None,
        job_url=job_url,
        external_job_id=(item.get("bulletFields") or [""])[0][:128] if item.get("bulletFields") else None,
        source_type="ats_workday",
        posted_at=posted_at,
        description=_coerce_str(item.get("jobDescription"))[:500] or None,
    )


async def fetch_workday(slug: str, career_url: str) -> list[JobIn]:
    """Fetch all jobs from a Workday tenant via their public JSON API."""
    parsed = _parse_workday_url(career_url)
    if not parsed:
        log.warning("workday.url_parse_failed", slug=slug, url=career_url)
        return []

    tenant, wd_n, board = parsed
    api_url = f"https://{tenant}.{wd_n}.myworkdayjobs.com/wday/cxs/{tenant}/{board}/jobs"
    base_url = f"https://{tenant}.{wd_n}.myworkdayjobs.com"

    jobs: list[JobIn] = []
    offset = 0
    limit = 20

    async with httpx.AsyncClient(timeout=30) as client:
        while True:
            try:
                r = await client.post(
                    api_url,
                    json={"limit": limit, "offset": offset, "searchText": ""},
                    headers={
                        "Accept": "application/json",
                        "Content-Type": "application/json",
                    },
                )
                r.raise_for_status()
                data = r.json()
            except Exception as exc:
                log.warning("workday.fetch_failed", slug=slug, url=api_url, offset=offset, error=str(exc))
                break

            postings = data.get("jobPostings") or []
            if not postings:
                break

            for item in postings:
                job = _parse_job(item, slug, base_url)
                if job:
                    jobs.append(job)

            total = data.get("total") or 0
            offset += limit
            if offset >= total or len(postings) < limit:
                break

    log.info("workday.fetched", slug=slug, count=len(jobs))
    return jobs
