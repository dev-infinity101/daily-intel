"""DarwinBox careers JSON API adapter.

DarwinBox career pages follow the pattern:
  https://{tenant}.darwinbox.in/ms/candidatev2/main/careers/allJobs

When called with Accept: application/json the endpoint returns a JSON response
instead of HTML. This adapter hits that endpoint directly and parses the
structured job listings.

Usage:
  from app.services.jobs.ats_adapters.darwinbox import fetch_darwinbox
  jobs = await fetch_darwinbox(
      slug="sparkminda",
      career_url="https://sparkminda-hris.darwinbox.in/ms/candidatev2/main/careers/allJobs"
  )
"""
import re
from urllib.parse import urlparse

import httpx
import structlog

from app.schemas.job import JobIn

log = structlog.get_logger()

_TENANT_RE = re.compile(r"https?://([^.]+)\.darwinbox\.in", re.I)

_HEADERS = {
    "Accept": "application/json, text/javascript, */*; q=0.01",
    "X-Requested-With": "XMLHttpRequest",
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
}


def _build_api_url(career_url: str) -> str:
    """Ensure the URL points to the allJobs endpoint with free_india search."""
    parsed = urlparse(career_url)
    base = f"{parsed.scheme}://{parsed.netloc}/ms/candidatev2/main/careers/allJobs"
    return base


def _parse_job(item: dict, slug: str, tenant: str) -> JobIn | None:
    title = (
        item.get("job_title")
        or item.get("title")
        or item.get("position_name")
        or ""
    )
    if not title:
        return None

    job_id = str(item.get("job_id") or item.get("id") or "")
    apply_url = item.get("apply_url") or item.get("job_url") or ""
    if not apply_url and job_id:
        apply_url = f"https://{tenant}.darwinbox.in/ms/candidatev2/main/careers/allJobs/{job_id}"

    location = item.get("location") or item.get("city") or item.get("work_location") or None

    return JobIn(
        company=item.get("company_name") or slug,
        job_title=title,
        location=location,
        job_url=apply_url,
        external_job_id=job_id[:128] or None,
        source_type="ats_darwinbox",
        description=(item.get("job_description") or item.get("description") or "")[:500] or None,
    )


def _extract_jobs_from_response(data: dict | list, slug: str, tenant: str) -> list[JobIn]:
    """Handle multiple known DarwinBox response shapes."""
    raw: list[dict] = []

    if isinstance(data, list):
        raw = data
    elif isinstance(data, dict):
        # Common shapes: {"data": [...]} or {"data": {"job_listing": [...]}} or {"jobs": [...]}
        inner = data.get("data") or data.get("jobs") or data.get("job_listing") or []
        if isinstance(inner, dict):
            inner = inner.get("job_listing") or inner.get("jobs") or []
        raw = inner if isinstance(inner, list) else []

    jobs = []
    for item in raw:
        if not isinstance(item, dict):
            continue
        job = _parse_job(item, slug, tenant)
        if job:
            jobs.append(job)
    return jobs


async def fetch_darwinbox(slug: str, career_url: str) -> list[JobIn]:
    """Fetch all jobs from a DarwinBox tenant via their JSON API."""
    m = _TENANT_RE.match(career_url)
    tenant = m.group(1) if m else slug
    api_url = _build_api_url(career_url)

    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        try:
            r = await client.get(api_url, headers=_HEADERS, params={"search": "free_india"})
            r.raise_for_status()

            # DarwinBox sometimes returns HTML for bots — check content type
            ct = r.headers.get("content-type", "")
            if "json" not in ct and r.text.strip().startswith("<"):
                log.warning("darwinbox.html_response", slug=slug, url=api_url, content_type=ct)
                return []

            data = r.json()
        except Exception as exc:
            log.warning("darwinbox.fetch_failed", slug=slug, url=api_url, error=str(exc))
            return []

    jobs = _extract_jobs_from_response(data, slug, tenant)
    log.info("darwinbox.fetched", slug=slug, count=len(jobs))
    return jobs
