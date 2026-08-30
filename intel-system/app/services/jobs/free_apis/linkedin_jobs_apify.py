"""LinkedIn Jobs Apify adapter — uses valig/linkedin-jobs-scraper actor.

Single-run scraper: fires one Apify run with fixed search parameters
(title, location, datePosted, limit) and returns normalised JobIn objects.

This is a COSTLY actor — run it exactly once per /scrape-now invocation.
The results are used for gap analysis against target companies before
falling back to the more expensive career-page T1/T2 scraping.
"""
import re
from datetime import datetime
from decimal import Decimal, InvalidOperation

import structlog

from app.config import settings
from app.database import SessionLocal
from app.schemas.job import JobIn
from app.services.jobs.job_pipeline import persist_filtered_jobs

log = structlog.get_logger(__name__)

# Actor ID on Apify
_ACTOR_ID = "curious_coder/linkedin-jobs-scraper"


def _parse_salary(salary_input: str | list | None) -> tuple[Decimal | None, Decimal | None]:
    """Best-effort parse of salary strings or lists like ['$17.00', '$19.00'].

    Returns (salary_min, salary_max). Non-parseable strings return (None, None).
    """
    if not salary_input:
        return None, None
    
    if isinstance(salary_input, list):
        salary_str = " - ".join(str(s) for s in salary_input)
    else:
        salary_str = str(salary_input)
    # Strip currency symbols, commas, and per-period suffixes
    cleaned = re.sub(r"[^\d.\-–]", " ", salary_str)
    parts = re.findall(r"[\d]+(?:\.[\d]+)?", cleaned)
    try:
        if len(parts) >= 2:
            return Decimal(parts[0]), Decimal(parts[1])
        if len(parts) == 1:
            return Decimal(parts[0]), None
    except (InvalidOperation, ValueError):
        pass
    return None, None


def normalize_linkedin_apify_job(raw: dict) -> JobIn | None:
    """Normalise a single curious_coder/linkedin-jobs-scraper output item to JobIn."""
    try:
        title = (raw.get("title") or "").strip()
        if not title:
            return None

        company = (raw.get("companyName") or "Unknown").strip()

        # Use the LinkedIn job page URL as the apply link
        job_url = (raw.get("link") or "").strip()
        if not job_url:
            return None

        location = (raw.get("location") or "India").strip()
        description_text = raw.get("descriptionText") or raw.get("descriptionHtml") or ""
        description = description_text[:500]

        # External ID from the actor output
        external_id = str(raw.get("id") or "")

        # Parse posted date (format: "2023-08-16")
        posted_at: datetime | None = None
        posted_raw = raw.get("postedAt") or raw.get("postedAtTimestamp")
        if posted_raw:
            try:
                if isinstance(posted_raw, (int, float)):
                    posted_at = datetime.fromtimestamp(posted_raw / 1000.0)
                else:
                    posted_at = datetime.fromisoformat(str(posted_raw))
            except (ValueError, TypeError):
                pass

        salary_min, salary_max = _parse_salary(raw.get("salaryInfo"))

        return JobIn(
            company=company,
            job_title=title,
            location=location,
            remote=raw.get("workRemoteAllowed", False),
            department=raw.get("jobFunction"),
            description=description,
            job_url=job_url,
            external_job_id=external_id,
            source_type="linkedin_jobs_apify",
            posted_at=posted_at,
            salary_min=salary_min,
            salary_max=salary_max,
        )
    except Exception as exc:
        log.error("linkedin_jobs_apify.normalize_failed", error=str(exc))
        return None


async def poll_linkedin_jobs_apify() -> list[JobIn]:
    """Run the valig/linkedin-jobs-scraper actor and return normalised jobs.

    Uses the existing Apify adapter helpers (_trigger_actor, _wait_for_run,
    _fetch_dataset) to manage the run lifecycle.

    Returns the full list of normalised JobIn objects (pre-filter).
    """
    token = settings.apify_token or settings.apify_token_secondary
    if not token:
        log.warning("linkedin_jobs_apify.disabled", reason="No APIFY_TOKEN configured")
        return []

    from app.services.jobs.apify_adapter import (
        _fetch_dataset,
        _trigger_actor,
        _wait_for_run,
    )

    actor_id = _ACTOR_ID

    run_input = {
        "urls": [
            "https://www.linkedin.com/jobs/search?keywords=EV%20jobs%20&location=India&geoId=&position=1&pageNum=0"
        ],
        "count": 100,
        "splitByLocation": False,
        "scrapeCompany": False,
    }

    log.info(
        "linkedin_jobs_apify.starting",
        actor=actor_id,
        urls=run_input["urls"],
        count=run_input["count"],
    )

    try:
        run_id, used_token = await _trigger_actor(actor_id, run_input)
        log.info("linkedin_jobs_apify.run_started", run_id=run_id)

        dataset_id = await _wait_for_run(run_id, used_token, timeout=300)
        raw_items = await _fetch_dataset(dataset_id, used_token)
        log.info("linkedin_jobs_apify.raw_items", count=len(raw_items))
    except Exception as exc:
        log.error("linkedin_jobs_apify.run_failed", error=str(exc))
        return []

    # Normalise and deduplicate in-memory by external ID
    seen_ids: set[str] = set()
    normalized: list[JobIn] = []

    for item in raw_items:
        job = normalize_linkedin_apify_job(item)
        if not job:
            continue
        ext_id = job.external_job_id or ""
        if ext_id and ext_id in seen_ids:
            continue
        if ext_id:
            seen_ids.add(ext_id)
        normalized.append(job)

    log.info("linkedin_jobs_apify.normalized", total=len(normalized))
    return normalized


async def poll_and_persist_linkedin_jobs_apify() -> tuple[list[JobIn], dict]:
    """Poll, persist, and return both the raw job list and persistence stats.

    Returns:
        (all_normalized_jobs, stats_dict) where all_normalized_jobs is the
        complete pre-filter list (needed for gap analysis) and stats_dict
        contains insertion metrics.
    """
    all_jobs = await poll_linkedin_jobs_apify()

    if not all_jobs:
        return [], {
            "status": "completed",
            "source": "linkedin_jobs_apify",
            "jobs_fetched": 0,
            "jobs_inserted": 0,
        }

    db = SessionLocal()
    try:
        inserted = await persist_filtered_jobs(all_jobs, db)
        stats = {
            "status": "completed",
            "source": "linkedin_jobs_apify",
            "jobs_fetched": len(all_jobs),
            "jobs_inserted": inserted,
        }
        log.info("linkedin_jobs_apify.persisted", **stats)
        return all_jobs, stats
    except Exception as exc:
        log.exception("linkedin_jobs_apify.persist_failed")
        return all_jobs, {
            "status": "error",
            "source": "linkedin_jobs_apify",
            "jobs_fetched": len(all_jobs),
            "jobs_inserted": 0,
            "error": str(exc),
        }
    finally:
        await db.close()
