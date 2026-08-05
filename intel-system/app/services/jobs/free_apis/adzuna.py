"""Adzuna adapter — integrates the Adzuna Job Search API."""
import asyncio
from datetime import datetime

import httpx
import structlog

from app.config import settings
from app.database import SessionLocal
from app.schemas.job import JobIn
from app.services.jobs.job_pipeline import persist_filtered_jobs

log = structlog.get_logger(__name__)

ADZUNA_API_BASE = "https://api.adzuna.com/v1/api/jobs/in/search"


async def _fetch_adzuna_page(
    client: httpx.AsyncClient, 
    keyword: str, 
    app_id: str, 
    app_key: str,
    page: int
) -> list[dict]:
    params = {
        "app_id": app_id,
        "app_key": app_key,
        "what": keyword,
        "where": "India",
        "results_per_page": 50,
        "sort_by": "date",
        "content-type": "application/json"
    }
    
    try:
        response = await client.get(f"{ADZUNA_API_BASE}/{page}", params=params, timeout=20.0)
        response.raise_for_status()
        data = response.json()
        results = data.get("results", [])
        for item in results:
            item["_search_keyword"] = keyword
        return results
    except Exception as exc:
        log.error("adzuna.fetch_failed", keyword=keyword, page=page, error=str(exc))
        return []


async def fetch_adzuna_keyword(
    client: httpx.AsyncClient, 
    keyword: str, 
    app_id: str, 
    app_key: str,
    pages: int = 4
) -> list[dict]:
    """Fetch jobs from Adzuna for a specific keyword across multiple pages concurrently.

    Each returned dict is tagged with ``_search_keyword`` so the
    normaliser can inject it into the description field for domain
    classification.
    """
    log.info("adzuna.fetch_keyword", keyword=keyword, pages=pages)
    
    tasks = [
        _fetch_adzuna_page(client, keyword, app_id, app_key, page)
        for page in range(1, pages + 1)
    ]
    
    results = await asyncio.gather(*tasks, return_exceptions=True)
    
    all_items = []
    for res in results:
        if isinstance(res, list):
            all_items.extend(res)
            
    return all_items


def normalize_adzuna_job(raw_job: dict) -> JobIn | None:
    """Normalize a raw Adzuna job dict into a JobIn schema.

    The Adzuna search API rarely returns full descriptions, so we build
    a lightweight description from the search keyword and category to
    give the domain classifier enough EV signal.
    """
    try:
        job_id = str(raw_job.get("id", ""))
        title = raw_job.get("title", "")
        if not title:
            return None
            
        company_data = raw_job.get("company") or {}
        company = company_data.get("display_name") or "Unknown"
        
        location_data = raw_job.get("location") or {}
        location = location_data.get("display_name") or "India"
        
        job_url = raw_job.get("redirect_url", "")
        if not job_url:
            return None
            
        posted_at_str = raw_job.get("created")
        posted_at = None
        if posted_at_str:
            try:
                # Adzuna format: "2023-10-25T14:48:00Z"
                posted_at = datetime.fromisoformat(posted_at_str.replace("Z", "+00:00"))
            except ValueError:
                pass
                
        salary_min = raw_job.get("salary_min")
        salary_max = raw_job.get("salary_max")
        
        category_data = raw_job.get("category") or {}
        department = category_data.get("label")

        # Build description from search keyword + category for domain context.
        # The search keyword carries the EV signal (e.g. "EV charging",
        # "Emobility") that the classifier needs for domain matching.
        search_keyword = raw_job.get("_search_keyword", "")
        raw_desc = raw_job.get("description", "")
        parts = []
        if search_keyword:
            parts.append(f"Found via: {search_keyword}.")
        if department:
            parts.append(f"Category: {department}.")
        if raw_desc:
            parts.append(raw_desc[:400])
        description = " ".join(parts) if parts else ""
        
        return JobIn(
            company=company,
            job_title=title,
            location=location,
            remote=False,
            department=department,
            description=description,
            job_url=job_url,
            external_job_id=job_id,
            source_type="adzuna",
            posted_at=posted_at,
            salary_min=salary_min,
            salary_max=salary_max,
        )
    except Exception as exc:
        log.error("adzuna.normalize_failed", error=str(exc))
        return None


async def poll_adzuna() -> dict:
    """
    Main entry point for Adzuna integration.
    Performs 3 API requests, merges results, deduplicates, and saves them.
    """
    app_id = settings.adzuna_app_id
    app_key = settings.adzuna_app_key
    
    if not app_id or not app_key:
        log.warning("adzuna.disabled", reason="Missing adzuna_app_id or adzuna_app_key in config")
        return {"status": "error", "error": "Missing ADZUNA_APP_ID or ADZUNA_APP_KEY"}
        
    keywords = ["Emobility", "Lastmile", "EV charging"]
    
    # Target watchlist companies that often fail the main scraper
    watchlist_companies = [
        "EY",
        "JSW Greentech",
        "Panasonic",  # Adjusted from pansonic
        "Climate Group",
        "Nestle",
        "KPMG",
        "Monterra",
        "Movnsync",
        "Uno Minda",
    ]
    for company in watchlist_companies:
        keywords.append(f"EV jobs {company}")
    all_raw_jobs = []
    
    async with httpx.AsyncClient() as client:
        tasks = [
            fetch_adzuna_keyword(client, kw, app_id, app_key) 
            for kw in keywords
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        
        for res in results:
            if isinstance(res, list):
                all_raw_jobs.extend(res)
            elif isinstance(res, Exception):
                log.error("adzuna.task_exception", error=str(res))
                
    # Deduplicate in-memory by ID
    unique_jobs_map = {}
    for job in all_raw_jobs:
        job_id = str(job.get("id"))
        if job_id and job_id not in unique_jobs_map:
            unique_jobs_map[job_id] = job
            
    normalized_jobs = []
    for raw_job in unique_jobs_map.values():
        job_in = normalize_adzuna_job(raw_job)
        if job_in:
            normalized_jobs.append(job_in)
            
    log.info("adzuna.normalized", total=len(normalized_jobs))
    
    if not normalized_jobs:
        return {"status": "completed", "jobs_fetched": 0, "jobs_inserted": 0}
        
    db = SessionLocal()
    try:
        inserted = await persist_filtered_jobs(normalized_jobs, db)
        return {
            "status": "completed",
            "jobs_fetched": len(normalized_jobs),
            "jobs_inserted": inserted
        }
    except Exception as exc:
        log.exception("adzuna.persist_failed")
        return {"status": "error", "error": str(exc)}
    finally:
        await db.close()


async def _fetch_adzuna_jobs() -> list[JobIn]:
    """Fetch and normalise Adzuna jobs without persisting.

    Shared helper used by both poll_adzuna() (legacy) and
    fetch_and_persist_adzuna() (V4 flow).
    """
    app_id = settings.adzuna_app_id
    app_key = settings.adzuna_app_key

    if not app_id or not app_key:
        log.warning("adzuna.disabled", reason="Missing adzuna_app_id or adzuna_app_key in config")
        return []

    keywords = ["Emobility", "Lastmile", "EV charging"]

    watchlist_companies = [
        "EY",
        "JSW Greentech",
        "Panasonic",
        "Climate Group",
        "Nestle",
        "KPMG",
        "Monterra",
        "Movnsync",
        "Uno Minda",
    ]
    for company in watchlist_companies:
        keywords.append(f"EV jobs {company}")

    all_raw_jobs: list[dict] = []

    async with httpx.AsyncClient() as client:
        tasks = [
            fetch_adzuna_keyword(client, kw, app_id, app_key)
            for kw in keywords
        ]
        results = await asyncio.gather(*tasks, return_exceptions=True)

        for res in results:
            if isinstance(res, list):
                all_raw_jobs.extend(res)
            elif isinstance(res, Exception):
                log.error("adzuna.task_exception", error=str(res))

    # Deduplicate in-memory by ID
    unique_jobs_map: dict[str, dict] = {}
    for job in all_raw_jobs:
        job_id = str(job.get("id"))
        if job_id and job_id not in unique_jobs_map:
            unique_jobs_map[job_id] = job

    normalized: list[JobIn] = []
    for raw_job in unique_jobs_map.values():
        job_in = normalize_adzuna_job(raw_job)
        if job_in:
            normalized.append(job_in)

    log.info("adzuna.normalized", total=len(normalized))
    return normalized


async def fetch_and_persist_adzuna() -> tuple[list[JobIn], dict]:
    """Fetch, persist, and return both the raw job list and persistence stats.

    Unlike poll_adzuna(), this returns the full normalised job list (pre-filter)
    alongside stats. The job list is needed for V4 gap analysis.

    Returns:
        (all_normalized_jobs, stats_dict)
    """
    all_jobs = await _fetch_adzuna_jobs()

    if not all_jobs:
        return [], {
            "status": "completed",
            "source": "adzuna",
            "jobs_fetched": 0,
            "jobs_inserted": 0,
        }

    db = SessionLocal()
    try:
        inserted = await persist_filtered_jobs(all_jobs, db)
        stats = {
            "status": "completed",
            "source": "adzuna",
            "jobs_fetched": len(all_jobs),
            "jobs_inserted": inserted,
        }
        log.info("adzuna.persisted", **stats)
        return all_jobs, stats
    except Exception as exc:
        log.exception("adzuna.persist_failed")
        return all_jobs, {
            "status": "error",
            "source": "adzuna",
            "jobs_fetched": len(all_jobs),
            "jobs_inserted": 0,
            "error": str(exc),
        }
    finally:
        await db.close()

