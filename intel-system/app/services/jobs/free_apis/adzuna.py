"""Adzuna adapter — integrates the Adzuna Job Search API."""
import asyncio
from datetime import datetime, timezone
import structlog
import httpx

from app.config import settings
from app.schemas.job import JobIn
from app.database import SessionLocal
from app.services.jobs.job_pipeline import persist_filtered_jobs

log = structlog.get_logger(__name__)

ADZUNA_API_URL = "https://api.adzuna.com/v1/api/jobs/in/search/1"


async def fetch_adzuna_keyword(
    client: httpx.AsyncClient, 
    keyword: str, 
    app_id: str, 
    app_key: str
) -> list[dict]:
    """Fetch jobs from Adzuna for a specific keyword."""
    params = {
        "app_id": app_id,
        "app_key": app_key,
        "what": keyword,
        "where": "India",
        "results_per_page": 50,
        "sort_by": "date",
        "content-type": "application/json"
    }
    
    log.info("adzuna.fetch_keyword", keyword=keyword)
    try:
        response = await client.get(ADZUNA_API_URL, params=params, timeout=20.0)
        response.raise_for_status()
        data = response.json()
        return data.get("results", [])
    except Exception as exc:
        log.error("adzuna.fetch_failed", keyword=keyword, error=str(exc))
        return []


def normalize_adzuna_job(raw_job: dict) -> JobIn | None:
    """Normalize a raw Adzuna job dict into a JobIn schema."""
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
        
        return JobIn(
            company=company,
            job_title=title,
            location=location,
            remote=False,
            department=department,
            description="",  # Adzuna search endpoint often omits full descriptions
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
