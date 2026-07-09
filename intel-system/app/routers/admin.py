from fastapi import APIRouter, Depends, Query
from sqlalchemy.ext.asyncio import AsyncSession

from app.database import get_db
from app.scheduler.tasks import run_daily_digest

router = APIRouter(prefix="/admin", tags=["admin"])


# ── General digest ────────────────────────────────────────────────────────────

@router.post("/digest/send-now")
async def send_now(db: AsyncSession = Depends(get_db)) -> dict[str, str]:
    await run_daily_digest(db)
    return {"status": "triggered"}


@router.get("/digest/preview")
async def preview_digest(db: AsyncSession = Depends(get_db)) -> dict[str, object]:
    from app.services.digest.assembler import assemble_html, fetch_today_items

    items = await fetch_today_items(db)
    html = assemble_html(items, subject="[PREVIEW] Daily Intel")
    return {"item_count": len(items), "html_length": len(html)}


# ── Jobs digest ───────────────────────────────────────────────────────────────

@router.post("/jobs/digest-now")
async def send_jobs_digest_now(
    db: AsyncSession = Depends(get_db),
    hours: int = Query(default=48, description="Max-age gate: exclude jobs older than this many hours"),
    include_experiment: bool = Query(default=False, description="Include browserbase_experiment rows"),
    force: bool = Query(default=False, description="Resend already-emailed jobs without re-stamping emailed_at (for testing)"),
) -> dict[str, object]:
    """Send the jobs-only EV business roles digest immediately.

    Normal (force=false): only unsent jobs (emailed_at IS NULL).
    Returns status='already_sent' with last_sent_at when all jobs in window are already mailed.

    Force (force=true): resends previously sent jobs; does NOT modify emailed_at.
    """
    from app.services.jobs.job_digest import send_jobs_digest

    return await send_jobs_digest(db, hours=hours, include_experiment=include_experiment, force=force)


@router.get("/jobs/preview")
async def preview_jobs_digest(
    db: AsyncSession = Depends(get_db),
    hours: int = Query(default=24, description="Look-back window in hours"),
    include_experiment: bool = Query(default=False, description="Include browserbase_experiment rows"),
) -> dict[str, object]:
    """Preview the jobs digest HTML without sending."""
    from app.services.jobs.job_digest import build_jobs_html, fetch_recent_jobs

    jobs = await fetch_recent_jobs(db, hours=hours, include_experiment=include_experiment)
    html = build_jobs_html(jobs)
    return {"job_count": len(jobs), "html_length": len(html), "html": html}


@router.post("/jobs/experiment-digest-now")
async def send_experiment_digest_now(
    db: AsyncSession = Depends(get_db),
    hours: int = Query(default=168, description="Look-back window in hours (default 7 days)"),
) -> dict[str, object]:
    """Send a digest email containing only Browserbase experiment jobs."""
    from app.services.jobs.job_digest import send_jobs_digest

    return await send_jobs_digest(db, hours=hours, only_experiment=True)


@router.get("/jobs/experiment-summary")
async def experiment_summary(db: AsyncSession = Depends(get_db)) -> dict[str, object]:
    """Show what the Browserbase experiment inserted — useful for verifying the run."""
    from sqlalchemy import text

    total = (await db.execute(
        text("SELECT COUNT(*) FROM jobs WHERE source_type = 'browserbase_experiment'")
    )).scalar()
    rows = (await db.execute(text("""
        SELECT company, job_title, location, first_seen_at
        FROM jobs
        WHERE source_type = 'browserbase_experiment'
        ORDER BY first_seen_at DESC
        LIMIT 50
    """))).fetchall()
    return {
        "total_experiment_jobs": total,
        "jobs": [
            {"company": r[0], "title": r[1], "location": r[2], "first_seen_at": str(r[3])}
            for r in rows
        ],
    }


@router.post("/jobs/scrape-now")
async def scrape_jobs_now(company: str | None = None) -> dict[str, object]:
    """Trigger an immediate V3 scrape of active target companies.

    Pass ?company=<slug> for a single-company run (fast — bypasses TTL skip
    and batch cooldowns for quick testing). Returns aggregate counts + per-
    company results including tier (t1/t2/t3) and outcome.
    """
    from app.services.jobs.scrape_orchestrator import orchestrate_scrape

    stats = await orchestrate_scrape(slug=company, is_manual=True)
    return {"status": "completed", **stats}


@router.post("/jobs/adzuna-jobs")
async def scrape_adzuna_now() -> dict[str, object]:
    """Trigger an immediate scrape of Adzuna API."""
    from app.services.jobs.free_apis.adzuna import poll_adzuna

    stats = await poll_adzuna()
    return stats


@router.get("/jobs/metrics")
async def jobs_metrics(db: AsyncSession = Depends(get_db)) -> dict[str, object]:
    """V3 run metrics — coverage, tier breakdown, budget usage, watchlist.

    Aggregates data from the scrape_attempts table for the last completed run
    and for the current calendar month.
    """
    from sqlalchemy import text

    # Last run summary
    last_run = (await db.execute(text("""
        SELECT run_id, COUNT(*) AS attempts,
               SUM(jobs_inserted) AS inserted,
               MAX(created_at) AS ts
        FROM scrape_attempts
        GROUP BY run_id
        ORDER BY ts DESC
        LIMIT 1
    """))).fetchone()

    last_run_id = last_run[0] if last_run else None

    # Per-tier breakdown for last run
    tier_rows = []
    if last_run_id:
        tier_rows = (await db.execute(text("""
            SELECT tier,
                   COUNT(*) AS companies,
                   SUM(jobs_found) AS fetched,
                   SUM(jobs_inserted) AS inserted,
                   SUM(CASE WHEN outcome = 'success' THEN 1 ELSE 0 END) AS successes
            FROM scrape_attempts
            WHERE run_id = :run_id
            GROUP BY tier
            ORDER BY tier
        """), {"run_id": last_run_id})).fetchall()

    # Monthly usage
    monthly = (await db.execute(text("""
        SELECT
          SUM(CASE WHEN tier = 't1' THEN 1 ELSE 0 END) AS t1_runs,
          SUM(CASE WHEN tier = 't2' THEN 1 ELSE 0 END) AS t2_sessions,
          SUM(jobs_inserted) AS total_inserted
        FROM scrape_attempts
        WHERE created_at >= date_trunc('month', now())
    """))).fetchone()

    # Watchlist — companies with ≥3 consecutive failures
    watchlist_rows = (await db.execute(text("""
        SELECT slug, display_name, consecutive_failures, preferred_scraper
        FROM target_companies
        WHERE consecutive_failures >= 3 AND is_active = true
        ORDER BY consecutive_failures DESC
        LIMIT 20
    """))).fetchall()

    from app.config import settings as cfg
    t1_soft = int(cfg.apify_monthly_cu_limit * 0.8)
    t2_soft = int(cfg.browserbase_monthly_minutes_limit * 0.8)
    monthly_t1 = monthly[0] if monthly else 0
    monthly_t2 = monthly[1] if monthly else 0

    return {
        "last_run": {
            "run_id": last_run_id,
            "companies": last_run[1] if last_run else 0,
            "jobs_inserted": last_run[2] if last_run else 0,
            "timestamp": str(last_run[3]) if last_run else None,
        },
        "last_run_tiers": [
            {
                "tier": r[0],
                "companies": r[1],
                "jobs_fetched": r[2],
                "jobs_inserted": r[3],
                "successes": r[4],
            }
            for r in tier_rows
        ],
        "monthly_budget": {
            "t1_runs": monthly_t1,
            "t1_soft_limit": t1_soft,
            "t1_pct": round(monthly_t1 / t1_soft * 100, 1) if t1_soft else 0,
            "t2_sessions": monthly_t2,
            "t2_soft_limit": t2_soft,
            "t2_pct": round(monthly_t2 / t2_soft * 100, 1) if t2_soft else 0,
            "total_inserted_this_month": monthly[2] if monthly else 0,
        },
        "watchlist": [
            {
                "slug": r[0],
                "display_name": r[1],
                "consecutive_failures": r[2],
                "preferred_scraper": r[3],
            }
            for r in watchlist_rows
        ],
    }


@router.get("/jobs/db-check")
async def jobs_db_check(db: AsyncSession = Depends(get_db)) -> dict[str, object]:
    """Verify DB schema, count jobs, and confirm pipeline can write."""
    from sqlalchemy import text
    from app.services.jobs.job_pipeline import check_schema

    schema = await check_schema(db)

    total = (await db.execute(text("SELECT COUNT(*) FROM jobs"))).scalar()
    recent = (await db.execute(
        text("SELECT COUNT(*) FROM jobs WHERE first_seen_at >= NOW() - INTERVAL '24 hours'")
    )).scalar()
    experiment = (await db.execute(
        text("SELECT COUNT(*) FROM jobs WHERE source_type = 'browserbase_experiment'")
    )).scalar()
    active_companies = (await db.execute(
        text("SELECT COUNT(*) FROM target_companies WHERE is_active = true")
    )).scalar()
    rows = (await db.execute(
        text("SELECT company, job_title, source_type, rank_score, first_seen_at FROM jobs ORDER BY first_seen_at DESC LIMIT 5")
    )).fetchall()
    recent_jobs = [
        {"company": r[0], "title": r[1], "source": r[2], "rank": r[3], "seen_at": str(r[4])}
        for r in rows
    ]

    return {
        "schema": schema,
        "jobs_total": total,
        "jobs_last_24h": recent,
        "jobs_experiment": experiment,
        "active_companies": active_companies,
        "recent_jobs": recent_jobs,
    }
