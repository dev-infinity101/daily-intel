import structlog

from app.database import SessionLocal
from app.scheduler.runner import scheduler

log = structlog.get_logger()


# ---------------------------------------------------------------------------
# Module 6 — Daily digest (Phase 1)
# ---------------------------------------------------------------------------

async def run_daily_digest(db=None) -> None:  # type: ignore[assignment]
    """Assemble and send today's digest. Accepts an optional db session for
    admin-triggered sends; creates its own session otherwise."""
    from app.services.digest.assembler import run_news_digest
    from app.services.jobs.job_digest import send_jobs_digest
    close_db = db is None
    if db is None:
        db = SessionLocal()
    try:
        await run_news_digest(db)
        await send_jobs_digest(db)
        log.info("digest.all_sent")
    except Exception:
        log.exception("digest.failed")
    finally:
        if close_db:
            await db.close()


@scheduler.scheduled_job(
    "cron",
    hour=6,
    minute=30,
    timezone="Asia/Kolkata",
    id="daily_digest",
    max_instances=1,
    misfire_grace_time=300,
)
async def daily_digest_job() -> None:
    await run_daily_digest()


# ---------------------------------------------------------------------------
# Module 5 — Jobs (Phase 2)
# ---------------------------------------------------------------------------

@scheduler.scheduled_job(
    "cron",
    day_of_week="wed",
    hour=3,
    minute=0,
    timezone="Asia/Kolkata",
    id="apify_poll",
    max_instances=1,
    misfire_grace_time=600,
)
async def apify_poll_job() -> None:
    from app.services.jobs.scrape_orchestrator import orchestrate_scrape

    try:
        await orchestrate_scrape()
    except Exception:
        log.exception("apify_poll.failed")


@scheduler.scheduled_job(
    "cron", day_of_week="wed", hour=4, minute=0, timezone="Asia/Kolkata", id="hn_poll", max_instances=1
)
async def hn_poll_job() -> None:
    from app.services.jobs.free_apis.hn import poll_hn

    try:
        await poll_hn()
    except Exception:
        log.exception("hn_poll.failed")


@scheduler.scheduled_job(
    "cron", day_of_week="wed", hour=4, minute=15, timezone="Asia/Kolkata", id="remotive_poll", max_instances=1
)
async def remotive_poll_job() -> None:
    from app.services.jobs.free_apis.remotive import poll_remotive

    try:
        await poll_remotive()
    except Exception:
        log.exception("remotive_poll.failed")


@scheduler.scheduled_job(
    "cron", day_of_week="wed", hour=4, minute=30, timezone="Asia/Kolkata", id="yc_poll", max_instances=1
)
async def yc_poll_job() -> None:
    from app.services.jobs.free_apis.yc import poll_yc

    try:
        await poll_yc()
    except Exception:
        log.exception("yc_poll.failed")


@scheduler.scheduled_job(
    "cron", day_of_week="wed", hour=4, minute=40, timezone="Asia/Kolkata", id="adzuna_poll", max_instances=1
)
async def adzuna_poll_job() -> None:
    from app.services.jobs.free_apis.adzuna import poll_adzuna

    try:
        await poll_adzuna()
    except Exception:
        log.exception("adzuna_poll.failed")



@scheduler.scheduled_job(
    "cron",
    day_of_week="wed",
    hour=4,
    minute=45,
    timezone="Asia/Kolkata",
    id="linkedin_poll",
    max_instances=1,
)
async def linkedin_poll_job() -> None:
    from app.services.jobs.linkedin_scraper import poll_linkedin

    try:
        await poll_linkedin()
    except Exception:
        log.exception("linkedin_poll.failed")


@scheduler.scheduled_job(
    "cron",
    day_of_week="wed",
    hour=5,
    minute=0,
    timezone="Asia/Kolkata",
    id="job_fuzzy_dedup",
    max_instances=1,
)
async def fuzzy_dedup_job() -> None:
    from app.services.jobs.dedup import run_fuzzy_dedup

    try:
        await run_fuzzy_dedup()
    except Exception:
        log.exception("fuzzy_dedup.failed")


@scheduler.scheduled_job(
    "cron",
    day_of_week="wed",
    hour=2,
    minute=0,
    timezone="Asia/Kolkata",
    id="job_lifecycle",
    max_instances=1,
    misfire_grace_time=300,
)
async def job_lifecycle_job() -> None:
    """Mark jobs stale when not re-seen for settings.job_stale_days days.

    Runs after fuzzy dedup (05:00) so is_closed from dedup doesn't conflict
    with is_closed from staleness. last_seen_at is refreshed by job_pipeline
    on every dedup hit during scraping.
    """
    from app.database import SessionLocal
    from app.services.jobs.job_lifecycle import run_job_staleness_check

    db = SessionLocal()
    try:
        result = await run_job_staleness_check(db)
        log.info("job_lifecycle.done", **result)
    except Exception:
        log.exception("job_lifecycle.failed")
    finally:
        await db.close()


@scheduler.scheduled_job(
    "cron",
    day_of_week="wed",
    hour=19,
    minute=0,
    timezone="Asia/Kolkata",
    id="jobs_digest",
    max_instances=1,
    misfire_grace_time=300,
)
async def jobs_digest_job() -> None:
    """Send the EV business roles jobs digest after all overnight scraping is done."""
    from app.database import SessionLocal
    from app.services.jobs.job_digest import send_jobs_digest

    db = SessionLocal()
    try:
        # Pass hours=168 to ensure we capture all jobs found over the last week
        result = await send_jobs_digest(db, hours=168)
        if result["status"] == "error":
            log.error("jobs_digest.failed", error=result.get("error"))
        else:
            log.info("jobs_digest.scheduled_result", **result)
    except Exception:
        log.exception("jobs_digest.failed")
    finally:
        await db.close()


# ---------------------------------------------------------------------------
# Module 4 — News (Phase 3)
# ---------------------------------------------------------------------------

# @scheduler.scheduled_job(
#     "cron",
#     hour="2,8,14,20",
#     minute=0,
#     timezone="Asia/Kolkata",
#     id="rss_poll",
#     max_instances=1,
#     misfire_grace_time=300,
# )
# async def rss_poll_job() -> None:
#     """Poll all global RSS feeds every 6 hours and ingest EV-relevant articles."""
#     from app.services.news.rss import poll_rss_feeds
#
#     try:
#         result = await poll_rss_feeds()
#         log.info("rss_poll.done", **result)
#     except Exception:
#         log.exception("rss_poll.failed")


@scheduler.scheduled_job(
    "cron",
    hour="3,9,15,21",
    minute=0,
    timezone="Asia/Kolkata",
    id="twitter_poll",
    max_instances=1,
    misfire_grace_time=300,
)
async def twitter_poll_job() -> None:
    """Run Apify Twitter handle scrape every 6 hours and ingest EV-relevant tweets."""
    from app.services.news.twitter import poll_twitter

    try:
        result = await poll_twitter()
        log.info("twitter_poll.done", **result)
    except Exception:
        log.exception("twitter_poll.failed")


@scheduler.scheduled_job(
    "cron",
    hour="4,10,16,22",
    minute=0,
    timezone="Asia/Kolkata",
    id="linkedin_news_poll",
    max_instances=1,
    misfire_grace_time=300,
)
async def linkedin_news_poll_job() -> None:
    """Run Apify LinkedIn hashtag search every 6 hours and ingest EV-relevant posts.

    Distinct from linkedin_poll_job (Module 5 — job scraping) above; this one
    feeds the news pipeline (source_type=linkedin_news), not the jobs table.
    """
    from app.services.news.linkedin import poll_linkedin

    try:
        result = await poll_linkedin()
        log.info("linkedin_news_poll.done", **result)
    except Exception:
        log.exception("linkedin_news_poll.failed")


@scheduler.scheduled_job(
    "cron",
    hour=6,
    minute=0,
    timezone="Asia/Kolkata",
    id="process_news_morning",
    max_instances=1,
    misfire_grace_time=300,
)
async def process_news_morning_job() -> None:
    """Process raw news items into LLM summaries before the morning digest."""
    from app.services.news.news_pipeline import process_unprocessed_news

    try:
        result = await process_unprocessed_news()
        log.info("process_news.done", **result)
    except Exception:
        log.exception("process_news.failed")


@scheduler.scheduled_job(
    "cron",
    hour=18,
    minute=0,
    timezone="Asia/Kolkata",
    id="process_news_evening",
    max_instances=1,
    misfire_grace_time=300,
)
async def process_news_evening_job() -> None:
    """Process raw news items into LLM summaries before the evening jobs digest."""
    from app.services.news.news_pipeline import process_unprocessed_news

    try:
        result = await process_unprocessed_news()
        log.info("process_news.done", **result)
    except Exception:
        log.exception("process_news.failed")
