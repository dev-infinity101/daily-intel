from apscheduler.schedulers.asyncio import AsyncIOScheduler

scheduler = AsyncIOScheduler(timezone="Asia/Kolkata")


def start_scheduler() -> None:
    from app.scheduler import tasks  # noqa: F401 — side-effect: registers all jobs

    if not scheduler.running:
        scheduler.start()
