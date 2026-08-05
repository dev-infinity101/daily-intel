"""
Jobs-specific digest: query unsent jobs from DB, render HTML, send email in batches.

Email deduplication: jobs are queried by ``emailed_at IS NULL`` so each
job appears in exactly one digest email. After a successful send, all
included jobs have their emailed_at set to now(). If the send fails,
emailed_at is NOT set and the jobs will be retried in the next digest run.

Batching: jobs are split into batches of at most BATCH_SIZE (60) and sent
as separate emails so no single message exceeds a reasonable payload size.
107 jobs → 2 emails (60 + 47).

Separate from the general daily digest so the jobs pipeline can be triggered
and tested in isolation without touching news/telegram/linkedin digest logic.
"""
import asyncio
from datetime import UTC, datetime, timedelta, timezone
from pathlib import Path
from typing import Any

import structlog
from jinja2 import Environment, FileSystemLoader
from sqlalchemy import and_, select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.job import Job
from app.services.email.sender import send_email
from app.services.jobs.classifier import evaluate_job_filter
from app.utils.llm_client import call_llm_with_rate_limit

log = structlog.get_logger()

IST = timezone(timedelta(hours=5, minutes=30))
_TEMPLATE_DIR = Path(__file__).parent.parent / "digest" / "templates"
_jinja = Environment(loader=FileSystemLoader(str(_TEMPLATE_DIR)), autoescape=True)

BATCH_SIZE = 60  # max jobs per email


async def fetch_recent_jobs(
    db: AsyncSession,
    hours: int = 48,
    include_experiment: bool = False,
    only_experiment: bool = False,
    limit: int = 300,
    unsent_only: bool = True,
) -> list[Job]:
    """Return non-closed jobs that qualify for the next digest.

    unsent_only=True (default, production digest):
        Filters emailed_at IS NULL — guarantees each job appears once.
        The ``hours`` parameter acts as a freshness gate: jobs older than
        ``hours`` hours are excluded even if unsent, preventing a digest
        backlog from surfacing very old roles after a long pause (default 48h).

    unsent_only=False (preview / manual inspection):
        Falls back to the old first_seen_at >= cutoff query.

    only_experiment=True  → experiment rows only (overrides include_experiment)
    include_experiment=False (default) → exclude experiment rows
    include_experiment=True → all rows
    """
    cutoff = (datetime.now(IST) - timedelta(hours=hours)).astimezone(UTC)
    filters = [
        Job.is_closed == False,  # noqa: E712
        Job.first_seen_at >= cutoff,
        Job.rank_score > 0,  # exclude zero-score jobs (no EV keyword match)
    ]
    if unsent_only:
        filters.append(Job.emailed_at == None)  # noqa: E711
    if only_experiment:
        filters.append(Job.source_type == "browserbase_experiment")
    elif not include_experiment:
        filters.append(Job.source_type != "browserbase_experiment")
    result = await db.execute(
        select(Job)
        .where(and_(*filters))
        .order_by(Job.rank_score.desc(), Job.first_seen_at.desc())
        .limit(limit * 4)
    )
    candidates = list(result.scalars().all())
    filtered = []
    for job in candidates:
        filter_result = evaluate_job_filter(
            job.job_title,
            job.description or "",
            location=job.location,
            is_target=job.source_type.startswith(("apify_", "direct_", "ats_", "browserbase_")),
            source_type=job.source_type,
        )
        if filter_result.passed:
            # Combine all matched keywords
            kw = set(filter_result.title_keywords + filter_result.description_keywords)
            # Add dynamic attribute to the job object for the template
            job.matched_keywords = ", ".join(sorted(kw))
            filtered.append(job)

    return filtered[:limit]


async def _mark_jobs_emailed(db: AsyncSession, job_ids: list[int]) -> None:
    """Stamp emailed_at on all sent jobs to prevent re-sending."""
    if not job_ids:
        return
    now = datetime.now(UTC)
    await db.execute(
        update(Job).where(Job.id.in_(job_ids)).values(emailed_at=now)
    )
    await db.commit()


def build_jobs_html(
    jobs: list[Job],
    batch_num: int = 1,
    total_batches: int = 1,
) -> str:
    template = _jinja.get_template("jobs.html.j2")
    return template.render(
        jobs=jobs,
        generated_at=datetime.now(IST).strftime("%d %b %Y, %I:%M %p IST"),
        count=len(jobs),
        batch_num=batch_num,
        total_batches=total_batches,
    )


async def _generate_summaries(jobs: list[Job]) -> dict[int, str]:
    from app.config import settings
    if not settings.tensormux_api_key or not jobs:
        return {}
    
    import json

    from openai import AsyncOpenAI
    
    client = AsyncOpenAI(
        api_key=settings.tensormux_api_key,
        base_url="https://api.tensormux.com/v1",
        default_headers={"HTTP-Referer": "https://daily-intel.app", "X-Title": "Daily Intel"},
    )
    
    batch_data = [{"id": j.id, "description": (j.description or "")[:400]} for j in jobs if j.description]
    if not batch_data:
        return {}
        
    prompt = f"""You are an expert HR assistant. Provide a highly concise ONE-LINE summary (max 15-20 words) for each job description.
Focus ONLY on the core responsibility and domain. Remove generic filler.

Input JSON:
{json.dumps(batch_data)}

Return ONLY a JSON object mapping the job "id" (as string) to the one-line "summary" string. No markdown fences, no explanation.
Example: {{"123": "Lead the development of high-power EV charging infrastructure."}}
"""
    try:
        response = await call_llm_with_rate_limit(
            client=client,
            model=settings.tensormux_model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
            max_tokens=3000,
        )
        if not getattr(response, "choices", None):
            log.warning("jobs_digest.empty_choices")
            return {}
        content = response.choices[0].message.content
        if not content:
            return {}
        content = content.strip()
        if content.startswith("```"):
            content = content.split("\n", 1)[1].rsplit("```", 1)[0].strip()
        if content.startswith("json"):
            content = content[4:].strip()
        result = json.loads(content)
        safe_result = {}
        for k, v in result.items():
            if isinstance(v, str):
                try:
                    safe_result[int(k)] = v
                except ValueError:
                    pass
        return safe_result
    except Exception as exc:
        log.error("jobs_digest.llm_summary_failed", error=str(exc))
        return {}


async def send_jobs_digest(
    hours: int = 48,
    include_experiment: bool = False,
    only_experiment: bool = False,
    force: bool = False,
) -> dict[str, Any]:
    """Fetch unsent jobs, split into batches of BATCH_SIZE, send each as an email.

    Jobs are split into batches of at most BATCH_SIZE (60). Each batch is a
    separate email. emailed_at is stamped per batch immediately after that
    batch sends successfully, so a mid-run failure doesn't re-send earlier batches.

    force=True bypasses the emailed_at IS NULL filter (for re-sends / testing).
    force=True does NOT re-stamp emailed_at.

    Returns:
      {"status": "sent",         "count": N, "batches": B, "provider_ids": [...]}
      {"status": "already_sent", "count": 0, "already_sent_count": N, ...}
      {"status": "no_jobs",      "count": 0}
      {"status": "error",        "count": N_sent_so_far, "batches_sent": B, "error": "..."}
    """
    from app.database import SessionLocal
    
    async with SessionLocal() as db:
        jobs = await fetch_recent_jobs(
            db,
            hours=hours,
            include_experiment=include_experiment,
            only_experiment=only_experiment,
            unsent_only=not force,
        )

        if not jobs and not force:
            all_jobs = await fetch_recent_jobs(
                db, hours=hours, include_experiment=include_experiment,
                only_experiment=only_experiment, unsent_only=False,
            )
            if all_jobs:
                last_sent = max((j.emailed_at for j in all_jobs if j.emailed_at), default=None)
                log.info("jobs_digest.all_already_sent", count=len(all_jobs), last_sent=str(last_sent))
                return {
                    "status": "already_sent",
                    "count": 0,
                    "already_sent_count": len(all_jobs),
                    "last_sent_at": str(last_sent) if last_sent else None,
                    "tip": "Use ?force=true to resend, or wait for new jobs to be scraped.",
                }
            log.info("jobs_digest.empty", hours=hours)
            return {"status": "no_jobs", "count": 0}

    # ── Split into batches ────────────────────────────────────────────────────
    batches = [jobs[i:i + BATCH_SIZE] for i in range(0, len(jobs), BATCH_SIZE)]
    total_batches = len(batches)
    total_jobs = len(jobs)
    provider_ids: list[str | None] = []
    sent_count = 0

    log.info(
        "jobs_digest.batching",
        total_jobs=total_jobs,
        total_batches=total_batches,
        batch_size=BATCH_SIZE,
    )

    for batch_num, batch_jobs in enumerate(batches, 1):
        n = len(batch_jobs)
        if total_batches > 1:
            subject = (
                f"EV Business Jobs — Batch {batch_num}/{total_batches} — "
                f"{n} role{'s' if n != 1 else ''}"
            )
        else:
            subject = f"EV Business Jobs — {n} new role{'s' if n != 1 else ''}"

        # Generate LLM one-line summaries for the batch
        summaries = await _generate_summaries(batch_jobs)
        for job in batch_jobs:
            job.ai_summary = summaries.get(job.id)

        html = build_jobs_html(batch_jobs, batch_num=batch_num, total_batches=total_batches)

        try:
            provider_id = await send_email(subject, html)
            provider_ids.append(provider_id)
            log.info(
                "jobs_digest.batch_sent",
                batch=batch_num,
                total_batches=total_batches,
                count=n,
                provider_id=provider_id,
            )
        except Exception as exc:
            log.error("jobs_digest.batch_failed", batch=batch_num, error=str(exc))
            return {
                "status": "error",
                "count": sent_count,
                "batches_sent": batch_num - 1,
                "total_batches": total_batches,
                "error": str(exc),
            }

        # Update DB emailed_at immediately for this batch
        if not force and batch_jobs:
            async with SessionLocal() as db_update:
                await _mark_jobs_emailed(db_update, [j.id for j in batch_jobs])

        sent_count += n

        # Brief pause between batches to respect Resend rate limits
        if batch_num < total_batches:
            await asyncio.sleep(2)

    log.info(
        "jobs_digest.done",
        total_jobs=total_jobs,
        total_batches=total_batches,
        force=force,
    )
    return {
        "status": "sent",
        "count": total_jobs,
        "batches": total_batches,
        "provider_ids": provider_ids,
    }
