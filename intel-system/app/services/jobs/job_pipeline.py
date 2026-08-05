"""
Central filtering + persistence pipeline for the Jobs module.

Every job source funnels scraped JobIn objects through here.
Only jobs passing the role filter (and EV domain check for public sources)
are written to the DB.

Resilience: each job is inserted inside its own savepoint so a single
constraint violation or schema error never silently drops the whole batch.
"""
from datetime import UTC, datetime

import structlog
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.models.job import Job
from app.models.processed_item import ProcessedItem
from app.schemas.job import JobIn
from app.services.jobs.classifier import (
    evaluate_job_filter,
    extract_skills,
    infer_experience_level,
    score_ev_relevance,
)
from app.services.jobs.normalizer import compute_job_dedup_hash

log = structlog.get_logger()

_TARGET_SOURCES: frozenset[str] = frozenset()


def _is_target_company_source(source_type: str) -> bool:
    """Target company sources are already curated EV companies — only check role."""
    return (
        source_type.startswith("apify_")
        or source_type.startswith("direct_")
        or source_type.startswith("ats_")
        or source_type in _TARGET_SOURCES
    )


async def check_schema(db: AsyncSession) -> dict:
    """Verify the jobs table has all expected columns. Call from /admin/jobs/db-check."""
    try:
        result = await db.execute(
            text("SELECT column_name FROM information_schema.columns WHERE table_name='jobs'")
        )
        columns = [r[0] for r in result.fetchall()]
        required = {"id", "company", "job_title", "dedup_hash", "rank_score", "source_type"}
        missing = required - set(columns)
        return {"columns": columns, "missing": list(missing), "ok": len(missing) == 0}
    except Exception as exc:
        return {"error": str(exc), "ok": False}


async def persist_filtered_jobs(
    jobs: list[JobIn],
    db: AsyncSession,
    target_company_id: int | None = None,
) -> int:
    """Filter, deduplicate, and persist jobs with per-job savepoints.

    target_company_id: when provided (target company scrape path), jobs are
    gated on is_business_role() instead of is_ev_relevant(). Public sources
    (LinkedIn, HN, Remotive, YC) still go through the EV relevance gate.

    Returns count of new rows inserted.
    """
    if not jobs:
        log.info("job_pipeline.empty_input")
        return 0

    log.info("job_pipeline.start", total=len(jobs))

    passed_filter = 0
    skipped_filter = 0
    skipped_dedup = 0
    inserted = 0
    failed = 0

    for idx, job in enumerate(jobs):
        title = job.job_title or ""
        desc = job.description or ""
        source_type = (job.source_type or "unknown")[:64]
        is_target = _is_target_company_source(source_type)

        # ── 1. Filter ────────────────────────────────────────────────────────
        # All sources: India-first EV/Mobility business-role fit.
        filter_result = evaluate_job_filter(
            title,
            desc,
            location=job.location,
            is_target=is_target,
            source_type=source_type,
        )
        passes = filter_result.passed
        reason = filter_result.reason

        if not passes:
            skipped_filter += 1
            log.info(
                "job_pipeline.filtered_out",
                idx=idx,
                title=title[:80],
                company=job.company[:40],
                source=source_type,
                is_target=is_target,
                reason=reason,
                location=job.location,
                location_status=filter_result.location_status,
                domain_status=filter_result.domain_status,
                role_status=filter_result.role_status,
                title_keywords=filter_result.title_keywords[:5],
            )
            continue

        passed_filter += 1
        log.info(
            "job_pipeline.filter_passed",
            title=title[:80],
            company=job.company[:40],
            source=source_type,
            is_target=is_target,
            score=filter_result.score,
            reason=reason,
            domain_status=filter_result.domain_status,
            role_status=filter_result.role_status,
            title_keywords=filter_result.title_keywords[:5],
        )

        # ── 2. Dedup check ───────────────────────────────────────────────────
        dedup_hash = compute_job_dedup_hash(
            job.company, title, job.location, job.job_url
        )
        try:
            existing = (
                await db.execute(select(Job).where(Job.dedup_hash == dedup_hash))
            ).scalar_one_or_none()
        except Exception as exc:
            log.error("job_pipeline.dedup_check_failed", title=title[:60], error=str(exc))
            failed += 1
            continue

        if existing:
            skipped_dedup += 1
            log.debug("job_pipeline.duplicate", title=title[:60], company=job.company[:40])
            # Refresh last_seen_at: signals the job is still active on the company site.
            # This is the only place last_seen_at advances post-insert; the staleness
            # cron uses it to detect vanished jobs.
            try:
                await db.execute(
                    update(Job)
                    .where(Job.id == existing.id)
                    .values(last_seen_at=datetime.now(UTC))
                )
                await db.flush()
            except Exception:
                pass
            continue

        # ── 3. Insert with savepoint (one failure never kills the batch) ─────
        try:
            async with db.begin_nested():
                rank = filter_result.score or score_ev_relevance(title, desc, job.location)

                new_job = Job(
                    raw_item_id=None,
                    company=job.company[:128],
                    target_company_id=target_company_id,
                    job_title=title,
                    location=job.location,
                    remote=job.remote,
                    department=job.department,
                    description=desc[:500] or None,
                    job_url=job.job_url,
                    external_job_id=(job.external_job_id or "")[:128] or None,
                    experience_level=infer_experience_level(title, desc),
                    salary_min=job.salary_min,
                    salary_max=job.salary_max,
                    salary_currency=job.salary_currency,
                    extracted_skills=extract_skills(title, desc),
                    source_type=source_type,
                    posted_at=job.posted_at,
                    dedup_hash=dedup_hash,
                    rank_score=rank,
                )
                db.add(new_job)
                await db.flush()

                log.info(
                    "job_pipeline.job_inserted",
                    company=job.company[:40],
                    title=title[:60],
                    source=source_type,
                    rank=round(rank, 2),
                )

                loc_str = job.location or ("Remote" if job.remote else "Unspecified")
                desc_snippet = desc[:120].rstrip()
                summary_parts = [job.company, title, loc_str]
                if desc_snippet:
                    summary_parts.append(desc_snippet)

                section = (
                    "jobs_target"
                    if source_type.startswith("apify_") or source_type.startswith("direct_")
                    else "jobs_open"
                )

                pi = ProcessedItem(
                    raw_item_id=None,
                    is_relevant=True,
                    relevance_score=rank,
                    summary=" | ".join(summary_parts),
                    section=section,
                    rank_score=rank,
                )
                db.add(pi)
                await db.flush()

            inserted += 1

        except Exception as exc:
            failed += 1
            log.error(
                "job_pipeline.insert_failed",
                company=job.company[:40],
                title=title[:60],
                error=str(exc),
                exc_info=True,
            )

    # ── 4. Commit all successful savepoints ──────────────────────────────────
    if inserted:
        try:
            await db.commit()
            log.info("job_pipeline.committed", inserted=inserted)
        except Exception as exc:
            log.error("job_pipeline.commit_failed", error=str(exc), exc_info=True)
            await db.rollback()
            return 0

    log.info(
        "job_pipeline.complete",
        total=len(jobs),
        passed_filter=passed_filter,
        skipped_filter=skipped_filter,
        skipped_dedup=skipped_dedup,
        inserted=inserted,
        failed=failed,
    )
    return inserted
