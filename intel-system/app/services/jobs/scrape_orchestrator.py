"""V3 scrape orchestrator — Apify T1 → Browserbase T2 → watchlist T3.

Replaces poll_all_target_companies() as the central entry point for target
company scraping. Implements the full V3 spec:

  • Two-tier cascade: T1 Apify (primary) → T2 Browserbase (SPA fallback)
  • T3 watchlist: companies with ≥3 consecutive failures are skipped
  • Batching: 10 companies/batch, T1 strictly sequential (free-tier: 1 run)
  • Cooldown: 60–90s between batches (Apify rate-window relief)
  • Inter-company delay: 15–25s between individual T1 runs
  • Success TTL: skip companies already scraped within 18 h
  • Budget guard: monthly Apify CU + Browserbase session soft limits
  • Quota stop: on ApifyQuotaError, all remaining T1 companies are skipped
  • Adaptive routing: pin known-good tier after first success; auto-watchlist
  • scrape_attempts logging: one row per company/tier/run for metrics
  • Run lock: asyncio.Lock prevents concurrent scrape sessions (scheduler +
    manual /admin/jobs/scrape-now racing each other)
"""
import asyncio
import random
import time
import uuid
from datetime import UTC, datetime, timedelta

import structlog

from app.config import settings
from app.schemas.job import JobIn

log = structlog.get_logger()

# ── Run-level lock (prevents scheduler + manual call racing) ──────────────────
_scrape_lock = asyncio.Lock()

# -----------------------------

# ── Outcome codes ─────────────────────────────────────────────────────────────

OUTCOME_SUCCESS        = "success"
OUTCOME_NO_RELEVANT    = "no_relevant_jobs"   # correct result, terminal — never escalate
OUTCOME_DETERMINISTIC  = "deterministic"      # empty/method-failure — escalate to T2 if SPA
OUTCOME_TRANSIENT      = "transient"          # network/rate — retry T1 or skip gracefully
OUTCOME_TERMINAL       = "terminal"           # no URL, 404, etc. — stop
OUTCOME_SKIPPED        = "skipped"            # TTL / budget guard
OUTCOME_QUOTA_EXCEEDED = "quota_exceeded"     # Apify 402/429 — stop all T1 for this run

# ── Tier codes ────────────────────────────────────────────────────────────────

TIER_T1 = "t1"
TIER_T2 = "t2"
TIER_T3 = "t3"

# ── Tuning constants ──────────────────────────────────────────────────────────

BATCH_SIZE              = 10
SUCCESS_TTL_HOURS       = 18
AUTO_WATCHLIST_FAILURES = 3    # consecutive_failures ≥ this → T3
COOLDOWN_MIN_S          = 25   # batch cooldown (was 60)
COOLDOWN_MAX_S          = 40   # batch cooldown (was 90)
T1_INTER_COMPANY_S      = 3    # inter-company delay (was 5)
T2_JITTER_MIN_S         = 3
T2_JITTER_MAX_S         = 5


# ── Routing helpers ───────────────────────────────────────────────────────────

def _is_within_ttl(last_success_at: datetime | None) -> bool:
    """Return True when a company was already successfully scraped within TTL."""
    if not last_success_at:
        return False
    cutoff = datetime.now(UTC) - timedelta(hours=SUCCESS_TTL_HOURS)
    return last_success_at > cutoff


def _resolve_route(company) -> str:  # type: ignore[no-untyped-def]
    """Determine starting tier for this company.

    Priority order:
      1. consecutive_failures ≥ threshold → T3 watchlist (stop spending)
      2. preferred_scraper = 'browserbase' → skip T1, go T2
      3. default → T1
    """
    failures = company.consecutive_failures or 0
    if failures >= AUTO_WATCHLIST_FAILURES:
        return TIER_T3
    if company.preferred_scraper == "browserbase":
        return TIER_T2
    return TIER_T1


def _classify_t1_outcome(
    jobs: list[JobIn],
    error: Exception | None,
    jobs_inserted: int,
) -> str:
    """Map a T1 Apify result to an outcome code.

    Critical distinction: NO_RELEVANT_JOBS means the page was parsed cleanly
    and had jobs — just none were EV-relevant. This is a TERMINAL outcome;
    do NOT escalate to Browserbase (expensive, already-correct result).
    """
    if error is None:
        if jobs_inserted > 0:
            return OUTCOME_SUCCESS
        if jobs:
            # Jobs were found but filtered out — correct behaviour, not a failure
            return OUTCOME_NO_RELEVANT
        # Empty dataset: Apify couldn't render the page — likely SPA
        return OUTCOME_DETERMINISTIC

    # Error path
    err_str = str(error).lower()
    if any(k in err_str for k in ("timeout", "429", "timed-out", "aborted", "connection")):
        return OUTCOME_TRANSIENT
    return OUTCOME_DETERMINISTIC


def _should_escalate_to_t2(outcome: str) -> bool:
    """T2 escalation: only on method-deterministic failures (empty/unrendered page).

    Never escalate NO_RELEVANT_JOBS — the page was correct, just empty of EV roles.
    """
    return outcome == OUTCOME_DETERMINISTIC


# ── DB helpers ────────────────────────────────────────────────────────────────

async def _record_attempt(
    db,
    run_id: str,
    company_slug: str,
    tier: str,
    outcome: str,
    jobs_found: int = 0,
    jobs_inserted: int = 0,
    duration_ms: int = 0,
    error_class: str | None = None,
    session_id: str | None = None,
) -> None:
    from app.models.scrape_attempt import ScrapeAttempt
    try:
        attempt = ScrapeAttempt(
            run_id=run_id,
            company_slug=company_slug,
            tier=tier,
            outcome=outcome,
            jobs_found=jobs_found,
            jobs_inserted=jobs_inserted,
            duration_ms=duration_ms,
            error_class=error_class,
            session_id=session_id,
        )
        db.add(attempt)
        await db.flush()
    except Exception as exc:
        log.warning("orchestrator.record_attempt_failed", slug=company_slug, error=str(exc))
        # Rollback so the session isn't left in PendingRollback state for the
        # next operation (_update_routing) that shares this same session.
        try:
            await db.rollback()
        except Exception:
            pass


async def _update_routing(db, company, tier: str, outcome: str, jobs_inserted: int) -> None:
    """Update preferred_scraper, last_success_at, and consecutive_failures."""
    from sqlalchemy import update

    from app.models.target_company import TargetCompany

    try:
        if outcome == OUTCOME_SUCCESS:
            await db.execute(
                update(TargetCompany)
                .where(TargetCompany.id == company.id)
                .values(
                    preferred_scraper=tier,           # pin winning tier
                    last_success_at=datetime.now(UTC),
                    consecutive_failures=0,
                )
            )
        elif outcome in (OUTCOME_DETERMINISTIC, OUTCOME_TRANSIENT, OUTCOME_TERMINAL):
            # Only genuine failures increment the counter; NO_RELEVANT_JOBS does not
            new_failures = (company.consecutive_failures or 0) + 1
            await db.execute(
                update(TargetCompany)
                .where(TargetCompany.id == company.id)
                .values(consecutive_failures=new_failures)
            )
        await db.flush()
    except Exception as exc:
        log.warning("orchestrator.update_routing_failed", slug=company.slug, error=str(exc))
        try:
            await db.rollback()
        except Exception:
            pass


async def _ingest_jobs(
    jobs: list[JobIn],
    company_slug: str,
    target_company_id: int | None,
) -> int:
    """Persist jobs through the EV pipeline. Returns rows inserted."""
    if not jobs:
        return 0
    from app.database import SessionLocal
    from app.services.jobs.job_pipeline import persist_filtered_jobs

    db = SessionLocal()
    try:
        return await persist_filtered_jobs(jobs, db, target_company_id=target_company_id)
    except Exception:
        log.exception("orchestrator.ingest_failed", company=company_slug)
        return 0
    finally:
        await db.close()


async def _get_monthly_t1_count(db) -> int:
    from sqlalchemy import text
    try:
        r = await db.execute(text(
            "SELECT COUNT(*) FROM scrape_attempts "
            "WHERE tier = 't1' AND created_at >= date_trunc('month', now())"
        ))
        return r.scalar() or 0
    except Exception:
        return 0


async def _get_monthly_t2_count(db) -> int:
    from sqlalchemy import text
    try:
        r = await db.execute(text(
            "SELECT COUNT(*) FROM scrape_attempts "
            "WHERE tier = 't2' AND created_at >= date_trunc('month', now())"
        ))
        return r.scalar() or 0
    except Exception:
        return 0


# ── Per-company tier runners ──────────────────────────────────────────────────

async def _run_t1_single(company, run_id: str) -> dict:
    """Run T1 Apify for one company. Returns a result dict.

    Returns OUTCOME_QUOTA_EXCEEDED when Apify signals a concurrent-run or
    rate limit (402/429). The outer loop stops all further T1 runs immediately
    on this outcome.
    """
    from app.database import SessionLocal
    from app.services.jobs.apify_adapter import ApifyQuotaError, run_t1_apify

    t_start = time.monotonic()
    career_urls: list[str] = company.career_urls or []

    if not career_urls:
        db = SessionLocal()
        try:
            await _record_attempt(db, run_id, company.slug, TIER_T1, OUTCOME_TERMINAL)
            await db.commit()
        finally:
            await db.close()
        return {
            "slug": company.slug, "display_name": company.display_name,
            "tier": TIER_T1, "outcome": OUTCOME_TERMINAL,
            "jobs_found": 0, "jobs_inserted": 0, "is_spa": False,
        }

    jobs: list[JobIn] = []
    error: Exception | None = None

    try:
        jobs = await run_t1_apify(company)
    except ApifyQuotaError as exc:
        # Quota/rate-limit — propagate immediately; orchestrator will stop T1
        duration_ms = int((time.monotonic() - t_start) * 1000)
        log.warning("orchestrator.t1_quota", company=company.slug, error=str(exc))
        db = SessionLocal()
        try:
            await _record_attempt(
                db, run_id, company.slug, TIER_T1, OUTCOME_QUOTA_EXCEEDED,
                duration_ms=duration_ms, error_class="ApifyQuotaError",
            )
            await _update_routing(db, company, TIER_T1, OUTCOME_TRANSIENT, 0)
            await db.commit()
        except Exception:
            await db.rollback()
        finally:
            await db.close()
        return {
            "slug": company.slug, "display_name": company.display_name,
            "tier": TIER_T1, "outcome": OUTCOME_QUOTA_EXCEEDED,
            "jobs_found": 0, "jobs_inserted": 0, "is_spa": False,
        }
    except Exception as exc:
        error = exc
        log.warning("orchestrator.t1_error", company=company.slug, error=str(exc))

    duration_ms = int((time.monotonic() - t_start) * 1000)

    jobs_inserted = await _ingest_jobs(jobs, company.slug, company.id)
    outcome = _classify_t1_outcome(jobs, error, jobs_inserted)

    # Empty Apify result → assume SPA (bootstraps adaptive routing on first run)
    is_spa = (not jobs)

    db = SessionLocal()
    try:
        await _record_attempt(
            db, run_id, company.slug, TIER_T1, outcome,
            jobs_found=len(jobs), jobs_inserted=jobs_inserted,
            duration_ms=duration_ms,
            error_class=type(error).__name__ if error else None,
        )
        await _update_routing(db, company, TIER_T1, outcome, jobs_inserted)
        await db.commit()
    except Exception:
        log.exception("orchestrator.t1_db_error", company=company.slug)
        await db.rollback()
    finally:
        await db.close()

    log.info(
        "orchestrator.t1_done",
        company=company.slug,
        outcome=outcome,
        jobs_found=len(jobs),
        jobs_inserted=jobs_inserted,
        duration_ms=duration_ms,
    )
    return {
        "slug": company.slug, "display_name": company.display_name,
        "tier": TIER_T1, "outcome": outcome,
        "jobs_found": len(jobs), "jobs_inserted": jobs_inserted,
        "is_spa": is_spa,
    }


async def _run_t2_single(company, run_id: str) -> dict:
    """Run T2 Browserbase for one company. Returns a result dict.

    Sessions are strictly sequential at the call site (orchestrator guarantees
    concurrency = 1 for T2).
    """
    from app.database import SessionLocal
    from app.services.jobs.browserbase_adapter import fetch_via_browserbase

    career_urls: list[str] = company.career_urls or []
    if not career_urls:
        return {
            "slug": company.slug, "display_name": company.display_name,
            "tier": TIER_T2, "outcome": OUTCOME_TERMINAL,
            "jobs_found": 0, "jobs_inserted": 0, "session_id": None,
        }

    t_start = time.monotonic()
    primary_url = career_urls[0]
    session_id: str | None = None

    try:
        jobs, session_id = await fetch_via_browserbase(primary_url, company.slug)
    except Exception as exc:
        duration_ms = int((time.monotonic() - t_start) * 1000)
        log.warning("orchestrator.t2_error", company=company.slug, error=str(exc))
        db = SessionLocal()
        try:
            await _record_attempt(
                db, run_id, company.slug, TIER_T2, OUTCOME_TRANSIENT,
                duration_ms=duration_ms, error_class=type(exc).__name__,
            )
            await _update_routing(db, company, TIER_T2, OUTCOME_TRANSIENT, 0)
            await db.commit()
        finally:
            await db.close()
        return {
            "slug": company.slug, "display_name": company.display_name,
            "tier": TIER_T2, "outcome": OUTCOME_TRANSIENT,
            "jobs_found": 0, "jobs_inserted": 0, "session_id": None,
        }

    duration_ms = int((time.monotonic() - t_start) * 1000)
    jobs_inserted = await _ingest_jobs(jobs, company.slug, company.id)

    if jobs_inserted > 0:
        outcome = OUTCOME_SUCCESS
    elif jobs:
        outcome = OUTCOME_NO_RELEVANT
    else:
        outcome = OUTCOME_DETERMINISTIC

    db = SessionLocal()
    try:
        await _record_attempt(
            db, run_id, company.slug, TIER_T2, outcome,
            jobs_found=len(jobs), jobs_inserted=jobs_inserted,
            duration_ms=duration_ms, session_id=session_id,
        )
        await _update_routing(db, company, TIER_T2, outcome, jobs_inserted)
        await db.commit()
    except Exception:
        log.exception("orchestrator.t2_db_error", company=company.slug)
        await db.rollback()
    finally:
        await db.close()

    log.info(
        "orchestrator.t2_done",
        company=company.slug,
        outcome=outcome,
        jobs_found=len(jobs),
        jobs_inserted=jobs_inserted,
        duration_ms=duration_ms,
        session_id=session_id,
    )
    return {
        "slug": company.slug, "display_name": company.display_name,
        "tier": TIER_T2, "outcome": outcome,
        "jobs_found": len(jobs), "jobs_inserted": jobs_inserted,
        "session_id": session_id,
    }


# ── Failed.md writer ──────────────────────────────────────────────────────────

_FAILED_MD = (
    __import__("pathlib").Path(__file__).resolve().parent.parent.parent.parent / "Failed.md"
)


def _write_failed_md(
    run_results: list[dict],
    all_companies: list,          # TargetCompany ORM objects — provides career_urls
    ttl_skipped: list,
    watchlist_slugs: list[str],
    run_id: str,
) -> None:
    """Write Failed.md listing every company that produced zero inserted jobs.

    Categories:
      FILTER BLOCKED   — Apify/BB found jobs but EV filter rejected all.
                         These are the most actionable: fix is_ev_relevant().
      SPA / EMPTY      — Apify returned zero items (page didn't render).
                         Fix: Browserbase, or update career URL.
      NETWORK FAILURE  — Transient timeout / rate-limit.
                         Fix: usually self-resolves; monitor consecutive_failures.
      TERMINAL         — No career URL configured, or 404.
                         Fix: update career URL in DB.
      WATCHLISTED      — ≥3 consecutive failures; skipped this run.
                         Fix: repair URL, then reset consecutive_failures in DB.
    """
    from datetime import datetime

    now = datetime.now(UTC).strftime("%Y-%m-%d %H:%M UTC")

    # Build slug → career_urls map from the in-memory company list
    url_map: dict[str, list[str]] = {
        c.slug: list(c.career_urls or []) for c in all_companies
    }
    # Also include ttl_skipped companies in the map
    for c in ttl_skipped:
        if c.slug not in url_map:
            url_map[c.slug] = list(c.career_urls or [])

    # Categorise run_results
    filter_blocked: list[dict] = []
    spa_empty:      list[dict] = []
    network_fail:   list[dict] = []
    terminal:       list[dict] = []

    for r in run_results:
        if r["outcome"] == OUTCOME_NO_RELEVANT:
            filter_blocked.append(r)
        elif r["outcome"] == OUTCOME_DETERMINISTIC:
            spa_empty.append(r)
        elif r["outcome"] == OUTCOME_TRANSIENT:
            network_fail.append(r)
        elif r["outcome"] == OUTCOME_TERMINAL:
            terminal.append(r)
        # OUTCOME_SUCCESS and OUTCOME_SKIPPED are not failures

    # Watchlisted companies (T3) pulled from run_results
    watchlisted_rows = [r for r in run_results if r.get("tier") == TIER_T3]

    lines: list[str] = [
        "# Failed Companies Report",
        "",
        f"**Run ID:** `{run_id}`  |  **Generated:** {now}",
        "",
        "Companies that produced **zero inserted jobs** in this scrape run.",
        "Use this file to prioritise filtering and URL fixes.",
        "",
        "| Category | Count |",
        "|----------|-------|",
        f"| 🔴 Filter Blocked (jobs found, EV filter rejected all) | {len(filter_blocked)} |",
        f"| 🟠 SPA / Empty Dataset (page didn't render) | {len(spa_empty)} |",
        f"| 🟡 Network / Rate-limit Failure | {len(network_fail)} |",
        f"| ⚫ Terminal (bad URL / 404) | {len(terminal)} |",
        f"| 🔵 Watchlisted (≥3 consecutive failures, skipped) | {len(watchlisted_rows)} |",
        "",
    ]

    def _url_cell(slug: str) -> str:
        urls = url_map.get(slug, [])
        if not urls:
            return "*(no URL)*"
        return " <br> ".join(f"[link]({u})" for u in urls[:2])

    def _company_table(rows: list[dict], show_jobs_found: bool = False) -> list[str]:
        if not rows:
            return ["*None*", ""]
        header = "| Company | Slug | Jobs Found | Career URLs |"
        sep    = "|---------|------|-----------|------------|"
        if not show_jobs_found:
            header = "| Company | Slug | Career URLs |"
            sep    = "|---------|------|------------|"
        out = [header, sep]
        for r in sorted(rows, key=lambda x: x.get("display_name", "")):
            name = r.get("display_name", r.get("slug", "—"))
            slug = r.get("slug", "—")
            urls = _url_cell(slug)
            if show_jobs_found:
                jf = r.get("jobs_found", 0)
                out.append(f"| {name} | `{slug}` | {jf} | {urls} |")
            else:
                out.append(f"| {name} | `{slug}` | {urls} |")
        return out + [""]

    # ── Section 1: Filter blocked ─────────────────────────────────────────────
    lines += [
        "---",
        "",
        "## 🔴 Filter Blocked — Jobs Found but EV Filter Rejected All",
        "",
        "> Apify or Browserbase extracted jobs from the page but **`is_ev_relevant()` rejected every one**.",
        "> These companies likely have EV-relevant roles not matching current keyword/role patterns.",
        "> **Fix:** Review job titles from these companies and expand `_EV_DOMAIN_PATTERNS` or `_BUSINESS_ROLES` in `classifier.py`.",
        "",
    ]
    lines += _company_table(filter_blocked, show_jobs_found=True)

    # ── Section 2: SPA / empty ────────────────────────────────────────────────
    lines += [
        "---",
        "",
        "## 🟠 SPA / Empty Dataset — Page Didn't Render",
        "",
        "> Apify returned zero items — the career page is a JavaScript SPA that Apify's",
        "> `website-content-crawler` couldn't render. These escalate to Browserbase (T2) automatically,",
        "> but if T2 also fails, consider updating the career URL or switching actor.",
        "> **Fix:** Verify career URL works in a browser; check if Browserbase session ran.",
        "",
    ]
    lines += _company_table(spa_empty)

    # ── Section 3: Network failures ───────────────────────────────────────────
    lines += [
        "---",
        "",
        "## 🟡 Network / Rate-limit Failures",
        "",
        "> Transient errors: timeouts, 429, connection resets. `consecutive_failures` was incremented.",
        "> These typically self-resolve on the next run.",
        "",
    ]
    lines += _company_table(network_fail)

    # ── Section 4: Terminal ───────────────────────────────────────────────────
    lines += [
        "---",
        "",
        "## ⚫ Terminal Failures — Bad URL or No URL Configured",
        "",
        "> No `career_urls` set in DB, or the URL returned a 404/5xx.",
        "> **Fix:** Update `career_urls` in the `target_companies` table.",
        "",
    ]
    lines += _company_table(terminal)

    # ── Section 5: Watchlist ──────────────────────────────────────────────────
    lines += [
        "---",
        "",
        "## 🔵 Watchlisted — Skipped (≥3 Consecutive Failures)",
        "",
        "> These companies were **not attempted** this run (T3 skip).",
        "> `consecutive_failures` was **reset to 0** at end of this run — they will be retried next cycle.",
        "> **Fix:** Repair the career URL or resolve the rendering issue, then monitor next run.",
        "",
    ]
    lines += _company_table(watchlisted_rows)

    # ── Footer ────────────────────────────────────────────────────────────────
    total_ttl = len(ttl_skipped)
    lines += [
        "---",
        "",
        f"*TTL-skipped (successfully scraped within last {SUCCESS_TTL_HOURS}h, not failures): {total_ttl} companies*",
        "",
    ]

    content = "\n".join(lines) + "\n"
    try:
        _FAILED_MD.write_text(content, encoding="utf-8")
        log.info("orchestrator.failed_md_written", path=str(_FAILED_MD),
                 filter_blocked=len(filter_blocked), spa_empty=len(spa_empty),
                 watchlisted=len(watchlisted_rows))
    except Exception as exc:
        log.warning("orchestrator.failed_md_error", error=str(exc))


# ── Main orchestration entry point ────────────────────────────────────────────

async def orchestrate_scrape(slug: str | None = None, is_manual: bool = False) -> dict:
    """V3 main entry point. Called by the scheduler and /admin/jobs/scrape-now.

    Runs the full T1 → T2 → T3 cascade with batching, cooldown, budget guards,
    adaptive routing, and per-attempt DB logging.

    Protected by _scrape_lock: if a scrape is already in progress (e.g. the
    03:00 scheduler cron is running and /admin/jobs/scrape-now is also called),
    the second caller returns immediately with status='already_running' instead
    of triggering a second concurrent Apify actor (which would cause 402).
    """
    if _scrape_lock.locked():
        log.warning("orchestrator.already_running", slug=slug or "all")
        return {
            "run_id": "busy",
            "status": "already_running",
            "companies_scraped": 0,
            "companies_with_jobs": 0,
            "companies_ttl_skipped": 0,
            "companies_watchlist": 0,
            "jobs_fetched": 0,
            "jobs_inserted": 0,
            "success_rate_pct": 0.0,
            "duration_ms": 0,
            "watchlist": [],
            "results": [],
        }

    async with _scrape_lock:
        from sqlalchemy import select

        from app.database import SessionLocal
        from app.models.target_company import TargetCompany

        run_id = str(uuid.uuid4())[:8]
        run_start = time.monotonic()
        log.info("orchestrator.run_start", run_id=run_id, slug=slug or "all")

        # ── Load companies ────────────────────────────────────────────────────
        db = SessionLocal()
        try:
            stmt = (
                select(TargetCompany)
                .where(TargetCompany.is_active == True)  # noqa: E712
            )
            if slug:
                stmt = stmt.where(TargetCompany.slug == slug)
            stmt = stmt.order_by(
                TargetCompany.priority.desc(),
                TargetCompany.last_success_at.asc().nullsfirst(),
            )
            result = await db.execute(stmt)
            all_companies = list(result.scalars().all())
        finally:
            await db.close()

        # ── TTL skip (full runs only) ─────────────────────────────────────────
        if not slug:
            ttl_skipped = [c for c in all_companies if _is_within_ttl(c.last_success_at)]
            work_queue  = [c for c in all_companies if not _is_within_ttl(c.last_success_at)]
        else:
            ttl_skipped = []
            work_queue  = all_companies

        # ── Monthly budget check ──────────────────────────────────────────────
        db = SessionLocal()
        try:
            monthly_t1 = await _get_monthly_t1_count(db)
            monthly_t2 = await _get_monthly_t2_count(db)
        finally:
            await db.close()

        t1_soft_limit = int(settings.apify_monthly_cu_limit * 0.8)
        t2_soft_limit = int(settings.browserbase_monthly_minutes_limit * 0.8)
        t1_budget_ok  = monthly_t1 < t1_soft_limit
        t2_budget_ok  = monthly_t2 < t2_soft_limit

        log.info(
            "orchestrator.budget",
            monthly_t1=monthly_t1, t1_limit=t1_soft_limit, t1_ok=t1_budget_ok,
            monthly_t2=monthly_t2, t2_limit=t2_soft_limit, t2_ok=t2_budget_ok,
        )

        # ── Batch loop ────────────────────────────────────────────────────────
        batches     = [work_queue[i:i + BATCH_SIZE] for i in range(0, len(work_queue), BATCH_SIZE)]


        run_results: list[dict] = []
        watchlist:   list[str]  = []
        t1_quota_hit: bool = False

        log.info(
            "orchestrator.batches",
            total_companies=len(work_queue),
            ttl_skipped=len(ttl_skipped),
            batches=len(batches),
        )

        for batch_idx, batch in enumerate(batches):
            log.info("orchestrator.batch_start", batch=batch_idx + 1, of=len(batches), size=len(batch))

            t1_candidates: list = []
            t2_candidates: list = []   # pinned Browserbase companies (skip T1)

            for company in batch:
                route = _resolve_route(company)
                if route == TIER_T3:
                    watchlist.append(company.slug)
                    run_results.append({
                        "slug": company.slug, "display_name": company.display_name,
                        "tier": TIER_T3, "outcome": OUTCOME_TERMINAL,
                        "jobs_found": 0, "jobs_inserted": 0,
                    })
                    log.info(
                        "orchestrator.watchlist",
                        company=company.slug,
                        consecutive_failures=company.consecutive_failures,
                    )
                elif route == TIER_T2:
                    t2_candidates.append(company)
                else:
                    t1_candidates.append(company)

            # ── T1: Apify — strictly sequential, one run at a time ───────────
            t2_escalations: list = []

            if t1_candidates:
                if not t1_budget_ok:
                    log.warning("orchestrator.t1_budget_exhausted", deferred=len(t1_candidates))
                    for company in t1_candidates:
                        run_results.append({
                            "slug": company.slug, "display_name": company.display_name,
                            "tier": TIER_T1, "outcome": OUTCOME_SKIPPED,
                            "jobs_found": 0, "jobs_inserted": 0,
                        })
                else:
                    for idx, company in enumerate(t1_candidates):
                        if t1_quota_hit:
                            # Quota already hit — skip remaining without touching Apify
                            run_results.append({
                                "slug": company.slug, "display_name": company.display_name,
                                "tier": TIER_T1, "outcome": OUTCOME_SKIPPED,
                                "jobs_found": 0, "jobs_inserted": 0,
                            })
                            continue

                        res = await _run_t1_single(company, run_id)
                        run_results.append(res)

                        if res["outcome"] == OUTCOME_QUOTA_EXCEEDED:
                            # Stop all further T1 in this run; do not escalate to T2
                            t1_quota_hit = True
                            log.warning(
                                "orchestrator.t1_quota_stop",
                                company=company.slug,
                                remaining=len(t1_candidates) - idx - 1,
                            )
                            continue

                        if (
                            _should_escalate_to_t2(res["outcome"])
                            and res.get("is_spa", True)
                            and t2_budget_ok
                        ):
                            t2_escalations.append(company)

                        # Fixed inter-company delay between consecutive Apify runs
                        if idx < len(t1_candidates) - 1:
                            await asyncio.sleep(T1_INTER_COMPANY_S)

            # ── T2: Browserbase (strictly sequential, concurrency = 1) ───────
            all_t2 = t2_candidates + t2_escalations

            if all_t2:
                if not t2_budget_ok:
                    log.warning("orchestrator.t2_budget_exhausted", deferred=len(all_t2))
                    for company in all_t2:
                        watchlist.append(company.slug)
                        run_results.append({
                            "slug": company.slug, "display_name": company.display_name,
                            "tier": TIER_T3, "outcome": OUTCOME_SKIPPED,
                            "jobs_found": 0, "jobs_inserted": 0,
                        })
                else:
                    for company in all_t2:
                        res = await _run_t2_single(company, run_id)
                        run_results.append(res)
                        await asyncio.sleep(random.uniform(T2_JITTER_MIN_S, T2_JITTER_MAX_S))

            log.info("orchestrator.batch_done", batch=batch_idx + 1, results_so_far=len(run_results))

            # Cooldown between batches (skip after the last one)
            if batch_idx < len(batches) - 1:
                cooldown = random.uniform(COOLDOWN_MIN_S, COOLDOWN_MAX_S)
                log.info("orchestrator.cooldown", seconds=round(cooldown, 1), next_batch=batch_idx + 2)
                await asyncio.sleep(cooldown)

        # ── Build and print summary ───────────────────────────────────────────
        total_duration_ms = int((time.monotonic() - run_start) * 1000)
        successful    = [r for r in run_results if r["outcome"] == OUTCOME_SUCCESS]
        jobs_fetched  = sum(r.get("jobs_found", 0) for r in run_results)
        jobs_inserted = sum(r.get("jobs_inserted", 0) for r in run_results)

        sep = "=" * 65
        print(f"\n{sep}")
        print(f"  V3 SCRAPE RUN  --  run_id={run_id}")
        print(sep)
        print(f"  Companies processed  : {len(run_results)}")
        print(f"  Successful           : {len(successful)}")
        print(f"  TTL-skipped          : {len(ttl_skipped)}")
        print(f"  Watchlist (T3)       : {len(watchlist)}")
        print(f"  Jobs extracted       : {jobs_fetched}")
        print(f"  Jobs inserted        : {jobs_inserted}")
        print(f"  Duration             : {total_duration_ms / 1000:.1f}s")
        if watchlist:
            print(f"\n  WATCHLIST: {', '.join(watchlist[:10])}")
        print(f"{sep}\n")

        log.info(
            "orchestrator.run_complete",
            run_id=run_id,
            total=len(run_results),
            successful=len(successful),
            ttl_skipped=len(ttl_skipped),
            watchlist=len(watchlist),
            jobs_fetched=jobs_fetched,
            jobs_inserted=jobs_inserted,
            duration_ms=total_duration_ms,
        )

        # ── Write Failed.md (non-blocking — failure must not abort the return) ─
        try:
            _write_failed_md(
                run_results=run_results,
                all_companies=all_companies,
                ttl_skipped=ttl_skipped,
                watchlist_slugs=watchlist,
                run_id=run_id,
            )
        except Exception as exc:
            log.warning("orchestrator.failed_md_exception", error=str(exc))

        # ── Reset consecutive_failures for watchlisted companies ──────────────
        # Gives them a fresh retry next run instead of staying permanently blocked.
        if watchlist:
            from sqlalchemy import update as _update

            from app.models.target_company import TargetCompany
            db = SessionLocal()
            try:
                await db.execute(
                    _update(TargetCompany)
                    .where(TargetCompany.slug.in_(watchlist))
                    .values(consecutive_failures=0)
                )
                await db.commit()
                log.info("orchestrator.watchlist_reset", count=len(watchlist), slugs=watchlist[:5])
            except Exception as exc:
                log.warning("orchestrator.watchlist_reset_failed", error=str(exc))
                await db.rollback()
            finally:
                await db.close()

        return {
            "run_id": run_id,
            "companies_scraped": len(run_results),
            "companies_with_jobs": len(successful),
            "companies_ttl_skipped": len(ttl_skipped),
            "companies_watchlist": len(watchlist),
            "jobs_fetched": jobs_fetched,
            "jobs_inserted": jobs_inserted,
            "success_rate_pct": (
                round(len(successful) / len(run_results) * 100, 1) if run_results else 0.0
            ),
            "duration_ms": total_duration_ms,
            "watchlist": watchlist,
            "results": run_results,
        }


# ── V4 orchestration: LinkedIn Jobs Apify → Adzuna → Gap → T1/T2 ─────────────

async def orchestrate_scrape_v4(
    slug: str | None = None,
    is_manual: bool = False,
) -> dict:
    """V4 entry point: broad scrapers first, then targeted career-page scraping.

    Flow:
      Phase 1 — LinkedIn Jobs Apify (valig/linkedin-jobs-scraper)
      Phase 2 — Adzuna free API
      Phase 3 — Gap analysis: which target companies are NOT covered?
      Phase 4 — T1 Apify → T2 Browserbase for uncovered companies only

    Single-company mode (?company=<slug>) skips phases 1-3 and directly
    runs T1→T2 for that company (identical to orchestrate_scrape behaviour).

    Protected by the same _scrape_lock to prevent concurrent runs.
    """
    # Single-company mode: skip broad scrapers, go straight to T1→T2
    if slug:
        return await orchestrate_scrape(slug=slug, is_manual=is_manual)

    if _scrape_lock.locked():
        log.warning("orchestrator_v4.already_running")
        return {
            "run_id": "busy",
            "status": "already_running",
            "phase1_linkedin": {},
            "phase2_adzuna": {},
            "phase3_gap_analysis": {},
            "phase4_targeted": {},
        }

    async with _scrape_lock:
        run_id = str(uuid.uuid4())[:8]
        run_start = time.monotonic()
        log.info("orchestrator_v4.run_start", run_id=run_id)

        # ── Phase 1: LinkedIn Jobs Apify ──────────────────────────────────────
        log.info("orchestrator_v4.phase1_start", phase="linkedin_jobs_apify")
        phase1_start = time.monotonic()

        try:
            from app.services.jobs.free_apis.linkedin_jobs_apify import (
                poll_and_persist_linkedin_jobs_apify,
            )
            linkedin_jobs, linkedin_stats = await poll_and_persist_linkedin_jobs_apify()
        except Exception as exc:
            log.exception("orchestrator_v4.phase1_failed")
            linkedin_jobs = []
            linkedin_stats = {"status": "error", "error": str(exc)}

        phase1_ms = int((time.monotonic() - phase1_start) * 1000)
        linkedin_stats["duration_ms"] = phase1_ms
        log.info(
            "orchestrator_v4.phase1_done",
            jobs_fetched=len(linkedin_jobs),
            inserted=linkedin_stats.get("jobs_inserted", 0),
            duration_ms=phase1_ms,
        )

        # ── Phase 2: Adzuna ───────────────────────────────────────────────────
        log.info("orchestrator_v4.phase2_start", phase="adzuna")
        phase2_start = time.monotonic()

        try:
            from app.services.jobs.free_apis.adzuna import fetch_and_persist_adzuna
            adzuna_jobs, adzuna_stats = await fetch_and_persist_adzuna()
        except Exception as exc:
            log.exception("orchestrator_v4.phase2_failed")
            adzuna_jobs = []
            adzuna_stats = {"status": "error", "error": str(exc)}

        phase2_ms = int((time.monotonic() - phase2_start) * 1000)
        adzuna_stats["duration_ms"] = phase2_ms
        log.info(
            "orchestrator_v4.phase2_done",
            jobs_fetched=len(adzuna_jobs),
            inserted=adzuna_stats.get("jobs_inserted", 0),
            duration_ms=phase2_ms,
        )

        # ── Phase 3: Gap analysis ─────────────────────────────────────────────
        log.info("orchestrator_v4.phase3_start", phase="gap_analysis")
        phase3_start = time.monotonic()

        from sqlalchemy import select
        from app.database import SessionLocal
        from app.models.target_company import TargetCompany

        # Load all active target companies
        db = SessionLocal()
        try:
            stmt = (
                select(TargetCompany)
                .where(TargetCompany.is_active == True)  # noqa: E712
                .order_by(
                    TargetCompany.priority.desc(),
                    TargetCompany.last_success_at.asc().nullsfirst(),
                )
            )
            result = await db.execute(stmt)
            all_companies = list(result.scalars().all())
        finally:
            await db.close()

        # Merge all scraped jobs for analysis
        all_scraped_jobs = linkedin_jobs + adzuna_jobs

        # Run gap analysis
        try:
            from app.services.jobs.gap_analyzer import analyze_coverage
            uncovered_companies = await analyze_coverage(all_scraped_jobs, all_companies)
        except Exception as exc:
            log.exception("orchestrator_v4.phase3_failed")
            # On failure, fall back to scraping all companies
            uncovered_companies = all_companies

        phase3_ms = int((time.monotonic() - phase3_start) * 1000)
        gap_stats = {
            "total_targets": len(all_companies),
            "covered_by_broad_scrapers": len(all_companies) - len(uncovered_companies),
            "uncovered_for_t1t2": len(uncovered_companies),
            "uncovered_slugs": [c.slug for c in uncovered_companies[:20]],
            "total_broad_jobs": len(all_scraped_jobs),
            "duration_ms": phase3_ms,
        }
        log.info("orchestrator_v4.phase3_done", **gap_stats)

        # ── Phase 4: T1→T2 for uncovered companies ───────────────────────────
        log.info(
            "orchestrator_v4.phase4_start",
            phase="targeted_t1t2",
            companies=len(uncovered_companies),
        )
        phase4_start = time.monotonic()

        # TTL skip for uncovered companies
        ttl_skipped = [c for c in uncovered_companies if _is_within_ttl(c.last_success_at)]
        work_queue = [c for c in uncovered_companies if not _is_within_ttl(c.last_success_at)]

        # Monthly budget check
        db = SessionLocal()
        try:
            monthly_t1 = await _get_monthly_t1_count(db)
            monthly_t2 = await _get_monthly_t2_count(db)
        finally:
            await db.close()

        t1_soft_limit = int(settings.apify_monthly_cu_limit * 0.8)
        t2_soft_limit = int(settings.browserbase_monthly_minutes_limit * 0.8)
        t1_budget_ok = monthly_t1 < t1_soft_limit
        t2_budget_ok = monthly_t2 < t2_soft_limit

        log.info(
            "orchestrator_v4.phase4_budget",
            monthly_t1=monthly_t1, t1_limit=t1_soft_limit, t1_ok=t1_budget_ok,
            monthly_t2=monthly_t2, t2_limit=t2_soft_limit, t2_ok=t2_budget_ok,
        )

        # Batch loop — reuses the exact T1/T2 logic from orchestrate_scrape
        batches = [work_queue[i:i + BATCH_SIZE] for i in range(0, len(work_queue), BATCH_SIZE)]
        run_results: list[dict] = []
        watchlist: list[str] = []
        t1_quota_hit: bool = False

        log.info(
            "orchestrator_v4.phase4_batches",
            total_companies=len(work_queue),
            ttl_skipped=len(ttl_skipped),
            batches=len(batches),
        )

        for batch_idx, batch in enumerate(batches):
            log.info("orchestrator_v4.batch_start", batch=batch_idx + 1, of=len(batches), size=len(batch))

            t1_candidates: list = []
            t2_candidates: list = []

            for company in batch:
                route = _resolve_route(company)
                if route == TIER_T3:
                    watchlist.append(company.slug)
                    run_results.append({
                        "slug": company.slug, "display_name": company.display_name,
                        "tier": TIER_T3, "outcome": OUTCOME_TERMINAL,
                        "jobs_found": 0, "jobs_inserted": 0,
                    })
                    log.info(
                        "orchestrator_v4.watchlist",
                        company=company.slug,
                        consecutive_failures=company.consecutive_failures,
                    )
                elif route == TIER_T2:
                    t2_candidates.append(company)
                else:
                    t1_candidates.append(company)

            # T1: Apify — strictly sequential
            t2_escalations: list = []

            if t1_candidates:
                if not t1_budget_ok:
                    log.warning("orchestrator_v4.t1_budget_exhausted", deferred=len(t1_candidates))
                    for company in t1_candidates:
                        run_results.append({
                            "slug": company.slug, "display_name": company.display_name,
                            "tier": TIER_T1, "outcome": OUTCOME_SKIPPED,
                            "jobs_found": 0, "jobs_inserted": 0,
                        })
                else:
                    for idx, company in enumerate(t1_candidates):
                        if t1_quota_hit:
                            run_results.append({
                                "slug": company.slug, "display_name": company.display_name,
                                "tier": TIER_T1, "outcome": OUTCOME_SKIPPED,
                                "jobs_found": 0, "jobs_inserted": 0,
                            })
                            continue

                        res = await _run_t1_single(company, run_id)
                        run_results.append(res)

                        if res["outcome"] == OUTCOME_QUOTA_EXCEEDED:
                            t1_quota_hit = True
                            log.warning(
                                "orchestrator_v4.t1_quota_stop",
                                company=company.slug,
                                remaining=len(t1_candidates) - idx - 1,
                            )
                            continue

                        if (
                            _should_escalate_to_t2(res["outcome"])
                            and res.get("is_spa", True)
                            and t2_budget_ok
                        ):
                            t2_escalations.append(company)

                        if idx < len(t1_candidates) - 1:
                            await asyncio.sleep(T1_INTER_COMPANY_S)

            # T2: Browserbase — strictly sequential
            all_t2 = t2_candidates + t2_escalations

            if all_t2:
                if not t2_budget_ok:
                    log.warning("orchestrator_v4.t2_budget_exhausted", deferred=len(all_t2))
                    for company in all_t2:
                        watchlist.append(company.slug)
                        run_results.append({
                            "slug": company.slug, "display_name": company.display_name,
                            "tier": TIER_T3, "outcome": OUTCOME_SKIPPED,
                            "jobs_found": 0, "jobs_inserted": 0,
                        })
                else:
                    for company in all_t2:
                        res = await _run_t2_single(company, run_id)
                        run_results.append(res)
                        await asyncio.sleep(random.uniform(T2_JITTER_MIN_S, T2_JITTER_MAX_S))

            log.info("orchestrator_v4.batch_done", batch=batch_idx + 1, results_so_far=len(run_results))

            if batch_idx < len(batches) - 1:
                cooldown = random.uniform(COOLDOWN_MIN_S, COOLDOWN_MAX_S)
                log.info("orchestrator_v4.cooldown", seconds=round(cooldown, 1))
                await asyncio.sleep(cooldown)

        phase4_ms = int((time.monotonic() - phase4_start) * 1000)

        # Phase 4 summary
        successful = [r for r in run_results if r["outcome"] == OUTCOME_SUCCESS]
        p4_jobs_fetched = sum(r.get("jobs_found", 0) for r in run_results)
        p4_jobs_inserted = sum(r.get("jobs_inserted", 0) for r in run_results)

        # Write Failed.md for Phase 4 results
        try:
            _write_failed_md(
                run_results=run_results,
                all_companies=uncovered_companies,
                ttl_skipped=ttl_skipped,
                watchlist_slugs=watchlist,
                run_id=run_id,
            )
        except Exception as exc:
            log.warning("orchestrator_v4.failed_md_exception", error=str(exc))

        # Reset watchlist consecutive_failures
        if watchlist:
            from sqlalchemy import update as _update
            from app.models.target_company import TargetCompany as TC
            db = SessionLocal()
            try:
                await db.execute(
                    _update(TC)
                    .where(TC.slug.in_(watchlist))
                    .values(consecutive_failures=0)
                )
                await db.commit()
                log.info("orchestrator_v4.watchlist_reset", count=len(watchlist))
            except Exception as exc:
                log.warning("orchestrator_v4.watchlist_reset_failed", error=str(exc))
                await db.rollback()
            finally:
                await db.close()

        targeted_stats = {
            "companies_attempted": len(run_results),
            "companies_with_jobs": len(successful),
            "companies_ttl_skipped": len(ttl_skipped),
            "companies_watchlist": len(watchlist),
            "jobs_fetched": p4_jobs_fetched,
            "jobs_inserted": p4_jobs_inserted,
            "duration_ms": phase4_ms,
            "results": run_results,
        }

        # ── Grand summary ─────────────────────────────────────────────────────
        total_duration_ms = int((time.monotonic() - run_start) * 1000)
        total_inserted = (
            linkedin_stats.get("jobs_inserted", 0)
            + adzuna_stats.get("jobs_inserted", 0)
            + p4_jobs_inserted
        )
        total_fetched = (
            len(linkedin_jobs) + len(adzuna_jobs) + p4_jobs_fetched
        )

        sep = "=" * 65
        print(f"\n{sep}")
        print(f"  V4 SCRAPE RUN  --  run_id={run_id}")
        print(sep)
        print(f"  Phase 1 (LinkedIn Jobs Apify) : {len(linkedin_jobs)} fetched, {linkedin_stats.get('jobs_inserted', 0)} inserted")
        print(f"  Phase 2 (Adzuna)              : {len(adzuna_jobs)} fetched, {adzuna_stats.get('jobs_inserted', 0)} inserted")
        print(f"  Phase 3 (Gap Analysis)        : {gap_stats['covered_by_broad_scrapers']} covered, {gap_stats['uncovered_for_t1t2']} uncovered")
        print(f"  Phase 4 (Targeted T1/T2)      : {p4_jobs_fetched} fetched, {p4_jobs_inserted} inserted")
        print(f"  ────────────────────────────────────────────")
        print(f"  Total jobs fetched            : {total_fetched}")
        print(f"  Total jobs inserted           : {total_inserted}")
        print(f"  Total duration                : {total_duration_ms / 1000:.1f}s")
        if watchlist:
            print(f"\n  WATCHLIST: {', '.join(watchlist[:10])}")
        print(f"{sep}\n")

        log.info(
            "orchestrator_v4.run_complete",
            run_id=run_id,
            total_fetched=total_fetched,
            total_inserted=total_inserted,
            duration_ms=total_duration_ms,
        )

        return {
            "run_id": run_id,
            "phase1_linkedin": linkedin_stats,
            "phase2_adzuna": adzuna_stats,
            "phase3_gap_analysis": gap_stats,
            "phase4_targeted": targeted_stats,
            "total_jobs_fetched": total_fetched,
            "total_jobs_inserted": total_inserted,
            "duration_ms": total_duration_ms,
        }


# ── Standalone Gap Analyzer Orchestration ────────────────────────────────────

async def orchestrate_gap_scrape(
    hours: int = 48,
    scrape_uncovered: bool = True,
    is_manual: bool = True,
) -> dict:
    """Standalone Gap Analyzer:
    1. Queries distinct company names from jobs in DB seen in the last `hours`.
    2. Runs detailed Gap Analysis against active TargetCompany catalog.
    3. If `scrape_uncovered` is True, runs T1→T2 targeted scraping for uncovered companies only.

    Leaves /jobs/scrape-now and orchestrate_scrape_v4 untouched.
    """
    if _scrape_lock.locked():
        log.warning("orchestrator_gap.already_running")
        return {
            "run_id": "busy",
            "status": "already_running",
            "gap_analysis": {},
            "targeted_scraping": {},
        }

    async with _scrape_lock:
        run_id = str(uuid.uuid4())[:8]
        run_start = time.monotonic()
        log.info("orchestrator_gap.run_start", run_id=run_id, hours=hours, scrape_uncovered=scrape_uncovered)

        from datetime import datetime, timezone, timedelta
        from sqlalchemy import select, distinct, or_
        from app.database import SessionLocal
        from app.models.job import Job
        from app.models.target_company import TargetCompany
        from app.schemas.job import JobIn
        from app.services.jobs.gap_analyzer import analyze_coverage_detailed

        # ── Step 1: Query DB for companies with jobs in last `hours` ───────
        cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
        db = SessionLocal()
        try:
            target_stmt = (
                select(TargetCompany)
                .where(TargetCompany.is_active == True)  # noqa: E712
                .order_by(
                    TargetCompany.priority.desc(),
                    TargetCompany.last_success_at.asc().nullsfirst(),
                )
            )
            target_result = await db.execute(target_stmt)
            all_companies = list(target_result.scalars().all())

            job_stmt = (
                select(distinct(Job.company))
                .where(
                    Job.company.isnot(None),
                    or_(Job.first_seen_at >= cutoff, Job.last_seen_at >= cutoff),
                )
            )
            job_result = await db.execute(job_stmt)
            recent_company_names = [r[0] for r in job_result.fetchall() if r[0]]
        finally:
            await db.close()

        # Create lightweight JobIn representations for gap matching
        mock_jobs = [
            JobIn(
                company=name,
                job_title="Representative Role",
                location="India",
                job_url="https://db-record.local",
                source_type="db_historical",
            )
            for name in recent_company_names
        ]

        # ── Step 2: Gap analysis ──────────────────────────────────────────
        phase_start = time.monotonic()
        try:
            gap_result = await analyze_coverage_detailed(mock_jobs, all_companies)
        except Exception as exc:
            log.exception("orchestrator_gap.analysis_failed")
            gap_result = {
                "covered": [],
                "uncovered": [
                    {
                        "id": c.id,
                        "slug": c.slug,
                        "display_name": c.display_name,
                        "priority": c.priority,
                        "preferred_scraper": c.preferred_scraper,
                        "last_success_at": str(c.last_success_at) if c.last_success_at else None,
                    }
                    for c in all_companies
                ],
                "uncovered_objects": all_companies,
                "stats": {
                    "total_targets": len(all_companies),
                    "covered_count": 0,
                    "uncovered_count": len(all_companies),
                    "deterministic_matches": 0,
                    "llm_matches": 0,
                    "coverage_pct": 0.0,
                    "scraped_jobs_count": len(mock_jobs),
                    "unique_scraped_companies_count": len(recent_company_names),
                },
                "unique_scraped_companies": recent_company_names,
            }

        gap_duration_ms = int((time.monotonic() - phase_start) * 1000)
        uncovered_objects = gap_result["uncovered_objects"]

        # ── Step 3: Targeted T1→T2 Scraping for Uncovered Companies ─────────
        targeted_stats: dict[str, Any] = {
            "status": "skipped",
            "reason": "scrape_uncovered_is_false" if not scrape_uncovered else "no_uncovered_companies",
            "companies_attempted": 0,
            "jobs_found": 0,
            "jobs_inserted": 0,
            "results": [],
        }

        if scrape_uncovered and uncovered_objects:
            ttl_skipped = [c for c in uncovered_objects if _is_within_ttl(c.last_success_at)]
            work_queue = [c for c in uncovered_objects if not _is_within_ttl(c.last_success_at)]

            db = SessionLocal()
            try:
                monthly_t1 = await _get_monthly_t1_count(db)
                monthly_t2 = await _get_monthly_t2_count(db)
            finally:
                await db.close()

            t1_soft_limit = int(settings.apify_monthly_cu_limit * 0.8)
            t2_soft_limit = int(settings.browserbase_monthly_minutes_limit * 0.8)
            t1_budget_ok = monthly_t1 < t1_soft_limit
            t2_budget_ok = monthly_t2 < t2_soft_limit

            batches = [work_queue[i:i + BATCH_SIZE] for i in range(0, len(work_queue), BATCH_SIZE)]
            run_results: list[dict] = []
            watchlist: list[str] = []
            t1_quota_hit: bool = False

            for batch_idx, batch in enumerate(batches):
                log.info("orchestrator_gap.batch_start", batch=batch_idx + 1, of=len(batches), size=len(batch))
                t1_candidates: list = []
                t2_candidates: list = []

                for company in batch:
                    route = _resolve_route(company)
                    if route == TIER_T3:
                        watchlist.append(company.slug)
                        run_results.append({
                            "slug": company.slug, "display_name": company.display_name,
                            "tier": TIER_T3, "outcome": OUTCOME_TERMINAL,
                            "jobs_found": 0, "jobs_inserted": 0,
                        })
                    elif route == TIER_T2:
                        t2_candidates.append(company)
                    else:
                        t1_candidates.append(company)

                t2_escalations: list = []

                if t1_candidates:
                    if not t1_budget_ok:
                        for company in t1_candidates:
                            run_results.append({
                                "slug": company.slug, "display_name": company.display_name,
                                "tier": TIER_T1, "outcome": OUTCOME_SKIPPED,
                                "jobs_found": 0, "jobs_inserted": 0,
                            })
                    else:
                        for idx, company in enumerate(t1_candidates):
                            if t1_quota_hit:
                                run_results.append({
                                    "slug": company.slug, "display_name": company.display_name,
                                    "tier": TIER_T1, "outcome": OUTCOME_SKIPPED,
                                    "jobs_found": 0, "jobs_inserted": 0,
                                })
                                continue

                            res = await _run_t1_single(company, run_id)
                            run_results.append(res)

                            if res["outcome"] == OUTCOME_QUOTA_EXCEEDED:
                                t1_quota_hit = True
                                log.warning(
                                    "orchestrator_gap.t1_quota_stop",
                                    company=company.slug,
                                    remaining=len(t1_candidates) - idx - 1,
                                )
                                continue

                            if (
                                _should_escalate_to_t2(res["outcome"])
                                and res.get("is_spa", True)
                                and t2_budget_ok
                            ):
                                t2_escalations.append(company)

                            if idx < len(t1_candidates) - 1:
                                await asyncio.sleep(T1_INTER_COMPANY_S)

                all_t2 = t2_candidates + t2_escalations
                if all_t2:
                    if not t2_budget_ok:
                        log.warning("orchestrator_gap.t2_budget_exhausted", deferred=len(all_t2))
                        for company in all_t2:
                            watchlist.append(company.slug)
                            run_results.append({
                                "slug": company.slug, "display_name": company.display_name,
                                "tier": TIER_T3, "outcome": OUTCOME_SKIPPED,
                                "jobs_found": 0, "jobs_inserted": 0,
                            })
                    else:
                        for company in all_t2:
                            res = await _run_t2_single(company, run_id)
                            run_results.append(res)
                            await asyncio.sleep(random.uniform(T2_JITTER_MIN_S, T2_JITTER_MAX_S))

                log.info("orchestrator_gap.batch_done", batch=batch_idx + 1, results_so_far=len(run_results))

                if batch_idx < len(batches) - 1:
                    cooldown = random.uniform(COOLDOWN_MIN_S, COOLDOWN_MAX_S)
                    log.info("orchestrator_gap.cooldown", seconds=round(cooldown, 1))
                    await asyncio.sleep(cooldown)

            _write_failed_md(run_results, all_companies, ttl_skipped, watchlist, run_id)

            p_jobs_found = sum(r.get("jobs_found", 0) for r in run_results)
            p_jobs_inserted = sum(r.get("jobs_inserted", 0) for r in run_results)
            p_successes = sum(1 for r in run_results if r.get("outcome") == OUTCOME_SUCCESS)
            total_attempted = len(run_results)
            success_rate = (
                round(p_successes / total_attempted * 100, 1)
                if total_attempted > 0
                else 0.0
            )

            targeted_stats = {
                "status": "completed",
                "companies_uncovered": len(uncovered_objects),
                "companies_attempted": total_attempted,
                "companies_ttl_skipped": len(ttl_skipped),
                "companies_watchlist": len(watchlist),
                "jobs_found": p_jobs_found,
                "jobs_inserted": p_jobs_inserted,
                "success_rate_pct": success_rate,
                "results": run_results,
                "watchlist": watchlist,
            }

        total_duration_ms = int((time.monotonic() - run_start) * 1000)

        return {
            "run_id": run_id,
            "status": "completed",
            "gap_analysis": {
                "lookback_hours": hours,
                "duration_ms": gap_duration_ms,
                "stats": gap_result["stats"],
                "covered_companies": gap_result["covered"],
                "uncovered_companies": gap_result["uncovered"],
                "db_scraped_company_names": gap_result["unique_scraped_companies"],
            },
            "targeted_scraping": targeted_stats,
            "total_duration_ms": total_duration_ms,
        }


