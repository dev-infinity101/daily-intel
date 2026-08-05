"""Browserbase T2 adapter — full headless Chrome rendering.

Triggered only when ALL four V3 Q6 conditions hold:
  1. T1 Apify returned 0 jobs via a method-deterministic failure
  2. _is_likely_spa() confirms it's a real SPA (not a correct empty page)
  3. Outcome was NOT NO_RELEVANT_JOBS (which is terminal, not a failure)
  4. Monthly Browserbase minutes budget is not exhausted

Each session is created, used, and immediately closed so idle minutes are
never leaked. Concurrency is capped at 1 by the orchestrator (free-tier limit).
Session duration is recorded in scrape_attempts for cost tracking.
"""
import asyncio
import time

import httpx
import structlog

from app.config import settings
from app.schemas.job import JobIn
from app.services.jobs.classifier import is_ev_domain_relevant

log = structlog.get_logger()

_BB_API_BASE = "https://api.browserbase.com/v1"


async def _create_session() -> tuple[str, str, str]:
    """Create a Browserbase session. Returns (session_id, connect_url, used_api_key)."""
    keys = [
        (settings.browserbase_api_key, settings.browserbase_project_id),
        (settings.browserbase_api_key_secondary, settings.browserbase_project_id_secondary),
    ]
    
    last_exc = None
    for api_key, project_id in keys:
        if not api_key or not project_id:
            continue
            
        try:
            async with httpx.AsyncClient(timeout=30) as client:
                r = await client.post(
                    f"{_BB_API_BASE}/sessions",
                    headers={
                        "x-bb-api-key": api_key,
                        "Content-Type": "application/json",
                    },
                    json={
                        "projectId": project_id,
                        "browserSettings": {
                            "viewport": {"width": 1280, "height": 900},
                        },
                    },
                )
                r.raise_for_status()
                data = r.json()
                session_id: str = data["id"]
                connect_url: str = data.get("connectUrl") or data.get("wsUrl") or ""
                return session_id, connect_url, api_key
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in (402, 429):
                log.warning(
                    "browserbase.api_error_trying_fallback",
                    status=exc.response.status_code,
                    error=str(exc),
                )
                last_exc = exc
                continue
            raise
            
    if last_exc:
        raise last_exc
        
    raise RuntimeError("No Browserbase API keys configured.")


async def _close_session(session_id: str, api_key: str) -> None:
    """Close a Browserbase session immediately — never leak idle minutes."""
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            await client.delete(
                f"{_BB_API_BASE}/sessions/{session_id}",
                headers={"x-bb-api-key": api_key},
            )
        log.info("browserbase.session_closed", session_id=session_id)
    except Exception as exc:
        log.warning("browserbase.close_failed", session_id=session_id, error=str(exc))


def _render_page_sync(connect_url: str, career_url: str) -> str:
    """Connect to Browserbase via CDP (sync Playwright) and return rendered HTML.

    Uses the sync Playwright API so the subprocess is managed by Playwright's
    own internal thread — not by the asyncio event loop. This avoids the
    NotImplementedError from asyncio.SelectorEventLoop.create_subprocess_exec
    on Windows, where the default loop does not support subprocess creation.
    """
    import time as _time

    try:
        from playwright.sync_api import sync_playwright
    except ImportError as exc:
        raise RuntimeError(
            "playwright not installed — run: pip install playwright && playwright install chromium"
        ) from exc

    with sync_playwright() as p:
        browser = p.chromium.connect_over_cdp(connect_url)
        try:
            ctx = browser.new_context()
            page = ctx.new_page()
            try:
                page.goto(career_url, wait_until="domcontentloaded", timeout=60_000)
            except Exception as e:
                log.warning("browserbase.page_goto_warning", url=career_url, error=str(e))
            
            _time.sleep(5)  # extra wait for JS hydration
            return page.content()
        finally:
            browser.close()


async def _render_page(connect_url: str, career_url: str) -> str:
    """Async wrapper — runs sync Playwright in a thread to avoid event-loop conflicts."""
    return await asyncio.to_thread(_render_page_sync, connect_url, career_url)


async def fetch_via_browserbase(
    career_url: str,
    company_slug: str,
) -> tuple[list[JobIn], str | None]:
    """T2 entry point: render career page and extract EV-relevant jobs.

    Returns (ev_jobs, session_id). session_id is None when unavailable.
    Always closes the session in a finally block.
    """
    if not settings.browserbase_api_key and not settings.browserbase_api_key_secondary:
        log.warning("browserbase.no_api_key", company=company_slug)
        return [], None

    session_id: str | None = None
    used_api_key: str | None = None
    t_start = time.monotonic()

    try:
        log.info("browserbase.creating_session", company=company_slug, url=career_url)
        session_id, connect_url, used_api_key = await _create_session()
        log.info("browserbase.session_created", company=company_slug, session_id=session_id)

        html = await _render_page(connect_url, career_url)
        duration_ms = int((time.monotonic() - t_start) * 1000)
        log.info(
            "browserbase.rendered",
            company=company_slug,
            chars=len(html),
            duration_ms=duration_ms,
        )

        from app.services.jobs.extraction_utils import extract_jobs_from_html

        all_jobs = await extract_jobs_from_html(html, career_url, company_slug)

        for j in all_jobs:
            j.source_type = f"browserbase_{company_slug}"
            if not j.company or j.company == career_url:
                j.company = company_slug

        ev_jobs = [
            j for j in all_jobs
            if is_ev_domain_relevant(j.job_title, j.description or "", is_target=True)
        ]
        log.info(
            "browserbase.extracted",
            company=company_slug,
            total=len(all_jobs),
            ev_relevant=len(ev_jobs),
            session_id=session_id,
        )
        return ev_jobs, session_id

    except Exception as exc:
        duration_ms = int((time.monotonic() - t_start) * 1000)
        log.exception(
            "browserbase.failed",
            company=company_slug,
            error=str(exc),
            duration_ms=duration_ms,
        )
        return [], session_id
    finally:
        if session_id and used_api_key:
            await _close_session(session_id, used_api_key)
