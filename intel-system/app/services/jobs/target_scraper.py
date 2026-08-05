"""Direct HTTP career page scraper + OpenRouter/Nemotron-3-Ultra extraction.

Used for companies whose ATS type is 'custom', 'workday', or unknown —
where no standard API adapter exists. Fetches the page, strips HTML,
and sends the text to OpenRouter for structured job extraction.

Pagination: fetch_career_page_paginated() follows rel=next and common
  href patterns (page=N, start=N, offset=N) until max_pages or no next link.
"""
import re
from urllib.parse import urljoin

import httpx
import structlog

from app.schemas.job import JobIn
from app.services.jobs.classifier import is_ev_domain_relevant

log = structlog.get_logger()

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "en-US,en;q=0.7",
}

# Pagination link patterns — ordered by specificity
_NEXT_REL     = re.compile(r'<a\b[^>]*\brel=["\']next["\'][^>]*\bhref=["\']([^"\']+)["\']', re.I)
_NEXT_REL_REV = re.compile(r'<a\b[^>]*\bhref=["\']([^"\']+)["\'][^>]*\brel=["\']next["\']', re.I)
_NEXT_TEXT    = re.compile(
    r'<a\b[^>]*\bhref=["\']([^"\']+)["\'][^>]*>\s*(?:next|›|»|&rsaquo;|&raquo;|\&gt;\&gt;|next\s+page)\s*</a>',
    re.I,
)
_NEXT_CLASS   = re.compile(
    r'<a\b[^>]*class=["\'][^"\']*\bnext\b[^"\']*["\'][^>]*\bhref=["\']([^"\']+)["\']', re.I
)
_NEXT_CLASS_REV = re.compile(
    r'<a\b[^>]*\bhref=["\']([^"\']+)["\'][^>]*class=["\'][^"\']*\bnext\b[^"\']*["\']', re.I
)


def _strip_html(html: str) -> str:
    text = re.sub(r"<script[^>]*>.*?</script>", " ", html, flags=re.S | re.I)
    text = re.sub(r"<style[^>]*>.*?</style>", " ", text, flags=re.S | re.I)
    text = re.sub(r"<[^>]+>", " ", text)
    text = re.sub(r"\s{2,}", " ", text)
    return text.strip()


def _is_likely_spa(html: str) -> tuple[bool, list[str]]:
    """Detect if page is a Single Page Application (React/Vue/Angular).
    Returns (is_spa, indicators)."""
    indicators = []

    # Check for common SPA framework markers
    if 'id="react-app"' in html or 'id="app"' in html or '<div id="root"' in html:
        indicators.append("React/Vue root div detected")
    if '__NEXT_DATA__' in html:
        indicators.append("Next.js data script found")
    if 'ng-app' in html or 'data-ng-app' in html:
        indicators.append("Angular app directive found")
    if 'window.__INITIAL_STATE__' in html or 'window.__APP_STATE__' in html:
        indicators.append("App state injection detected")
    if 'defer' in html and 'type="module"' in html:
        indicators.append("Module scripts (likely SPA)")

    # Check for dynamic data in script tags (JSON API responses)
    script_count = html.count('<script')
    if script_count > 5:
        indicators.append(f"Many script tags ({script_count}) — likely SPA")

    # If body is very short but head is long, it's probably SPA
    body_match = re.search(r'<body[^>]*>(.*?)</body>', html, re.I | re.S)
    if body_match:
        body_content = body_match.group(1)
        if len(body_content) < 2000 and len(html) > 50000:
            indicators.append("Tiny body vs. large head — SPA skeleton")

    return len(indicators) > 0, indicators


def _find_next_page_url(html: str, current_url: str) -> str | None:
    """Detect pagination 'next page' link in HTML. Returns absolute URL or None."""
    for pattern in (
        _NEXT_REL, _NEXT_REL_REV, _NEXT_TEXT, _NEXT_CLASS, _NEXT_CLASS_REV
    ):
        m = pattern.search(html)
        if m:
            href = m.group(1).strip()
            # Ignore anchors and javascript: hrefs
            if href.startswith("#") or href.lower().startswith("javascript"):
                continue
            # Make absolute
            abs_url = urljoin(current_url, href)
            # Skip if identical to current (infinite loop guard)
            if abs_url.rstrip("/") == current_url.rstrip("/"):
                continue
            return abs_url

    return None



# [DIAG-PATCH-APPLIED]
def _extract_spa_embedded_data(html: str) -> str | None:
    """Mine embedded job data from SPA skeleton HTML before falling back to LLM."""
    candidates: list[str] = []
    # application/json script blocks (common in Next.js / Gatsby)
    for m in re.finditer(
        r'<script[^>]*type=["\']application/json["\'][^>]*>(.*?)</script>',
        html, re.DOTALL | re.I,
    ):
        candidates.append(m.group(1))
    # Inline app state patterns
    for pattern in (
        r'window\.__(?:INITIAL|APP|NEXT)_STATE__\s*=\s*(\{.*?\});',
        r'"jobs"\s*:\s*(\[.*?\])',
        r'"jobPostings"\s*:\s*(\[.*?\])',
        r'"positions"\s*:\s*(\[.*?\])',
        r'"openings"\s*:\s*(\[.*?\])',
        r'"careers"\s*:\s*(\[.*?\])',
    ):
        m = re.search(pattern, html, re.DOTALL | re.I)
        if m:
            candidates.append(m.group(1))
    # Return longest candidate above noise threshold
    best = max(candidates, key=len, default="")
    return best[:12000] if len(best) > 150 else None


async def fetch_career_page(url: str, company_slug: str, debug: bool = False) -> list[JobIn]:
    """Fetch a career page and extract EV-relevant jobs via Gemini."""
    try:
        async with httpx.AsyncClient(
            timeout=30, headers=_HEADERS, follow_redirects=True
        ) as client:
            r = await client.get(url)
            r.raise_for_status()
            html = r.text
    except Exception as exc:
        log.warning("target_scraper.fetch_failed", url=url, company=company_slug, error=str(exc))
        return []

    if debug:
        print(f"\n  [DEBUG] RAW HTML (first 1500 chars):\n{html[:1500]}\n  ---")

    # Detect SPA
    is_spa, spa_indicators = _is_likely_spa(html)
    if is_spa and debug:
        print("\n  [!!] WARNING: Page appears to be a Single Page App (SPA)")
        for indicator in spa_indicators:
            print(f"       → {indicator}")
        print("      Plain HTTP fetch won't load dynamic job content.")
        print("      Recommend using Apify actor instead.\n")

    # [DIAG-PATCH-APPLIED]
    # Try to extract embedded JSON job data from SPA skeleton FIRST
    embedded = _extract_spa_embedded_data(html)
    if embedded:
        log.info("target_scraper.using_embedded_data", company=company_slug, chars=len(embedded))
        if debug:
            print(f"\n  [PATCH] Found embedded SPA data: {len(embedded)} chars")
        text = embedded
    else:
        text = _strip_html(html)

    if len(text) < 200 and not embedded:
        if is_spa:
            log.info("target_scraper.spa_short_fallback_to_raw_html", url=url, company=company_slug, stripped_chars=len(text))
        else:
            log.warning("target_scraper.short_text_raw_html_fallback", url=url, company=company_slug, stripped_chars=len(text), spa_indicators=spa_indicators)
        if debug:
            print(f"\n  [!!] Stripped text too short ({len(text)} chars) — trying raw HTML for LLM extraction")
        if len(html) < 200:
            log.warning("target_scraper.raw_html_empty", url=url, company=company_slug)
            return []
        text = html[:12000]

    log.info("target_scraper.fetched_page", url=url, company=company_slug, chars=len(text), is_spa=is_spa)

    if debug:
        print(f"\n  [DEBUG] CONTENT SENT TO OPENROUTER (first 1000 chars):\n{text[:1000]}\n  ---")

    from app.services.jobs.changedetection import extract_jobs_from_diff

    jobs = await extract_jobs_from_diff(text[:12000], url)

    # Target companies are pre-curated EV employers. Use is_target=True
    # to avoid dropping relevant roles due to missing explicit EV keywords in the text.
    result: list[JobIn] = []
    for j in jobs:
        j.source_type = f"direct_{company_slug}"
        if not j.company or j.company == url:
            j.company = company_slug
        if is_ev_domain_relevant(j.job_title, j.description or "", is_target=True):
            result.append(j)

    log.info("target_scraper.ev_filtered", url=url, total=len(jobs), ev_relevant=len(result), is_spa=is_spa)
    return result


async def fetch_career_page_paginated(
    base_url: str,
    company_slug: str,
    max_pages: int = 5,
    debug: bool = False,
) -> list[JobIn]:
    """Crawl all pages of a company career site, following pagination links.

    Strategy:
      1. Fetch current URL
      2. Extract jobs via Gemini
      3. Look for a 'next page' link in the raw HTML
      4. Repeat until no next link, max_pages reached, or zero new jobs
    """
    all_jobs: list[JobIn] = []
    seen_urls: set[str] = set()
    current_url = base_url

    try:
        async with httpx.AsyncClient(
            timeout=30, headers=_HEADERS, follow_redirects=True
        ) as client:
            for page_num in range(1, max_pages + 1):
                if current_url in seen_urls:
                    break
                seen_urls.add(current_url)

                log.info(
                    "target_scraper.paginated_fetch",
                    company=company_slug,
                    page=page_num,
                    url=current_url,
                )

                try:
                    r = await client.get(current_url)
                    r.raise_for_status()
                    html = r.text
                except Exception as exc:
                    log.warning(
                        "target_scraper.page_fetch_failed",
                        company=company_slug,
                        page=page_num,
                        url=current_url,
                        error=str(exc),
                    )
                    break

                if debug:
                    print(f"\n  [DEBUG PAGE {page_num}] RAW HTML (first 1200 chars):\n{html[:1200]}\n  ---")

                # Detect SPA
                is_spa, spa_indicators = _is_likely_spa(html)
                if is_spa and debug:
                    print(f"  [!!] Page {page_num} appears to be SPA: {', '.join(spa_indicators)}")

                text = _strip_html(html)
                if len(text) < 200:
                    if debug:
                        print(f"  [!!] Page {page_num}: Stripped text {len(text)} chars — using raw HTML for LLM extraction")
                    if len(html) < 200:
                        break
                    text = html[:12000]

                if debug:
                    print(f"  [DEBUG PAGE {page_num}] CONTENT (first 1000 chars):\n{text[:1000]}\n  ---")

                from app.services.jobs.changedetection import extract_jobs_from_diff

                page_jobs = await extract_jobs_from_diff(text[:12000], current_url)
                new_count = 0

                for j in page_jobs:
                    j.source_type = f"direct_{company_slug}"
                    if not j.company or j.company == current_url:
                        j.company = company_slug
                    if not is_ev_domain_relevant(j.job_title, j.description or "", is_target=True):
                        continue
                    dedup_key = f"{j.job_title}|{j.company}|{j.job_url}"
                    if dedup_key not in seen_urls:
                        seen_urls.add(dedup_key)
                        all_jobs.append(j)
                        new_count += 1

                log.info(
                    "target_scraper.page_done",
                    company=company_slug,
                    page=page_num,
                    new_jobs=new_count,
                    total=len(all_jobs),
                )

                # Find next page
                next_url = _find_next_page_url(html, current_url)
                if not next_url:
                    log.info(
                        "target_scraper.no_next_page",
                        company=company_slug,
                        page=page_num,
                    )
                    break

                current_url = next_url

                # Polite crawl delay
                import asyncio
                await asyncio.sleep(2.0)

    except Exception as exc:
        log.exception(
            "target_scraper.paginated_error",
            company=company_slug,
            error=str(exc),
        )

    log.info(
        "target_scraper.paginated_complete",
        company=company_slug,
        total_jobs=len(all_jobs),
        pages_crawled=len(seen_urls),
    )
    return all_jobs
