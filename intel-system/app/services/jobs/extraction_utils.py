"""V3 extraction utilities — enhanced HTML-to-JobIn pipeline.

Replaces the old `html[:12000]` truncation with three layers:
  1. Embedded JSON mining  — mine <script type="application/json"> and window
                             state variables before any LLM call (zero cost)
  2. Job-region isolation  — scan for the substring with the highest density of
                             job-card markers instead of blindly slicing the head
  3. Chunked LLM extraction — split the candidate region into ~15K windows,
                              extract per window, merge+dedup (caps at 4 windows)

Expected impact: recovers Uber/Schneider/Hitachi/Eaton-class pages that render
fine but previously extracted 0 jobs because the LLM saw only <0.5% of the DOM.

Eightfold AI fallback (extract_jobs_eightfold):
  Triggered when source_url contains .eightfold.ai AND the generic pipeline
  returns 0 jobs.  Uses Eightfold-specific prompts and smart chunk scoring to
  skip the large themeOptions/varTheme config blob that dominates page content.
"""
import html as _html_lib
import json as _json
import re

import structlog

from app.schemas.job import JobIn

log = structlog.get_logger()

# ── Constants ─────────────────────────────────────────────────────────────────

CHUNK_SIZE = 15_000
MAX_CHUNKS = 4
REGION_WINDOW = 40_000   # max chars for isolated job region

# ── Embedded JSON patterns ────────────────────────────────────────────────────

_APP_JSON_RE = re.compile(
    r'<script[^>]*type=["\']application/json["\'][^>]*>(.*?)</script>',
    re.DOTALL | re.I,
)
_WINDOW_STATE_PATTERNS: list[re.Pattern[str]] = [
    re.compile(r'window\.__(?:INITIAL|APP|NEXT)_STATE__\s*=\s*(\{.*?\});', re.DOTALL | re.I),
    re.compile(r'"jobs"\s*:\s*(\[.*?\])', re.DOTALL | re.I),
    re.compile(r'"jobPostings"\s*:\s*(\[.*?\])', re.DOTALL | re.I),
    re.compile(r'"positions"\s*:\s*(\[.*?\])', re.DOTALL | re.I),
    re.compile(r'"openings"\s*:\s*(\[.*?\])', re.DOTALL | re.I),
    re.compile(r'"careers"\s*:\s*(\[.*?\])', re.DOTALL | re.I),
    re.compile(r'"listings"\s*:\s*(\[.*?\])', re.DOTALL | re.I),
]

# ── Job-density pattern ───────────────────────────────────────────────────────

_JOB_LINK_RE = re.compile(
    r'href=["\'][^"\']*(?:job|career|position|role|opening|vacanc)[^"\']*["\']',
    re.I,
)


# ── Layer 1: embedded JSON ────────────────────────────────────────────────────

def extract_embedded_json(html: str) -> str | None:
    """Mine embedded job data from SPA skeleton HTML (no LLM cost).

    Checks application/json script blocks and window state variables.
    Returns the longest candidate above a noise threshold, or None.
    """
    candidates: list[str] = []
    for m in _APP_JSON_RE.finditer(html):
        candidates.append(m.group(1))
    for pattern in _WINDOW_STATE_PATTERNS:
        m = pattern.search(html)
        if m:
            candidates.append(m.group(1))
    best = max(candidates, key=len, default="")
    return best[:REGION_WINDOW] if len(best) > 150 else None


# ── Layer 2: job-region isolation ─────────────────────────────────────────────

def _count_job_links(text: str) -> int:
    return len(_JOB_LINK_RE.findall(text))


def isolate_job_region(html: str) -> str:
    """Return the substring of `html` with the highest job-link density.

    Scans in overlapping half-window steps so the winning region isn't
    artificially split. Falls back to raw html when it's already short.
    """
    if len(html) <= REGION_WINDOW:
        return html

    step = REGION_WINDOW // 2
    best_start = 0
    best_count = 0

    for start in range(0, len(html) - step, step):
        region = html[start:start + REGION_WINDOW]
        count = _count_job_links(region)
        if count > best_count:
            best_count = count
            best_start = start

    log.debug(
        "extraction_utils.region_isolated",
        html_len=len(html),
        best_start=best_start,
        job_links=best_count,
    )
    return html[best_start:best_start + REGION_WINDOW]


# ── Layer 3: chunked LLM extraction ──────────────────────────────────────────

async def extract_jobs_chunked(
    content: str,
    source_url: str,
    company_slug: str,
    chunk_size: int = CHUNK_SIZE,
    max_chunks: int = MAX_CHUNKS,
) -> list[JobIn]:
    """Split content into windows and extract jobs from each, then merge+dedup."""
    from app.services.jobs.changedetection import extract_jobs_from_diff

    if len(content) <= chunk_size:
        return await extract_jobs_from_diff(content, source_url)

    chunks = [content[i:i + chunk_size] for i in range(0, len(content), chunk_size)][:max_chunks]
    log.info(
        "extraction_utils.chunked",
        company=company_slug,
        chunks=len(chunks),
        total_chars=len(content),
    )

    all_jobs: list[JobIn] = []
    seen_keys: set[str] = set()

    for idx, chunk in enumerate(chunks):
        try:
            chunk_jobs = await extract_jobs_from_diff(chunk, source_url)
            new_count = 0
            for j in chunk_jobs:
                key = f"{(j.job_title or '').lower().strip()}|{(j.job_url or '').strip()}"
                if key not in seen_keys:
                    seen_keys.add(key)
                    all_jobs.append(j)
                    new_count += 1
            log.info(
                "extraction_utils.chunk_done",
                company=company_slug,
                chunk=idx + 1,
                new_jobs=new_count,
            )
        except Exception as exc:
            log.warning(
                "extraction_utils.chunk_failed",
                company=company_slug,
                chunk=idx + 1,
                error=str(exc),
            )

    return all_jobs


# ── Eightfold AI fallback extractor ──────────────────────────────────────────
#
# Eightfold SPA portals (*.eightfold.ai) embed a large themeOptions/varTheme
# config JSON blob as the FIRST visible text after stripping HTML.  This blob
# (often 180-220 K chars) fills every chunk the generic pipeline looks at, so
# the LLM sees zero job data and returns [].
#
# This extractor is triggered only when:
#   (a) source_url contains .eightfold.ai, AND
#   (b) the three-layer generic pipeline returned 0 jobs.
#
# It uses two strategies in order:
#   1. JSON mining  — bracket-balance scan for a "positions"/"jobPostings" array
#                     in the entity-decoded content blob (zero extra LLM calls)
#   2. Smart chunks — rank every 12 K slice by job-signal / config-noise score;
#                     send the top-scoring slices so the LLM sees job cards,
#                     not CSS variable definitions.
# ─────────────────────────────────────────────────────────────────────────────

_EIGHTFOLD_HOST_RE = re.compile(r'\.eightfold\.ai', re.I)
_EIGHTFOLD_BASE_RE = re.compile(r'(https?://[^/?#\s]+\.eightfold\.ai)', re.I)

_EF_CONFIG_NOISE = re.compile(
    r"themeOptions|varTheme|customTheme|button-primary|pcsx-theme"
    r"|linear-gradient|color-\d{2,3}|border-radius|font-weight|font-size",
    re.I,
)
_EF_JOB_SIGNAL = re.compile(
    r"View\s+Job|apply\s+now|job_title|Engineering|Operations|Manufacturing"
    r"|Hyderabad|Pune|Bengaluru|India|Chennai|Mumbai|Noida|Gurugram",
    re.I,
)

_EF_CHUNK_SIZE = 12_000
_EF_MAX_CHUNKS = 5

_EF_TEXT_PROMPT = """\
You are extracting job listings from an Eightfold AI career portal ({base_url}).
NOTE: Content may open with a large config JSON blob (themeOptions, varTheme, \
customTheme, pcsx-theme-*, color-*, etc.) — SKIP it entirely.
Look further in the content for "View Job" anchors, job-card text blocks, \
or a "positions"/"jobPostings" array.

Each Eightfold job card pattern:
  <Job Title>
  <City, Country>  [· Department · Employment Type]
  View Job

Return ONLY a valid JSON array. Each element:
  "job_title"  : string (required; never null)
  "company"    : "{company}"
  "location"   : string or null
  "department" : string or null
  "job_url"    : string or null  (prepend "{base_url}" to /careers/<id> paths)
  "description": string — title + dept if no other text is available

Count every "View Job" marker — that is your minimum job count.
Do NOT skip jobs with missing descriptions. Output raw JSON only.

Source: {source_url}
Content:
{content}"""

_EF_JSON_PROMPT = """\
You are extracting job listings from an Eightfold AI career portal ({base_url}).
Input is a JSON object with a positions / jobPostings / roles array extracted \
from {company}'s career page.

Return ONLY a valid JSON array. Each element:
  "job_title"  : string — from the "name" field (required; never null)
  "company"    : "{company}"
  "location"   : string or null — from "location", "city", or "country" field
  "department" : string or null — from "department", "category", or "function" field
  "job_url"    : string or null — refs.landing_page prepended with "{base_url}",
                 or "{base_url}/careers/" + id
  "description": string — department + domain keywords, or one line from title + dept

Extract EVERY entry — do not skip any. Output raw JSON only — no markdown fences.

Source: {source_url}
Data:
{content}"""


def _is_eightfold_url(url: str) -> bool:
    return bool(_EIGHTFOLD_HOST_RE.search(url))


def _mine_eightfold_positions(text: str) -> str | None:
    """Extract a positions/jobPostings array from Eightfold's page-state JSON blob.

    Decodes HTML entities, then bracket-balances from the first matching key
    to find the full array. Returns compact JSON or None.
    """
    decoded = _html_lib.unescape(text)
    for key in ("positions", "jobPostings", "roles", "openings"):
        m = re.search(rf'"(?:{key})"\s*:\s*(\[)', decoded, re.IGNORECASE)
        if not m:
            continue
        start = m.start(1)
        depth = end = 0
        for i, ch in enumerate(decoded[start:], start):
            if ch == "[":
                depth += 1
            elif ch == "]":
                depth -= 1
                if depth == 0:
                    end = i + 1
                    break
        if end <= start:
            continue
        try:
            arr = _json.loads(decoded[start:end])
            if (
                arr
                and isinstance(arr, list)
                and isinstance(arr[0], dict)
                and any(k in arr[0] for k in ("name", "title", "id", "job_title"))
            ):
                log.info(
                    "extraction_utils.eightfold_mine_hit",
                    key=key,
                    count=len(arr),
                )
                return _json.dumps({key: arr}, ensure_ascii=False)
        except (ValueError, KeyError, IndexError):
            continue
    return None


def _find_eightfold_chunks(text: str, chunk_size: int, max_chunks: int) -> list[str]:
    """Rank 12 K slices by job-signal score, return top max_chunks in original order."""
    all_chunks = [text[i : i + chunk_size] for i in range(0, len(text), chunk_size)]
    scored = [
        (idx, len(_EF_JOB_SIGNAL.findall(c)) * 3 - len(_EF_CONFIG_NOISE.findall(c)))
        for idx, c in enumerate(all_chunks)
    ]
    scored.sort(key=lambda x: x[1], reverse=True)
    selected = sorted(idx for idx, _ in scored[:max_chunks])
    return [all_chunks[i] for i in selected]


def _parse_ef_llm_json(text: str) -> list[dict]:
    if "</think>" in text:
        text = text.split("</think>")[-1]
    text = text.strip()
    if not text or text in ("null", "{}", "[]"):
        return []
    if text.startswith("```"):
        text = re.sub(r"^```[a-z]*\n?", "", text, flags=re.M)
        text = re.sub(r"\n?```$", "", text, flags=re.M)
        text = text.strip()
    try:
        result = _json.loads(text)
        if isinstance(result, list):
            return result
        if isinstance(result, dict):
            for key in ("jobs", "positions", "jobPostings", "data", "results", "listings", "openings"):
                if isinstance(result.get(key), list):
                    return result[key]
        return []
    except (ValueError, _json.JSONDecodeError):
        pass
    m = re.search(r"\[.*\]", text, re.DOTALL)
    if m:
        try:
            return _json.loads(m.group())
        except (ValueError, _json.JSONDecodeError):
            pass
    return []


async def extract_jobs_eightfold(
    content: str,
    source_url: str,
    company_slug: str,
    company_name: str | None = None,
) -> list[JobIn]:
    """Eightfold AI-specific LLM extraction — called as fallback only.

    Only reached when source_url contains .eightfold.ai AND the generic
    three-layer pipeline returned 0 jobs.  Tries JSON mining first (no extra
    LLM cost), then smart chunk selection to skip the themeOptions config blob.
    """
    import asyncio

    from openai import AsyncOpenAI

    from app.config import settings

    if not content.strip() or not settings.tensormux_api_key:
        log.warning("extraction_utils.eightfold_skip", company=company_slug)
        return []

    base_m = _EIGHTFOLD_BASE_RE.search(source_url)
    base_url = base_m.group(1) if base_m else source_url.rstrip("/")
    company_display = company_name or company_slug

    client = AsyncOpenAI(
        api_key=settings.tensormux_api_key,
        base_url="https://api.tensormux.com/v1",
        default_headers={
            "HTTP-Referer": "https://daily-intel.app",
            "X-Title": "Daily Intel",
        },
    )

    mined = _mine_eightfold_positions(content)
    if mined:
        chunks     = [mined[i : i + _EF_CHUNK_SIZE] for i in range(0, len(mined), _EF_CHUNK_SIZE)][:_EF_MAX_CHUNKS]
        prompt_tpl = _EF_JSON_PROMPT
        log.info("extraction_utils.eightfold_json_path", company=company_slug, chars=len(mined))
    else:
        decoded    = _html_lib.unescape(content)
        chunks     = _find_eightfold_chunks(decoded, _EF_CHUNK_SIZE, _EF_MAX_CHUNKS)
        prompt_tpl = _EF_TEXT_PROMPT
        log.info("extraction_utils.eightfold_text_path", company=company_slug, chunks=len(chunks))

    seen: set[str] = set()
    raw_jobs: list[dict] = []

    for chunk in chunks:
        prompt = prompt_tpl.format(
            base_url=base_url,
            company=company_display,
            source_url=source_url,
            content=chunk,
        )
        raw = ""
        for attempt in range(1, 3):
            try:
                resp = await client.chat.completions.create(
                    model=settings.tensormux_model,
                    messages=[{"role": "user", "content": prompt}],
                    temperature=0,
                    timeout=60,
                )
                if resp.choices:
                    raw = (resp.choices[0].message.content or "").strip()
                if raw:
                    break
                await asyncio.sleep(4)
            except Exception as exc:
                log.warning(
                    "extraction_utils.eightfold_llm_fail",
                    company=company_slug,
                    attempt=attempt,
                    error=str(exc),
                )
                if attempt < 2:
                    await asyncio.sleep(6)

        for j in _parse_ef_llm_json(raw):
            if not isinstance(j, dict):
                continue
            title = str(j.get("job_title") or "").strip()
            if not title or title.lower()[:60] in seen:
                continue
            seen.add(title.lower()[:60])
            url_val = str(j.get("job_url") or source_url)
            if url_val.startswith("/"):
                url_val = base_url + url_val
            raw_jobs.append({**j, "job_url": url_val})

    log.info("extraction_utils.eightfold_done", company=company_slug, jobs=len(raw_jobs))

    return [
        JobIn(
            company=str(j.get("company") or company_display),
            job_title=str(j.get("job_title") or "").strip(),
            location=j.get("location") or None,
            description=(str(j.get("description") or "")[:300]) or None,
            job_url=j.get("job_url") or source_url,
            source_type=f"eightfold_{company_slug}",
        )
        for j in raw_jobs
    ]


# ── Public entry point ────────────────────────────────────────────────────────

async def extract_jobs_from_html(
    html: str,
    source_url: str,
    company_slug: str,
) -> list[JobIn]:
    """V3 three-layer extraction pipeline.

    1. Embedded JSON mining  (no LLM cost — tries SPA state data first)
    2. Job-region isolation  (find job-dense substring)
    3. Chunked LLM extraction (split, extract, merge, dedup)

    Works on both raw HTML (Browserbase output) and pre-processed
    text/markdown (Apify crawler output) — the embedded JSON step is
    a no-op on plain text but the region isolation + chunking still help.
    """
    # 1. Embedded JSON first
    embedded = extract_embedded_json(html)
    if embedded:
        log.info(
            "extraction_utils.using_embedded_json",
            company=company_slug,
            chars=len(embedded),
        )
        jobs = await extract_jobs_chunked(embedded, source_url, company_slug)
        if jobs:
            log.info(
                "extraction_utils.embedded_json_hit",
                company=company_slug,
                jobs=len(jobs),
            )
            return jobs
        log.info("extraction_utils.embedded_json_empty", company=company_slug)

    # 2. Isolate job-dense region, then chunk-extract
    region = isolate_job_region(html)
    log.info(
        "extraction_utils.region_extraction",
        company=company_slug,
        region_chars=len(region),
        html_total=len(html),
    )
    jobs = await extract_jobs_chunked(region, source_url, company_slug)

    # 3. Eightfold fallback — only when URL is *.eightfold.ai and got 0 jobs.
    #    The generic pipeline fails on Eightfold because a 180-220 K themeOptions
    #    JSON blob fills all chunks before any job card is reached.
    if not jobs and _is_eightfold_url(source_url):
        log.info("extraction_utils.eightfold_fallback_triggered", company=company_slug)
        jobs = await extract_jobs_eightfold(html, source_url, company_slug)

    return jobs
