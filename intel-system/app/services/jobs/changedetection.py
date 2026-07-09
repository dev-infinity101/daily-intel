"""changedetection.io webhook handler.

When a monitored careers page changes, changedetection.io fires a webhook to
POST /ingest/jobs/changedetection-webhook. This module validates the payload
and routes the diff HTML to Gemini for structured job extraction.
"""
import hashlib
import hmac
import json
import time
import structlog

from app.config import settings
from app.schemas.job import JobIn

log = structlog.get_logger()


class ExtractionError(RuntimeError):
    """Raised when Gemini extraction fails so callers can distinguish LLM errors from empty results."""


def verify_webhook_signature(body: bytes, signature: str) -> bool:
    if not settings.changedetection_webhook_secret:
        return True  # skip verification in local dev if no secret set
    expected = hmac.new(
        settings.changedetection_webhook_secret.encode(),
        body,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


# [DIAG-PATCH-APPLIED]
def _parse_llm_json_safe(text: str) -> list:
    """Parse LLM JSON with fallbacks for common model quirks."""
    import re as _re
    text = text.strip()
    if not text or text in ("null", "{}", "[]"):
        return []
    try:
        result = json.loads(text)
        if isinstance(result, list):
            return result
        if isinstance(result, dict):
            for key in ("jobs", "data", "results", "listings", "items", "positions"):
                if isinstance(result.get(key), list):
                    return result[key]
        return []
    except (json.JSONDecodeError, ValueError):
        pass
    m = _re.search(r'\[.*\]', text, _re.DOTALL)
    if m:
        try:
            return json.loads(m.group())
        except (json.JSONDecodeError, ValueError):
            pass
    if _re.search(r'\b(no\s+jobs?|0\s+jobs?|none\s+found|empty|nothing)\b', text, _re.I):
        return []
    return []


async def extract_jobs_from_diff(diff_html: str, source_url: str) -> list[JobIn]:
    """Send the page content to OpenRouter/Tencent-Hunyuan and extract structured job listings."""
    if not settings.openrouter_api_key:
        log.warning("changedetection.openrouter_key_missing")
        return []

    import asyncio
    import json
    from openai import AsyncOpenAI, APIStatusError

    client = AsyncOpenAI(
        api_key=settings.openrouter_api_key,
        base_url="https://openrouter.ai/api/v1",
        default_headers={
            "HTTP-Referer": "https://daily-intel.app",
            "X-Title": "Daily Intel",
        },
    )

    prompt = f"""You are a job listing extractor. Extract ALL job listings from the careers page content below.

Return ONLY a valid JSON array — no markdown, no explanation, no fences.
Each element MUST have:
  "job_title": string (exact full title as shown — preserve domain prefix like "EV", "Electric", "Charging", required)
  "company": string (company name, required)
  "location": string or null
  "job_url": string or null (direct link to apply/view)
  "description": string max 500 chars — include: role summary, key responsibilities,
                 domain context (e.g. EV charging, fleet, electric vehicle, mobility software,
                 EVSE, last mile, emobility), required technologies or sector keywords

Rules:
- Extract EVERY visible job regardless of department, domain, or seniority
- Preserve domain keywords (EV, Electric Vehicle, Emobility, Charging, EVSE, Last Mile, Mobility) in the job_title exactly as they appear
- Include domain and industry context in the description field
- If the page has NO job listings at all, return []
- Output raw JSON only — no markdown code fences

Source URL: {source_url}
Content:
{diff_html}"""

    last_exc: Exception | None = None
    for attempt in range(1, 4):
        try:
            response = await client.chat.completions.create(
                model=settings.openrouter_model,
                messages=[{"role": "user", "content": prompt}],
                temperature=0,
            )
            if not response.choices:
                log.warning("changedetection.empty_choices", url=source_url, attempt=attempt, model=settings.openrouter_model)
                if attempt < 3:
                    await asyncio.sleep(5 * attempt)
                    continue
                return []
            content = response.choices[0].message.content
            # [DIAG-PATCH-APPLIED]
            if not content or not content.strip():
                log.warning("changedetection.null_content", url=source_url, attempt=attempt, model=settings.openrouter_model)
                if attempt < 3:
                    wait = 3 * attempt
                    await asyncio.sleep(wait)
                    continue
                return []
            text = content.strip()
            if text.startswith("```"):
                text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()
            raw_jobs = _parse_llm_json_safe(text)  # [DIAG-PATCH-APPLIED]
            return [
                JobIn(
                    company=j.get("company") or source_url,
                    job_title=j.get("job_title") or "Role",
                    location=j.get("location"),
                    job_url=j.get("job_url") or source_url,
                    description=(j.get("description") or "")[:500],
                    source_type="changedetection",
                )
                for j in raw_jobs
                if isinstance(j, dict)
            ]
        except APIStatusError as exc:
            last_exc = exc
            if exc.status_code in (429, 503) and attempt < 3:
                wait = 5 * attempt
                log.warning(
                    "changedetection.openrouter_unavailable_retrying",
                    url=source_url,
                    attempt=attempt,
                    wait_seconds=wait,
                )
                await asyncio.sleep(wait)
                continue
            break
        except Exception as exc:
            last_exc = exc
            break

    log.exception(
        "changedetection.extraction_failed",
        url=source_url,
        error=str(last_exc),
        exc_info=last_exc,
    )
    raise ExtractionError(f"OpenRouter extraction failed for {source_url}: {last_exc}") from last_exc
