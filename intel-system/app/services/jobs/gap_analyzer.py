"""Gap analyzer — determines which target companies lack coverage.

After LinkedIn Jobs Apify and Adzuna have scraped, this module analyses
the collected JobIn results to identify which target companies received
NO jobs at all. Companies that received jobs (even if those jobs were
subsequently filtered out by the classifier) are considered "covered".

The matching uses a two-pass strategy:
  1. Deterministic pass — slug normalisation + substring matching (fast, free)
  2. LLM pass — fuzzy matching via Tensormux for remaining unmatched targets

Only truly uncovered companies are forwarded to the T1→T2 career-page
scraping pipeline.
"""
import json
import re

import structlog

from app.config import settings
from app.schemas.job import JobIn

log = structlog.get_logger(__name__)


def _slugify(name: str) -> str:
    """Normalise a company name to a comparable slug."""
    s = name.lower().strip()
    s = re.sub(r"[^a-z0-9]+", " ", s)
    return s.strip()


def _match_companies_deterministic(
    scraped_names: set[str],
    target_companies: list,
) -> set[int]:
    """Fast deterministic matching: slug normalisation + substring.

    Returns set of target company IDs that matched.
    """
    matched_ids: set[int] = set()

    # Build normalised lookup sets
    scraped_slugs = {_slugify(n) for n in scraped_names}
    scraped_lower = {n.lower().strip() for n in scraped_names}

    for company in target_companies:
        display_slug = _slugify(company.display_name)
        db_slug = (company.slug or "").lower().replace("-", " ")

        # Exact slug match
        if display_slug in scraped_slugs or db_slug in scraped_slugs:
            matched_ids.add(company.id)
            continue

        # Substring match: target name appears in any scraped name or vice versa
        for scraped_name in scraped_lower:
            if (
                display_slug in scraped_name
                or scraped_name in display_slug
                or db_slug in scraped_name
                or scraped_name in db_slug
            ):
                matched_ids.add(company.id)
                break

    return matched_ids


async def _match_companies_llm(
    scraped_names: list[str],
    unmatched_targets: list,
) -> set[int]:
    """Use LLM to fuzzy-match remaining unmatched target companies.

    Single batched call — maps each unmatched target to the closest scraped
    company name, or null if no reasonable match exists.

    Returns set of target company IDs that the LLM matched.
    """
    if not unmatched_targets or not scraped_names:
        return set()

    if not settings.tensormux_api_key:
        log.warning("gap_analyzer.llm_disabled", reason="No tensormux_api_key")
        return set()

    target_list = [
        {"id": c.id, "name": c.display_name}
        for c in unmatched_targets
    ]

    prompt = f"""You are a company name matching expert. I have two lists:

SCRAPED JOB COMPANY NAMES (from job boards):
{json.dumps(scraped_names[:200], indent=2)}

TARGET COMPANIES TO MATCH:
{json.dumps([t["name"] for t in target_list], indent=2)}

For each TARGET company, determine if any SCRAPED company name refers to the
same organisation (considering abbreviations, subsidiaries, alternate names,
"Pvt Ltd" vs full name, etc.).

Return a JSON array with one object per target company, in order:
[
  {{"target": "Company Name", "matched_scraped": "Scraped Name" or null}},
  ...
]

Only match if you are reasonably confident they are the same company.
Return ONLY the JSON array, no markdown fences or explanation."""

    from app.utils.llm_client import call_llm_with_rate_limit
    from openai import AsyncOpenAI

    client = AsyncOpenAI(
        api_key=settings.tensormux_api_key,
        base_url="https://api.tensormux.com/v1",
        default_headers={
            "HTTP-Referer": "https://daily-intel.app",
            "X-Title": "Daily Intel",
        },
    )

    matched_ids: set[int] = set()

    try:
        response = await call_llm_with_rate_limit(
            client=client,
            model=settings.tensormux_model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0,
        )
        text = (response.choices[0].message.content or "").strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()

        results = json.loads(text)

        if isinstance(results, list):
            for i, result in enumerate(results):
                if i < len(target_list) and result.get("matched_scraped"):
                    matched_ids.add(target_list[i]["id"])
                    log.info(
                        "gap_analyzer.llm_match",
                        target=target_list[i]["name"],
                        matched=result["matched_scraped"],
                    )

    except Exception as exc:
        log.warning("gap_analyzer.llm_match_failed", error=str(exc))

    return matched_ids


async def analyze_coverage(
    scraped_jobs: list[JobIn],
    target_companies: list,
) -> list:
    """Determine which target companies are NOT covered by the scraped jobs.

    A target company is "covered" if ANY scraped job has a company name that
    matches the target — even if those jobs were later filtered out by the
    classifier. The key insight is: if a scraper returned results for a
    company, there's no need to do expensive career-page scraping.

    Args:
        scraped_jobs: All normalised JobIn objects from LinkedIn Jobs Apify + Adzuna
        target_companies: List of active TargetCompany ORM objects

    Returns:
        List of uncovered TargetCompany objects that need T1→T2 scraping.
    """
    if not target_companies:
        return []

    if not scraped_jobs:
        log.info("gap_analyzer.no_scraped_jobs", uncovered=len(target_companies))
        return list(target_companies)

    # Collect all unique company names from scraped results
    scraped_names: set[str] = {j.company for j in scraped_jobs if j.company}
    log.info(
        "gap_analyzer.start",
        scraped_companies=len(scraped_names),
        target_companies=len(target_companies),
    )

    # Pass 1: Deterministic matching
    matched_ids = _match_companies_deterministic(scraped_names, target_companies)
    log.info("gap_analyzer.deterministic_pass", matched=len(matched_ids))

    # Pass 2: LLM matching for remaining unmatched
    unmatched = [c for c in target_companies if c.id not in matched_ids]

    if unmatched and scraped_names:
        llm_matched_ids = await _match_companies_llm(
            list(scraped_names), unmatched
        )
        matched_ids |= llm_matched_ids
        log.info("gap_analyzer.llm_pass", additional_matched=len(llm_matched_ids))

    # Final uncovered list
    uncovered = [c for c in target_companies if c.id not in matched_ids]

    log.info(
        "gap_analyzer.complete",
        total_targets=len(target_companies),
        covered=len(matched_ids),
        uncovered=len(uncovered),
        uncovered_slugs=[c.slug for c in uncovered[:15]],
    )

    return uncovered
