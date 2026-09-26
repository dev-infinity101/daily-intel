"""Gap analyzer — determines which target companies lack coverage.

After LinkedIn Jobs Apify, Adzuna, or recent DB job ingestion, this module
analyses the collected JobIn results to identify which target companies
received NO jobs at all. Companies that received jobs (even if those jobs were
subsequently filtered out by the classifier) are considered "covered".

The matching uses a two-pass strategy:
  1. Deterministic pass — slug normalisation + substring matching (fast, free)
  2. LLM pass — fuzzy matching via Tensormux for remaining unmatched targets

Only truly uncovered companies are forwarded to the T1→T2 career-page
scraping pipeline.
"""
import json
import re
from typing import Any

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
) -> dict[int, str]:
    """Fast deterministic matching: slug normalisation + substring.

    Returns dict mapping target company ID -> matched scraped company name.
    """
    matched: dict[int, str] = {}

    # Build normalised lookup sets and map back to original scraped names
    scraped_slug_map = {_slugify(n): n for n in scraped_names if n}
    scraped_lower_map = {n.lower().strip(): n for n in scraped_names if n}

    for company in target_companies:
        display_slug = _slugify(company.display_name)
        db_slug = (company.slug or "").lower().replace("-", " ")

        # Exact slug match
        if display_slug in scraped_slug_map:
            matched[company.id] = scraped_slug_map[display_slug]
            continue
        if db_slug in scraped_slug_map:
            matched[company.id] = scraped_slug_map[db_slug]
            continue

        # Substring match: target name appears in any scraped name or vice versa
        for s_lower, orig_name in scraped_lower_map.items():
            if (
                display_slug in s_lower
                or s_lower in display_slug
                or db_slug in s_lower
                or s_lower in db_slug
            ):
                matched[company.id] = orig_name
                break

    return matched


async def _match_companies_llm(
    scraped_names: list[str],
    unmatched_targets: list,
) -> dict[int, str]:
    """Use LLM to fuzzy-match remaining unmatched target companies.

    Single batched call — maps each unmatched target to the closest scraped
    company name, or null if no reasonable match exists.

    Returns dict mapping target company ID -> matched scraped company name.
    """
    if not unmatched_targets or not scraped_names:
        return {}

    if not settings.tensormux_api_key:
        log.warning("gap_analyzer.llm_disabled", reason="No tensormux_api_key")
        return {}

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

    matched: dict[int, str] = {}

    try:
        response = await call_llm_with_rate_limit(
            client=client,
            model=settings.tensormux_model,
            messages=[{"role": "user", "content": prompt}],
            temperature=0.01,
        )
        text = (response.choices[0].message.content or "").strip()
        if text.startswith("```"):
            text = text.split("\n", 1)[1].rsplit("```", 1)[0].strip()

        results = json.loads(text)

        if isinstance(results, list):
            for i, result in enumerate(results):
                if i < len(target_list) and result.get("matched_scraped"):
                    matched_name = str(result["matched_scraped"])
                    target_id = target_list[i]["id"]
                    matched[target_id] = matched_name
                    log.info(
                        "gap_analyzer.llm_match",
                        target=target_list[i]["name"],
                        matched=matched_name,
                    )

    except Exception as exc:
        log.warning("gap_analyzer.llm_match_failed", error=str(exc))

    return matched


async def analyze_coverage_detailed(
    scraped_jobs: list[JobIn],
    target_companies: list,
) -> dict[str, Any]:
    """Detailed coverage analysis returning structured diagnostics and metadata.

    Returns:
        {
            "covered": [
                {
                    "id": int,
                    "slug": str,
                    "display_name": str,
                    "matched_via": "deterministic" | "llm",
                    "matched_scraped_name": str,
                }
            ],
            "uncovered": [
                {
                    "id": int,
                    "slug": str,
                    "display_name": str,
                    "priority": int,
                    "preferred_scraper": str | None,
                    "last_success_at": str | None,
                }
            ],
            "uncovered_objects": list[TargetCompany],
            "stats": {
                "total_targets": int,
                "covered_count": int,
                "uncovered_count": int,
                "deterministic_matches": int,
                "llm_matches": int,
                "coverage_pct": float,
                "scraped_jobs_count": int,
                "unique_scraped_companies_count": int,
            },
            "unique_scraped_companies": list[str],
        }
    """
    if not target_companies:
        return {
            "covered": [],
            "uncovered": [],
            "uncovered_objects": [],
            "stats": {
                "total_targets": 0,
                "covered_count": 0,
                "uncovered_count": 0,
                "deterministic_matches": 0,
                "llm_matches": 0,
                "coverage_pct": 0.0,
                "scraped_jobs_count": 0,
                "unique_scraped_companies_count": 0,
            },
            "unique_scraped_companies": [],
        }

    scraped_names: set[str] = {j.company.strip() for j in scraped_jobs if j.company and j.company.strip()}

    if not scraped_names:
        uncovered_info = [
            {
                "id": c.id,
                "slug": c.slug,
                "display_name": c.display_name,
                "priority": c.priority,
                "preferred_scraper": c.preferred_scraper,
                "last_success_at": str(c.last_success_at) if c.last_success_at else None,
            }
            for c in target_companies
        ]
        return {
            "covered": [],
            "uncovered": uncovered_info,
            "uncovered_objects": list(target_companies),
            "stats": {
                "total_targets": len(target_companies),
                "covered_count": 0,
                "uncovered_count": len(target_companies),
                "deterministic_matches": 0,
                "llm_matches": 0,
                "coverage_pct": 0.0,
                "scraped_jobs_count": len(scraped_jobs),
                "unique_scraped_companies_count": 0,
            },
            "unique_scraped_companies": [],
        }

    # Pass 1: Deterministic
    det_matched = _match_companies_deterministic(scraped_names, target_companies)
    log.info("gap_analyzer.deterministic_pass", matched=len(det_matched))

    # Pass 2: LLM
    unmatched_for_llm = [c for c in target_companies if c.id not in det_matched]
    llm_matched: dict[int, str] = {}
    if unmatched_for_llm and scraped_names:
        llm_matched = await _match_companies_llm(list(scraped_names), unmatched_for_llm)
        log.info("gap_analyzer.llm_pass", additional_matched=len(llm_matched))

    # Build covered & uncovered structures
    company_by_id = {c.id: c for c in target_companies}
    covered: list[dict[str, Any]] = []

    for cid, matched_name in det_matched.items():
        comp = company_by_id[cid]
        covered.append({
            "id": comp.id,
            "slug": comp.slug,
            "display_name": comp.display_name,
            "matched_via": "deterministic",
            "matched_scraped_name": matched_name,
        })

    for cid, matched_name in llm_matched.items():
        comp = company_by_id[cid]
        covered.append({
            "id": comp.id,
            "slug": comp.slug,
            "display_name": comp.display_name,
            "matched_via": "llm",
            "matched_scraped_name": matched_name,
        })

    all_matched_ids = set(det_matched.keys()) | set(llm_matched.keys())
    uncovered_objects = [c for c in target_companies if c.id not in all_matched_ids]

    uncovered: list[dict[str, Any]] = [
        {
            "id": c.id,
            "slug": c.slug,
            "display_name": c.display_name,
            "priority": c.priority,
            "preferred_scraper": c.preferred_scraper,
            "last_success_at": str(c.last_success_at) if c.last_success_at else None,
        }
        for c in uncovered_objects
    ]

    total = len(target_companies)
    covered_cnt = len(covered)
    coverage_pct = round((covered_cnt / total) * 100, 1) if total > 0 else 0.0

    return {
        "covered": covered,
        "uncovered": uncovered,
        "uncovered_objects": uncovered_objects,
        "stats": {
            "total_targets": total,
            "covered_count": covered_cnt,
            "uncovered_count": len(uncovered),
            "deterministic_matches": len(det_matched),
            "llm_matches": len(llm_matched),
            "coverage_pct": coverage_pct,
            "scraped_jobs_count": len(scraped_jobs),
            "unique_scraped_companies_count": len(scraped_names),
        },
        "unique_scraped_companies": sorted(list(scraped_names)),
    }


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
    detailed = await analyze_coverage_detailed(scraped_jobs, target_companies)
    uncovered = detailed["uncovered_objects"]
    stats = detailed["stats"]

    log.info(
        "gap_analyzer.complete",
        total_targets=stats["total_targets"],
        covered=stats["covered_count"],
        uncovered=stats["uncovered_count"],
        uncovered_slugs=[c.slug for c in uncovered[:15]],
    )

    return uncovered
