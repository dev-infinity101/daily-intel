"""Regression tests for job filtering across LinkedIn Jobs Apify and Adzuna pipelines.

These are pure unit tests — no DB or network calls. They verify that the
classifier correctly accepts or rejects jobs based on title, description,
location, and source_type after the domain bypass was removed.
"""
from app.services.jobs.classifier import evaluate_job_filter
from app.services.jobs.free_apis.adzuna import normalize_adzuna_job


# ═══════════════════════════════════════════════════════════════════════════════
#  LinkedIn Jobs Apify pipeline
# ═══════════════════════════════════════════════════════════════════════════════

class TestLinkedInJobsApifyFiltering:
    """Jobs from linkedin_jobs_apify must pass EV domain, India location,
    and business role filters — no domain bypass."""

    def test_accepts_ev_charging_deputy_manager_india(self) -> None:
        """EV keyword in title + India location + business role → pass."""
        result = evaluate_job_filter(
            "Deputy Manager - EV Charging Operations",
            "Manage EV charging station rollouts across Maharashtra.",
            location="Pune, Maharashtra, India",
            source_type="linkedin_jobs_apify",
        )
        assert result.passed is True
        assert result.location_status == "india_match"
        assert result.role_status == "target_role"
        assert result.score > 0

    def test_accepts_ev_keyword_in_description_only(self) -> None:
        """EV keyword in description (not title) + India + business role → pass."""
        result = evaluate_job_filter(
            "Deputy Manager - Business Development",
            "Drive growth for EV charging infrastructure partnerships across India.",
            location="Mumbai, India",
            source_type="linkedin_jobs_apify",
        )
        assert result.passed is True
        assert result.domain_status in (
            "domain_keyword_present",
            "domain_keyword_in_title",
            "multiple_domain_keywords_in_description",
        )

    def test_rejects_non_ev_deputy_manager(self) -> None:
        """Deputy Manager in banking with no EV keywords → rejected by domain."""
        result = evaluate_job_filter(
            "Deputy Manager - Retail Banking",
            "Manage retail banking operations and customer relationships.",
            location="Delhi, India",
            source_type="linkedin_jobs_apify",
        )
        assert result.passed is False
        assert result.domain_status == "no_primary_ev_mobility_domain"

    def test_rejects_fmcg_deputy_manager(self) -> None:
        """Deputy Manager in FMCG with no EV keywords → rejected."""
        result = evaluate_job_filter(
            "Deputy Manager - Sales FMCG",
            "Drive FMCG product distribution and retail partnerships.",
            location="Bengaluru, India",
            source_type="linkedin_jobs_apify",
        )
        assert result.passed is False
        assert result.domain_status == "no_primary_ev_mobility_domain"

    def test_rejects_pharma_deputy_manager(self) -> None:
        """Deputy Manager in pharma with no EV keywords → rejected."""
        result = evaluate_job_filter(
            "Deputy Manager - Pharma Sales",
            "Lead pharma product launches and territory management.",
            location="Hyderabad, India",
            source_type="linkedin_jobs_apify",
        )
        assert result.passed is False
        assert result.domain_status == "no_primary_ev_mobility_domain"

    def test_rejects_engineering_role_even_with_ev(self) -> None:
        """EV keyword present but engineering role → rejected by role filter."""
        result = evaluate_job_filter(
            "EV Charging Software Engineer",
            "Build OCPP backend services for EV charging stations.",
            location="Gurugram, India",
            source_type="linkedin_jobs_apify",
        )
        assert result.passed is False
        assert result.role_status == "engineering_heavy_role"

    def test_rejects_non_india_ev_job(self) -> None:
        """EV keyword + business role but non-India location → rejected."""
        result = evaluate_job_filter(
            "Deputy Manager - EV Fleet Operations",
            "Manage EV fleet across Southeast Asia.",
            location="Jakarta, Indonesia",
            source_type="linkedin_jobs_apify",
        )
        assert result.passed is False
        assert result.location_status == "explicit_non_india"

    def test_rejects_hr_role_with_ev_keywords(self) -> None:
        """HR role at an EV company → rejected (unrelated business role)."""
        result = evaluate_job_filter(
            "HR Manager - Talent Acquisition",
            "Recruit talent for our growing EV charging business.",
            location="Noida, India",
            source_type="linkedin_jobs_apify",
        )
        assert result.passed is False
        assert result.role_status == "unrelated_business_role"

    def test_accepts_emobility_manager(self) -> None:
        """E-Mobility keyword in title → domain pass."""
        result = evaluate_job_filter(
            "Manager - E-Mobility Solutions",
            "Drive e-mobility product strategy.",
            location="Chennai, Tamil Nadu, India",
            source_type="linkedin_jobs_apify",
        )
        assert result.passed is True
        assert result.role_status == "target_role"

    def test_accepts_fleet_operations_lead(self) -> None:
        """Fleet keyword in title → domain pass."""
        result = evaluate_job_filter(
            "Lead - Fleet Operations & Mobility",
            "Oversee fleet management and last-mile delivery operations.",
            location="Bengaluru, India",
            source_type="linkedin_jobs_apify",
        )
        assert result.passed is True


# ═══════════════════════════════════════════════════════════════════════════════
#  Adzuna pipeline
# ═══════════════════════════════════════════════════════════════════════════════

class TestAdzunaFiltering:
    """Jobs from adzuna must pass EV domain, India location,
    and business role filters — no domain bypass."""

    def test_accepts_ev_keyword_in_title(self) -> None:
        """EV keyword in Adzuna job title → passes domain filter."""
        result = evaluate_job_filter(
            "EV Charging Business Development Manager",
            "Found via: EV charging. Category: Sales.",
            location="Mumbai, India",
            source_type="adzuna",
        )
        assert result.passed is True
        assert result.location_status == "india_match"

    def test_accepts_ev_keyword_from_search_context(self) -> None:
        """No EV in title, but search keyword in description → passes domain."""
        result = evaluate_job_filter(
            "Deputy Manager - Operations",
            "Found via: EV charging. Category: Management.",
            location="Pune, India",
            source_type="adzuna",
        )
        # Domain check sees "EV charging" in description
        assert result.passed is True

    def test_rejects_non_ev_adzuna_job(self) -> None:
        """Job from watchlist company but no EV keywords anywhere → rejected."""
        result = evaluate_job_filter(
            "Audit Manager",
            "Found via: EV jobs KPMG. Category: Accounting.",
            location="Delhi, India",
            source_type="adzuna",
        )
        # "EV" appears in the search keyword context! But the job itself
        # (title = "Audit Manager") fails the role filter, not the domain.
        # The domain sees "EV" and passes. But role filter catches it.
        # Actually "Audit" matches _UNRELATED_BUSINESS_ROLES
        assert result.passed is False

    def test_rejects_pure_finance_role(self) -> None:
        """Finance role even with EV keyword context → rejected by role."""
        result = evaluate_job_filter(
            "Finance Manager - Accounting",
            "Found via: EV jobs Nestle. Category: Finance.",
            location="Mumbai, India",
            source_type="adzuna",
        )
        assert result.passed is False

    def test_rejects_non_india_adzuna_job(self) -> None:
        """EV keyword but non-India location → rejected."""
        result = evaluate_job_filter(
            "EV Fleet Manager",
            "Found via: Emobility. Category: Transport.",
            location="London, UK",
            source_type="adzuna",
        )
        assert result.passed is False
        assert result.location_status == "explicit_non_india"

    def test_rejects_engineering_adzuna_job(self) -> None:
        """EV keyword + India but engineering role → rejected."""
        result = evaluate_job_filter(
            "EV Battery Test Engineer",
            "Found via: EV charging. Category: Engineering.",
            location="Bengaluru, India",
            source_type="adzuna",
        )
        assert result.passed is False
        assert result.role_status == "engineering_heavy_role"

    def test_accepts_emobility_sales_role(self) -> None:
        """Emobility in description from search keyword → domain passes."""
        result = evaluate_job_filter(
            "Sales Manager - Clean Energy",
            "Found via: Emobility. Category: Sales.",
            location="Hyderabad, India",
            source_type="adzuna",
        )
        # "Emobility" in description provides domain match
        assert result.passed is True


# ═══════════════════════════════════════════════════════════════════════════════
#  Adzuna normalizer — description enrichment
# ═══════════════════════════════════════════════════════════════════════════════

class TestAdzunaNormalizer:
    """Verify that normalize_adzuna_job injects search keyword into description."""

    def test_description_includes_search_keyword(self) -> None:
        raw = {
            "id": "12345",
            "title": "Deputy Manager - Operations",
            "company": {"display_name": "TechCo"},
            "location": {"display_name": "Mumbai, India"},
            "redirect_url": "https://example.com/job/12345",
            "category": {"label": "Management"},
            "_search_keyword": "EV charging",
        }
        job = normalize_adzuna_job(raw)
        assert job is not None
        assert "EV charging" in job.description
        assert "Management" in job.description

    def test_description_without_search_keyword(self) -> None:
        raw = {
            "id": "12346",
            "title": "Manager",
            "company": {"display_name": "Corp"},
            "location": {"display_name": "Delhi"},
            "redirect_url": "https://example.com/job/12346",
            "category": {"label": "Sales"},
        }
        job = normalize_adzuna_job(raw)
        assert job is not None
        # No _search_keyword → no "Found via:" prefix
        assert "Found via:" not in job.description
        # Category still included
        assert "Sales" in job.description

    def test_description_includes_raw_description(self) -> None:
        raw = {
            "id": "12347",
            "title": "EV Sales Lead",
            "company": {"display_name": "EVCo"},
            "location": {"display_name": "Pune, India"},
            "redirect_url": "https://example.com/job/12347",
            "category": {"label": "Sales"},
            "_search_keyword": "EV charging",
            "description": "Looking for a sales leader to drive EV charging growth.",
        }
        job = normalize_adzuna_job(raw)
        assert job is not None
        assert "EV charging" in job.description
        assert "sales leader" in job.description

    def test_source_type_is_adzuna(self) -> None:
        raw = {
            "id": "12348",
            "title": "Manager",
            "company": {"display_name": "Corp"},
            "location": {"display_name": "Delhi"},
            "redirect_url": "https://example.com/job/12348",
        }
        job = normalize_adzuna_job(raw)
        assert job is not None
        assert job.source_type == "adzuna"


# ═══════════════════════════════════════════════════════════════════════════════
#  Cross-source parity: same job title behaves identically
# ═══════════════════════════════════════════════════════════════════════════════

class TestCrossSourceParity:
    """After domain bypass removal, adzuna and linkedin_jobs_apify should
    be filtered the same way as any non-target source."""

    def test_non_ev_job_rejected_across_all_sources(self) -> None:
        """A non-EV job should be rejected regardless of source_type."""
        for source in ["adzuna", "linkedin_jobs_apify", "unknown", ""]:
            result = evaluate_job_filter(
                "Deputy Manager - Retail Banking",
                "Manage retail banking operations.",
                location="Delhi, India",
                source_type=source,
            )
            assert result.passed is False, (
                f"Non-EV job should be rejected for source={source!r}, "
                f"but got passed=True"
            )

    def test_ev_business_role_accepted_across_sources(self) -> None:
        """A genuine EV business role should pass regardless of source_type."""
        for source in ["adzuna", "linkedin_jobs_apify", "unknown"]:
            result = evaluate_job_filter(
                "Deputy Manager - EV Charging Operations",
                "Manage EV charging rollout and partnerships.",
                location="Bengaluru, India",
                source_type=source,
            )
            assert result.passed is True, (
                f"EV business role should pass for source={source!r}, "
                f"but got passed=False (reason={result.reason})"
            )

    def test_target_company_still_bypasses_domain(self) -> None:
        """Target companies should still bypass the domain check."""
        result = evaluate_job_filter(
            "Deputy Manager - Operations",
            "General operations management.",
            location="Mumbai, India",
            is_target=True,
            source_type="apify_tata-motors",
        )
        assert result.domain_status == "target_company_assumed_ev"
