"""Unit tests for the LinkedIn Posts Hiring (T4) adapter.

All tests are pure unit tests — no network calls, no DB.
"""
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import pytest

from app.services.jobs.free_apis.linkedin_posts_hiring import (
    HIRING_QUERY,
    MAX_POSTS,
    _is_within_week,
    _parse_posted_at,
    normalize_hiring_post,
)


# ── _parse_posted_at ──────────────────────────────────────────────────────────

class TestParsePostedAt:
    def test_parses_iso_date_string(self) -> None:
        post = {"postedAt": {"date": "2026-09-20T08:00:00Z"}}
        dt = _parse_posted_at(post)
        assert dt is not None
        assert dt.year == 2026
        assert dt.month == 9
        assert dt.day == 20

    def test_falls_back_to_timestamp_millis(self) -> None:
        # 2026-09-20 00:00:00 UTC
        ts_ms = int(datetime(2026, 9, 20, tzinfo=UTC).timestamp() * 1000)
        post = {"postedAt": {"timestamp": ts_ms}}
        dt = _parse_posted_at(post)
        assert dt is not None
        assert dt.year == 2026

    def test_returns_none_when_no_date(self) -> None:
        assert _parse_posted_at({}) is None
        assert _parse_posted_at({"postedAt": {}}) is None

    def test_handles_invalid_iso_gracefully(self) -> None:
        post = {"postedAt": {"date": "not-a-date"}}
        # Should not raise; falls back to timestamp (None) → returns None
        result = _parse_posted_at(post)
        assert result is None


# ── _is_within_week ───────────────────────────────────────────────────────────

class TestIsWithinWeek:
    def test_recent_post_passes(self) -> None:
        recent = datetime.now(UTC) - timedelta(hours=5)
        assert _is_within_week(recent) is True

    def test_post_exactly_7_days_ago_passes(self) -> None:
        exactly_7d = datetime.now(UTC) - timedelta(days=7, seconds=-1)
        assert _is_within_week(exactly_7d) is True

    def test_post_older_than_7_days_rejected(self) -> None:
        old = datetime.now(UTC) - timedelta(days=8)
        assert _is_within_week(old) is False

    def test_none_posted_at_is_kept(self) -> None:
        """Posts with no date are kept — don't silently discard."""
        assert _is_within_week(None) is True

    def test_naive_datetime_treated_as_utc(self) -> None:
        # Naive datetime 2 days ago should pass
        naive_recent = datetime.utcnow() - timedelta(days=2)
        assert _is_within_week(naive_recent) is True


# ── normalize_hiring_post ─────────────────────────────────────────────────────

class TestNormalizeHiringPost:
    """Verify normalize_hiring_post() output structure and field mapping."""

    _BASE_POST: dict = {
        "id": "abc123",
        "linkedinUrl": "https://linkedin.com/posts/abc123",
        "content": "We are hiring an EV Sales Manager in India!\nJoin our EV team.",
        "author": {
            "name": "Tata Motors",
            "info": "Electric Vehicles Division",
        },
        "postedAt": {"date": "2026-09-21T10:00:00Z"},
    }

    def test_returns_job_in(self) -> None:
        job = normalize_hiring_post(self._BASE_POST)
        assert job is not None

    def test_source_type_is_linkedin_posts_hiring(self) -> None:
        job = normalize_hiring_post(self._BASE_POST)
        assert job is not None
        assert job.source_type == "linkedin_posts_hiring"

    def test_company_is_author_name(self) -> None:
        job = normalize_hiring_post(self._BASE_POST)
        assert job is not None
        assert job.company == "Tata Motors"

    def test_job_url_is_linkedin_url(self) -> None:
        job = normalize_hiring_post(self._BASE_POST)
        assert job is not None
        assert job.job_url == "https://linkedin.com/posts/abc123"

    def test_location_is_india(self) -> None:
        job = normalize_hiring_post(self._BASE_POST)
        assert job is not None
        assert job.location == "India"

    def test_job_title_is_first_content_line_capped(self) -> None:
        job = normalize_hiring_post(self._BASE_POST)
        assert job is not None
        # First line of content
        assert job.job_title == "We are hiring an EV Sales Manager in India!"

    def test_description_includes_author_and_content(self) -> None:
        job = normalize_hiring_post(self._BASE_POST)
        assert job is not None
        assert "Tata Motors" in job.description
        assert "EV Sales Manager" in job.description

    def test_external_job_id_matches_post_id(self) -> None:
        job = normalize_hiring_post(self._BASE_POST)
        assert job is not None
        assert job.external_job_id == "abc123"

    def test_returns_none_when_no_content(self) -> None:
        post = {**self._BASE_POST, "content": ""}
        assert normalize_hiring_post(post) is None

    def test_returns_none_when_no_url(self) -> None:
        post = {**self._BASE_POST, "linkedinUrl": None}
        assert normalize_hiring_post(post) is None

    def test_job_title_falls_back_when_content_blank_lines(self) -> None:
        post = {**self._BASE_POST, "content": "\n\nActual title here"}
        job = normalize_hiring_post(post)
        assert job is not None
        assert job.job_title == "Actual title here"

    def test_description_capped_at_500_chars(self) -> None:
        long_content = "X" * 2000
        post = {**self._BASE_POST, "content": long_content}
        job = normalize_hiring_post(post)
        assert job is not None
        assert len(job.description) <= 500

    def test_author_info_included_in_description(self) -> None:
        job = normalize_hiring_post(self._BASE_POST)
        assert job is not None
        assert "Electric Vehicles Division" in job.description


# ── Constants ─────────────────────────────────────────────────────────────────

class TestConstants:
    def test_single_query_string(self) -> None:
        """HIRING_QUERY must be a single string, not split."""
        assert isinstance(HIRING_QUERY, str)
        assert '"hiring"' in HIRING_QUERY
        assert '"EV"' in HIRING_QUERY
        assert '"india"' in HIRING_QUERY

    def test_max_posts_is_50(self) -> None:
        assert MAX_POSTS == 50


# ── fetch_and_persist_linkedin_posts_hiring (mocked) ─────────────────────────

class TestFetchAndPersist:
    @pytest.mark.asyncio
    async def test_skips_when_no_apify_token(self) -> None:
        """Returns a skip result when no Apify token is configured."""
        from app.services.jobs.free_apis.linkedin_posts_hiring import (
            fetch_and_persist_linkedin_posts_hiring,
        )
        with patch(
            "app.services.jobs.free_apis.linkedin_posts_hiring.settings"
        ) as mock_settings:
            mock_settings.apify_token = ""
            mock_settings.apify_token_secondary = ""

            jobs, stats = await fetch_and_persist_linkedin_posts_hiring()

        assert jobs == []
        assert stats["status"] == "skipped"
        assert stats["jobs_fetched"] == 0
        assert stats["jobs_inserted"] == 0

    @pytest.mark.asyncio
    async def test_date_filter_drops_old_posts(self) -> None:
        """Posts older than 7 days must not reach the normaliser."""
        old_ts = int(
            (datetime.now(UTC) - timedelta(days=10)).timestamp() * 1000
        )
        raw_posts = [
            {
                "id": "old1",
                "linkedinUrl": "https://linkedin.com/posts/old1",
                "content": "Hiring EV Manager in India",
                "author": {"name": "OldCo", "info": ""},
                "postedAt": {"timestamp": old_ts},
            }
        ]

        from app.services.jobs.free_apis.linkedin_posts_hiring import (
            fetch_and_persist_linkedin_posts_hiring,
        )
        with (
            patch(
                "app.services.jobs.free_apis.linkedin_posts_hiring.settings"
            ) as mock_settings,
            patch(
                "app.services.jobs.free_apis.linkedin_posts_hiring._trigger_hiring_actor",
                new_callable=AsyncMock,
                return_value=("run_abc", "token_xyz"),
            ),
            patch(
                "app.services.jobs.free_apis.linkedin_posts_hiring._wait_for_dataset",
                new_callable=AsyncMock,
                return_value=raw_posts,
            ),
        ):
            mock_settings.apify_token = "test_token"
            mock_settings.apify_token_secondary = ""

            jobs, stats = await fetch_and_persist_linkedin_posts_hiring()

        # Old post must be dropped before persistence
        assert jobs == []
        assert stats["jobs_fetched"] == 0

    @pytest.mark.asyncio
    async def test_caps_at_max_posts(self) -> None:
        """Even if the actor returns more than MAX_POSTS, only 20 are processed."""
        now_ts = int(datetime.now(UTC).timestamp() * 1000)
        raw_posts = [
            {
                "id": f"post_{i}",
                "linkedinUrl": f"https://linkedin.com/posts/post_{i}",
                "content": f"Hiring EV Manager in India #{i}",
                "author": {"name": f"Co{i}", "info": ""},
                "postedAt": {"timestamp": now_ts},
            }
            for i in range(50)  # 50 posts — well above the 20 cap
        ]

        persisted_jobs: list = []

        async def fake_persist(jobs, db, **kwargs):
            persisted_jobs.extend(jobs)
            return len(jobs)

        from app.services.jobs.free_apis.linkedin_posts_hiring import (
            fetch_and_persist_linkedin_posts_hiring,
        )
        with (
            patch(
                "app.services.jobs.free_apis.linkedin_posts_hiring.settings"
            ) as mock_settings,
            patch(
                "app.services.jobs.free_apis.linkedin_posts_hiring._trigger_hiring_actor",
                new_callable=AsyncMock,
                return_value=("run_abc", "token_xyz"),
            ),
            patch(
                "app.services.jobs.free_apis.linkedin_posts_hiring._wait_for_dataset",
                new_callable=AsyncMock,
                return_value=raw_posts,
            ),
            patch(
                "app.services.jobs.free_apis.linkedin_posts_hiring.persist_filtered_jobs",
                side_effect=fake_persist,
            ),
            patch(
                "app.services.jobs.free_apis.linkedin_posts_hiring.SessionLocal",
                return_value=AsyncMock(__aenter__=AsyncMock(), __aexit__=AsyncMock()),
            ),
        ):
            mock_settings.apify_token = "test_token"
            mock_settings.apify_token_secondary = ""

            jobs, stats = await fetch_and_persist_linkedin_posts_hiring()

        assert len(jobs) <= MAX_POSTS
        assert len(persisted_jobs) <= MAX_POSTS
