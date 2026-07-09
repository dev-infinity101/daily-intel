"""Regression test for POST /admin/jobs/scrape-now (V3 orchestrator).

Guards that:
  - The endpoint calls the V3 orchestrator (orchestrate_scrape)
  - run_t1_apify is the monkeypatched T1 entry point
  - Correct counts surface: 2 fetched, 1 inserted (EV role passes, engineering fails)
  - Second run deduplicates correctly (0 inserted)
"""
import pytest
import pytest_asyncio
from httpx import AsyncClient
from sqlalchemy import text
from sqlalchemy.ext.asyncio import async_sessionmaker, create_async_engine

import app.database as database
from app.config import settings
from app.models.target_company import TargetCompany
from app.schemas.job import JobIn

TEST_DB_URL = settings.database_url
_SLUG = "scrape_test_co"

# Fixture jobs: one EV business role + one engineering role.
# Only the business role should survive the pipeline filter.
_FIXTURE = [
    JobIn(
        company="Scrape Test Co",
        job_title="EV Charging Business Development Manager",
        location="Bangalore, India",
        description="Own OCPP EV charging sales and partnerships. EVSE business development.",
        job_url="https://scrape-test.example.com/jobs/bd-1",
        source_type=f"apify_{_SLUG}",
    ),
    JobIn(
        company="Scrape Test Co",
        job_title="Powertrain Maintenance Engineer",
        location="Pune, India",
        description="BIW line maintenance and crank shaft production.",
        job_url="https://scrape-test.example.com/jobs/eng-1",
        source_type=f"apify_{_SLUG}",
    ),
]


@pytest_asyncio.fixture
async def test_session(monkeypatch):
    """Point app.database.SessionLocal at the test DB for all scrape sub-calls."""
    engine = create_async_engine(TEST_DB_URL, echo=False)
    session_factory = async_sessionmaker(engine, expire_on_commit=False)
    monkeypatch.setattr(database, "SessionLocal", session_factory)
    yield session_factory
    await engine.dispose()


@pytest_asyncio.fixture
async def seeded_company(test_session):
    """Insert one active target company and clean up rows afterwards."""
    async with test_session() as db:
        await db.execute(text("DELETE FROM jobs WHERE source_type = :s"), {"s": f"apify_{_SLUG}"})
        await db.execute(text("DELETE FROM scrape_attempts WHERE company_slug = :s"), {"s": _SLUG})
        await db.execute(text("DELETE FROM target_companies WHERE slug = :s"), {"s": _SLUG})
        db.add(
            TargetCompany(
                slug=_SLUG,
                display_name="Scrape Test Co",
                ats_type="custom",
                career_urls=["https://scrape-test.example.com/careers"],
                is_active=True,
            )
        )
        await db.commit()
    yield
    async with test_session() as db:
        await db.execute(text("DELETE FROM jobs WHERE source_type = :s"), {"s": f"apify_{_SLUG}"}),
        await db.execute(text("DELETE FROM scrape_attempts WHERE company_slug = :s"), {"s": _SLUG})
        await db.execute(text("DELETE FROM target_companies WHERE slug = :s"), {"s": _SLUG})
        await db.commit()


@pytest.mark.asyncio
@pytest.mark.integration
async def test_scrape_now_ingests_and_dedups(
    client: AsyncClient, seeded_company, monkeypatch
) -> None:
    """V3: run_t1_apify is the T1 entry point — mock it to return fixture jobs."""

    async def _fake_t1(company):  # noqa: ANN001
        return list(_FIXTURE)

    monkeypatch.setattr("app.services.jobs.apify_adapter.run_t1_apify", _fake_t1)

    # First run: fetch 2 jobs, insert 1 (EV business role passes, engineering filtered)
    r1 = await client.post(f"/admin/jobs/scrape-now?company={_SLUG}")
    assert r1.status_code == 200
    d1 = r1.json()
    assert d1["status"] == "completed"
    assert d1["companies_scraped"] == 1
    assert d1["jobs_fetched"] == 2
    assert d1["jobs_inserted"] == 1

    # Second run: same fixture → dedup blocks insert → 0 new rows
    r2 = await client.post(f"/admin/jobs/scrape-now?company={_SLUG}")
    assert r2.status_code == 200
    d2 = r2.json()
    assert d2["jobs_fetched"] == 2
    assert d2["jobs_inserted"] == 0
