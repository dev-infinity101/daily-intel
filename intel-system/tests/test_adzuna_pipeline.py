import pytest
from unittest.mock import AsyncMock, MagicMock

from app.schemas.job import JobIn
from app.services.jobs.job_pipeline import persist_filtered_jobs


@pytest.mark.asyncio
async def test_adzuna_ev_charging_non_business_rejected():
    """
    Test that an Adzuna job with 'Found via: EV charging' and a non-business
    category (e.g., Technical, PR) is filtered out before db operations.
    """
    mock_db = AsyncMock()
    
    jobs = [
        JobIn(
            company="Test Co",
            job_title="Software Engineer",
            location="Bengaluru, India",
            remote=False,
            department="Technical", # The Adzuna Category
            description="Found via: EV charging. Category: Technical. We need an engineer.",
            job_url="http://example.com/1",
            external_job_id="1",
            source_type="adzuna"
        )
    ]
    
    inserted = await persist_filtered_jobs(jobs, mock_db)
    
    # Should be filtered out, so 0 inserted and no DB executes
    assert inserted == 0
    mock_db.execute.assert_not_called()
    mock_db.add.assert_not_called()


@pytest.mark.asyncio
async def test_adzuna_ev_charging_business_accepted():
    """
    Test that an Adzuna job with 'Found via: EV charging' and a business
    category (e.g., Sales) passes the Adzuna filter and proceeds to classification.
    """
    mock_db = AsyncMock()
    
    # Mocking the scalar_one_or_none for deduplication check to return None
    mock_result = MagicMock()
    mock_result.scalar_one_or_none.return_value = None
    mock_db.execute.return_value = mock_result
    
    # Mocking async with db.begin_nested()
    class DummyAsyncContextManager:
        async def __aenter__(self):
            return self
        async def __aexit__(self, exc_type, exc, tb):
            return False
            
    mock_db.begin_nested = MagicMock(return_value=DummyAsyncContextManager())
    
    jobs = [
        JobIn(
            company="Test Co",
            job_title="Sales Manager - EV Charging",
            location="Bengaluru, India",
            remote=False,
            department="Sales", # The Adzuna Category
            description="Found via: EV charging. Category: Sales. We need a sales manager.",
            job_url="http://example.com/2",
            external_job_id="2",
            source_type="adzuna"
        )
    ]
    
    inserted = await persist_filtered_jobs(jobs, mock_db)
    
    # Should pass the Adzuna pre-filter, pass classification, and be inserted
    assert inserted == 1
    mock_db.execute.assert_called()
    assert mock_db.add.call_count == 2  # 1 for Job, 1 for ProcessedItem
