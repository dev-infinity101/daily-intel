from datetime import datetime
from decimal import Decimal

from pydantic import BaseModel


class JobIn(BaseModel):
    """Normalized job posting — output of every adapter's normalizer."""

    company: str
    job_title: str
    location: str | None = None
    remote: bool | None = None
    department: str | None = None
    description: str | None = None
    job_url: str
    external_job_id: str | None = None
    source_type: str
    posted_at: datetime | None = None
    salary_min: Decimal | None = None
    salary_max: Decimal | None = None
    salary_currency: str | None = None


class JobOut(JobIn):
    id: int
    experience_level: str
    extracted_skills: list[str]
    dedup_hash: str
    first_seen_at: datetime
    last_seen_at: datetime
    is_closed: bool
    rank_score: float

    model_config = {"from_attributes": True}
