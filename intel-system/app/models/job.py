from datetime import datetime
from decimal import Decimal

from sqlalchemy import BigInteger, Boolean, DateTime, Float, ForeignKey, Integer, Numeric, String, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class Job(Base):
    __tablename__ = "jobs"
    __table_args__ = (UniqueConstraint("dedup_hash"),)

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    raw_item_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("raw_items.id", ondelete="SET NULL"), nullable=True
    )
    company: Mapped[str] = mapped_column(String(128), nullable=False)
    target_company_id: Mapped[int | None] = mapped_column(
        Integer, ForeignKey("target_companies.id", ondelete="SET NULL"), nullable=True
    )
    job_title: Mapped[str] = mapped_column(Text, nullable=False)
    location: Mapped[str | None] = mapped_column(Text, nullable=True)
    remote: Mapped[bool | None] = mapped_column(Boolean, nullable=True)
    department: Mapped[str | None] = mapped_column(Text, nullable=True)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    job_url: Mapped[str] = mapped_column(Text, nullable=False)
    external_job_id: Mapped[str | None] = mapped_column(String(128), nullable=True)
    experience_level: Mapped[str] = mapped_column(String(16), nullable=False, default="unknown")
    salary_min: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
    salary_max: Mapped[Decimal | None] = mapped_column(Numeric, nullable=True)
    salary_currency: Mapped[str | None] = mapped_column(String(3), nullable=True)
    extracted_skills: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False, default=list)
    source_type: Mapped[str] = mapped_column(String(64), nullable=False)
    posted_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    first_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    last_seen_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    is_closed: Mapped[bool] = mapped_column(Boolean, nullable=False, default=False)
    emailed_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    dedup_hash: Mapped[str] = mapped_column(String(64), nullable=False)
    rank_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
