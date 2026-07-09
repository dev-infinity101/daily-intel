from datetime import datetime

from sqlalchemy import Boolean, DateTime, Integer, SmallInteger, String, Text, UniqueConstraint, func
from sqlalchemy.dialects.postgresql import ARRAY
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class TargetCompany(Base):
    __tablename__ = "target_companies"
    __table_args__ = (UniqueConstraint("slug"),)

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    slug: Mapped[str] = mapped_column(String(64), nullable=False, unique=True)
    display_name: Mapped[str] = mapped_column(Text, nullable=False)
    ats_type: Mapped[str | None] = mapped_column(String(32), nullable=True)
    ats_slug: Mapped[str | None] = mapped_column(String(128), nullable=True)
    career_urls: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False, default=list)
    changedetection_urls: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False, default=list)
    location_filters: Mapped[list[str]] = mapped_column(ARRAY(Text), nullable=False, default=list)
    apify_actor_id: Mapped[str | None] = mapped_column(String(256), nullable=True)
    priority: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=5)
    is_active: Mapped[bool] = mapped_column(Boolean, nullable=False, default=True)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
    # V3 adaptive routing
    preferred_scraper: Mapped[str | None] = mapped_column(String(16), nullable=True)   # 'apify' | 'browserbase'
    last_success_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    consecutive_failures: Mapped[int] = mapped_column(SmallInteger, nullable=False, default=0)
