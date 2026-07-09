from datetime import datetime

from sqlalchemy import BigInteger, Boolean, DateTime, Float, ForeignKey, Index, String, Text, func
from sqlalchemy.orm import Mapped, mapped_column

from app.database import Base


class ProcessedItem(Base):
    __tablename__ = "processed_items"
    __table_args__ = (
        Index("idx_processed_section_rank", "section", "rank_score"),
    )

    id: Mapped[int] = mapped_column(BigInteger, primary_key=True)
    raw_item_id: Mapped[int | None] = mapped_column(
        BigInteger, ForeignKey("raw_items.id", ondelete="CASCADE"), nullable=True
    )
    is_relevant: Mapped[bool] = mapped_column(Boolean, nullable=False)
    relevance_score: Mapped[float | None] = mapped_column(Float, nullable=True)
    summary: Mapped[str | None] = mapped_column(Text, nullable=True)
    section: Mapped[str] = mapped_column(String(32), nullable=False)
    rank_score: Mapped[float] = mapped_column(Float, nullable=False, default=0.0)
    processed_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True), nullable=False, server_default=func.now()
    )
