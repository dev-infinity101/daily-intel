"""Add apify_actor_id to target_companies

Revision ID: 004
Revises: 003
Create Date: 2026-06-03

Adds an optional Apify actor ID per target company so the scheduler can
trigger a real Apify JS-rendering run for companies whose career pages are
SPAs. When set, _fetch_company_jobs_by_ats() will call poll_actor() instead
of the plain HTTP scraper.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "004"
down_revision: Union[str, None] = "003"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "target_companies",
        sa.Column("apify_actor_id", sa.String(256), nullable=True),
    )


def downgrade() -> None:
    op.drop_column("target_companies", "apify_actor_id")
