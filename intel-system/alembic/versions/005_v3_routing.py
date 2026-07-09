"""V3 routing metadata and scrape_attempts table

Revision ID: 005
Revises: 004
Create Date: 2026-06-09

Adds adaptive routing columns to target_companies and creates the
scrape_attempts table for per-run logging, cost tracking, and adaptive
tier pinning per the V3 architecture.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "005"
down_revision: Union[str, None] = "004"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # ── Routing metadata on target_companies ─────────────────────────────────
    op.add_column(
        "target_companies",
        sa.Column("preferred_scraper", sa.String(16), nullable=True),
    )
    op.add_column(
        "target_companies",
        sa.Column("last_success_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.add_column(
        "target_companies",
        sa.Column(
            "consecutive_failures",
            sa.SmallInteger(),
            nullable=False,
            server_default="0",
        ),
    )

    # ── scrape_attempts table ─────────────────────────────────────────────────
    op.create_table(
        "scrape_attempts",
        sa.Column("id", sa.BigInteger(), primary_key=True, autoincrement=True),
        sa.Column("run_id", sa.Text(), nullable=False),
        sa.Column("company_slug", sa.String(64), nullable=False),
        sa.Column("tier", sa.String(8), nullable=False),       # t1 / t2 / t3
        sa.Column("outcome", sa.String(32), nullable=False),   # success / deterministic / ...
        sa.Column("jobs_found", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("jobs_inserted", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("duration_ms", sa.Integer(), nullable=False, server_default="0"),
        sa.Column("error_class", sa.String(64), nullable=True),
        sa.Column("session_id", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            nullable=False,
            server_default=sa.text("now()"),
        ),
    )
    op.create_index("idx_scrape_attempts_company", "scrape_attempts", ["company_slug"])
    op.create_index("idx_scrape_attempts_run",     "scrape_attempts", ["run_id"])
    op.create_index("idx_scrape_attempts_created", "scrape_attempts", ["created_at"])


def downgrade() -> None:
    op.drop_index("idx_scrape_attempts_created", table_name="scrape_attempts")
    op.drop_index("idx_scrape_attempts_run",     table_name="scrape_attempts")
    op.drop_index("idx_scrape_attempts_company", table_name="scrape_attempts")
    op.drop_table("scrape_attempts")
    op.drop_column("target_companies", "consecutive_failures")
    op.drop_column("target_companies", "last_success_at")
    op.drop_column("target_companies", "preferred_scraper")
