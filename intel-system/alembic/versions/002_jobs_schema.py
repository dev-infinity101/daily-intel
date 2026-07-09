"""jobs schema — target_companies, jobs tables

Revision ID: 002
Revises: 001
Create Date: 2026-05-16
"""
from typing import Sequence, Union
from alembic import op
import sqlalchemy as sa
from sqlalchemy.dialects import postgresql

revision: str = "002"
down_revision: Union[str, None] = "001"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "target_companies",
        sa.Column("id", sa.Integer(), nullable=False),
        sa.Column("slug", sa.String(64), nullable=False),
        sa.Column("display_name", sa.Text(), nullable=False),
        sa.Column("ats_type", sa.String(32), nullable=True),
        sa.Column("ats_slug", sa.String(128), nullable=True),
        sa.Column("career_urls", postgresql.ARRAY(sa.Text()), nullable=False, server_default="{}"),
        sa.Column("changedetection_urls", postgresql.ARRAY(sa.Text()), nullable=False, server_default="{}"),
        sa.Column("location_filters", postgresql.ARRAY(sa.Text()), nullable=False, server_default="{}"),
        sa.Column("priority", sa.SmallInteger(), nullable=False, server_default="5"),
        sa.Column("is_active", sa.Boolean(), nullable=False, server_default="true"),
        sa.Column("created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("slug"),
    )

    op.create_table(
        "jobs",
        sa.Column("id", sa.BigInteger(), nullable=False),
        sa.Column("raw_item_id", sa.BigInteger(), nullable=False),
        sa.Column("company", sa.String(128), nullable=False),
        sa.Column("target_company_id", sa.Integer(), nullable=True),
        sa.Column("job_title", sa.Text(), nullable=False),
        sa.Column("location", sa.Text(), nullable=True),
        sa.Column("remote", sa.Boolean(), nullable=True),
        sa.Column("department", sa.Text(), nullable=True),
        sa.Column("description", sa.Text(), nullable=True),
        sa.Column("job_url", sa.Text(), nullable=False),
        sa.Column("external_job_id", sa.String(128), nullable=True),
        sa.Column("experience_level", sa.String(16), nullable=False, server_default="unknown"),
        sa.Column("salary_min", sa.Numeric(), nullable=True),
        sa.Column("salary_max", sa.Numeric(), nullable=True),
        sa.Column("salary_currency", sa.String(3), nullable=True),
        sa.Column("extracted_skills", postgresql.ARRAY(sa.Text()), nullable=False, server_default="{}"),
        sa.Column("source_type", sa.String(32), nullable=False),
        sa.Column("posted_at", sa.DateTime(timezone=True), nullable=True),
        sa.Column("first_seen_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("last_seen_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.text("NOW()")),
        sa.Column("is_closed", sa.Boolean(), nullable=False, server_default="false"),
        sa.Column("dedup_hash", sa.String(64), nullable=False),
        sa.ForeignKeyConstraint(["raw_item_id"], ["raw_items.id"], ondelete="CASCADE"),
        sa.ForeignKeyConstraint(["target_company_id"], ["target_companies.id"], ondelete="SET NULL"),
        sa.PrimaryKeyConstraint("id"),
        sa.UniqueConstraint("dedup_hash"),
    )
    op.create_index("idx_jobs_target_company", "jobs", ["target_company_id", "posted_at"], postgresql_using="btree")
    op.create_index("idx_jobs_first_seen", "jobs", ["first_seen_at"], postgresql_using="btree")
    # trgm index for fuzzy dedup — requires pg_trgm (already enabled in migration 001)
    op.execute(
        "CREATE INDEX idx_jobs_company_title_trgm ON jobs "
        "USING gin (company gin_trgm_ops, job_title gin_trgm_ops)"
    )


def downgrade() -> None:
    op.drop_table("jobs")
    op.drop_table("target_companies")
