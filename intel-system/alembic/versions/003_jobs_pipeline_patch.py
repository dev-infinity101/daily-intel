"""jobs pipeline patch — nullable raw_item_id, rank_score on jobs, wider source_type

Revision ID: 003
Revises: 002
Create Date: 2026-05-26

Rationale:
- jobs.raw_item_id made nullable so the direct-pipeline path (scrape → filter →
  Job record) does not require a RawItem intermediary.
- processed_items.raw_item_id made nullable for the same reason — job-sourced
  ProcessedItems are created without a RawItem parent.
- jobs.rank_score added (was in ORM model but missing from migration 002).
- jobs.source_type widened from String(32) to String(64) to accommodate
  "apify_<company_slug>" values where slug can be up to 64 chars.
"""
from typing import Sequence, Union

import sqlalchemy as sa
from alembic import op

revision: str = "003"
down_revision: Union[str, None] = "002"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    # Drop FK on jobs.raw_item_id so we can alter the column
    op.drop_constraint("jobs_raw_item_id_fkey", "jobs", type_="foreignkey")
    op.alter_column("jobs", "raw_item_id", existing_type=sa.BigInteger(), nullable=True)
    op.create_foreign_key(
        "jobs_raw_item_id_fkey",
        "jobs",
        "raw_items",
        ["raw_item_id"],
        ["id"],
        ondelete="SET NULL",
    )

    # Add rank_score to jobs (present in ORM model but absent from migration 002)
    op.add_column(
        "jobs",
        sa.Column("rank_score", sa.Float(), nullable=False, server_default="0"),
    )

    # Widen source_type to accommodate "apify_<slug>" where slug ≤ 64 chars
    op.alter_column(
        "jobs",
        "source_type",
        existing_type=sa.String(32),
        type_=sa.String(64),
        existing_nullable=False,
    )

    # Drop FK on processed_items.raw_item_id before making it nullable
    op.drop_constraint(
        "processed_items_raw_item_id_fkey", "processed_items", type_="foreignkey"
    )
    # Drop unique constraint so multiple NULL rows are explicitly supported
    op.drop_constraint("processed_items_raw_item_id_key", "processed_items", type_="unique")
    op.alter_column(
        "processed_items", "raw_item_id", existing_type=sa.BigInteger(), nullable=True
    )
    op.create_foreign_key(
        "processed_items_raw_item_id_fkey",
        "processed_items",
        "raw_items",
        ["raw_item_id"],
        ["id"],
        ondelete="CASCADE",
    )


def downgrade() -> None:
    # Reverse processed_items changes
    op.drop_constraint(
        "processed_items_raw_item_id_fkey", "processed_items", type_="foreignkey"
    )
    op.alter_column(
        "processed_items", "raw_item_id", existing_type=sa.BigInteger(), nullable=False
    )
    op.create_unique_constraint(
        "processed_items_raw_item_id_key", "processed_items", ["raw_item_id"]
    )
    op.create_foreign_key(
        "processed_items_raw_item_id_fkey",
        "processed_items",
        "raw_items",
        ["raw_item_id"],
        ["id"],
        ondelete="CASCADE",
    )

    # Reverse jobs changes
    op.drop_column("jobs", "rank_score")
    op.alter_column(
        "jobs",
        "source_type",
        existing_type=sa.String(64),
        type_=sa.String(32),
        existing_nullable=False,
    )
    op.drop_constraint("jobs_raw_item_id_fkey", "jobs", type_="foreignkey")
    op.alter_column("jobs", "raw_item_id", existing_type=sa.BigInteger(), nullable=False)
    op.create_foreign_key(
        "jobs_raw_item_id_fkey",
        "jobs",
        "raw_items",
        ["raw_item_id"],
        ["id"],
        ondelete="CASCADE",
    )
