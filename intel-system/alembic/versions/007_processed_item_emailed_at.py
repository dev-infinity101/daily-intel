"""Migration 007 — Add emailed_at to processed_items

Adds emailed_at (nullable TIMESTAMPTZ) to the processed_items table.

NULL     → item has never been included in a digest email
NOT NULL → timestamp of when the item was first emailed

This prevents news items from being resent across multiple digest runs.
The assembler filters to emailed_at IS NULL and stamps it on successful send.

Revision ID: 007
Revises: 006
"""
from typing import Union

import sqlalchemy as sa
from alembic import op

revision: str = "007"
down_revision: Union[str, None] = "006"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "processed_items",
        sa.Column("emailed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("idx_processed_emailed_at", "processed_items", ["emailed_at"])


def downgrade() -> None:
    op.drop_index("idx_processed_emailed_at", table_name="processed_items")
    op.drop_column("processed_items", "emailed_at")
