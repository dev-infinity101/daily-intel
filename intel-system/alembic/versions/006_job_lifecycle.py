"""Job lifecycle — emailed_at for digest deduplication

Adds emailed_at (nullable TIMESTAMPTZ) to the jobs table.

NULL  → job has never been included in a digest email
NOT NULL → timestamp of the first digest it was sent in

This enables the digest to query ``emailed_at IS NULL`` and mark rows
after a successful send, guaranteeing each job appears in exactly one
digest regardless of how many times the digest job runs.

Revision ID: 006
Revises: 005
"""
from typing import Union

import sqlalchemy as sa
from alembic import op

revision: str = "006"
down_revision: Union[str, None] = "005"
branch_labels = None
depends_on = None


def upgrade() -> None:
    op.add_column(
        "jobs",
        sa.Column("emailed_at", sa.DateTime(timezone=True), nullable=True),
    )
    op.create_index("ix_jobs_emailed_at", "jobs", ["emailed_at"])


def downgrade() -> None:
    op.drop_index("ix_jobs_emailed_at", table_name="jobs")
    op.drop_column("jobs", "emailed_at")
