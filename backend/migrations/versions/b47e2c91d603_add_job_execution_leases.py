
"""Add execution lease fields to jobs.

Revision ID: b47e2c91d603
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "b47e2c91d603"
down_revision: Union[str, Sequence[str], None] = "8a659745777d"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.add_column(
        "jobs",
        sa.Column(
            "lease_expires_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
    )
    op.add_column(
        "jobs",
        sa.Column(
            "claim_token",
            sa.Uuid(),
            nullable=True,
        ),
    )
    op.create_index(
        "ix_jobs_lease_expires_at",
        "jobs",
        ["lease_expires_at"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_jobs_lease_expires_at",
        table_name="jobs",
    )
    op.drop_column("jobs", "claim_token")
    op.drop_column("jobs", "lease_expires_at")
