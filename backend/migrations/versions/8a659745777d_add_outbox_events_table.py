
"""Add outbox events table.

Revision ID: 8a659745777d
"""

from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


revision: str = "8a659745777d"
down_revision: Union[str, Sequence[str], None] = "c69f52d65f5a"
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    op.create_table(
        "outbox_events",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("event_type", sa.String(length=100), nullable=False),
        sa.Column("aggregate_id", sa.Uuid(), nullable=False),
        sa.Column("payload", sa.JSON(), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "published_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.PrimaryKeyConstraint("id"),
    )

    op.create_index(
        "ix_outbox_events_pending",
        "outbox_events",
        ["published_at", "created_at"],
        unique=False,
    )

    op.create_index(
        "ix_outbox_events_aggregate_id",
        "outbox_events",
        ["aggregate_id"],
        unique=False,
    )


def downgrade() -> None:
    op.drop_index(
        "ix_outbox_events_aggregate_id",
        table_name="outbox_events",
    )
    op.drop_index(
        "ix_outbox_events_pending",
        table_name="outbox_events",
    )
    op.drop_table("outbox_events")
