
from uuid import uuid4

from sqlalchemy import JSON, DateTime, String, Text, Uuid

from backend.app.database import Base
from backend.app.models.outbox import OutboxEvent


def test_outbox_table_is_registered():
    assert "outbox_events" in Base.metadata.tables


def test_outbox_model_has_expected_columns():
    table = OutboxEvent.__table__

    assert isinstance(table.c.id.type, Uuid)
    assert isinstance(table.c.event_type.type, String)
    assert isinstance(table.c.aggregate_id.type, Uuid)
    assert isinstance(table.c.payload.type, JSON)
    assert isinstance(table.c.created_at.type, DateTime)
    assert isinstance(table.c.published_at.type, DateTime)
    assert isinstance(table.c.last_error.type, Text)


def test_outbox_pending_index_exists():
    index_names = {
        index.name for index in OutboxEvent.__table__.indexes
    }

    assert "ix_outbox_events_pending" in index_names
    assert "ix_outbox_events_aggregate_id" in index_names

def test_outbox_id_has_uuid_default():
    event = OutboxEvent(
        event_type="job.created",
        aggregate_id=uuid4(),
        payload={"job_id": str(uuid4())},
    )

    # SQLAlchemy column defaults are applied when an INSERT is executed,
    # not necessarily when the model object is first constructed.
    assert OutboxEvent.__table__.c.id.default is not None
    assert event.event_type == "job.created"


def test_outbox_created_at_has_server_default():
    column = OutboxEvent.__table__.c.created_at

    assert column.server_default is not None