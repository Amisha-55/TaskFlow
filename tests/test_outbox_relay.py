
from datetime import datetime, timedelta, timezone
from types import SimpleNamespace
from unittest.mock import Mock
from uuid import uuid4

import pytest

from backend.app.queue.outbox_relay import publish_pending_events


def make_event(payload=None):
    job_id = uuid4()

    if payload is None:
        payload = {
            "message_version": 1,
            "job_id": str(job_id),
            "task_type": "cleanup",
            "priority": 5,
            "enqueued_at": datetime.now(timezone.utc).isoformat(),
        }

    return SimpleNamespace(
        id=uuid4(),
        event_type="job.created",
        aggregate_id=job_id,
        payload=payload,
        created_at=datetime.now(timezone.utc),
        published_at=None,
        last_error=None,
    )



def make_db(*events):
    """Create a mocked database query returning events in order."""
    db = Mock()
    query = db.query.return_value

    # Every query stage returns the same query mock, including
    # additional filters applied when retry events are skipped.
    query.filter.return_value = query
    query.order_by.return_value = query
    query.with_for_update.return_value = query
    query.first.side_effect = [*events, None]

    return db



def test_publish_pending_event_marks_it_published():
    event = make_event()
    db = make_db(event)
    publisher = Mock()

    published_count = publish_pending_events(db, publisher)

    assert published_count == 1
    publisher.publish.assert_called_once()

    message = publisher.publish.call_args.args[0]
    assert str(message.job_id) == event.payload["job_id"]
    assert message.task_type == "cleanup"
    assert message.priority == 5

    assert event.published_at is not None
    assert event.last_error is None
    db.commit.assert_called_once()


def test_publish_failure_preserves_event_for_retry():
    event = make_event()
    db = make_db(event)
    publisher = Mock()
    publisher.publish.side_effect = ConnectionError("Redis unavailable")

    published_count = publish_pending_events(db, publisher)

    assert published_count == 0
    assert event.published_at is None
    assert "ConnectionError" in event.last_error
    assert "Redis unavailable" in event.last_error
    db.commit.assert_called_once()
    db.rollback.assert_not_called()


def test_invalid_payload_is_preserved_for_retry():
    event = make_event(
        payload={
            "message_version": 1,
            "job_id": "not-a-valid-uuid",
            "task_type": "cleanup",
            "priority": 5,
            "enqueued_at": datetime.now(timezone.utc).isoformat(),
        }
    )
    db = make_db(event)
    publisher = Mock()

    published_count = publish_pending_events(db, publisher)

    assert published_count == 0
    publisher.publish.assert_not_called()
    assert event.published_at is None
    assert event.last_error is not None
    assert event.last_error != ""
    db.commit.assert_called_once()


def test_empty_outbox_returns_zero():
    db = make_db()
    publisher = Mock()

    published_count = publish_pending_events(db, publisher)

    assert published_count == 0
    publisher.publish.assert_not_called()
    db.commit.assert_not_called()


def test_future_retry_event_is_not_published():
    event = make_event()
    event.event_type = "job.retry_scheduled"
    event.payload["enqueued_at"] = (
        datetime.now(timezone.utc) + timedelta(minutes=1)
    ).isoformat()

    db = make_db(event)
    publisher = Mock()

    published_count = publish_pending_events(db, publisher)

    assert published_count == 0
    publisher.publish.assert_not_called()
    assert event.published_at is None
    db.rollback.assert_called_once()
    db.commit.assert_not_called()


def test_due_retry_event_is_published():
    event = make_event()
    event.event_type = "job.retry_scheduled"
    event.payload["enqueued_at"] = (
        datetime.now(timezone.utc) - timedelta(seconds=1)
    ).isoformat()

    db = make_db(event)
    publisher = Mock()

    published_count = publish_pending_events(db, publisher)

    assert published_count == 1
    publisher.publish.assert_called_once()
    assert event.published_at is not None
    assert event.last_error is None
    db.commit.assert_called_once()


def test_batch_size_limits_number_of_events_processed():
    first_event = make_event()
    second_event = make_event()
    db = make_db(first_event, second_event)
    publisher = Mock()

    published_count = publish_pending_events(
        db,
        publisher,
        batch_size=1,
    )

    assert published_count == 1
    publisher.publish.assert_called_once()
    assert first_event.published_at is not None
    assert second_event.published_at is None
    db.commit.assert_called_once()


@pytest.mark.parametrize("batch_size", [0, -1, -100])
def test_invalid_batch_size_raises_error(batch_size):
    db = Mock()
    publisher = Mock()

    with pytest.raises(ValueError, match="batch_size must be at least 1"):
        publish_pending_events(db, publisher, batch_size=batch_size)

    db.query.assert_not_called()
    publisher.publish.assert_not_called()


def test_database_commit_failure_rolls_back_and_reraises():
    event = make_event()
    db = make_db(event)
    publisher = Mock()
    db.commit.side_effect = RuntimeError("Database commit failed")

    with pytest.raises(RuntimeError, match="Database commit failed"):
        publish_pending_events(db, publisher)

    publisher.publish.assert_called_once()
    db.rollback.assert_called_once()
