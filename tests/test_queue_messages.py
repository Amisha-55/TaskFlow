from datetime import datetime, timezone
from uuid import uuid4

import pytest
from pydantic import ValidationError

from backend.app.queue.messages import JobMessage


def test_valid_job_message():
    job_id = uuid4()
    enqueued_at = datetime.now(timezone.utc)

    message = JobMessage(
        job_id=job_id,
        task_type="cleanup",
        priority=5,
        enqueued_at=enqueued_at,
    )

    assert message.message_version == 1
    assert message.job_id == job_id
    assert message.task_type == "cleanup"
    assert message.priority == 5
    assert message.enqueued_at == enqueued_at


def test_invalid_priority_is_rejected():
    with pytest.raises(ValidationError):
        JobMessage(
            job_id=uuid4(),
            task_type="cleanup",
            priority=101,
            enqueued_at=datetime.now(timezone.utc),
        )


def test_unknown_fields_are_rejected():
    data = {
        "job_id": str(uuid4()),
        "task_type": "cleanup",
        "priority": 5,
        "enqueued_at": datetime.now(timezone.utc).isoformat(),
        "unexpected_field": "not-allowed",
    }

    with pytest.raises(ValidationError):
        JobMessage.model_validate(data)


def test_job_id_is_serializable():
    job_id = uuid4()

    message = JobMessage(
        job_id=job_id,
        task_type="cleanup",
        priority=0,
        enqueued_at=datetime.now(timezone.utc),
    )

    serialized = message.model_dump_json()
    restored = JobMessage.model_validate_json(serialized)

    assert restored.job_id == job_id
