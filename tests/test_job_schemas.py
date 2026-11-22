
import pytest
from pydantic import ValidationError

from backend.app.schemas.job import JobCreate


def test_valid_job_request():
    job = JobCreate(
        task_type="send_email",
        payload={"to": "test@example.com"},
        priority=5,
        max_attempts=3,
    )

    assert job.task_type == "send_email"
    assert job.payload["to"] == "test@example.com"
    assert job.priority == 5
    assert job.max_attempts == 3


def test_defaults_are_applied():
    job = JobCreate(task_type="cleanup")

    assert job.payload == {}
    assert job.priority == 0
    assert job.max_attempts == 3
    assert job.scheduled_at is None


def test_blank_task_type_is_rejected():
    with pytest.raises(ValidationError):
        JobCreate(task_type="   ")


def test_priority_out_of_range_is_rejected():
    with pytest.raises(ValidationError):
        JobCreate(task_type="cleanup", priority=101)


def test_invalid_max_attempts_is_rejected():
    with pytest.raises(ValidationError):
        JobCreate(task_type="cleanup", max_attempts=0)


def test_timezone_is_required_for_scheduled_jobs():
    with pytest.raises(ValidationError):
        JobCreate(
            task_type="cleanup",
            scheduled_at="2026-10-20T10:00:00",
        )


def test_timezone_aware_schedule_is_accepted():
    job = JobCreate(
        task_type="cleanup",
        scheduled_at="2026-10-20T10:00:00+05:30",
    )

    assert job.scheduled_at is not None
    assert job.scheduled_at.utcoffset() is not None
