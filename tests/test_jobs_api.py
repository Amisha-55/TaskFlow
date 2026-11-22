import pytest
from fastapi.testclient import TestClient

from backend.app.main import app
from datetime import datetime, timezone
from unittest.mock import MagicMock
from uuid import uuid4
from backend.app.models.job import Job, JobStatus
from backend.app.database import get_db
from backend.app.models.outbox import OutboxEvent

client = TestClient(app)


def test_create_job_rejects_blank_task_type():
    response = client.post(
        "/api/v1/jobs",
        json={"task_type": "   "},
    )

    assert response.status_code == 422


def test_create_job_rejects_priority_out_of_range():
    response = client.post(
        "/api/v1/jobs",
        json={
            "task_type": "cleanup",
            "priority": 101,
        },
    )

    assert response.status_code == 422


def test_create_job_rejects_timezone_naive_schedule():
    response = client.post(
        "/api/v1/jobs",
        json={
            "task_type": "cleanup",
            "scheduled_at": "2026-10-20T10:00:00",
        },
    )

    assert response.status_code == 422

    
def test_create_job_persists_valid_request_with_mocked_db():
    fake_db = MagicMock()

    job_id = uuid4()
    now = datetime.now(timezone.utc)

    def fake_refresh(job):
        job.id = job_id
        job.status = JobStatus.PENDING
        job.attempts = 0
        job.created_at = now
        job.updated_at = now
        job.started_at = None
        job.finished_at = None
        job.result = None
        job.last_error = None

    def fake_flush():
        for call_args in fake_db.add.call_args_list:
            obj = call_args.args[0]
            if isinstance(obj, Job) and obj.id is None:
                obj.id = job_id

    fake_db.refresh.side_effect = fake_refresh
    fake_db.flush.side_effect = fake_flush

    def override_get_db():
        yield fake_db

    app.dependency_overrides[get_db] = override_get_db

    try:
        response = client.post(
            "/api/v1/jobs",
            json={
                "task_type": "cleanup",
                "payload": {"target": "temporary-files"},
                "priority": 5,
                "max_attempts": 3,
            },
        )

        assert response.status_code == 201

        body = response.json()
        assert body["id"] == str(job_id)
        assert body["task_type"] == "cleanup"
        assert body["payload"] == {"target": "temporary-files"}
        assert body["status"] == "pending"

        added_objects = [
            call.args[0] for call in fake_db.add.call_args_list
        ]

        job_objects = [
            obj for obj in added_objects if isinstance(obj, Job)
        ]
        outbox_objects = [
            obj for obj in added_objects if isinstance(obj, OutboxEvent)
        ]

        assert len(job_objects) == 1
        assert len(outbox_objects) == 1

        created_job = job_objects[0]
        created_event = outbox_objects[0]

        assert created_job.id == job_id
        assert created_job.task_type == "cleanup"
        assert created_job.payload == {"target": "temporary-files"}
        assert created_job.priority == 5
        assert created_job.max_attempts == 3
        assert created_job.status == JobStatus.PENDING

        assert created_event.event_type == "job.created"
        assert created_event.aggregate_id == job_id
        assert created_event.payload["task_type"] == "cleanup"
        assert created_event.payload["job_id"] == str(job_id)
        assert created_event.payload["priority"] == 5

        fake_db.flush.assert_called_once()
        fake_db.commit.assert_called_once()
        fake_db.refresh.assert_called_once_with(created_job)
    finally:
        app.dependency_overrides.pop(get_db, None)