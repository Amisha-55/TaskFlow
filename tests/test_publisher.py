
import json
from unittest.mock import Mock
from uuid import uuid4

from backend.app.queue.messages import JobMessage
from backend.app.queue.publisher import RedisJobPublisher


def test_publish_serializes_message_and_adds_to_stream():
    redis_client = Mock()
    publisher = RedisJobPublisher(redis_client)
    job_id = uuid4()

    message = JobMessage(
        job_id=job_id,
        task_type="send_email",
        priority=5,
        enqueued_at="2026-10-20T10:00:00Z",
    )

    publisher.publish(message)

    redis_client.xadd.assert_called_once()
    stream_name, fields = redis_client.xadd.call_args.args

    assert stream_name == "taskflow:jobs"
    assert set(fields) == {"message"}

    decoded = json.loads(fields["message"])
    assert decoded["job_id"] == str(job_id)
    assert decoded["task_type"] == "send_email"
    assert decoded["priority"] == 5
    assert decoded["message_version"] == 1


def test_publisher_uses_custom_stream_name():
    redis_client = Mock()
    publisher = RedisJobPublisher(
        redis_client,
        queue_name="taskflow:jobs:high",
    )

    message = JobMessage(
        job_id=uuid4(),
        task_type="generate_report",
        priority=10,
        enqueued_at="2026-10-20T10:00:00Z",
    )

    publisher.publish(message)

    assert redis_client.xadd.call_args.args[0] == "taskflow:jobs:high"
