
import json
from uuid import uuid4

from backend.app.queue.messages import JobMessage
from backend.app.queue.publisher import RedisJobPublisher
from backend.app.queue.redis_client import create_redis_client


def test_publish_to_real_redis():
    client = create_redis_client()
    stream_name = "taskflow:test:publisher"

    try:
        client.delete(stream_name)

        job_id = uuid4()
        message = JobMessage(
            job_id=job_id,
            task_type="test_task",
            priority=3,
            enqueued_at="2026-10-20T10:00:00Z",
        )

        publisher = RedisJobPublisher(
            client,
            queue_name=stream_name,
        )
        publisher.publish(message)

        entries = client.xrange(stream_name, min="-", max="+", count=1)
        assert len(entries) == 1

        _, fields = entries[0]
        decoded = json.loads(fields["message"])

        assert decoded["job_id"] == str(job_id)
        assert decoded["task_type"] == "test_task"
        assert decoded["priority"] == 3

    finally:
        client.delete(stream_name)
        client.close()
