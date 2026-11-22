
import json

from redis import Redis

from backend.app.queue.messages import JobMessage


class RedisJobPublisher:
    """Publish validated job messages to a Redis Stream."""

    def __init__(
        self,
        client: Redis,
        queue_name: str = "taskflow:jobs",
    ) -> None:
        self.client = client
        self.queue_name = queue_name

    def publish(self, message: JobMessage) -> None:
        """Append a serialized job message to the stream."""
        serialized_message = json.dumps(
            message.model_dump(mode="json")
        )
        self.client.xadd(
            self.queue_name,
            {"message": serialized_message},
        )
