
import redis
from redis import Redis

from backend.app.config import settings


def create_redis_client() -> Redis:
    """Create a Redis client using TaskFlow settings."""
    return redis.Redis.from_url(
        settings.redis_url,
        decode_responses=True,
        socket_connect_timeout=3,
        socket_timeout=3,
        health_check_interval=30,
    )


redis_client = create_redis_client()
