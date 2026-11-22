import logging
import socket
from uuid import uuid4

from backend.app.database import SessionLocal
from backend.app.queue.outbox_relay import publish_pending_events
from backend.app.queue.publisher import RedisJobPublisher
from backend.app.queue.redis_client import redis_client
from backend.app.worker.handlers import create_default_registry
from backend.app.worker.consumer import (
    consume_once,
    ensure_consumer_group,
    recover_pending_once,
)
from backend.app.worker.recovery import recover_expired_leases

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)

logger = logging.getLogger(__name__)


def run_worker() -> None:
    consumer_name = f"{socket.gethostname()}-{uuid4().hex[:8]}"
    registry = create_default_registry()
    publisher = RedisJobPublisher(redis_client)

    ensure_consumer_group(redis_client)

    logger.info("TaskFlow worker started: %s", consumer_name)

    iterations = 0

    try:
        while True:
            with SessionLocal() as db:
                # 1. Publish eligible outbox events, including due retries.
                try:
                    published = publish_pending_events(
                        db=db,
                        publisher=publisher,
                        batch_size=100,
                    )

                    if published:
                        logger.info(
                            "Published %s outbox event(s)",
                            published,
                        )
                except Exception:
                    logger.exception("Outbox relay iteration failed")

                # 2. Consume newly published jobs.
                try:
                    processed = consume_once(
                        db=db,
                        client=redis_client,
                        registry=registry,
                        consumer_name=consumer_name,
                        block_ms=1000,
                    )

                    if processed:
                        logger.info(
                            "Read %s stream message(s)",
                            processed,
                        )
                except Exception:
                    logger.exception("Worker iteration failed")

                # 3. Periodically recover abandoned Redis messages
                #    and jobs whose database leases have expired.
                iterations += 1

                if iterations % 10 == 0:
                    try:
                        recovered_messages = recover_pending_once(
                            db=db,
                            client=redis_client,
                            registry=registry,
                            consumer_name=consumer_name,
                            min_idle_ms=30_000,
                            batch_size=10,
                        )

                        if recovered_messages:
                            logger.info(
                                "Recovered %s pending Redis message(s)",
                                recovered_messages,
                            )
                    except Exception:
                        logger.exception(
                            "Pending-message recovery failed"
                        )

                    try:
                        recovered_jobs = recover_expired_leases(
                            db=db,
                            batch_size=100,
                        )

                        if recovered_jobs:
                            logger.info(
                                "Recovered or failed %s expired-lease job(s)",
                                recovered_jobs,
                            )
                    except Exception:
                        logger.exception(
                            "Database lease recovery failed"
                        )
    except KeyboardInterrupt:
        logger.info("Worker shutting down")

    finally:
        redis_client.close()


if __name__ == "__main__":
    run_worker()