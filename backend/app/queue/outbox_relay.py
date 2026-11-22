
from datetime import datetime, timezone

from sqlalchemy.orm import Session

from backend.app.models.outbox import OutboxEvent
from backend.app.queue.messages import JobMessage
from backend.app.queue.publisher import RedisJobPublisher


def publish_pending_events(
    db: Session,
    publisher: RedisJobPublisher,
    batch_size: int = 100,
) -> int:
    """Publish eligible outbox events to Redis."""
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")

    published_count = 0
    skipped_ids = set()

    for _ in range(batch_size):
        query = (
            db.query(OutboxEvent)
            .filter(OutboxEvent.published_at.is_(None))
        )

        if skipped_ids:
            query = query.filter(~OutboxEvent.id.in_(skipped_ids))

        event = (
            query
            .order_by(OutboxEvent.created_at, OutboxEvent.id)
            .with_for_update(skip_locked=True)
            .first()
        )

        if event is None:
            break

        try:
            if event.event_type == "job.retry_scheduled":
                retry_at = datetime.fromisoformat(
                    event.payload["enqueued_at"]
                )

                if retry_at.tzinfo is None:
                    retry_at = retry_at.replace(tzinfo=timezone.utc)

                if retry_at > datetime.now(timezone.utc):
                    skipped_ids.add(event.id)
                    db.rollback()
                    continue

            message = JobMessage.model_validate(event.payload)
            publisher.publish(message)

        except Exception as exc:
            event.last_error = f"{type(exc).__name__}: {exc}"[:2000]
            db.commit()
            break

        event.published_at = datetime.now(timezone.utc)
        event.last_error = None

        try:
            db.commit()
        except Exception:
            db.rollback()
            # Redis may already contain the message. Duplicate delivery
            # remains possible, so consumers must be idempotent.
            raise

        published_count += 1

    return published_count
