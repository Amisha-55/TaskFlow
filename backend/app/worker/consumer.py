
import json
import logging
from datetime import datetime, timezone
from typing import Any

from redis import Redis
from sqlalchemy.orm import Session

from backend.app.models.job import Job, JobStatus
from backend.app.queue.messages import JobMessage
from backend.app.worker.executor import (
    ExecutionStatus,
    execute_job,
)
from backend.app.worker.handlers import HandlerRegistry

logger = logging.getLogger(__name__)

STREAM_NAME = "taskflow:jobs"
GROUP_NAME = "taskflow:workers"


def ensure_consumer_group(
    client: Redis,
    stream_name: str = STREAM_NAME,
    group_name: str = GROUP_NAME,
) -> None:
    """Create the consumer group if it does not exist."""
    try:
        client.xgroup_create(
            name=stream_name,
            groupname=group_name,
            id="0",
            mkstream=True,
        )
    except Exception as exc:
        # BUSYGROUP means another process already created it.
        if "BUSYGROUP" not in str(exc):
            raise


def process_stream_message(
    db: Session,
    client: Redis,
    registry: HandlerRegistry,
    message_id: str,
    fields: dict[str, Any],
    stream_name: str = STREAM_NAME,
    group_name: str = GROUP_NAME,
) -> bool:
    """
    Process one stream entry.

    Returns True when the entry is acknowledged.
    Returns False when it must remain pending.
    """
    try:
        raw_message = fields["message"]
        message = JobMessage.model_validate_json(raw_message)
    except Exception:
        logger.exception("Invalid message %s; acknowledging poison message", message_id)
        client.xack(stream_name, group_name, message_id)
        return True

    try:
        outcome = execute_job(db, message.job_id, registry)
    except Exception:
        logger.exception("Could not process job %s", message.job_id)
        # Leave the entry pending so it can be recovered.
        return False

    if outcome.status == ExecutionStatus.RETRY_SCHEDULED:
        client.xack(stream_name, group_name, message_id)
        return True

    if outcome.status in (
        ExecutionStatus.SUCCEEDED,
        ExecutionStatus.FAILED,
    ):
        client.xack(stream_name, group_name, message_id)
        return True

    # The executor did not claim the job. Check whether it is safe
    # to acknowledge this delivery or whether it must wait.
    job = db.get(Job, message.job_id)

    if job is None:
        # The referenced job no longer exists.
        client.xack(stream_name, group_name, message_id)
        return True

    if job.status != JobStatus.PENDING:
        # A duplicate delivery for a running or terminal job.
        client.xack(stream_name, group_name, message_id)
        return True

    now = datetime.now(timezone.utc)
    scheduled_at = job.scheduled_at

    if scheduled_at is not None:
        if scheduled_at.tzinfo is None:
            scheduled_at = scheduled_at.replace(tzinfo=timezone.utc)

        if scheduled_at > now:
            # Do not lose a job that is not due yet.
            return False

    # Keep ineligible pending jobs available for investigation/recovery.
    return False


def consume_once(
    db: Session,
    client: Redis,
    registry: HandlerRegistry,
    consumer_name: str,
    block_ms: int = 1000,
) -> int:
    """Read and process up to one new message."""
    ensure_consumer_group(client)

    batches = client.xreadgroup(
        groupname=GROUP_NAME,
        consumername=consumer_name,
        streams={STREAM_NAME: ">"},
        count=1,
        block=block_ms,
    )

    processed = 0

    for stream_name, entries in batches:
        for message_id, fields in entries:
            process_stream_message(
                db=db,
                client=client,
                registry=registry,
                message_id=message_id,
                fields=fields,
                stream_name=stream_name,
            )
            processed += 1

    return processed


def recover_pending_once(
    db: Session,
    client: Redis,
    registry: HandlerRegistry,
    consumer_name: str,
    min_idle_ms: int = 30_000,
    batch_size: int = 10,
) -> int:
    """Reclaim and process messages abandoned by another worker."""
    ensure_consumer_group(client)

    _, entries, _ = client.xautoclaim(
        name=STREAM_NAME,
        groupname=GROUP_NAME,
        consumername=consumer_name,
        min_idle_time=min_idle_ms,
        start_id="0-0",
        count=batch_size,
    )

    for message_id, fields in entries:
        process_stream_message(
            db=db,
            client=client,
            registry=registry,
            message_id=message_id,
            fields=fields,
        )

    return len(entries)
