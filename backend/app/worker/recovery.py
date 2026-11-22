
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from sqlalchemy import select
from sqlalchemy.orm import Session

from backend.app.models.job import Job, JobStatus
from backend.app.models.outbox import OutboxEvent


def recover_expired_leases(
    db: Session,
    *,
    batch_size: int = 100,
    now: datetime | None = None,
) -> int:
    """Recover jobs whose worker leases have expired.

    Returns the number of jobs recovered or marked failed.
    """
    if batch_size < 1:
        raise ValueError("batch_size must be at least 1")

    now = now or datetime.now(timezone.utc)
    recovered = 0

    try:
        jobs = db.scalars(
            select(Job)
            .where(
                Job.status == JobStatus.RUNNING,
                Job.lease_expires_at.is_not(None),
                Job.lease_expires_at <= now,
            )
            .order_by(Job.lease_expires_at, Job.id)
            .limit(batch_size)
            .with_for_update(skip_locked=True)
        ).all()

        for job in jobs:
            error = "Worker lease expired before job completion."

            # A crashed attempt counts toward the retry limit.
            if job.attempts >= job.max_attempts:
                job.status = JobStatus.FAILED
                job.finished_at = now
                job.last_error = error
                job.updated_at = now
                job.claim_token = None
                job.lease_expires_at = None
                recovered += 1
                continue

            delay_seconds = min(2 ** max(job.attempts - 1, 0), 30)
            retry_at = now + timedelta(seconds=delay_seconds)

            job.status = JobStatus.PENDING
            job.scheduled_at = retry_at
            job.finished_at = None
            job.last_error = error
            job.updated_at = now
            job.claim_token = None
            job.lease_expires_at = None

            db.add(
                OutboxEvent(
                    id=uuid4(),
                    event_type="job.retry_scheduled",
                    aggregate_id=job.id,
                    payload={
                        "job_id": str(job.id),
                        "task_type": job.task_type,
                        "enqueued_at": retry_at.isoformat(),
                    },
                )
            )
            recovered += 1

        db.commit()
        return recovered

    except Exception:
        db.rollback()
        raise
