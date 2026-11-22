from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from enum import Enum
from typing import Any
from uuid import UUID, uuid4

from sqlalchemy import or_, update
from sqlalchemy.orm import Session

from backend.app.models.job import Job, JobStatus
from backend.app.models.outbox import OutboxEvent
from backend.app.worker.handlers import HandlerRegistry
from backend.app.worker.lease import LEASE_DURATION, LeaseHeartbeat


class ExecutionStatus(str, Enum):
    SUCCEEDED = "succeeded"
    FAILED = "failed"
    NOT_CLAIMED = "not_claimed"
    RETRY_SCHEDULED = "retry_scheduled"
    LEASE_LOST = "lease_lost"


@dataclass(frozen=True)
class ExecutionResult:
    job_id: UUID
    status: ExecutionStatus
    result: Any | None = None
    error: str | None = None


def execute_job(
    db: Session,
    job_id: UUID,
    registry: HandlerRegistry,
) -> ExecutionResult:
    """Claim a pending job, execute it, and finalize only if ownership remains."""
    now = datetime.now(timezone.utc)
    claim_token = uuid4()
    lease_expires_at = now + LEASE_DURATION

    claim = (
        update(Job)
        .where(
            Job.id == job_id,
            Job.status == JobStatus.PENDING,
            or_(
                Job.scheduled_at.is_(None),
                Job.scheduled_at <= now,
            ),
            Job.attempts < Job.max_attempts,
        )
        .values(
            status=JobStatus.RUNNING,
            attempts=Job.attempts + 1,
            started_at=now,
            finished_at=None,
            updated_at=now,
            claim_token=claim_token,
            lease_expires_at=lease_expires_at,
        )
        .execution_options(synchronize_session=False)
    )

    try:
        claim_result = db.execute(claim)
        if claim_result.rowcount != 1:
            db.rollback()
            return ExecutionResult(
                job_id=job_id,
                status=ExecutionStatus.NOT_CLAIMED,
            )
        db.commit()
    except Exception:
        db.rollback()
        raise

    db.expire_all()
    job = db.get(Job, job_id)
    if job is None:
        raise RuntimeError(f"Claimed job {job_id} could not be loaded.")

    try:
        heartbeat = LeaseHeartbeat(job_id, claim_token)
        heartbeat.start()

        try:
            task_result = registry.execute(job.task_type, job.payload)
        except Exception:
            raise
        finally:
            heartbeat.stop()

        if heartbeat.lease_lost:
            return ExecutionResult(
                job_id=job_id,
                status=ExecutionStatus.LEASE_LOST,
                error="Lease ownership was lost during execution.",
            )

    except Exception as exc:
        error_message = f"{type(exc).__name__}: {exc}"[:2000]
        failure_time = datetime.now(timezone.utc)

        try:
            # The token check fences off a stale worker.
            owned = db.execute(
                update(Job)
                .where(
                    Job.id == job_id,
                    Job.status == JobStatus.RUNNING,
                    Job.claim_token == claim_token,
                )
                .values(
                    last_error=error_message,
                    updated_at=failure_time,
                )
                .execution_options(synchronize_session=False)
            )

            if owned.rowcount != 1:
                db.rollback()
                return ExecutionResult(
                    job_id=job_id,
                    status=ExecutionStatus.LEASE_LOST,
                    error=error_message,
                )

            db.expire_all()
            failed_job = db.get(Job, job_id)
            if failed_job is None:
                raise RuntimeError(
                    f"Job {job_id} disappeared after execution failure."
                )

            if failed_job.attempts < failed_job.max_attempts:
                delay_seconds = min(
                    2 ** (failed_job.attempts - 1), 30
                )
                retry_at = failure_time + timedelta(
                    seconds=delay_seconds
                )

                db.execute(
                    update(Job)
                    .where(
                        Job.id == job_id,
                        Job.status == JobStatus.RUNNING,
                        Job.claim_token == claim_token,
                    )
                    .values(
                        status=JobStatus.PENDING,
                        scheduled_at=retry_at,
                        finished_at=None,
                        claim_token=None,
                        lease_expires_at=None,
                        updated_at=failure_time,
                    )
                    .execution_options(synchronize_session=False)
                )

                db.add(
                    OutboxEvent(
                        event_type="job.retry_scheduled",
                        aggregate_id=job_id,
                        payload={
                            "message_version": 1,
                            "job_id": str(job_id),
                            "task_type": failed_job.task_type,
                            "priority": failed_job.priority,
                            "enqueued_at": retry_at.isoformat(),
                        },
                    )
                )
                db.commit()

                return ExecutionResult(
                    job_id=job_id,
                    status=ExecutionStatus.RETRY_SCHEDULED,
                    error=error_message,
                )

            db.execute(
                update(Job)
                .where(
                    Job.id == job_id,
                    Job.status == JobStatus.RUNNING,
                    Job.claim_token == claim_token,
                )
                .values(
                    status=JobStatus.FAILED,
                    finished_at=failure_time,
                    claim_token=None,
                    lease_expires_at=None,
                    updated_at=failure_time,
                )
                .execution_options(synchronize_session=False)
            )
            db.commit()

            return ExecutionResult(
                job_id=job_id,
                status=ExecutionStatus.FAILED,
                error=error_message,
            )
        except Exception:
            db.rollback()
            raise

    # Finalize success only while this attempt still owns the job.
    finished_at = datetime.now(timezone.utc)
    try:
        completion = db.execute(
            update(Job)
            .where(
                Job.id == job_id,
                Job.status == JobStatus.RUNNING,
                Job.claim_token == claim_token,
            )
            .values(
                status=JobStatus.SUCCEEDED,
                result=task_result,
                last_error=None,
                finished_at=finished_at,
                updated_at=finished_at,
                claim_token=None,
                lease_expires_at=None,
            )
            .execution_options(synchronize_session=False)
        )

        if completion.rowcount != 1:
            db.rollback()
            return ExecutionResult(
                job_id=job_id,
                status=ExecutionStatus.LEASE_LOST,
            )

        db.commit()
    except Exception:
        db.rollback()
        raise

    return ExecutionResult(
        job_id=job_id,
        status=ExecutionStatus.SUCCEEDED,
        result=task_result,
    )
