from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException, status
from sqlalchemy.orm import Session

from backend.app.database import get_db
from backend.app.models.job import Job
from backend.app.models.outbox import OutboxEvent
from backend.app.schemas.job import JobCreate, JobRead
from datetime import datetime, timezone
from uuid import UUID

from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session

from backend.app.database import get_db
from backend.app.models.job import Job, JobStatus
from backend.app.models.outbox import OutboxEvent
from backend.app.schemas.job import JobCreate, JobRead

router = APIRouter(prefix="/api/v1/jobs", tags=["jobs"])


@router.post(
    "",
    response_model=JobRead,
    status_code=status.HTTP_201_CREATED,
)
def create_job(
    job_data: JobCreate,
    db: Session = Depends(get_db),
) -> Job:
    """Persist a new job and its outbox event atomically."""

    job = Job(
        task_type=job_data.task_type,
        payload=job_data.payload,
        priority=job_data.priority,
        max_attempts=job_data.max_attempts,
        scheduled_at=job_data.scheduled_at,
    )

    try:
        db.add(job)

        # Insert the job first so its generated ID is available.
        db.flush()

        # Record the event in the same transaction as the job.
        event = OutboxEvent(
            event_type="job.created",
            aggregate_id=job.id,
            payload={
                "message_version": 1,
                "job_id": str(job.id),
                "task_type": job.task_type,
                "priority": job.priority,
                "enqueued_at": datetime.now(timezone.utc).isoformat(),
            },
        )
        db.add(event)

        # Both records become durable together.
        db.commit()
        db.refresh(job)

    except Exception as exc:
        db.rollback()
        # Log the exception in a later observability step.
        raise HTTPException(
            status_code=status.HTTP_503_SERVICE_UNAVAILABLE,
            detail="Could not persist the job.",
        ) from exc

    return job

    
@router.get("", response_model=list[JobRead])
def list_jobs(
    status_filter: JobStatus | None = Query(
        default=None,
        alias="status",
    ),
    limit: int = Query(default=20, ge=1, le=100),
    offset: int = Query(default=0, ge=0),
    db: Session = Depends(get_db),
) -> list[Job]:
    """List jobs with optional status filtering and pagination."""

    query = db.query(Job)

    if status_filter is not None:
        query = query.filter(Job.status == status_filter)

    return (
        query
        .order_by(Job.created_at.desc(), Job.id.desc())
        .offset(offset)
        .limit(limit)
        .all()
    )


@router.get("/{job_id}", response_model=JobRead)
def get_job(
    job_id: UUID,
    db: Session = Depends(get_db),
) -> Job:
    """Retrieve a single job by its ID."""

    job = db.get(Job, job_id)

    if job is None:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND,
            detail="Job not found.",
        )

    return job
