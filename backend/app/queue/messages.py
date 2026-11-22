from datetime import datetime
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field


class JobMessage(BaseModel):
    """Message used to dispatch a persisted job to a worker."""

    model_config = ConfigDict(extra="forbid")

    message_version: int = 1
    job_id: UUID
    task_type: str = Field(min_length=1, max_length=100)
    priority: int = Field(ge=-100, le=100)
    enqueued_at: datetime