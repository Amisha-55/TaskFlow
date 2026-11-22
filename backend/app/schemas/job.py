
from datetime import datetime
from typing import Any
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, field_validator

from backend.app.models.job import JobStatus


class JobCreate(BaseModel):
    """Fields accepted when a client submits a new job."""

    task_type: str = Field(min_length=1, max_length=100)
    payload: dict[str, Any] = Field(default_factory=dict)
    priority: int = Field(default=0, ge=-100, le=100)
    max_attempts: int = Field(default=3, ge=1, le=10)
    scheduled_at: datetime | None = None

    @field_validator("task_type")
    @classmethod
    def task_type_must_not_be_blank(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("task_type must not be blank")
        return value

    @field_validator("scheduled_at")
    @classmethod
    def scheduled_at_must_include_timezone(
        cls, value: datetime | None
    ) -> datetime | None:
        if value is not None and (
            value.tzinfo is None or value.utcoffset() is None
        ):
            raise ValueError("scheduled_at must include a timezone")
        return value


class JobRead(BaseModel):
    """Fields returned to clients when reading a job."""

    model_config = ConfigDict(from_attributes=True)

    id: UUID
    task_type: str
    payload: dict[str, Any]
    status: JobStatus
    priority: int
    attempts: int
    max_attempts: int
    scheduled_at: datetime | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    updated_at: datetime
    result: Any | None
    last_error: str | None
