from sqlalchemy import Column, Enum, String, DateTime, Integer, Index, Float, Boolean
from sqlalchemy.dialects.postgresql import UUID, JSONB
import uuid
from sqlalchemy.sql import func
from datetime import datetime, timezone
from .connection import Base

import enum

class JobStatus(str, enum.Enum):
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    BLOCKED = "blocked"

class Job(Base):
    __tablename__ = "jobs"
    id = Column(
        UUID(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )
    status = Column(
        Enum(JobStatus),
        nullable=False,
        default=JobStatus.PENDING,
        index=True,  # you'll query "give me all pending jobs" constantly — index it
    )
    type = Column(
        String(),
        nullable=False,
        index=True,
    )
    payload = Column(
        JSONB(),
        nullable=False,
    )
    
    created_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),   # refreshes automatically on every update
        nullable=False,
    )
    scheduled_at = Column(
        DateTime(timezone=True),
        nullable=True,
    )
    started_at = Column(
        DateTime(timezone=True),
        nullable=True,
    )
    completed_at = Column(
        DateTime(timezone=True),
        nullable=True,
    )
    locked_at = Column(
        DateTime(timezone=True),
        nullable=True,
    )
    priority = Column(
        Integer(),
        nullable=False,
        default=0,
    )
    worker_id = Column(
        String(),
        nullable=True,
        index=True,
    )
    idempotency_key = Column(
        String(),
        nullable=True,
        unique=True,
        index=True,
    )
    max_retries = Column(
        Integer(),
        nullable=False,
        default=3,
    )
    retry_count = Column(
        Integer(),
        nullable=False,
        default=0,
    )
    result = Column(
        JSONB(),
        nullable=True,
    )
    error = Column(
        String(),
        nullable=True,
    )
    queue = Column(
        String(),
        nullable=False,
        default="default",
        index=True,
    )

    depends_on = Column(
        JSONB(),
        nullable=False,
        default=list,
        server_default="[]",
    )
    duration_seconds = Column(
        Float(),
        nullable=True,  # only set once a job reaches COMPLETED
    )
    anomaly_score = Column(
        Float(),
        nullable=True,  # z-score vs this job_type's recent history; null until enough samples exist
    )
    is_anomaly = Column(
        Boolean(),
        nullable=False,
        default=False,
        index=True,  # the anomalies API endpoint filters on this
    )

    __table_args__ = (
        Index("ix_jobs_status_queue_priority", "status", "queue", "priority"),
    )