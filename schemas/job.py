"""
Pydantic schemas for Job.

These sit between the API layer and the SQLAlchemy `Job` model.
They define what a client is *allowed to send* (JobCreate) and
what the API *sends back* (JobResponse) — deliberately narrower
and shaped differently than the DB model itself.
"""

from datetime import datetime
from typing import Any, Optional
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field

# Reuse the same enum the DB model uses — it's shared vocabulary
# ("what states can a job be in"), not a DB-specific detail.
from db.models import JobStatus  # adjust import path to match your project layout


# ---------------------------------------------------------------------------
# Input: what a client may send to create a job
# ---------------------------------------------------------------------------
class JobCreate(BaseModel):
    """
    Request body for POST /jobs (or Producer.enqueue(...)).

    Deliberately excludes: id, status, created_at, updated_at,
    started_at, completed_at, locked_at, worker_id, result, error.
    Those are owned by the server/worker, never the caller.
    """

    type: str = Field(
        ...,
        min_length=1,
        description="Job type identifier, e.g. 'send_email' or 'resize_image'.",
    )
    payload: dict[str, Any] = Field(
        ...,
        description="Arbitrary JSON payload the worker needs to process this job.",
    )
    priority: int = Field(
        default=0,
        ge=0,
        description="Higher runs first. Defaults to 0.",
    )
    queue: str = Field(
        default="default",
        min_length=1,
        description="Named queue this job belongs to.",
    )
    idempotency_key: Optional[str] = Field(
        default=None,
        description="Optional client-supplied key to prevent duplicate enqueues.",
    )
    scheduled_at: Optional[datetime] = Field(
        default=None,
        description="If set, job should not run before this time (delayed execution).",
    )
    batch_ref: Optional[str] = Field(
        default=None,
        description=(
            "Client-supplied label used to reference this job from other "
            "jobs' `depends_on` within the same POST /jobs/batch submission. "
            "Required for batch/DAG submissions; ignored (and not persisted) "
            "for single-job POST /jobs."
        ),
    )
    depends_on: list[str] = Field(
        default_factory=list,
        description=(
            "List of `batch_ref` values (from the same batch submission) that "
            "must reach COMPLETED before this job is eligible to run. Only "
            "meaningful for POST /jobs/batch — ignored for single-job POST /jobs."
        ),
    )
    # max_retries has a DB default of 3. If you want callers to be able to
    # override it, uncomment this. Left out for now since it wasn't in your
    # original spec — add it if Producer needs to support custom retry counts.
    # max_retries: int = Field(default=3, ge=0)


# ---------------------------------------------------------------------------
# Output: what the API returns for a job
# ---------------------------------------------------------------------------
class JobResponse(BaseModel):
    """
    Response shape for a job — returned after creation, and from any
    GET /jobs/{id} style endpoint. Close to the full DB model, since
    the caller wants visibility into what the system did with their job.

    `locked_at` is intentionally omitted — it's an internal worker-locking
    mechanism the caller has no use for. Add it back if you need to expose it.
    """

    model_config = ConfigDict(from_attributes=True)  # lets you return the SQLAlchemy object directly

    id: UUID
    status: JobStatus
    type: str
    payload: dict[str, Any]
    priority: int
    queue: str
    idempotency_key: Optional[str] = None

    created_at: datetime
    updated_at: datetime
    scheduled_at: Optional[datetime] = None
    started_at: Optional[datetime] = None
    completed_at: Optional[datetime] = None

    worker_id: Optional[str] = None
    max_retries: int
    result: Optional[dict[str, Any]] = None
    error: Optional[str] = None
    depends_on: list[str] = Field(
        default_factory=list,
        description="Actual job IDs (not batch_refs) this job depends on. Empty for non-DAG jobs.",
    )
    duration_seconds: Optional[float] = None
    anomaly_score: Optional[float] = None
    is_anomaly: bool = False

class JobBatchCreate(BaseModel):
    """
    Request body for POST /jobs/batch.

    Every job in `jobs` should set `batch_ref` (a label unique within this
    request) if other jobs in the batch need to depend on it. `depends_on`
    on each job refers to `batch_ref` values within this same list.
    """
    jobs: list[JobCreate] = Field(..., min_length=1)


class JobBatchResponse(BaseModel):
    """
    Response for POST /jobs/batch: maps each submitted job's `batch_ref`
    to the created job. Jobs submitted without a `batch_ref` are keyed by
    their generated job id string instead.
    """
    jobs: dict[str, JobResponse]


from typing import Literal, Union
from pydantic import Field

class BulkRepeatCreate(BaseModel):
    mode: Literal["repeat"] = "repeat"
    type: str
    payload: dict = {}
    priority: int = 0
    queue: str = "default"
    count: int = Field(..., ge=1, le=500)

class BulkListItem(BaseModel):
    type: str
    payload: dict = {}
    priority: int = 0
    queue: str = "default"

class BulkListCreate(BaseModel):
    mode: Literal["list"] = "list"
    jobs: list[BulkListItem] = Field(..., min_length=1, max_length=500)

JobBulkCreate = Union[BulkRepeatCreate, BulkListCreate]

class JobBulkResponse(BaseModel):
    submitted: int
    jobs: list[JobResponse]