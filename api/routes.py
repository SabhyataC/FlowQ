import uuid

from fastapi import APIRouter, Depends, HTTPException
import redis.asyncio as redis
from sqlalchemy.ext.asyncio import AsyncSession

from db.connection import get_db
from db.redis_client import get_redis
from db.models import Job
from schemas.job import JobCreate, JobResponse, JobBatchCreate, JobBatchResponse
from core.queue import enqueue, enqueue_batch
from core.dag import DAGValidationError
from typing import Optional
from sqlalchemy import select

router = APIRouter()


@router.post("/jobs", response_model=JobResponse, status_code=201)
async def submit_job(
    job_data: JobCreate,
    db: AsyncSession = Depends(get_db),
    redis_client: redis.Redis = Depends(get_redis),
):
    """
    Submit a new job. Writes the Job row to Postgres and pushes it
    onto the appropriate Redis stream for a worker to pick up.
    """
    job = await enqueue(db, redis_client, job_data)
    return job


@router.get("/jobs/anomalies", response_model=list[JobResponse])
async def list_anomalies(
    job_type: Optional[str] = None,
    limit: int = 50,
    db: AsyncSession = Depends(get_db),
):
    """
    List jobs flagged as anomalous (is_anomaly=True), most recent first.
    Optionally filter by job_type.
    """
    query = select(Job).where(Job.is_anomaly == True).order_by(Job.completed_at.desc())

    if job_type is not None:
        query = query.where(Job.type == job_type)

    query = query.limit(limit)

    result = await db.execute(query)
    return result.scalars().all()

@router.get("/jobs/{job_id}", response_model=JobResponse)
async def get_job_status(
    job_id: uuid.UUID,
    db: AsyncSession = Depends(get_db),
):
    """
    Check the status of a job by id.
    """

    job = await db.get(Job, job_id)

    if job is None:
        raise HTTPException(status_code=404, detail="Job not found")

    return job


@router.post("/jobs/batch", response_model=JobBatchResponse, status_code=201)
async def submit_job_batch(
    batch: JobBatchCreate,
    db: AsyncSession = Depends(get_db),
    redis_client: redis.Redis = Depends(get_redis),
):
    try:
        jobs_by_ref = await enqueue_batch(db, redis_client, batch.jobs)
    except DAGValidationError as e:
        raise HTTPException(status_code=422, detail=str(e))
    return JobBatchResponse(jobs=jobs_by_ref)
