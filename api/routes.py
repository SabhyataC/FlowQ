import asyncio
import json
import uuid
from datetime import datetime, timezone

from fastapi import APIRouter, Depends, HTTPException
from fastapi.responses import StreamingResponse
import redis.asyncio as redis
from sqlalchemy.ext.asyncio import AsyncSession

from db.connection import get_db, AsyncSessionLocal
from db.redis_client import get_redis
from db.models import Job, JobStatus
from schemas.job import JobCreate, JobResponse, JobBatchCreate, JobBatchResponse
from core.queue import enqueue, enqueue_batch
from core.scaler import SCALER_STATUS_KEY
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

@router.get("/jobs", response_model=list[JobResponse])
async def list_jobs(
    status: Optional[JobStatus] = None,
    type: Optional[str] = None,
    queue: Optional[str] = None,
    limit: int = 200,
    db: AsyncSession = Depends(get_db),
):
    """
    List recent jobs, most recently created first. Powers the dashboard's
    job table; DAG edges are reconstructed client-side from each job's
    `depends_on` (actual job ids) rather than a separate endpoint.
    """
    query = select(Job).order_by(Job.created_at.desc())
    if status is not None:
        query = query.where(Job.status == status)
    if type is not None:
        query = query.where(Job.type == type)
    if queue is not None:
        query = query.where(Job.queue == queue)
    query = query.limit(min(limit, 500))

    result = await db.execute(query)
    return result.scalars().all()


@router.get("/scaler/status")
async def scaler_status(redis_client: redis.Redis = Depends(get_redis)):
    """
    Live autoscaler snapshot (queue depth, active/target/pool size),
    written by core/scaler.py's run_autoscaler() on each tick. Returns
    all-zero defaults if the scaler process isn't running yet.
    """
    data = await redis_client.hgetall(SCALER_STATUS_KEY)
    return {
        "depth": int(data.get("depth", 0)),
        "active": int(data.get("active", 0)),
        "target": int(data.get("target", 0)),
        "pool_size": int(data.get("pool_size", 0)),
        "updated_at": data.get("updated_at"),
        "running": bool(data),
    }


@router.get("/jobs/stream")
async def stream_jobs(
    redis_client: redis.Redis = Depends(get_redis),
):
    """
    Server-Sent Events feed for the dashboard: one long-lived connection
    instead of the browser polling. On connect, emits a `snapshot` of
    recent jobs and current scaler status; every ~700ms afterwards,
    emits a `jobs` event with only rows whose updated_at has moved
    since the last tick, plus a fresh `scaler` event.

    Opens a fresh short-lived DB session per poll tick (via
    AsyncSessionLocal directly) rather than holding one FastAPI-injected
    session for the connection's whole lifetime -- a long-held
    Depends(get_db) session got torn down mid-stream and caused the
    dashboard to loop on "reconnecting...".

    Implementation note, stated plainly: this is a short-interval DB
    poll under the hood, not true event-driven pub/sub -- workers write
    to Postgres directly with no event bus. Real push (core/worker.py
    publishing to Redis on every status change) would remove the 700ms
    poll entirely; left as a follow-up since it touches the worker's
    hot path. From the browser's side it's indistinguishable -- it just
    receives pushed events, no polling logic of its own.
    """
    async def event_generator():
        last_seen = datetime.now(timezone.utc)

        async with AsyncSessionLocal() as db:
            result = await db.execute(select(Job).order_by(Job.created_at.desc()).limit(200))
            snapshot = result.scalars().all()
        payload = [JobResponse.model_validate(j).model_dump(mode="json") for j in snapshot]
        yield f"event: snapshot\ndata: {json.dumps(payload)}\n\n"

        try:
            while True:
                await asyncio.sleep(0.7)

                async with AsyncSessionLocal() as db:
                    result = await db.execute(
                        select(Job)
                        .where(Job.updated_at > last_seen)
                        .order_by(Job.updated_at.asc())
                    )
                    changed = result.scalars().all()
                if changed:
                    last_seen = changed[-1].updated_at
                    payload = [JobResponse.model_validate(j).model_dump(mode="json") for j in changed]
                    yield f"event: jobs\ndata: {json.dumps(payload)}\n\n"

                scaler_data = await redis_client.hgetall(SCALER_STATUS_KEY)
                yield f"event: scaler\ndata: {json.dumps(scaler_data)}\n\n"
        except asyncio.CancelledError:
            # Client disconnected -- let the generator end quietly.
            pass

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={"Cache-Control": "no-cache", "X-Accel-Buffering": "no"},
    )
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