import uuid

import redis.asyncio as redis
from redis.exceptions import ResponseError
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy import select

from db.models import Job, JobStatus
from schemas.job import JobCreate
from core.dag import build_dependency_graph, topological_sort


async def ensure_consumer_group(
    redis_client: redis.Redis,
    stream_key: str,
    group_name: str,
) -> None:
    """
    Creates the consumer group for a stream if it doesn't already exist.
    Safe to call every time the app starts up.
    """
    try:
        await redis_client.xgroup_create(stream_key, group_name, id='0', mkstream=True)
    except ResponseError as e:
        if "BUSYGROUP" in str(e):
            print(f"Consumer group '{group_name}' already exists. Proceeding safely.")
        else:
            raise e


async def enqueue(
    db: AsyncSession,
    redis_client: redis.Redis,
    job_data: JobCreate,
) -> Job:
    """
    Producer: writes a Job to Postgres, then pushes its id onto the
    appropriate Redis stream so a worker can pick it up.
    """

    # --- Step 1: idempotency check ---
    if job_data.idempotency_key:
        existing_job = await db.scalar(
            select(Job).where(Job.idempotency_key == job_data.idempotency_key)
        )
        if existing_job is not None:
            return existing_job

    # --- Step 2: build and insert the Job row ---
    job = Job(
        type=job_data.type,
        payload=job_data.payload,
        priority=job_data.priority,
        queue=job_data.queue,
        idempotency_key=job_data.idempotency_key,
        scheduled_at=job_data.scheduled_at,
    )
    db.add(job)
    await db.commit()
    await db.refresh(job)

    # --- Step 3: push onto the stream ---
    await _push_to_stream(redis_client, job)

    return job


async def _push_to_stream(redis_client: redis.Redis, job: Job) -> None:
    """Push a job id onto its queue's Redis stream so a worker picks it up."""
    await redis_client.xadd(
        f"stream:{job.queue}",
        {"job_id": str(job.id)},
    )


async def enqueue_batch(
    db: AsyncSession,
    redis_client: redis.Redis,
    jobs_data: list[JobCreate],
) -> dict[str, Job]:

    graph = build_dependency_graph(jobs_data)
    topo_order = topological_sort(graph)  # raises DAGValidationError on cycles

    by_ref = {job.batch_ref: job for job in jobs_data}

    id_by_ref: dict[str, uuid.UUID] = {}
    created: dict[str, Job] = {}

    for ref in topo_order:
        job_data = by_ref[ref]

        dep_ids = [str(id_by_ref[dep_ref]) for dep_ref in job_data.depends_on]
        status = JobStatus.BLOCKED if dep_ids else JobStatus.PENDING

        job = Job(
            type=job_data.type,
            payload=job_data.payload,
            priority=job_data.priority,
            queue=job_data.queue,
            idempotency_key=job_data.idempotency_key,
            scheduled_at=job_data.scheduled_at,
            depends_on=dep_ids,
            status=status,
        )
        db.add(job)
        await db.flush()  # assigns job.id without committing yet

        id_by_ref[ref] = job.id
        created[ref] = job

    await db.commit()
    for job in created.values():
        await db.refresh(job)

    for job in created.values():
        if job.status == JobStatus.PENDING:
            await _push_to_stream(redis_client, job)

    return created


async def check_and_unblock_dependents(
    db: AsyncSession,
    redis_client: redis.Redis,
    completed_job_id: uuid.UUID,
) -> None:
    """
    Call this right after a job transitions to COMPLETED.

    Finds any BLOCKED job whose `depends_on` includes this job's id,
    and — for each one — checks whether *all* of its dependencies are
    now COMPLETED. If so, flips it to PENDING and pushes it onto its
    queue's stream so a worker can pick it up.
    """
    completed_id_str = str(completed_job_id)

    candidates = await db.scalars(
        select(Job).where(
            Job.status == JobStatus.BLOCKED,
            Job.depends_on.contains([completed_id_str]),
        )
    )

    for candidate in candidates:
        dep_uuids = [uuid.UUID(d) for d in candidate.depends_on]
        deps = list(await db.scalars(select(Job).where(Job.id.in_(dep_uuids))))
        print(f"[unblock-check] candidate={candidate.id} deps={[(str(d.id), d.status) for d in deps]}")

        if len(deps) != len(dep_uuids):
            continue

        if all(dep.status == JobStatus.COMPLETED for dep in deps):
            print(f"[unblock-check] UNBLOCKING {candidate.id}")
            candidate.status = JobStatus.PENDING
            await db.commit()
            await db.refresh(candidate)
            await _push_to_stream(redis_client, candidate)


async def cascade_fail_dependents(
    db: AsyncSession,
    failed_job_id: uuid.UUID,
    reason: str | None = None,
) -> None:
    """
    Call this right after a job transitions to FAILED.

    A BLOCKED job that (directly or transitively) depends on a job that
    just failed can never become runnable, so mark it FAILED too instead
    of leaving it stuck in BLOCKED forever.
    """
    error = f"upstream dependency {failed_job_id} failed" + (f": {reason}" if reason else "")
    frontier = [failed_job_id]

    while frontier:
        parent_id = frontier.pop()
        dependents = list(await db.scalars(
            select(Job).where(
                Job.status == JobStatus.BLOCKED,
                Job.depends_on.contains([str(parent_id)]),
            )
        ))

        for dependent in dependents:
            dependent.status = JobStatus.FAILED
            dependent.error = error
        if dependents:
            await db.commit()

        frontier.extend(d.id for d in dependents)