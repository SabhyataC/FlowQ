from datetime import datetime, timezone
import uuid
import redis.asyncio as redis
from sqlalchemy import func
import asyncio
from core.stats import record_completion_and_score
from db.connection import AsyncSessionLocal
from db.models import Job, JobStatus
from core.queue import check_and_unblock_dependents, cascade_fail_dependents


STREAM_KEY_TEMPLATE = "stream:{queue}"
GROUP_NAME = "worker"


async def process_batch(
    redis_client: redis.Redis,
    worker_id: str,
    queue: str = "default",
    count: int = 10,
    block_ms: int = 5000,
) -> int:
    """
    Single pass: read up to `count` pending messages from the stream,
    execute each job, update its status in Postgres, and ACK it.

    Returns the number of messages processed. Safe to call directly
    in a test/script (no infinite loop), or from `run_forever` below.
    """
    stream_key = STREAM_KEY_TEMPLATE.format(queue=queue)

    # --- Step 1: read a batch from the stream via the consumer group ---
    messages = await redis_client.xreadgroup(
        groupname=GROUP_NAME,
        consumername=worker_id,
        streams={stream_key: ">"},
        count=count,
        block=block_ms,
    )

    if not messages:
        return 0
    
    processed = 0

    for _stream, entries in messages:
        for message_id, fields in entries:
            job_id = fields.get("job_id")

            async with AsyncSessionLocal() as db:
                # --- Step 2: fetch the Job row ---
                job = await db.get(Job, uuid.UUID(job_id))
                if job is None:
                    await redis_client.xack(stream_key, GROUP_NAME, message_id)
                    continue

                # --- Step 3: mark RUNNING ---
                job.status = JobStatus.RUNNING
                job.started_at = datetime.now(timezone.utc) 
                job.worker_id = worker_id
                await db.commit()

                # --- Step 4: execute ---
                try:
                    result, error = await execute_job(job)
                except Exception as e:
                    result, error = None, str(e)

                # --- Step 5: mark COMPLETED or FAILED ---
                if error is None:
                    job.status = JobStatus.COMPLETED
                    job.result = result
                    job.completed_at = datetime.now(timezone.utc)
                    job.duration_seconds = (job.completed_at - job.started_at).total_seconds()

                    job.anomaly_score, job.is_anomaly = await record_completion_and_score(
                        db, job.type, job.duration_seconds
                    )
                else:
                    job.status = JobStatus.FAILED
                    job.error = error
                    job.completed_at = datetime.now(timezone.utc)

                await db.commit()

                # --- Step 5b: propagate to any DAG dependents ---
                if error is None:
                    await check_and_unblock_dependents(db, redis_client, job.id)
                else:
                    await cascade_fail_dependents(db, job.id, reason=error)

            # --- Step 6: ACK the message so it leaves the PEL ---
            await redis_client.xack(stream_key, GROUP_NAME, message_id)

            processed += 1

    return processed


async def claim_stale_jobs(
    redis_client: redis.Redis,
    worker_id: str,
    queue: str = "default",
    idle_ms: int = 30_000,
    count: int = 10,
) -> int:
    """
    Finds messages in the consumer group's PEL that have been idle
    (unacknowledged) for longer than `idle_ms` — a sign the worker
    that originally claimed them died mid-job — and reassigns them
    to this worker to retry from scratch.
    """
    stream_key = STREAM_KEY_TEMPLATE.format(queue=queue)

    _next_start, claimed_messages, _deleted = await redis_client.xautoclaim(
        stream_key, GROUP_NAME, worker_id,
        min_idle_time=idle_ms, start_id='0-0', count=count,
    )

    reclaimed = 0

    for message_id, fields in claimed_messages:
        job_id = fields.get("job_id")

        async with AsyncSessionLocal() as db:
            job = await db.get(Job, uuid.UUID(job_id))
            if job is None:
                await redis_client.xack(stream_key, GROUP_NAME, message_id)
                continue

            if job.retry_count >= job.max_retries:
                job.status = JobStatus.FAILED
                job.error = "exceeded max_retries after worker crash"
                job.completed_at = datetime.now(timezone.utc)
                await db.commit()
                await cascade_fail_dependents(db, job.id, reason=job.error)
                await redis_client.xack(stream_key, GROUP_NAME, message_id)
                continue
            else:
                job.retry_count += 1
                job.worker_id = worker_id
                job.status = JobStatus.RUNNING
                job.started_at = datetime.now(timezone.utc)
                await db.commit()
                try:
                    result, error = await execute_job(job)
                except Exception as e:
                    result, error = None, str(e)

                if error is None:
                    job.status = JobStatus.COMPLETED
                    job.result = result
                    job.completed_at = datetime.now(timezone.utc)
                    job.duration_seconds = (job.completed_at - job.started_at).total_seconds()

                    job.anomaly_score, job.is_anomaly = await record_completion_and_score(
                        db, job.type, job.duration_seconds
                    )
                else:
                    job.status = JobStatus.FAILED
                    job.error = error
                    job.completed_at = datetime.now(timezone.utc)
                await db.commit()

                if error is None:
                    await check_and_unblock_dependents(db, redis_client, job.id)
                else:
                    await cascade_fail_dependents(db, job.id, reason=error)

                await redis_client.xack(stream_key, GROUP_NAME, message_id)
                reclaimed += 1

    return reclaimed


async def run_forever(
    redis_client: redis.Redis,
    queue: str = "default",
    worker_id: str | None = None,
    idle_ms: int = 30_000,
    stop_event: asyncio.Event | None = None,
) -> None:
    """
    Long-running consumer process. Intended to be run as its own
    process (`python -m core.worker`), separate from the API process,
    both pointed at the same Redis/Postgres.

    idle_ms controls how long a message must sit unacknowledged before
    claim_stale_jobs treats it as orphaned (crashed worker). Lower it
    for fast manual testing (e.g. 3000 = 3s) instead of waiting the
    30s default.

    stop_event: if provided, checked at the TOP of each loop iteration
    only -- i.e. after any in-flight job from the previous iteration
    has already fully completed and been ack'd. Setting it never
    interrupts a job in progress; it just stops this worker from
    picking up a new one. This is what makes scale-down (and later,
    SIGTERM) safe.
    """
    worker_id = worker_id or f"worker-{uuid.uuid4().hex[:8]}"

    while True:
        if stop_event is not None and stop_event.is_set():
            return
        await claim_stale_jobs(redis_client, worker_id=worker_id, queue=queue, idle_ms=idle_ms)
        await process_batch(redis_client, worker_id=worker_id, queue=queue)


async def execute_job(job: Job) -> tuple[dict | None, str | None]:
    """
    Simulated job execution. Returns (result, error) — exactly one
    of the two should be non-None. 
    TODO: for now, just `await asyncio.sleep(...)` to simulate work
    and return a canned result. Later this becomes a dispatch table
    keyed on job.type (see core/registry.py).
    """
    sleep_seconds = job.payload.get("_sleep_seconds", 2)  # TODO: remove once real dispatch table exists; test-only hook
    await asyncio.sleep(sleep_seconds)

    result = {
        "message": f"Simulated execution of job type '{job.type}'",
        "payload_received": job.payload,
    }
    error = None
    return result, error

if __name__ == "__main__":
    from db.redis_client import redis_client
    asyncio.run(run_forever(redis_client))