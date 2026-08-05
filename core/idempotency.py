"""
Idempotency handling for job submission.

A client can pass an idempotency_key on POST /jobs. Submitting the
same key twice should not create two rows — later calls should return
the existing job's current state.

find_existing_job() alone is NOT sufficient under concurrency: two
requests with the same key can both see "not found" before either
commits. The real guarantee comes from the unique constraint on
Job.idempotency_key (db/models.py) — enqueue() catches the resulting
IntegrityError and re-queries, rather than relying on this check alone.
"""
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from db.models import Job


async def find_existing_job(db: AsyncSession, idempotency_key: str) -> Job | None:
    """Best-effort pre-check — skips a wasted insert in the common case."""
    return await db.scalar(
        select(Job).where(Job.idempotency_key == idempotency_key)
    )