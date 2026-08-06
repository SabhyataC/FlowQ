import math
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from db.models import JobTypeStats
from sqlalchemy.dialects.postgresql import insert as pg_insert

MIN_SAMPLES_FOR_SCORING = 5

async def record_completion_and_score(
    session: AsyncSession,
    job_type: str,
    duration_seconds: float,
) -> tuple[float | None, bool]:
    """
    Score this job's duration against the job_type's stats as they stood
    BEFORE this sample (to avoid the new sample masking itself), then
    fold this sample into the running Welford stats for future jobs.

    Must be called inside the same transaction that marks the job
    COMPLETED, so the stats row is locked for the duration of the update.

    Returns (anomaly_score, is_anomaly).
    """
    stmt = pg_insert(JobTypeStats).values(
        job_type=job_type, count=0, mean=0.0, m2=0.0
    ).on_conflict_do_nothing(index_elements=["job_type"])
    await session.execute(stmt)

    result = await session.execute(
        select(JobTypeStats).where(JobTypeStats.job_type == job_type).with_for_update()
    )
    stats_row = result.scalar_one_or_none()

    # --- Score FIRST, against pre-update stats ---
    prior_count = stats_row.count
    if prior_count < MIN_SAMPLES_FOR_SCORING:
        anomaly_score, is_anomaly = None, False
    else:
        prior_variance = stats_row.m2 / prior_count
        prior_stddev = math.sqrt(prior_variance)
        if prior_stddev == 0:
            anomaly_score, is_anomaly = None, False
        else:
            z = (duration_seconds - stats_row.mean) / prior_stddev
            anomaly_score, is_anomaly = z, abs(z) > 3.0

    # --- THEN update running stats with this sample, for future jobs ---
    count = stats_row.count + 1
    delta = duration_seconds - stats_row.mean
    mean = stats_row.mean + delta / count
    delta2 = duration_seconds - mean
    m2 = stats_row.m2 + delta * delta2
    stats_row.count = count
    stats_row.mean = mean
    stats_row.m2 = m2

    return (anomaly_score, is_anomaly)