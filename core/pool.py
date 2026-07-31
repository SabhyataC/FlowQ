import asyncio
import argparse
import uuid

from db.redis_client import redis_client
from core.worker import run_forever

DEFAULT_POOL_SIZE = 4
DEFAULT_IDLE_MS = 30_000


async def run_pool(
    queue: str = "default",
    pool_size: int = DEFAULT_POOL_SIZE,
    idle_ms: int = DEFAULT_IDLE_MS,
) -> None:
    """
    Spawns `pool_size` concurrent workers, each with its own worker_id,
    all consuming from the same Redis consumer group so jobs are
    distributed across them (no two workers get the same message).

    NOTE: asyncio.gather() here has no return_exceptions=True — if any
    one worker's run_forever raises unhandled, the whole pool tears
    down and the other workers get cancelled too. That's fine for now;
    task 2 (crash handling) is where this gets revisited.
    """
    worker_ids = [f"worker-{uuid.uuid4().hex[:8]}" for _ in range(pool_size)]
    tasks = [
        run_forever(redis_client, queue=queue, worker_id=wid, idle_ms=idle_ms)
        for wid in worker_ids
    ]
    await asyncio.gather(*tasks)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--pool-size", type=int, default=DEFAULT_POOL_SIZE)
    parser.add_argument(
        "--idle-ms", type=int, default=DEFAULT_IDLE_MS,
        help="How long (ms) a message sits unacked before it's treated as "
             "orphaned/crashed and reclaimed. Lower for fast manual testing, "
             "e.g. --idle-ms 3000.",
    )
    args = parser.parse_args()
    asyncio.run(run_pool(pool_size=args.pool_size, idle_ms=args.idle_ms))