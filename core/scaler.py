"""
Auto-scaling: watches queue depth and grows/shrinks the worker pool
to match load.

Task order (per plan):
  1. Monitor queue depth to detect load           <- get_queue_depth()
  2. Spin workers up/down based on load            <- decide_target_size() + run_autoscaler()
  3. SIGTERM handling for graceful shutdown        <- core/worker.py / core/pool.py (TODO, separate task)
  4. Test that scaling down mid-job doesn't drop a job   <- scripts/test_autoscale.py (TODO, separate task)

Scale-down safety: a worker is never cancelled mid-job. Instead it's
handed an asyncio.Event (see core/worker.run_forever's stop_event
param) that it only checks between jobs, so "spinning a worker down"
really means "stop giving this worker new work; let it finish what
it's already holding, then let its task end naturally."
"""
import asyncio
import uuid

import redis.asyncio as redis

from core.worker import STREAM_KEY_TEMPLATE, GROUP_NAME, run_forever


async def get_queue_depth(
    redis_client: redis.Redis,
    queue: str = "default",
) -> int:
    """
    "Queue depth" here means *lag*: how many entries in the stream
    have not yet been delivered to any consumer in our group. This is
    the right load signal for scaling, distinct from XLEN (total
    entries ever added, including ones already delivered/ack'd) and
    from XPENDING count (delivered but not yet ack'd -- that's "work
    in flight", not "work waiting").

    Requires Redis >= 7.0 for the `lag` field on XINFO GROUPS.
    Returns 0 if the stream/group doesn't exist yet (nothing submitted).
    """
    stream_key = STREAM_KEY_TEMPLATE.format(queue=queue)

    try:
        groups = await redis_client.xinfo_groups(stream_key)
    except redis.ResponseError as e:
        if "no such key" in str(e).lower():
            return 0
        raise

    for group in groups:
        if group.get("name") == GROUP_NAME:
            # `lag` can be None if Redis can't compute it -- treat
            # unknown as 0 rather than crashing the scaler loop.
            return group.get("lag") or 0

    return 0


def decide_target_size(
    queue_depth: int,
    current_size: int,
    min_size: int,
    max_size: int,
    jobs_per_worker: int = 5,
) -> int:
    """
    Pure function, no I/O -- easy to unit test in isolation.

    Policy:
      - scale UP by 1 worker whenever backlog-per-worker exceeds
        `jobs_per_worker` (responsive to spikes; run_autoscaler acts
        on this immediately, no debounce)
      - scale DOWN by 1 worker only when the queue is fully drained
        (depth == 0); run_autoscaler requires several consecutive
        low-depth ticks before actually acting on this, to avoid
        thrashing on a queue that's briefly empty between bursts
      - one worker at a time in either direction -- gradual, avoids
        overshooting on a noisy signal
      - never outside [min_size, max_size]
    """
    if current_size <= 0:
        desired = 1 if queue_depth > 0 else min_size
    else:
        load_per_worker = queue_depth / current_size
        if load_per_worker > jobs_per_worker and current_size < max_size:
            desired = current_size + 1
        elif queue_depth == 0 and current_size > min_size:
            desired = current_size - 1
        else:
            desired = current_size

    return max(min_size, min(max_size, desired))


async def run_autoscaler(
    redis_client: redis.Redis,
    queue: str = "default",
    min_size: int = 1,
    max_size: int = 8,
    interval_s: float = 5.0,
    jobs_per_worker: int = 5,
    scale_down_confirm_ticks: int = 3,
) -> None:
    """
    Owns its own dynamic set of worker tasks (does not depend on
    core/pool.py's static pool -- run this INSTEAD of run_pool()).

    Each tick: reap any workers that finished shutting down, measure
    queue depth, decide a target size, and move the live pool one
    step towards it. Scale-up is immediate; scale-down requires
    `scale_down_confirm_ticks` consecutive ticks agreeing the pool
    should shrink, so a momentary lull doesn't yo-yo the pool size.
    """
    workers: dict[str, tuple[asyncio.Task, asyncio.Event]] = {}

    def _spawn_worker() -> None:
        worker_id = f"worker-{uuid.uuid4().hex[:8]}"
        stop_event = asyncio.Event()
        task = asyncio.create_task(
            run_forever(redis_client, queue=queue, worker_id=worker_id, stop_event=stop_event)
        )
        workers[worker_id] = (task, stop_event)
        print(f"[scaler] UP   +{worker_id}  (pool size {len(workers)})", flush=True)

    def _signal_one_to_stop() -> bool:
        """Ask one still-active worker to stop after its current job. Returns False if none are eligible (all already shutting down)."""
        for worker_id, (_task, stop_event) in workers.items():
            if not stop_event.is_set():
                stop_event.set()
                print(f"[scaler] DOWN -{worker_id} signalled (will finish current job first)", flush=True)
                return True
        return False

    def _reap_finished() -> None:
        done_ids = [wid for wid, (task, _ev) in workers.items() if task.done()]
        for worker_id in done_ids:
            task, _ev = workers.pop(worker_id)
            if not task.cancelled() and task.exception() is not None:
                print(f"[scaler] {worker_id} exited with error: {task.exception()}", flush=True)
            else:
                print(f"[scaler] {worker_id} stopped cleanly (pool size {len(workers)})", flush=True)

    for _ in range(min_size):
        _spawn_worker()

    low_ticks = 0

    try:
        while True:
            await asyncio.sleep(interval_s)
            _reap_finished()

            depth = await get_queue_depth(redis_client, queue)
            # "active" excludes workers already mid-shutdown -- they're
            # not available to take new target-size credit.
            active = sum(1 for _t, ev in workers.values() if not ev.is_set())
            target = decide_target_size(depth, active, min_size, max_size, jobs_per_worker)

            if target > active:
                low_ticks = 0
                _spawn_worker()
            elif target < active:
                low_ticks += 1
                if low_ticks >= scale_down_confirm_ticks:
                    if _signal_one_to_stop():
                        low_ticks = 0
            else:
                low_ticks = 0

            print(
                f"[scaler] depth={depth} active={active} target={target} "
                f"pool_size={len(workers)} low_ticks={low_ticks}",
                flush=True,
            )
    except asyncio.CancelledError:
        # Autoscaler itself is being shut down -- signal every worker
        # to stop after its current job, then wait for them, rather
        # than cancelling their tasks (which could drop a job mid-flight).
        for _task, stop_event in workers.values():
            stop_event.set()
        if workers:
            await asyncio.gather(*(t for t, _ev in workers.values()), return_exceptions=True)
        raise


if __name__ == "__main__":
    import argparse
    import signal
    from db.redis_client import redis_client

    parser = argparse.ArgumentParser()
    parser.add_argument("--queue", default="default")
    parser.add_argument("--min-size", type=int, default=1)
    parser.add_argument("--max-size", type=int, default=8)
    parser.add_argument("--interval-s", type=float, default=5.0)
    parser.add_argument("--jobs-per-worker", type=int, default=5)
    args = parser.parse_args()

    async def _main() -> None:
        task = asyncio.create_task(run_autoscaler(
            redis_client,
            queue=args.queue,
            min_size=args.min_size,
            max_size=args.max_size,
            interval_s=args.interval_s,
            jobs_per_worker=args.jobs_per_worker,
        ))

        loop = asyncio.get_running_loop()

        def _handle_sigterm() -> None:
            print("[scaler] SIGTERM received -- draining workers, "
                  "no new jobs will start", flush=True)
            task.cancel()

        # POSIX-only (fine -- Redis runs in WSL2/Linux per project setup).
        # Degrades to a printed warning rather than crashing on import if
        # unavailable (e.g. native Windows); Ctrl+C/SIGINT still works there.
        try:
            loop.add_signal_handler(signal.SIGTERM, _handle_sigterm)
        except NotImplementedError:
            print("[scaler] add_signal_handler unavailable on this platform "
                  "-- SIGTERM won't trigger a graceful drain here.", flush=True)

        try:
            await task
        except asyncio.CancelledError:
            print("[scaler] shutdown complete", flush=True)

    asyncio.run(_main())