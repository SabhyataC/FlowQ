"""
Auto-scaling: watches queue depth and grows/shrinks the worker pool
to match load.
"""
import asyncio
import uuid
from datetime import datetime, timezone

import redis.asyncio as redis

from core.worker import STREAM_KEY_TEMPLATE, GROUP_NAME, run_forever

SCALER_STATUS_KEY = "scaler:status"
SCALER_OVERRIDE_KEY = "scaler:override"
SCALER_KILL_CHANNEL = "scaler:kill"


async def get_queue_depth(
    redis_client: redis.Redis,
    queue: str = "default",
) -> int:
    stream_key = STREAM_KEY_TEMPLATE.format(queue=queue)

    try:
        groups = await redis_client.xinfo_groups(stream_key)
    except redis.ResponseError as e:
        if "no such key" in str(e).lower():
            return 0
        raise

    for group in groups:
        if group.get("name") == GROUP_NAME:
            return group.get("lag") or 0

    return 0


def decide_target_size(
    queue_depth: int,
    current_size: int,
    min_size: int,
    max_size: int,
    jobs_per_worker: int = 5,
) -> int:
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
    workers: dict[str, tuple[asyncio.Task, asyncio.Event]] = {}
    killed_ids: set[str] = set()

    def _spawn_worker() -> None:
        worker_id = f"worker-{uuid.uuid4().hex[:8]}"
        stop_event = asyncio.Event()
        task = asyncio.create_task(
            run_forever(redis_client, queue=queue, worker_id=worker_id, stop_event=stop_event)
        )
        workers[worker_id] = (task, stop_event)
        print(f"[scaler] UP   +{worker_id}  (pool size {len(workers)})", flush=True)

    def _signal_one_to_stop() -> bool:
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
            if worker_id in killed_ids:
                killed_ids.discard(worker_id)
                print(f"[scaler] KILLED {worker_id} torn down mid-job (demo trigger, pool size {len(workers)})", flush=True)
            elif not task.cancelled() and task.exception() is not None:
                print(f"[scaler] {worker_id} exited with error: {task.exception()}", flush=True)
            else:
                print(f"[scaler] {worker_id} stopped cleanly (pool size {len(workers)})", flush=True)

    async def _listen_for_kill_signals() -> None:
        """
        Subscribes to SCALER_KILL_CHANNEL. The API publishes a worker_id
        here (looked up from Job.worker_id) when the dashboard's kill
        button is used. Unlike _signal_one_to_stop, this cancels the
        task immediately -- mid-job -- rather than waiting for it to
        finish its current job first.
        """
        pubsub = redis_client.pubsub()
        await pubsub.subscribe(SCALER_KILL_CHANNEL)
        print(f"[scaler] kill listener subscribed to '{SCALER_KILL_CHANNEL}'", flush=True)
        try:
            async for message in pubsub.listen():
                if message["type"] != "message":
                    continue
                worker_id = message["data"]
                if isinstance(worker_id, bytes):
                    worker_id = worker_id.decode()
                entry = workers.get(worker_id)
                if entry is None:
                    print(f"[scaler] kill signal for unknown/already-gone {worker_id}", flush=True)
                    continue
                task, _stop_event = entry
                killed_ids.add(worker_id)
                task.cancel()
                print(f"[scaler] KILL  {worker_id} cancel requested (demo trigger)", flush=True)
        finally:
            await pubsub.unsubscribe(SCALER_KILL_CHANNEL)
            await pubsub.close()

    def _log_kill_listener_crash(task: asyncio.Task) -> None:
        if task.cancelled():
            return
        exc = task.exception()
        if exc is not None:
            print(f"[scaler] kill listener died: {exc!r}", flush=True)

    kill_listener_task = asyncio.create_task(_listen_for_kill_signals())
    kill_listener_task.add_done_callback(_log_kill_listener_crash)
    for _ in range(min_size):
        _spawn_worker()

    low_ticks = 0

    try:
        while True:
            await asyncio.sleep(interval_s)
            _reap_finished()

            depth = await get_queue_depth(redis_client, queue)
            active = sum(1 for _t, ev in workers.values() if not ev.is_set())

            override_raw = await redis_client.get(SCALER_OVERRIDE_KEY)
            manual_override = int(override_raw) if override_raw is not None else None

            if manual_override is not None:
                # Manual mode: converge to the requested count immediately
                # (no debounce -- this is an explicit user action, not a
                # load signal that needs smoothing).
                target = max(min_size, min(max_size, manual_override))
                low_ticks = 0
                while active < target:
                    _spawn_worker()
                    active += 1
                while active > target:
                    if _signal_one_to_stop():
                        active -= 1
                    else:
                        break
            else:
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
                f"pool_size={len(workers)} low_ticks={low_ticks} "
                f"mode={'manual' if manual_override is not None else 'auto'}",
                flush=True,
            )

            try:
                await redis_client.hset(
                    SCALER_STATUS_KEY,
                    mapping={
                        "depth": depth,
                        "active": active,
                        "target": target,
                        "pool_size": len(workers),
                        "updated_at": datetime.now(timezone.utc).isoformat(),
                        "manual": "1" if manual_override is not None else "0",
                    },
                )
            except Exception as e:
                print(f"[scaler] status publish failed (non-fatal): {e}", flush=True)
    except asyncio.CancelledError:
        kill_listener_task.cancel()
        for _task, stop_event in workers.values():
            stop_event.set()
        if workers:
            await asyncio.gather(*(t for t, _ev in workers.values()), return_exceptions=True)
        await asyncio.gather(kill_listener_task, return_exceptions=True)
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