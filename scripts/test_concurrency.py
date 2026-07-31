"""
Task 4: fires N jobs at the exact same time (true concurrency, not a
sequential loop) and verifies:
  1. Every job reaches COMPLETED with no error.
  2. Each job's result actually matches the payload it was submitted
     with (catches cross-job mixups or duplicate processing, which a
     "did it complete" check alone wouldn't).
  3. (Bonus, informational) how many distinct workers picked up the
     batch, so you can eyeball that the pool is actually spreading
     work out.

Stdlib only (urllib + concurrent.futures) so there's no new
`pip install` needed. Requires the API (uvicorn) and pool
(core.pool) already running.

Usage:
    python scripts/test_concurrency.py --n 8
"""
import argparse
import json
import time
import urllib.request
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "http://localhost:8000"


def submit_job(n: int) -> dict:
    """POST one job with payload {"n": n}. Returns the parsed JSON response."""
    body = json.dumps({"type": "test_job", "payload": {"n": n}}).encode()
    req = urllib.request.Request(
        f"{BASE_URL}/jobs",
        data=body,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def get_job(job_id: str) -> dict:
    with urllib.request.urlopen(f"{BASE_URL}/jobs/{job_id}") as resp:
        return json.loads(resp.read())


def poll_until_done(job_id: str, timeout_s: int = 30, interval_s: float = 0.5) -> dict:
    """Poll a single job until it's COMPLETED/FAILED or timeout_s elapses."""
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        job = get_job(job_id)
        if job["status"] in ("completed", "failed"):
            return job
        time.sleep(interval_s)
    raise TimeoutError(f"job {job_id} did not finish within {timeout_s}s")


def main(n: int) -> None:
    print(f"Submitting {n} jobs concurrently...")

    # --- Fire all N submissions at once via a thread pool ---
    with ThreadPoolExecutor(max_workers=n) as pool:
        submitted = list(pool.map(submit_job, range(n)))

    job_ids = [job["id"] for job in submitted]
    print(f"Submitted {len(job_ids)} jobs: {job_ids}")

    # --- Poll all of them concurrently until each finishes ---
    print("Waiting for all jobs to finish...")
    with ThreadPoolExecutor(max_workers=n) as pool:
        futures = {pool.submit(poll_until_done, jid): jid for jid in job_ids}
        results = []
        for future in as_completed(futures):
            results.append(future.result())

    # --- Check 1: everything completed, nothing failed ---
    failed = [r for r in results if r["status"] != "completed"]
    if failed:
        print(f"FAIL: {len(failed)} job(s) did not complete: {failed}")
    else:
        print(f"PASS: all {len(results)} jobs completed successfully.")

    # --- Check 2: each job's result matches what it was submitted with ---
    mismatches = []
    for r in results:
        submitted_n = r["payload"]["n"]
        returned_n = r["result"]["payload_received"]["n"]
        if submitted_n != returned_n:
            mismatches.append((r["id"], submitted_n, returned_n))

    if mismatches:
        print(f"FAIL: {len(mismatches)} job(s) had mismatched payload/result:")
        for job_id, sent, got in mismatches:
            print(f"  job {job_id}: sent n={sent}, result shows n={got}")
    else:
        print("PASS: every job's result matches its own submitted payload (no mixups).")

    # --- Bonus: worker distribution, just to eyeball parallelism ---
    worker_counts: dict[str, int] = {}
    for r in results:
        worker_counts[r["worker_id"]] = worker_counts.get(r["worker_id"], 0) + 1
    print(f"Worker distribution: {worker_counts}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--n", type=int, default=8, help="number of concurrent jobs to submit")
    args = parser.parse_args()
    main(args.n)