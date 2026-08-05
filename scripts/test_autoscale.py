"""
Task 4: verifies that sending SIGTERM to the autoscaler process while
a job is mid-flight does NOT drop that job.

Sequence:
  1. Spawn `python -m core.scaler` as a real subprocess (min_size=1,
     short interval so it starts a worker immediately).
  2. Submit one job via POST /jobs.
  3. Poll until the job hits RUNNING (confirms a worker picked it up
     and is inside execute_job's sleep).
  4. Send SIGTERM to the subprocess *at that moment*.
  5. Poll until the job reaches COMPLETED -- if SIGTERM had interrupted
     the in-flight job instead of draining gracefully, this would time
     out with the job stuck at RUNNING.
  6. Confirm the subprocess itself exits shortly after, proving it
     drained and stopped rather than hanging or needing a SIGKILL.

Requires the API (uvicorn) already running. Spawns/kills the scaler
subprocess itself -- don't run core.scaler separately for this test.

Usage:
    python scripts/test_autoscale.py
"""
import json
import signal
import subprocess
import sys
import time
import urllib.request

BASE_URL = "http://127.0.0.1:8000" 


def post_job() -> dict:
    body = json.dumps({"type": "test_job", "payload": {"name": "autoscale-check"}}).encode()
    req = urllib.request.Request(
        f"{BASE_URL}/jobs", data=body,
        headers={"Content-Type": "application/json"}, method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def get_job(job_id: str) -> dict:
    with urllib.request.urlopen(f"{BASE_URL}/jobs/{job_id}") as resp:
        return json.loads(resp.read())


def wait_for_status(job_id: str, statuses: tuple[str, ...], timeout_s: float) -> dict:
    deadline = time.time() + timeout_s
    job = get_job(job_id)
    while job["status"] not in statuses and time.time() < deadline:
        time.sleep(0.2)
        job = get_job(job_id)
    return job


def main() -> None:
    print("--- Scale-down (SIGTERM) mid-job doesn't drop the job ---")

    proc = subprocess.Popen(
        [sys.executable, "-m", "core.scaler",
         "--min-size", "1", "--max-size", "2", "--interval-s", "1"],
    )

    try:
        # Give the scaler a moment to spawn its first worker and for
        # the consumer group to be ready.
        time.sleep(2)

        job = post_job()
        job_id = job["id"]
        print(f"Submitted job {job_id}, waiting for it to start running...")

        job = wait_for_status(job_id, ("running", "completed", "failed"), timeout_s=10)
        if job["status"] != "running":
            print(f"FAIL: job never reached RUNNING before test could "
                  f"SIGTERM mid-flight (status={job['status']})")
            return

        print(f"Job is RUNNING -- sending SIGTERM to scaler pid={proc.pid} now.")
        proc.send_signal(signal.SIGTERM)

        # execute_job sleeps ~2s; give generous headroom.
        job = wait_for_status(job_id, ("completed", "failed"), timeout_s=15)
        if job["status"] != "completed":
            print(f"FAIL: job did not reach COMPLETED after mid-flight "
                  f"SIGTERM (status={job['status']}) -- job was dropped "
                  f"or left stuck.")
            return
        print(f"PASS: job completed despite SIGTERM arriving mid-flight "
              f"(result={job['result']}).")

        # The scaler should drain and exit on its own -- not hang,
        # and not need to be force-killed.
        try:
            proc.wait(timeout=10)
            print(f"PASS: scaler process exited cleanly (code={proc.returncode}) "
                  f"after draining.")
        except subprocess.TimeoutExpired:
            print("FAIL: scaler process did not exit within 10s of the job "
                  "completing -- SIGTERM handling may be hung.")
            return

        print()
        print("ALL AUTOSCALE TESTS PASSED")

    finally:
        if proc.poll() is None:
            proc.kill()
            proc.wait()


if __name__ == "__main__":
    main()