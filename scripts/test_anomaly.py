import json
import time
import urllib.request
import random

NORMAL_SLEEP_RANGE = (1.8, 2.2)  # realistic jitter instead of a fixed 2.0

BASE_URL = "http://127.0.0.1:8000"

NORMAL_SLEEP = 2       # matches execute_job's default
SLOW_SLEEP = 20         # deliberately anomalous
SEED_COUNT = 5          # must be >= MIN_SAMPLES_FOR_SCORING in core/stats.py


def post_job(sleep_seconds: float) -> dict:
    body = json.dumps({
        "type": "anomaly_test_job",
        "payload": {"_sleep_seconds": sleep_seconds},
    }).encode()
    req = urllib.request.Request(
        f"{BASE_URL}/jobs", data=body,
        headers={"Content-Type": "application/json"}, method="POST",
    )
    with urllib.request.urlopen(req) as resp:
        return json.loads(resp.read())


def get_job(job_id: str) -> dict:
    with urllib.request.urlopen(f"{BASE_URL}/jobs/{job_id}") as resp:
        return json.loads(resp.read())


def get_anomalies(job_type: str) -> list[dict]:
    with urllib.request.urlopen(f"{BASE_URL}/jobs/anomalies?job_type={job_type}") as resp:
        return json.loads(resp.read())


def wait_for_status(job_id: str, statuses: tuple[str, ...], timeout_s: float) -> dict:
    deadline = time.time() + timeout_s
    job = get_job(job_id)
    while job["status"] not in statuses and time.time() < deadline:
        time.sleep(0.2)
        job = get_job(job_id)
    return job


def main() -> None:
    print("--- Phase 6: anomaly detection flags an artificially slow job ---")

    # Step 1: seed job_type_stats with enough normal-duration samples
    # to clear MIN_SAMPLES_FOR_SCORING before we submit the slow one.
    print(f"Seeding {SEED_COUNT} normal jobs (~{NORMAL_SLEEP}s each)...")
    seed_job_ids = []
    for i in range(SEED_COUNT):
        job = post_job(random.uniform(*NORMAL_SLEEP_RANGE))
        seed_job_ids.append(job["id"])

    for job_id in seed_job_ids:
        job = wait_for_status(job_id, ("completed", "failed"), timeout_s=15)
        if job["status"] != "completed":
            print(f"FAIL: seed job {job_id} did not complete (status={job['status']})")
            return
        if job["is_anomaly"]:
            print(f"FAIL: seed job {job_id} was flagged anomalous unexpectedly "
                  f"(anomaly_score={job['anomaly_score']}) -- threshold or "
                  f"MIN_SAMPLES_FOR_SCORING may be miscalibrated.")
            return

    print("PASS: all seed jobs completed and none were flagged.")

    # Step 2: submit the deliberately slow job.
    print(f"Submitting one slow job (~{SLOW_SLEEP}s)...")
    slow_job = post_job(SLOW_SLEEP)
    slow_job_id = slow_job["id"]

    slow_job = wait_for_status(slow_job_id, ("completed", "failed"), timeout_s=SLOW_SLEEP + 15)
    if slow_job["status"] != "completed":
        print(f"FAIL: slow job did not complete (status={slow_job['status']})")
        return

    if not slow_job["is_anomaly"]:
        print(f"FAIL: slow job was NOT flagged as anomalous "
              f"(duration_seconds={slow_job['duration_seconds']}, "
              f"anomaly_score={slow_job['anomaly_score']}).")
        return

    print(f"PASS: slow job flagged as anomalous "
          f"(duration_seconds={slow_job['duration_seconds']}, "
          f"anomaly_score={slow_job['anomaly_score']}).")

    # Step 3: confirm it also shows up via the anomalies API.
    anomalies = get_anomalies("anomaly_test_job")
    anomaly_ids = {j["id"] for j in anomalies}
    if slow_job_id not in anomaly_ids:
        print(f"FAIL: slow job {slow_job_id} completed and was flagged on its "
              f"own record, but does not appear in GET /jobs/anomalies "
              f"(returned {len(anomalies)} anomalies for this job_type).")
        return

    print(f"PASS: GET /jobs/anomalies?job_type=anomaly_test_job returned the "
          f"slow job ({len(anomalies)} anomaly(ies) total for this type).")

    print()
    print("ALL PHASE 6 ANOMALY TESTS PASSED")


if __name__ == "__main__":
    main()