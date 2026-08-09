"""
Full E2E test: exercises DAG scheduling, idempotency, concurrency, and
anomaly detection together against one live stack (API + autoscaler-managed
worker pool), rather than each phase in isolation.

Requires:
  - uvicorn main:app running on :8000
  - python -m core.scaler running (so workers appear dynamically under load,
    not core.pool run manually)

Stdlib only (urllib), matching the existing scripts/test_*.py files.

Usage:
    python scripts/test_e2e.py
"""
import json
import time
import uuid
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "http://localhost:8000"

FAILURES: list[str] = []


def post(path: str, body: dict) -> tuple[int, dict]:
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        f"{BASE_URL}{path}",
        data=data,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req) as resp:
            return resp.status, json.loads(resp.read())
    except urllib.error.HTTPError as e:
        return e.code, json.loads(e.read())


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
        time.sleep(0.3)
        job = get_job(job_id)
    return job


def check(condition: bool, label: str) -> None:
    status = "PASS" if condition else "FAIL"
    print(f"  [{status}] {label}")
    if not condition:
        FAILURES.append(label)


# ---------------------------------------------------------------------------
# Part 1: DAG batch (diamond A -> B,C -> D), submitted under load
# ---------------------------------------------------------------------------
def run_dag_part() -> None:
    print("\n--- Part 1: DAG batch (diamond A -> B,C -> D) ---")
    batch = {
        "jobs": [
            {"type": "e2e_dag", "payload": {}, "batch_ref": "A", "depends_on": []},
            {"type": "e2e_dag", "payload": {}, "batch_ref": "B", "depends_on": ["A"]},
            {"type": "e2e_dag", "payload": {}, "batch_ref": "C", "depends_on": ["A"]},
            {"type": "e2e_dag", "payload": {}, "batch_ref": "D", "depends_on": ["B", "C"]},
        ]
    }
    status, resp = post("/jobs/batch", batch)
    check(status == 201, f"batch accepted (status={status})")
    if status != 201:
        return

    jobs_by_ref = resp["jobs"]
    for ref, job in jobs_by_ref.items():
        done = wait_for_status(job["id"], ("completed", "failed"), timeout_s=30)
        jobs_by_ref[ref] = done

    all_completed = all(j["status"] == "completed" for j in jobs_by_ref.values())
    check(all_completed, "all 4 DAG jobs reached completed")

    if all_completed:
        a_done = jobs_by_ref["A"]["completed_at"]
        b_started = jobs_by_ref["B"]["started_at"]
        c_started = jobs_by_ref["C"]["started_at"]
        d_started = jobs_by_ref["D"]["started_at"]
        b_done = jobs_by_ref["B"]["completed_at"]
        c_done = jobs_by_ref["C"]["completed_at"]
        check(a_done <= b_started and a_done <= c_started, "A finished before B/C started")
        check(b_done <= d_started and c_done <= d_started, "B and C finished before D started")

    # Cycle rejection sanity check
    cyclic = {
        "jobs": [
            {"type": "e2e_dag", "payload": {}, "batch_ref": "X", "depends_on": ["Y"]},
            {"type": "e2e_dag", "payload": {}, "batch_ref": "Y", "depends_on": ["X"]},
        ]
    }
    status, _ = post("/jobs/batch", cyclic)
    check(status == 422, f"cyclic batch rejected with 422 (got {status})")


# ---------------------------------------------------------------------------
# Part 2: idempotency (single-job path — enqueue_batch doesn't dedupe, see note)
# ---------------------------------------------------------------------------
def run_idempotency_part() -> None:
    print("\n--- Part 2: idempotency (single-job POST /jobs) ---")
    key = f"e2e-idem-{uuid.uuid4().hex[:8]}"
    body = {"type": "e2e_idem", "payload": {}, "idempotency_key": key}

    status1, job1 = post("/jobs", body)
    status2, job2 = post("/jobs", body)
    check(status1 == 201 and status2 == 201, "both submissions return 201")
    check(job1["id"] == job2["id"], "both submissions return the same job id")

    status3, job3 = post("/jobs", {"type": "e2e_idem", "payload": {}})
    check(status3 == 201 and job3["id"] != job1["id"], "no idempotency_key creates a distinct job")

    print("  [NOTE] enqueue_batch has no idempotency pre-check (only single-job "
          "enqueue does) — duplicate keys inside a /jobs/batch submission will "
          "NOT be deduped. Known gap, not covered by this test.")


# ---------------------------------------------------------------------------
# Part 3: concurrency (fired while Part 1/2 jobs may still be draining)
# ---------------------------------------------------------------------------
def run_concurrency_part(n: int = 8) -> None:
    print(f"\n--- Part 3: {n} concurrent submissions ---")
    def submit(i: int) -> dict:
        status, job = post("/jobs", {"type": "e2e_concurrent", "payload": {"n": i}})
        return {"status": status, "job": job}

    with ThreadPoolExecutor(max_workers=n) as pool:
        results = list(pool.map(submit, range(n)))

    check(all(r["status"] == 201 for r in results), "all submissions accepted")

    finals = []
    for r in results:
        finals.append(wait_for_status(r["job"]["id"], ("completed", "failed"), timeout_s=30))

    check(all(f["status"] == "completed" for f in finals), "all concurrent jobs completed")
    payloads = sorted(f["payload"]["n"] for f in finals)
    check(payloads == list(range(n)), "no payload mixups across concurrent jobs")


# ---------------------------------------------------------------------------
# Part 4: anomaly detection, seeded last so job_type_stats isn't polluted
# by the DAG/idempotency/concurrency jobs above (different job_type).
# ---------------------------------------------------------------------------
def run_anomaly_part() -> None:
    print("\n--- Part 4: anomaly detection ---")
    import random
    SEED_COUNT = 5
    NORMAL_RANGE = (1.8, 2.2)
    SLOW_SLEEP = 20

    seed_ids = []
    for _ in range(SEED_COUNT):
        _, job = post("/jobs", {"type": "e2e_anomaly", "payload": {"_sleep_seconds": random.uniform(*NORMAL_RANGE)}})
        seed_ids.append(job["id"])

    seed_jobs = [wait_for_status(jid, ("completed", "failed"), timeout_s=15) for jid in seed_ids]
    check(all(j["status"] == "completed" for j in seed_jobs), "seed jobs completed")
    check(all(not j["is_anomaly"] for j in seed_jobs), "no seed job flagged anomalous")

    _, slow_job = post("/jobs", {"type": "e2e_anomaly", "payload": {"_sleep_seconds": SLOW_SLEEP}})
    slow_job = wait_for_status(slow_job["id"], ("completed", "failed"), timeout_s=SLOW_SLEEP + 15)
    check(slow_job["status"] == "completed", "slow job completed")
    check(slow_job["is_anomaly"], f"slow job flagged anomalous (score={slow_job.get('anomaly_score')})")

    anomalies = get_anomalies("e2e_anomaly")
    check(slow_job["id"] in {a["id"] for a in anomalies}, "slow job appears in GET /jobs/anomalies")


def main() -> None:
    print("=== FlowQ Full E2E Test ===")
    run_dag_part()
    run_idempotency_part()
    run_concurrency_part()
    run_anomaly_part()

    print("\n=== SUMMARY ===")
    if not FAILURES:
        print(f"ALL CHECKS PASSED")
    else:
        print(f"{len(FAILURES)} CHECK(S) FAILED:")
        for f in FAILURES:
            print(f"  - {f}")


if __name__ == "__main__":
    main()