"""
Task: idempotency_key handling for single job submission.

Submits the same job payload twice via POST /jobs, using the same
idempotency_key both times. Verifies:

  1. Both submissions return 201 (idempotent replay is not an error).
  2. Both responses have the same job id (no duplicate row created).
  3. The second response reflects the job's actual current status
     (not just echoing back the original submission unchanged).
  4. Two DIFFERENT idempotency_keys (or none at all) DO create
     distinct jobs, as a sanity check the key is actually load-bearing.

Stdlib only (urllib), matching scripts/test_dag.py. Requires the API
(uvicorn) already running.

Usage:
    python scripts/test_idempotency.py
"""
import json
import time
import uuid
import urllib.request
import urllib.error
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_URL = "http://localhost:8000"


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


def test_duplicate_submission_returns_same_job() -> bool:
    print("--- Duplicate submission with same idempotency_key ---")
    key = f"test-idem-{uuid.uuid4().hex[:8]}"
    body = {
        "type": "test_job",
        "payload": {"name": "idempotency-check"},
        "idempotency_key": key,
    }

    status1, job1 = post("/jobs", body)
    if status1 != 201:
        print(f"FAIL: first submission rejected: {status1} {job1}")
        return False
    print(f"First submission: id={job1['id']} status={job1['status']}")

    status2, job2 = post("/jobs", body)
    if status2 != 201:
        print(f"FAIL: duplicate submission rejected: {status2} {job2}")
        return False
    print(f"Second submission: id={job2['id']} status={job2['status']}")

    if job1["id"] != job2["id"]:
        print(f"FAIL: duplicate submission created a new job "
              f"({job1['id']} != {job2['id']})")
        return False
    print("PASS: duplicate submission returned the same job id.")

    # Let the first (real) job actually run, then re-submit the
    # duplicate again and confirm we get its up-to-date status back,
    # not a stale snapshot from the original insert.
    deadline = time.time() + 10
    current = job1
    while current["status"] not in ("completed", "failed") and time.time() < deadline:
        time.sleep(0.5)
        current = get_job(job1["id"])

    if current["status"] not in ("completed", "failed"):
        print(f"FAIL: job {job1['id']} never finished within 10s "
              f"(stuck at status={current['status']})")
        return False

    status3, job3 = post("/jobs", body)
    if status3 != 201:
        print(f"FAIL: third (post-completion) submission rejected: {status3} {job3}")
        return False

    if job3["id"] != job1["id"]:
        print(f"FAIL: post-completion duplicate created a new job")
        return False
    if job3["status"] != current["status"]:
        print(f"FAIL: duplicate response status ({job3['status']}) doesn't match "
              f"actual job status ({current['status']})")
        return False
    print(f"PASS: post-completion duplicate returned live status "
          f"({job3['status']}), not a stale snapshot.")

    return True


def test_different_keys_create_different_jobs() -> bool:
    print("--- Distinct idempotency_keys (and no key) create distinct jobs ---")
    body_a = {
        "type": "test_job",
        "payload": {"name": "distinct-a"},
        "idempotency_key": f"test-idem-{uuid.uuid4().hex[:8]}",
    }
    body_b = {
        "type": "test_job",
        "payload": {"name": "distinct-b"},
        "idempotency_key": f"test-idem-{uuid.uuid4().hex[:8]}",
    }
    body_c = {
        "type": "test_job",
        "payload": {"name": "no-key"},
    }

    ids = []
    for label, body in (("A", body_a), ("B", body_b), ("C (no key)", body_c)):
        status, job = post("/jobs", body)
        if status != 201:
            print(f"FAIL: submission {label} rejected: {status} {job}")
            return False
        ids.append(job["id"])

    if len(set(ids)) != 3:
        print(f"FAIL: expected 3 distinct job ids, got {ids}")
        return False
    print("PASS: distinct keys (and no key) each created a distinct job.")
    return True

def test_concurrent_duplicate_submission_is_race_safe() -> bool:
    print("--- N concurrent submissions, same idempotency_key ---")
    key = f"test-idem-race-{uuid.uuid4().hex[:8]}"
    body = {
        "type": "test_job",
        "payload": {"name": "idempotency-race-check"},
        "idempotency_key": key,
    }
    n = 8

    results = []
    with ThreadPoolExecutor(max_workers=n) as pool:
        futures = [pool.submit(post, "/jobs", body) for _ in range(n)]
        for future in as_completed(futures):
            results.append(future.result())

    statuses = [status for status, _ in results]
    bad = [(s, j) for s, j in results if s != 201]
    if bad:
        print(f"FAIL: {len(bad)}/{n} concurrent submissions were rejected: {bad}")
        return False

    ids = {job["id"] for _, job in results}
    if len(ids) != 1:
        print(f"FAIL: {n} concurrent submissions with the same key produced "
              f"{len(ids)} distinct job ids (expected 1): {ids}")
        return False

    print(f"PASS: all {n} concurrent submissions resolved to the same job id "
          f"({ids.pop()}), no IntegrityError leaked to the caller.")
    return True


def main() -> None:
    results = [
        test_duplicate_submission_returns_same_job(),
        test_different_keys_create_different_jobs(),
        test_concurrent_duplicate_submission_is_race_safe(),
    ]


if __name__ == "__main__":
    main()