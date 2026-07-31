"""
Task 6: end-to-end test for a multi-job DAG.

Submits a diamond-shaped dependency graph via POST /jobs/batch:

        A
       / \
      B   C
       \ /
        D

(D depends on B and C; B and C each depend on A; A has no dependencies.)

Verifies:
  1. The batch is accepted and every job eventually reaches COMPLETED.
  2. Execution actually respected the dependency order: A finished
     before B/C started, and B/C both finished before D started.
  3. A batch containing a cycle is rejected with 422.

Stdlib only (urllib), matching scripts/test_concurrency.py. Requires
the API (uvicorn) and pool (core.pool) already running.

Usage:
    python scripts/test_dag.py
"""
import json
import time
import urllib.request
import urllib.error

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


def poll_until_done(job_id: str, timeout_s: int = 30, interval_s: float = 0.5) -> dict:
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        job = get_job(job_id)
        if job["status"] in ("completed", "failed"):
            return job
        time.sleep(interval_s)
    raise TimeoutError(f"job {job_id} did not finish within {timeout_s}s")


def parse_ts(ts: str) -> float:
    from datetime import datetime
    return datetime.fromisoformat(ts).timestamp()


def test_valid_dag() -> bool:
    print("--- Diamond DAG: A -> (B, C) -> D ---")
    batch = {
        "jobs": [
            {"type": "test_job", "payload": {"name": "A"}, "batch_ref": "A", "depends_on": []},
            {"type": "test_job", "payload": {"name": "B"}, "batch_ref": "B", "depends_on": ["A"]},
            {"type": "test_job", "payload": {"name": "C"}, "batch_ref": "C", "depends_on": ["A"]},
            {"type": "test_job", "payload": {"name": "D"}, "batch_ref": "D", "depends_on": ["B", "C"]},
        ]
    }
    status, resp = post("/jobs/batch", batch)
    if status != 201:
        print(f"FAIL: batch submission rejected unexpectedly: {status} {resp}")
        return False

    jobs = resp["jobs"]  # batch_ref -> JobResponse
    print(f"Submitted: { {ref: j['id'] for ref, j in jobs.items()} }")

    for ref, j in jobs.items():
        print(ref, j["id"], j.get("depends_on"))

    initial_status = {ref: j["status"] for ref, j in jobs.items()}
    if initial_status["A"] != "pending":
        print(f"FAIL: A should start pending, got {initial_status['A']}")
        return False
    for ref in ("B", "C", "D"):
        if initial_status[ref] != "blocked":
            print(f"FAIL: {ref} should start blocked, got {initial_status[ref]}")
            return False
    print("PASS: initial statuses correct (A pending, B/C/D blocked).")

    finished = {ref: poll_until_done(j["id"]) for ref, j in jobs.items()}

    failed = {ref: j for ref, j in finished.items() if j["status"] != "completed"}
    if failed:
        print(f"FAIL: job(s) did not complete: {failed}")
        return False
    print("PASS: all 4 jobs reached COMPLETED.")

    a_done = parse_ts(finished["A"]["completed_at"])
    b_start = parse_ts(finished["B"]["started_at"])
    c_start = parse_ts(finished["C"]["started_at"])
    b_done = parse_ts(finished["B"]["completed_at"])
    c_done = parse_ts(finished["C"]["completed_at"])
    d_start = parse_ts(finished["D"]["started_at"])

    ok = True
    if not (a_done <= b_start and a_done <= c_start):
        print(f"FAIL: B/C started before A completed (a_done={a_done}, b_start={b_start}, c_start={c_start})")
        ok = False
    if not (b_done <= d_start and c_done <= d_start):
        print(f"FAIL: D started before B/C completed (b_done={b_done}, c_done={c_done}, d_start={d_start})")
        ok = False
    if ok:
        print("PASS: execution order respected dependencies (A before B/C, B&C before D).")
    return ok


def test_cyclic_dag_rejected() -> bool:
    print("--- Cyclic batch: X -> Y -> X ---")
    batch = {
        "jobs": [
            {"type": "test_job", "payload": {}, "batch_ref": "X", "depends_on": ["Y"]},
            {"type": "test_job", "payload": {}, "batch_ref": "Y", "depends_on": ["X"]},
        ]
    }
    status, resp = post("/jobs/batch", batch)
    if status == 422:
        print(f"PASS: cyclic batch rejected with 422: {resp.get('detail')}")
        return True
    print(f"FAIL: expected 422 for cyclic batch, got {status} {resp}")
    return False


def main() -> None:
    results = [test_valid_dag(), test_cyclic_dag_rejected()]
    print()
    if all(results):
        print("ALL DAG TESTS PASSED")
    else:
        print("SOME DAG TESTS FAILED")


if __name__ == "__main__":
    main()