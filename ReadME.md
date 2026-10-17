# FlowQ

A distributed job queue in Python with DAG scheduling, autoscaling workers, and statistical anomaly detection. It is built on Redis Streams, PostgreSQL, and FastAPI and comes with a live dashboard.

## Features

- **Reliable delivery**: Redis Streams with consumer groups; crashed workers' jobs are reclaimed and retried automatically.
- **Durable history**: PostgreSQL is the source of truth for every job; it survives restarts.
- **DAG scheduling**: jobs can declare `depends_on`; dependents stay `BLOCKED` until all dependencies complete. Cycles are rejected.
- **Idempotency**: resubmitting a job with the same `idempotency_key` returns the existing job instead of creating a duplicate, even under concurrent submission.
- **Autoscaling workers**: a scaler watches queue depth and grows or shrinks the worker pool, with graceful SIGTERM drain so in-flight jobs finish.
- **Anomaly detection**: per-job-type running mean/variance (Welford's algorithm) and z-score flagging of unusually slow jobs, with no queries on the hot path.
- **Dashboard**: single-file, no-build UI with live updates over SSE, bulk submission, DAG graph view, worker-pool control, and anomaly explanations.

## Architecture

```
                 ┌──────────────┐
   Client ─────▶ │   FastAPI    │ ──── write Job row ───▶ ┌────────────┐
 (API / UI)      │  (main.py)   │                          │ PostgreSQL │
                 └──────┬───────┘                          │ (source of │
                        │ XADD job_id                      │   truth)   │
                        ▼                                  └─────▲──────┘
                 ┌──────────────┐   XREADGROUP / XACK            │
                 │ Redis Stream │ ◀────────────────┐             │ status, result,
                 └──────────────┘                  │             │ duration, anomaly
                                          ┌────────┴───────┐     │
                                          │  Worker pool   │─────┘
                                          │ (N async       │
                                          │  workers)      │
                                          └────────▲───────┘
                                                   │ spawn / stop
                                          ┌────────┴───────┐
                                          │    Scaler      │ ◀── reads queue depth
                                          │ (core/scaler)  │     publishes status to Redis
                                          └────────────────┘
```

**Dual-write design**: Postgres holds the full job record; Redis is only the messaging layer. A job is written to Postgres first, then its ID is pushed to the stream.

**Key design decisions**

| Decision | Why |
|---|---|
| Redis Streams over Lists | Consumer groups and the pending entries list (PEL) give reliable delivery and `XAUTOCLAIM`-based crash recovery |
| Each worker runs one job at a time | Simple, predictable concurrency; scale by adding workers |
| Reclaimed jobs restart from scratch | Jobs are not resumable; retry count is tracked on the job |
| Crash recovery lives inside each worker's loop | No separate reaper process to run or fail |
| Atomic conditional `UPDATE` to unblock DAG dependents | Prevents two concurrent dependency completions from double-pushing the same dependent |
| `IntegrityError` catch on idempotency insert | Closes the check-then-insert race |
| Welford stats table for anomaly detection | O(1) updates, no history queries; scores use pre-update stats so an outlier can't mask itself |

## Tech stack

Python 3.12, FastAPI, SQLAlchemy (async) + asyncpg, PostgreSQL, Redis Streams, Alembic, Pydantic v2, vanilla HTML/JS dashboard.

## Getting started

### Prerequisites

- Python 3.12+
- PostgreSQL and Redis running locally (WSL2 works fine on Windows)

### Setup

```bash
git clone <your-repo-url>   # VERIFY
cd Job_Manager
python -m venv venv
source venv/bin/activate    # Windows: venv\Scripts\activate
pip install -r requirements.txt
```

Create a `.env` file in the project root:

```env
DATABASE_URL=postgresql+asyncpg://user:password@localhost:5432/flowq   # VERIFY variable name
REDIS_URL=redis://localhost:6379                                         # VERIFY variable name
WORKER_POOL_SIZE=3
ANTHROPIC_API_KEY=...   # only needed for anomaly explanations; VERIFY variable name
```

Run migrations:

```bash
alembic upgrade head
```

### Run

Run these in separate terminals from the project root:

```bash
# 1. API + dashboard
uvicorn main:app --reload

# 2. Scaler (manages the worker pool)
python -m core.scaler
```

The scaler is its own process; the API does not start it. Open the dashboard at `http://localhost:8000/dashboard`.

## API overview <!-- VERIFY paths and methods against main.py -->

| Method | Path | Description |
|---|---|---|
| POST | `/jobs` | Submit a job (supports `depends_on`, `idempotency_key`) |
| POST | `/jobs/bulk` | Submit many independent jobs |
| GET | `/jobs` | List jobs |
| GET | `/jobs/{job_id}` | Get one job |
| DELETE | `/jobs/{job_id}` | Delete a blocked, completed, or failed job |
| GET | `/jobs/stream` | Live job updates (SSE) |
| GET | `/jobs/anomalies` | Jobs flagged as anomalous |
| POST | `/jobs/{job_id}/explain` | LLM explanation for an anomaly, cached on the job |
| GET | `/scaler/status` | Queue depth, pool size, manual/auto mode |
| POST | `/scaler/target` | Set or clear a manual worker count |

Interactive API docs are available at `/docs`.

## Testing

Tests are end-to-end scripts that run against a live stack:

```bash
python scripts/test_e2e.py        # DAG, idempotency, concurrency, anomaly detection together
python scripts/test_anomaly.py    # anomaly flagging
python scripts/test_autoscale.py  # SIGTERM graceful drain (requires Linux/WSL2)
```

> **Note:** the SIGTERM test needs a real POSIX signal, so run it in WSL2 or Linux. On native Windows, `SIGTERM` is a hard kill and can't be caught.

Anomaly scoring needs at least 5 completed jobs of the same type before it flags a 6th.

## Project layout <!-- VERIFY -->

```
Job_Manager/
├── main.py            # FastAPI app, routes, dashboard mount
├── core/
│   ├── worker.py      # worker loop, crash recovery, job execution
│   ├── scaler.py      # queue-depth monitor, autoscaling, SIGTERM handling
│   ├── dag.py         # dependency graph, cycle detection
│   └── idempotency.py # duplicate-submission handling
├── dashboard/
│   └── index.html     # single-file dashboard
├── scripts/           # end-to-end test scripts
└── alembic/           # database migrations
```

## Known limitations

- Reclaimed jobs restart from the beginning rather than resuming.
- Anomaly scoring is based on duration only; the system doesn't track CPU, network, or other resource metrics.
- Deleting `PENDING`/`RUNNING` jobs from the dashboard is currently disabled.

## License

MIT. See [LICENSE](LICENSE).