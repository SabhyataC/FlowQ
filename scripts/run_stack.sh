#!/usr/bin/env bash
# Start the FlowQ stack: check Redis and Postgres, then run the API and the scaler.
# Ctrl+C stops both. The scaler gets SIGTERM so it can drain in-flight jobs.
set -euo pipefail

cd "$(dirname "$0")/.."   # run from the project root

REDIS_HOST="${REDIS_HOST:-localhost}"
REDIS_PORT="${REDIS_PORT:-6379}"
PG_HOST="${PG_HOST:-localhost}"
PG_PORT="${PG_PORT:-5432}"
API_PORT="${API_PORT:-8000}"

# Use the project's virtualenv if there is one
if [ -f venv/bin/activate ]; then
    # shellcheck disable=SC1091
    source venv/bin/activate
fi

# --- Dependency checks -------------------------------------------------------
if ! command -v redis-cli >/dev/null 2>&1; then
    echo "ERROR: redis-cli not found (install redis-tools)." >&2
    exit 1
fi
if [ "$(redis-cli -h "$REDIS_HOST" -p "$REDIS_PORT" ping 2>/dev/null)" != "PONG" ]; then
    echo "ERROR: Redis is not reachable at $REDIS_HOST:$REDIS_PORT." >&2
    echo "       Start it with: sudo service redis-server start" >&2
    exit 1
fi
echo "Redis is up."

if ! command -v pg_isready >/dev/null 2>&1; then
    echo "ERROR: pg_isready not found (install postgresql-client)." >&2
    exit 1
fi
if ! pg_isready -h "$PG_HOST" -p "$PG_PORT" >/dev/null 2>&1; then
    echo "ERROR: Postgres is not reachable at $PG_HOST:$PG_PORT." >&2
    echo "       Start it with: sudo service postgresql start" >&2
    exit 1
fi
echo "Postgres is up."

# --- Start services ----------------------------------------------------------
uvicorn main:app --host 0.0.0.0 --port "$API_PORT" --timeout-graceful-shutdown 3 &
API_PID=$!

python -m core.scaler &
SCALER_PID=$!

echo "FlowQ running: API pid $API_PID, scaler pid $SCALER_PID"
echo "Dashboard: http://localhost:$API_PORT/dashboard   (Ctrl+C to stop)"

# --- Shutdown ----------------------------------------------------------------
cleanup() {
    trap - INT TERM
    echo
    echo "Stopping FlowQ..."
    kill -TERM "$SCALER_PID" 2>/dev/null || true   # scaler drains workers first
    wait "$SCALER_PID" 2>/dev/null || true
    kill -TERM "$API_PID" 2>/dev/null || true
    wait "$API_PID" 2>/dev/null || true
    echo "Stopped."
}
trap cleanup INT TERM

# Exit when either process dies, then stop the other one too
wait -n || true
cleanup