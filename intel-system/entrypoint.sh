#!/bin/sh
set -e

echo "[entrypoint] Running Alembic migrations..."
alembic upgrade head

echo "[entrypoint] Starting FastAPI..."
exec uvicorn app.main:app --host 0.0.0.0 --port 8000
