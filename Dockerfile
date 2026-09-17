# Agent Relay container image (homework Q3).
#
# Production note (deliberately simplified here): COPY below copies the whole
# repository -- tests/ included, on purpose, so the *same* image can also run
# "uv sync --frozen && pytest tests/integration" from a separate test
# container.  A production image should instead use a multi-stage build, or
# copy only the runtime modules (main.py, database.py, storage.py, schemas.py,
# errors.py, dashboard.py, dashboard.html).
#
# .dockerignore excludes .python-version (pinned 3.11) so uv provisions from
# this base image's interpreter instead of downloading a second CPython;
# pyproject.toml requires >=3.11 and uv.lock is universal, so 3.12 is valid.
FROM python:3.12-slim

WORKDIR /app

# uv itself is installed with pip; dependency layers are cached on
# pyproject.toml/uv.lock only, so code edits do not reinstall packages.
RUN pip install --no-cache-dir uv

COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev

COPY . .

# The SQLite database is meant to live in /data on a named volume
# (-v agent-relay-data:/data  -e RELAY_DATABASE_URL=sqlite:////data/agent-relay.db
# -- four slashes make the path absolute).  SQLite creates files but not
# directories, and WAL mode (set in database.py) also needs the directory to be
# writable for the -wal/-shm sidecar files, so create it and mount it writable.
RUN mkdir -p /data

# Run uvicorn straight from the synced venv: uvicorn becomes PID 1 and receives
# SIGTERM directly for the lifespan shutdown, with no "uv run" environment check
# at container start.
ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000

# --host 0.0.0.0 is required: uvicorn defaults to 127.0.0.1, which inside a
# container is the container's own loopback, and published port mappings (-p)
# then look broken (verified by A/B experiment in the Q3 commit message).
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
