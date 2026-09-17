"""Shared fixtures for Agent Relay integration tests.

These tests run against the real FastAPI app and the real database.  Nothing in
``storage``/``database`` is mocked: the point of this suite is to keep the
HTTP protocol and the storage seam honest, so Q3-Q6 can reuse it as a
regression check for the same task flow.

Why this file is required
-------------------------
* ``main``/``database`` live at the repository root.  With pytest's default
  ``prepend`` import mode only ``tests/integration`` is added to ``sys.path``,
  so ``import main`` fails there unless the root is inserted explicitly
  (verified: ``ModuleNotFoundError: No module named 'main'``).
* ``database.DATABASE_URL`` is read once, at import time
  (``database.py`` reads ``RELAY_DATABASE_URL`` in a module-level constant), so
  the environment must be set *before* anything imports :mod:`database`.
* The autouse fixture below drops and recreates every table, exactly like the
  root ``test_agent_relay.py`` fixture.  A scratch database is therefore
  mandatory, and a separate filename (``agent-relay-it.db``) keeps this suite
  from fighting the root suite over ``agent-relay-test.db``.
"""

from __future__ import annotations

import os
import sys
from collections.abc import Iterator
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

# Scratch database, set before :mod:`database` is imported anywhere.  Deliberate
# default (not an override): ``RELAY_DATABASE_URL``/``DATABASE_URL`` already in
# the environment win, so CI can point this suite at PostgreSQL later.
os.environ.setdefault("RELAY_DATABASE_URL", "sqlite:////tmp/agent-relay-it.db")

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest
from fastapi.testclient import TestClient

import main
from database import DATABASE_URL, Base, engine

# The dev server's default database.  Wiping it would destroy real work, and
# ``sqlite:////tmp/...`` vs ``sqlite:///./agent-relay.db`` differ by one slash.
_DEV_DB_NEEDLES = ("agent-relay.db",)


def _guard_against_destructive_target() -> None:
    if DATABASE_URL.startswith("sqlite"):
        if any(needle in DATABASE_URL for needle in _DEV_DB_NEEDLES):
            raise RuntimeError(
                "Refusing to run integration tests against the dev database "
                f"({DATABASE_URL!r}). These fixtures call Base.metadata.drop_all(). "
                "Point RELAY_DATABASE_URL at a scratch file, e.g. "
                "sqlite:////tmp/agent-relay-it.db"
            )
    # PostgreSQL (Q3-Q6): assume the operator knows what they are doing, but
    # refuse an obvious production-looking target.
    elif DATABASE_URL.startswith(("postgres", "postgresql")):
        if "test" not in DATABASE_URL.lower() and os.getenv("RELAY_IT_ALLOW_DB") != "1":
            raise RuntimeError(
                f"Integration fixtures will TRUNCATE/drop tables in {DATABASE_URL!r}. "
                "Use a database whose name contains 'test', or set RELAY_IT_ALLOW_DB=1."
            )


@pytest.fixture(scope="session", autouse=True)
def _check_target_database() -> None:
    _guard_against_destructive_target()


@pytest.fixture
def client() -> Iterator[TestClient]:
    """Live TestClient (real app, real DB, lifespan + recovery loop running)."""

    with TestClient(main.app) as test_client:
        yield test_client


@pytest.fixture(autouse=True)
def fresh_db(client) -> None:
    """Start every test from an empty database.

    Depends on ``client`` so the application (and its import-time ``init_db()``)
    is up first, then resets the tables before the test body runs.  ``yield``
    keeps teardown ordering predictable for Q3-Q6 fixtures that may add steps.
    """

    del client  # ordering dependency only
    Base.metadata.drop_all(engine)
    Base.metadata.create_all(engine)
    yield
    Base.metadata.drop_all(engine)


__all__ = ["client", "fresh_db"]
