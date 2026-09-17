"""Shared fixtures for Agent Relay integration tests.

These tests run against the real FastAPI app and the real database.  Nothing in
``storage``/``database`` is mocked: the point of this suite is to keep the
HTTP protocol and the storage seam honest, so Q3-Q6 can reuse it as a
regression check for the same task flow.

Responsibility split with the repository-root ``conftest.py``
------------------------------------------------------------
* the root file sets the scratch ``RELAY_DATABASE_URL`` default, guards against
  destructive targets, inserts the repository root on ``sys.path`` and prints
  the backend in the pytest header;
* this file adds the two fixtures the integration suite needs.

The environment default lives in the root file because pytest loads ancestor
conftests *first*, so a ``setdefault`` here could never win -- it would only
mislead the reader about which URL is in effect.

One consequence worth naming: both suites now default to the same scratch file
(``/tmp/agent-relay-test.db``), so running ``pytest`` over both in one process
means they interleave resets of one database.  That is harmless for correctness
because ``fresh_db`` empties everything before each test either way; a separate
filename would only buy isolation if one suite ever stopped resetting state.
"""

from __future__ import annotations

import sys
from collections.abc import Iterator
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[2]

# Still needed here: pytest's ``prepend`` import mode only adds this directory
# (``tests/integration``) to ``sys.path``, so ``import main`` fails in an
# integration test without the repository root (verified: ``ModuleNotFoundError:
# No module named 'main'``).
if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

import pytest
from fastapi.testclient import TestClient

import main
from testsupport.db_reset import reset_database


@pytest.fixture
def client() -> Iterator[TestClient]:
    """Live TestClient (real app, real DB, lifespan + recovery loop running)."""

    with TestClient(main.app) as test_client:
        yield test_client


@pytest.fixture(autouse=True)
def fresh_db(client) -> str:
    """Start every test from an empty database and report the backend used.

    Depends on ``client`` so the application (and its import-time ``init_db()``)
    is up first, then resets the tables before the test body runs.  SQLite
    rebuilds the schema; other backends truncate rows and restart identities --
    see :func:`testsupport.db_reset.reset_database`.  ``yield`` keeps teardown
    ordering predictable for Q3-Q6 fixtures that may add steps.
    """

    del client  # ordering dependency only
    backend = reset_database()
    yield backend
    reset_database()


__all__ = ["client", "fresh_db"]
