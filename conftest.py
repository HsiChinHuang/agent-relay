"""Shared bootstrap at the repository root, for *every* test file in the project.

This file lives at the repository root -- not ``tests/conftest.py`` -- precisely
because ``test_agent_relay.py`` also lives at the repository root.  A conftest
under ``tests/`` is never an ancestor of it, so the root suite runs completely
unguarded there.  That is not hypothetical: with the guard parked in
``tests/conftest.py``, pointing ``RELAY_DATABASE_URL`` at ``./agent-relay.db``
still ran the root suite, which calls ``Base.metadata.drop_all()``, and wiped
the developer database.  Root-level placement is what makes the guard cover all
10 tests.

pytest loads this file before any test module is imported.  That ordering is the
whole point: ``database.DATABASE_URL`` is frozen at import time (``database.py``
reads the environment into a module-level constant), so the scratch-database
default has to be in place before anything imports :mod:`database`.

What this provides
------------------
1. ``sys.path`` for the repository root.  With pytest's default ``prepend``
   import mode, a test under ``tests/integration`` only gets
   ``tests/integration`` on ``sys.path``, so ``import main`` fails there
   (verified: ``ModuleNotFoundError: No module named 'main'``).
2. A backend self-attestation header (:func:`pytest_report_header`).  Every
   assertion in the root suite is backend-neutral, so without it a run that
   silently stayed on SQLite is indistinguishable from one that reached
   PostgreSQL.  Printing the URL the test process actually resolved makes that
   impossible to confuse.  The password is masked.
3. ``RELAY_EXPECT_BACKEND`` (:func:`_require_expected_backend`), which turns a
   silent backend fallback into a hard error instead of a mislabelled "N passed".
4. A guard against pointing the suites at the developer's running database
   (:func:`_refuse_destructive_target`).  The README warns about this, but a
   warning is not a guard.

A note on ``test_agent_relay.py``: its own
``os.environ.setdefault("RELAY_DATABASE_URL", "sqlite:////tmp/agent-relay-test.db")``
is left in place on purpose.  ``setdefault`` is a no-op when the variable is
already set, so it does not pin the suite to SQLite (a run with
``RELAY_DATABASE_URL`` pointed at PostgreSQL really does run against
PostgreSQL -- verified: 4 passed, header shows ``postgresql+psycopg``), and
keeping it means the safety default still applies if anyone runs that file
through a tool that skips conftest collection.
"""

from __future__ import annotations

import os
import sys
from pathlib import Path
from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:  # pragma: no cover - typing only
    from _pytest.config import Config

REPO_ROOT = Path(__file__).resolve().parent

# Scratch database for the root suite, matching test_agent_relay.py's default.
# Deliberately a *default*: an explicit RELAY_DATABASE_URL/DATABASE_URL in the
# environment wins, which is how the same suite is pointed at PostgreSQL.
os.environ.setdefault("RELAY_DATABASE_URL", "sqlite:////tmp/agent-relay-test.db")

if str(REPO_ROOT) not in sys.path:
    sys.path.insert(0, str(REPO_ROOT))

from testsupport.db_reset import describe_url  # noqa: E402  (needs sys.path/env first)

# ``./agent-relay.db`` is the developer's live database (README: "The default
# database is ./agent-relay.db").  sqlite:////tmp/x.db and sqlite:///./x.db
# differ by one slash, which is exactly how a scratch path becomes a real one.
_DEV_DB_NEEDLES = ("agent-relay.db",)

# Optional pin: RELAY_EXPECT_BACKEND=sqlite|postgresql makes "which backend did
# this run actually use?" a hard failure instead of an inference.
EXPECTED_BACKEND_ENV = "RELAY_EXPECT_BACKEND"


def pytest_report_header(config: "Config") -> str:
    """Print the backend this process resolved, so results cannot be mislabelled."""

    import database

    return f"agent-relay test backend: {describe_url(database.DATABASE_URL)}"


@pytest.fixture(scope="session", autouse=True)
def _refuse_destructive_target() -> None:
    """Abort the session if the target database looks like someone's real data."""

    import database

    url = database.DATABASE_URL
    if url.startswith("sqlite"):
        if any(needle in url for needle in _DEV_DB_NEEDLES):
            raise RuntimeError(
                "Refusing to run the test suite against the dev database "
                f"({url!r}); the fixtures empty every table. Point "
                "RELAY_DATABASE_URL at a scratch file, e.g. "
                "sqlite:////tmp/agent-relay-test.db"
            )
    elif url.startswith(("postgres", "postgresql")):
        if "test" not in url.lower() and os.getenv("RELAY_IT_ALLOW_DB") != "1":
            raise RuntimeError(
                f"The test fixtures empty every table in {url!r}. Use a database "
                "whose name contains 'test', or set RELAY_IT_ALLOW_DB=1."
            )


@pytest.fixture(scope="session", autouse=True)
def _require_expected_backend() -> None:
    """Turn a silent backend fallback into a failure.

    Every behavioural assertion in this repository is backend-neutral, so a run
    that meant to reach PostgreSQL but quietly kept the SQLite default prints an
    identical "N passed".  That has already produced one unsupportable claim
    here.  Passing ``RELAY_EXPECT_BACKEND=postgresql`` (or ``sqlite``) makes the
    mismatch fail; leaving it unset keeps plain ``pytest`` runs working.
    """

    import database

    expected = os.getenv(EXPECTED_BACKEND_ENV, "").strip().lower()
    if not expected:
        return
    resolved = database.DATABASE_URL
    if not resolved.startswith(expected):
        raise RuntimeError(
            f"{EXPECTED_BACKEND_ENV}={expected!r} but this process resolved "
            f"{describe_url(resolved)!r}; results from this run cannot be "
            "attributed to the intended backend"
        )


__all__ = ["EXPECTED_BACKEND_ENV", "pytest_report_header"]
