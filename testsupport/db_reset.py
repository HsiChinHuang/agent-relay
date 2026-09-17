"""Backend-aware database reset and self-attestation for the test suites.

Why this exists
---------------
``tests/integration/conftest.py`` used to call ``Base.metadata.drop_all()`` /
``create_all()`` unconditionally.  On SQLite that is exactly right and cheap.
On PostgreSQL it rebuilds three tables plus every index and constraint before
*and* after each test, which is slow, and it also throws away the schema that
``init_db()`` just created -- so a port ever exercises the real schema
lifecycle.  ``TRUNCATE ... RESTART IDENTITY CASCADE`` clears the rows, keeps the
constraints the port depends on (``uq_task_sender_idempotency``, the unique
``claim_token_hash``), and resets ``attempts.id`` so attempt numbering is
identical to a freshly created SQLite database.

The ``RESTART IDENTITY`` part only matters because ``Attempt.id`` is an
autoincrement integer primary key; on SQLite the rowid restarts on its own once
the table is recreated.

Self-attestation
----------------
The ``pytest_report_header`` hook prints the database URL that *this* process
resolved.  Without it, a run that silently fell back to SQLite is
indistinguishable from a run that really reached PostgreSQL, because every
assertion in the root suite is backend-neutral.  That failure mode has already
bitten this project once, so the backend is now printed on every run and cannot
be forgotten.
"""

from __future__ import annotations


def backend_label() -> str:
    """Human-readable backend identity, safe to print (no password)."""

    from database import DATABASE_URL

    return describe_url(DATABASE_URL)


def describe_url(url: str) -> str:
    """Return ``scheme://host/db`` with any password replaced by ``***``."""

    scheme, separator, rest = url.partition("://")
    if not separator:
        return url
    credentials, _, location = rest.rpartition("@")
    if not credentials:
        return url
    user, _, _password = credentials.partition(":")
    return f"{scheme}://{user}:***@{location}"


def reset_database() -> str:
    """Empty every table so the next test starts from a clean schema.

    Returns the label of the backend that was reset, so callers can include it
    in failure messages.
    """

    from database import DATABASE_URL, Base, engine

    if DATABASE_URL.startswith("sqlite"):
        # Original Q2 behaviour, unchanged: rebuild the schema each test.  The
        # default file-backed SQLite database is recreated (not truncated) so
        # WAL sidecars never carry stale pages into the next test.
        Base.metadata.drop_all(engine)
        Base.metadata.create_all(engine)
    else:
        # Row-only reset.  Table order respects the FK chain
        # attempts -> tasks -> agents, and CASCADE covers anything missed.
        with engine.begin() as connection:
            connection.exec_driver_sql(
                "TRUNCATE TABLE attempts, tasks, agents RESTART IDENTITY CASCADE"
            )
    return backend_label()


__all__ = ["backend_label", "describe_url", "reset_database"]
