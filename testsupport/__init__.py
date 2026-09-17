"""Helper package shared by the root suite and ``tests/integration``.

Kept out of ``tests/`` so both suites can import it: ``test_agent_relay.py``
lives at the repository root, and pytest only puts the *ancestors* of a test
file's directory on ``sys.path`` once a ``conftest.py`` exists there.  The root
``conftest.py`` inserts the repository root, which makes ``testsupport`` (and
``main``/``database``/``storage``) importable from anywhere under the repo.

Nothing here runs at import time.  ``database`` is imported lazily inside the
functions because ``database.DATABASE_URL`` is frozen at *its* import time, and
the suites' conftests must be allowed to set ``RELAY_DATABASE_URL`` first.
"""
