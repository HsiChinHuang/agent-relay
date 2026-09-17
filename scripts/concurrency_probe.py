#!/usr/bin/env python3
"""Concurrency probe: does a claim ever hand the same task to two workers?

Q4's core claim is that claims stay safe when several API processes race.  The
test suite cannot show that -- every existing test is single-threaded.  This
probe starts N workers at a start-file barrier against M queued tasks and checks
the one invariant that matters: no task is claimed twice.

Why N separate *processes* (not N threads on one engine)
--------------------------------------------------------
Each real API process owns its own engine, connection pool and transaction
state, and that is what the claim path is written for.  N threads sharing the
module-level engine would share one pool (default ``pool_size=5,
max_overflow=10``) and interleave inside single connections -- an artefact that
looks exactly like the bug being hunted while having nothing to do with
``SKIP LOCKED``.  Swapping ``database.engine`` per thread cannot fix this:
``database.SessionLocal`` is bound to the original engine at import time, so the
functions reached through ``db_session()`` would keep using the shared engine,
and every worker would briefly read whichever engine was set last anyway.
Subprocesses remove the whole class of problem, and they match the deployment
this migration is about (N API replicas, one PostgreSQL).

Why the workers call :func:`storage.claim_one` instead of HTTP
--------------------------------------------------------------
The HTTP ``/claim`` endpoint long-polls (``wait_seconds`` defaults to 30, the
handler re-loops every 0.5s while the queue is empty), so an empty-queue
observation over HTTP is indistinguishable from a timeout.  The lock contention
lives in storage, and the root suite's
``test_sqlite_atomic_claims_distribute_without_overlap`` also drives
``claim_one`` directly.  ``--transport http`` adds a small run through the ASGI
app (no server, no port) using ``asyncio.to_thread``, which is literally the
mechanism ``main.py`` uses, so auth + the SQLSTATE retry wrapper + long-poll are
covered too.

The two signals, kept separate on purpose
------------------------------------------
A run reports two different counters, and reading them as one number produces the
wrong conclusion (it did, once, during this stage):

``duplicate_task_claims``
    The same task was handed out *successfully* to two workers.  This is THE
    INVARIANT under test: it must be 0 in every row, baseline and break variants
    alike, at every N.  A non-zero value here means the design is broken.

``concurrent_claim_conflicts``
    A worker's ``INSERT`` into ``attempts`` was rejected by the
    ``uq_attempt_task_number`` unique constraint, i.e. two transactions had READ
    the same task and both tried to create ``(task_id, attempt_number)``.  This is
    a PARTIAL-PROTECTION / contention signal, not an invariant violation: it is
    expected to be 0 in the baseline and MAY be > 0 in break variants, where it is
    the evidence that the removed lock was load-bearing.

Keeping them apart is what separates "the safeguard works" from "the safeguard is
necessary": the three protection layers are not interchangeable (``FOR UPDATE``
provides the safety, ``SKIP LOCKED`` provides throughput by keeping losers from
blocking, ``uq_attempt_task_number`` is the last line of defence), so *which*
counter moves tells you which layer you just broke.

How the break variants work (and why nothing needs restoring)
-------------------------------------------------------------
``--break`` patches the *source string* of ``storage.py`` in memory and execs the
result as a private module.  **No file on disk is ever modified.**  Deliberate:
a variant that rewrote ``storage.py`` and restored it in a ``finally`` block
could still leave a broken tree if killed mid-run, or triple-patch if a flag
leaked through a loop.  Here the on-disk hash equals HEAD before and after by
construction, and :func:`verify_clean` proves it independently.

The patched source is exec'd under ``__name__ = "storage"`` and never registered
in ``sys.modules``, so nothing else can import the mutant.

Two breaks, chosen by hand (not random mutation):
  no-skip-locked  ``FOR UPDATE OF tasks SKIP LOCKED`` -> ``FOR UPDATE OF tasks``
  no-for-update   removes the ``FOR UPDATE`` clause entirely

One calibration control:
  sleep-only      keeps ``SKIP LOCKED`` and only widens the read->write window

Every broken variant is paired with a sleep-only control.  Without that pair, a
double claim found while the lock is removed proves only that a pause between
reading and writing is dangerous -- not which change caused it.

Backend caveat, stated up front
-------------------------------
SQLite claims take the ``locking=False`` branch and are serialized by
``BEGIN IMMEDIATE``, a whole-database writer reservation.  There is no
``SKIP LOCKED`` on that path to remove, so the break variants are only
meaningful for PostgreSQL (:func:`check_combination` refuses rather than silently
running a meaningless experiment).

PostgreSQL runs at ``read committed`` here (verified: ``SHOW
default_transaction_isolation``), which is the app's actual setting.  Under that
isolation level a plain ``FOR UPDATE`` makes the loser wait and then re-read the
committed row, so ``no-skip-locked`` may well still show zero duplicates.  The
duplicate-generating experiment is ``no-for-update``.  Expectations are recorded
per case; the conclusion wording is chosen from what was actually observed.

Usage
-----
    python scripts/concurrency_probe.py --variant sqlite   --break none
    python scripts/concurrency_probe.py --variant postgres --break none
    python scripts/concurrency_probe.py --variant postgres --break no-for-update --n 16 --sleep-ms 100
    python scripts/concurrency_probe.py --variant postgres --break sleep-only  --n 16 --sleep-ms 100
    python scripts/concurrency_probe.py --verify-clean

Exit codes: 0 = invariant held, 3 = broken variant showed no signal
(inconclusive, not a failure), 4 = duplicate claim observed, 5 = refused.
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import shutil
import subprocess
import sys
import tempfile
import threading
import time
import traceback
import types
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_PG_URL = "postgresql+psycopg://agent:agent@localhost:5433/agent_relay_test"
# NOTE: this is the local Stage-A dev container (relay-pg-q4), password is
# intentionally trivial.  Do NOT reuse this URL format in compose/K8s/CI.
# Port 5433 (not 5432) only because an unrelated project's postgres:10 container
# already binds 5432 on this host.  Inside compose / K8s / CI the server listens
# on 5432 as usual; the mapping is the deploy layer's business.
DEFAULT_SQLITE_URL = "sqlite:///" + (
    Path(tempfile.gettempdir()) / "agent-relay-test.db"
).as_posix().lstrip("/")
# Same scratch file the pytest suites use, on purpose: one scratch database for
# this project, so nothing has to learn a second path.

TRUTHY = {"1", "true", "yes", "on"}
GUARDED_FILES = ("database.py", "storage.py", "main.py")
RETRYABLE_SQLSTATES = {"40001", "40P01", "55P03"}  # mirrors main.py; that one is authoritative
WORKER_START_TIMEOUT_S = 45.0
WORKER_JOIN_TIMEOUT_S = 180.0

# --- exact source anchors for the in-memory break patches ---------------------
# The only whitespace-sensitive literals in this file.  Each must match exactly
# once (checked in :func:`load_storage_module`), so a future reformat of
# storage.py fails loudly here instead of quietly probing an unmodified module.
FOR_UPDATE_LINE = "        stmt = stmt.with_for_update(of=Task, skip_locked=True)"
SLEEP_ANCHOR = '    claim_token = new_secret("clm")'
SLEEP_INJECT = "    _probe_sleep()\n" + SLEEP_ANCHOR

BREAK_REMOVALS: dict[str, dict[str, str]] = {
    "no-skip-locked": {FOR_UPDATE_LINE: FOR_UPDATE_LINE.replace(", skip_locked=True", "")},
    "no-for-update": {"    if locking:\n" + FOR_UPDATE_LINE + "\n": ""},
    "sleep-only": {},  # control: widen the window, keep the lock
}
SLEEPING_BREAKS = {"sleep-only", "no-skip-locked+sleep", "no-for-update+sleep"}
BREAK_CHOICES = ("none", *BREAK_REMOVALS, *tuple(f"{k}+sleep" for k in ("no-skip-locked", "no-for-update")))


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Claim-race probe for Agent Relay (SQLite vs PostgreSQL).",
    )
    parser.add_argument("--variant", choices=("sqlite", "postgres"), help="Backend to probe.")
    parser.add_argument("--break", dest="break_kind", choices=BREAK_CHOICES, default="none",
                        help="Remove a concurrency safeguard in the loaded module (in memory only).")
    parser.add_argument("--n", type=int, default=8, help="Concurrent claimers (baseline 8).")
    parser.add_argument("--m", type=int, default=12, help="Queued tasks per round (baseline 12).")
    parser.add_argument("--rounds", type=int, default=1)
    parser.add_argument("--sleep-ms", type=int, default=0,
                        help="Widen the read->write window this many ms (50-100 is the useful range).")
    parser.add_argument("--transport", choices=("direct", "http"), default="direct",
                        help="direct = claim_one in N processes; http = ASGI app + asyncio.to_thread.")
    parser.add_argument("--http-n", type=int, default=4, help="Claimers for --transport http (small by design).")
    parser.add_argument(
        "--http-raise-app-exceptions",
        choices=("true", "false"),
        default="true",
        help="ASGITransport raise_app_exceptions. true (default) surfaces handler "
             "exceptions at the client; false lets Starlette's error middleware turn "
             "them into the 500 a real server would send.",
    )
    parser.add_argument("--sqlite-url", default=DEFAULT_SQLITE_URL)
    parser.add_argument("--postgres-url", default=DEFAULT_PG_URL)
    parser.add_argument("--allow-any-db", action="store_true",
                        help="Permit a PostgreSQL name without 'test' (destructive; be sure).")
    parser.add_argument("--json-only", action="store_true", help="Print only the JSON result.")
    parser.add_argument("--verify-clean", action="store_true",
                        help="Compare working-tree sources against HEAD (bytes, bypassing EOL normalization) and exit.")
    parser.add_argument("--_worker-config", dest="worker_config", help=argparse.SUPPRESS)
    return parser.parse_args(argv)


def sha256_bytes(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def verify_clean() -> int:
    """Reminder 3: make "restore happened" a machine check, not a memory test.

    Compares bytes from ``git show HEAD:<file>`` with the bytes on disk *and*
    asks ``git diff --ignore-cr-at-eol``.  Both are reported because a plain
    ``git diff`` can be silenced by ``.gitattributes`` EOL normalization while the
    working file is genuinely different.
    """

    print("== verify-clean: working tree vs HEAD (bytes + git diff) ==")
    dirty = False
    for name in GUARDED_FILES:
        committed = subprocess.run(
            ["git", "show", f"HEAD:{name}"], cwd=REPO_ROOT, capture_output=True, check=True
        ).stdout
        on_disk = (REPO_ROOT / name).read_bytes()
        diff = subprocess.run(
            ["git", "diff", "--ignore-cr-at-eol", "--", name], cwd=REPO_ROOT, capture_output=True, text=True
        ).stdout
        head_hash, disk_hash = sha256_bytes(committed), sha256_bytes(on_disk)
        ok = head_hash == disk_hash and not diff
        dirty |= not ok
        print(f"  {name:<12} clean={ok}  head={head_hash[:12]}  disk={disk_hash[:12]}  gitdiff={len(diff)}B")
    print("RESULT:", "CLEAN" if not dirty else "DIRTY -- do not commit")
    return 0 if not dirty else 1


def check_combination(args: argparse.Namespace) -> str | None:
    """Refuse combinations that cannot demonstrate anything."""

    if args.break_kind not in BREAK_CHOICES:
        return f"unknown --break {args.break_kind!r}"
    if args.n < 2:
        return "--n must be at least 2: one worker cannot race anything."
    if args.m < 1:
        return "--m must be at least 1."
    if args.rounds < 1:
        return "--rounds must be at least 1."
    if args.break_kind != "none" and args.variant == "sqlite":
        return (
            "--break is meaningless on SQLite: claim_one takes the locking=False branch "
            "there and is serialized by BEGIN IMMEDIATE, so there is no SKIP LOCKED / FOR "
            "UPDATE to remove. Use --variant postgres."
        )
    if args.sleep_ms:
        if args.break_kind == "none" and os.getenv("RELAY_PROBE_ALLOW_SLEEP") not in TRUTHY:
            return (
                "--break none with --sleep-ms needs RELAY_PROBE_ALLOW_SLEEP=1. Widening the "
                "window on the intact variant is a labelled calibration control, never a baseline."
            )
        # Only the two *lock-removing* breaks need the +sleep spelling.  sleep-only
        # is itself the widened control, so refusing "sleep-only --sleep-ms 100"
        # would make the control group unrunnable (verified: exit 5).
        if args.break_kind in {"no-skip-locked", "no-for-update"}:
            return (
                f"{args.break_kind} with --sleep-ms double-counts: use the paired "
                f"'{args.break_kind}+sleep' name so the label says what ran."
            )
    return None


def resolve_url(args: argparse.Namespace) -> str:
    url = args.postgres_url if args.variant == "postgres" else args.sqlite_url
    if "test" not in url.lower() and not args.allow_any_db:
        print(f"REFUSING destructive target: {url!r} (no 'test' in name). Use --allow-any-db.", file=sys.stderr)
        raise SystemExit(5)
    return url


def install_url(url: str) -> None:
    """Set the URL before :mod:`database` is imported (it is frozen at import)."""

    os.environ["RELAY_DATABASE_URL"] = url
    if str(REPO_ROOT) not in sys.path:
        sys.path.insert(0, str(REPO_ROOT))


def attest(args: argparse.Namespace) -> None:
    """Self-attestation, same discipline as Stage C.

    Prints what *this process* resolved.  On WSL a bash-prefixed env var never
    reaches a Windows .exe, so without this line "postgres" results can silently
    be SQLite results -- which is the exact failure this project already hit.
    """

    import database

    scheme = database.DATABASE_URL.split("://", 1)[0]
    print(f"SEEN DATABASE_URL = {database.DATABASE_URL}")
    if args.variant == "postgres" and not database.DATABASE_URL.startswith("postgresql+psycopg"):
        print("REFUSING: --variant postgres but the resolved URL is not psycopg3.", file=sys.stderr)
        raise SystemExit(5)
    if args.variant == "sqlite" and not database.DATABASE_URL.startswith("sqlite"):
        print("REFUSING: --variant sqlite but the resolved URL is not SQLite.", file=sys.stderr)
        raise SystemExit(5)
    print(f"backend_scheme = {scheme}")


def load_storage_module(break_kind: str, sleep_ms: int, *, verbose: bool = True):
    """Load storage.py, optionally patched, without touching the file on disk."""

    source = (REPO_ROOT / "storage.py").read_text(encoding="utf-8")
    baseline_hash = sha256_bytes(source.encode())
    removals = BREAK_REMOVALS.get(break_kind.split("+")[0], {}) if break_kind != "none" else {}
    patched = source
    for old, new in removals.items():
        count = patched.count(old)
        if count != 1:
            raise SystemExit(
                f"break anchor matched {count} times, expected exactly 1:\n{old!r}\n"
                "storage.py changed shape; update the anchors instead of probing an unpatched module."
            )
        patched = patched.replace(old, new)

    inject_sleep = bool(sleep_ms) or break_kind in SLEEPING_BREAKS
    if inject_sleep:
        count = patched.count(SLEEP_ANCHOR)
        if count != 1:
            raise SystemExit(f"sleep anchor matched {count} times, expected 1: {SLEEP_ANCHOR!r}")
        patched = patched.replace(SLEEP_ANCHOR, SLEEP_INJECT)

    if patched == source and (removals or sleep_ms):
        raise SystemExit("patch was a no-op; refusing to label an unmodified module as broken")

    module = types.ModuleType("storage")
    module.__file__ = str(REPO_ROOT / "storage.py")
    module.__name__ = "storage"
    module.__dict__["time"] = time  # storage.py does not import time; _probe_sleep needs it
    module.__dict__["_PROBE_SLEEP_SECONDS"] = (sleep_ms / 1000.0) if inject_sleep else 0.0
    module.__dict__["_probe_sleep"] = lambda: (
        time.sleep(module.__dict__["_PROBE_SLEEP_SECONDS"])
        if module.__dict__["_PROBE_SLEEP_SECONDS"]
        else None
    )
    exec(compile(patched, "storage.py (probe-patched)", "exec"), module.__dict__)
    if "claim_one" not in module.__dict__:
        raise SystemExit("patched module lost claim_one; refusing")

    if patched != source and verbose:
        print(diff_between(source, patched, break_kind, sleep_ms), end="")
        print(f"=== on-disk storage.py sha256 = {baseline_hash[:16]}... (file never written) ===")
    return module, patched, baseline_hash


def diff_between(source: str, patched: str, break_kind: str, sleep_ms: int) -> str:
    import difflib

    return "".join(
        difflib.unified_diff(
            source.splitlines(keepends=True),
            patched.splitlines(keepends=True),
            fromfile="storage.py (on disk)",
            tofile=f"storage.py (in-memory: break={break_kind}, sleep_ms={sleep_ms})",
            n=2,
        )
    )


def patched_claim_sql(storage, database, *, locking: bool = True) -> str:
    """Compile the *patched* module's claim statement with the real dialect."""

    statement = storage._claimable_task_stmt("agent_probe", locking=locking)
    return " ".join(str(statement.compile(dialect=database.engine.dialect)).split())


def assert_patch_effective(args: argparse.Namespace, storage, database) -> str:
    """The patch must reach the executed SQL, not just the file text.

    Without this, a silently ineffective break produces "0 duplicates" and reads
    like proof that the safeguard is unnecessary.
    """

    sql = patched_claim_sql(storage, database)
    upper = sql.upper()
    print("PATCHED CLAIM SQL (compiled by the real dialect):")
    print(f"  {sql}")
    if args.break_kind == "no-for-update":
        if "FOR UPDATE" in upper:
            raise SystemExit("REFUSING: --break no-for-update but the statement still has FOR UPDATE")
    elif args.break_kind == "no-skip-locked":
        if "SKIP LOCKED" in upper or "FOR UPDATE" not in upper:
            raise SystemExit("REFUSING: --break no-skip-locked did not produce a plain FOR UPDATE")
    elif args.break_kind == "sleep-only":
        if "SKIP LOCKED" not in upper:
            raise SystemExit("REFUSING: sleep-only control lost SKIP LOCKED; the pair is invalid")
    return sql


# --- parent-side database helpers (imported lazily, after install_url) --------


def reset_schema(database) -> None:
    """Backend-aware empty-the-world reset (mirrors testsupport.db_reset)."""

    if database.DATABASE_URL.startswith("sqlite"):
        database.Base.metadata.drop_all(database.engine)
        database.Base.metadata.create_all(database.engine)
    else:
        with database.engine.begin() as connection:
            connection.exec_driver_sql("TRUNCATE TABLE attempts, tasks, agents RESTART IDENTITY CASCADE")


def seed_round(*, m: int, claimers: int) -> dict:
    """One sender, one recipient (the contention point), M queued tasks, N claimers.

    ``claim_one(recipient_id, worker_id)`` filters on the *recipient*, so the
    contention this probe creates is on the recipient row set: N claimers asking
    for the same recipient is what makes them compete.  The N claimer identities
    are still registered -- they are what the HTTP supplement authenticates as,
    and they keep ``worker_id`` values distinct in the attempt rows.
    """

    import storage as real_storage

    sender = real_storage.register_agent("probe-sender", None)
    recipient = real_storage.register_agent("probe-recipient", None)
    task_ids = [
        real_storage.create_task(sender["agent_id"], recipient["agent_id"], f"probe-{i}", None)["task_id"]
        for i in range(m)
    ]
    claimer_tokens = [real_storage.register_agent(f"probe-claimer-{i}", None)["token"] for i in range(claimers)]
    return {"task_ids": task_ids, "recipient_id": recipient["agent_id"], "claimer_tokens": claimer_tokens}


def db_truth(database) -> dict:
    """Server-side truth: does the database agree with what the workers saw?"""

    from sqlalchemy import func, select

    from database import Attempt, Task

    with database.engine.connect() as connection:
        processing = int(
            connection.execute(select(func.count()).select_from(Task).where(Task.status == "processing")).scalar()
        )
        attempt_rows = int(connection.execute(select(func.count()).select_from(Attempt)).scalar())
        tasks_with_many_attempts = int(
            connection.execute(
                select(func.count()).select_from(
                    select(Attempt.task_id)
                    .group_by(Attempt.task_id)
                    .having(func.count() > 1)
                    .subquery()
                )
            ).scalar()
        )
    return {"tasks_processing": processing, "attempt_rows": attempt_rows,
            "tasks_with_multiple_attempts": tasks_with_many_attempts}


def backend_activity_monitor(database, samples: list[int], stop: threading.Event) -> None:
    """Sample server-side active backends while the race runs (PostgreSQL only).

    Uses its own engine so the monitor is not counted among the racing backends
    (it filters ``pid <> pg_backend_pid()``).
    """

    from sqlalchemy import create_engine

    monitor_engine = create_engine(database.DATABASE_URL, future=True, pool_pre_ping=True)
    try:
        while not stop.is_set():
            try:
                with monitor_engine.connect() as connection:
                    count = int(connection.exec_driver_sql(
                        "SELECT count(*) FROM pg_stat_activity "
                        "WHERE datname = current_database() AND pid <> pg_backend_pid() "
                        "AND state <> 'idle'"
                    ).scalar_one())
                    samples.append(count)
            except Exception:  # noqa: BLE001 - monitoring must never break the probe
                samples.append(-1)
            time.sleep(0.008)
    finally:
        monitor_engine.dispose()


# --- worker (one OS process per claimer) -------------------------------------


def run_worker(config: dict) -> int:
    """Child process: wait for the start file, claim once, print one JSON line."""

    install_url(config["database_url"])
    import database

    storage, _, _ = load_storage_module(config["break_kind"], config["sleep_ms"], verbose=False)
    start_path = Path(config["start_file"])
    deadline = time.monotonic() + WORKER_START_TIMEOUT_S
    while not start_path.exists():
        if time.monotonic() > deadline:
            print(json.dumps({"worker": config["index"], "ok": False, "error": "start file never appeared"}))
            return 2
        time.sleep(0.002)

    outcome = {"worker": config["index"], "ok": False, "task_id": None, "attempt": None,
               "started_at": None, "elapsed_ms": None, "backend_pid": None, "retries": 0, "error": None}
    if database.DATABASE_URL.startswith("postgres"):
        with database.engine.connect() as connection:
            outcome["backend_pid"] = int(
                connection.exec_driver_sql("SELECT pg_backend_pid()").scalar_one()
            )
    started = time.perf_counter()
    outcome["started_at"] = datetime.now(timezone.utc).isoformat(timespec="milliseconds")
    try:
        while True:
            try:
                claimed = storage.claim_one(config["recipient_id"], f"w{config['index']}")
                outcome.update(ok=True, elapsed_ms=round((time.perf_counter() - started) * 1000, 2))
                if claimed is not None:
                    outcome["task_id"] = claimed["task_id"]
                    outcome["attempt"] = claimed["attempt"]
                return 0
            except Exception as exc:  # noqa: BLE001 - classify, retry like main.py, then report
                if is_retryable(exc) and outcome["retries"] < 3:
                    outcome["retries"] += 1
                    time.sleep(0.02 * outcome["retries"])
                    continue
                outcome.update(ok=False, elapsed_ms=round((time.perf_counter() - started) * 1000, 2),
                               error=f"{type(exc).__name__}: {str(exc)[:400]}")
                return 1
    finally:
        # The parent reads exactly one JSON line per worker.  Reporting from
        # finally means a worker can never exit without being heard from -- the
        # first version of this script returned on both paths without printing,
        # so the parent saw "no result" while the database showed 8 claims.
        print(json.dumps(outcome), flush=True)


def is_retryable(exc: Exception) -> bool:
    """Local copy of main.py's predicate, for counting only.

    main.py owns the retry policy; this exists so the probe can report how often
    the retryable path fired without importing main (which would start the
    recovery loop).  Kept byte-identical in behaviour to the real one.
    """

    from sqlalchemy.exc import DBAPIError, OperationalError

    if isinstance(exc, OperationalError) and "locked" in str(exc).lower():
        return True
    if isinstance(exc, DBAPIError) and exc.orig is not None:
        state = getattr(exc.orig, "sqlstate", None) or getattr(exc.orig, "pgcode", None)
        return state is not None and str(state) in RETRYABLE_SQLSTATES
    return False


def spawn_workers(args: argparse.Namespace, database, seed: dict, tmpdir: Path) -> list[dict]:
    """Launch N worker processes, then release them with one file write."""

    start_file = tmpdir / f"start-r{seed['round']}"
    start_file.unlink(missing_ok=True)
    commands, children = [], []
    for index in range(args.n):
        config = {
            "index": index,
            "database_url": database.DATABASE_URL,
            "break_kind": args.break_kind,
            "sleep_ms": args.sleep_ms,
            "recipient_id": seed["recipient_id"],
            "start_file": str(start_file),
        }
        config_path = tmpdir / f"worker-{seed['round']}-{index}.json"
        config_path.write_text(json.dumps(config), encoding="utf-8")
        command = [sys.executable, str(Path(__file__).resolve()), "--_worker-config", str(config_path)]
        commands.append(command)
        children.append(subprocess.Popen(command, cwd=REPO_ROOT, stdout=subprocess.PIPE,
                                        stderr=subprocess.PIPE, text=True))
    print(f"launched {args.n} worker processes, one engine/pool/transaction each")
    print(f"  worker command shape: {Path(commands[0][0]).name} {Path(commands[0][1]).name} "
          f"--_worker-config worker-<round>-<i>.json")
    start_file.touch()

    results: list[dict] = []
    for index, child in enumerate(children):
        try:
            stdout, stderr = child.communicate(timeout=WORKER_JOIN_TIMEOUT_S)
        except subprocess.TimeoutExpired:
            child.kill()
            stdout, stderr = child.communicate()
        record: dict = {"worker": index, "ok": False, "error": f"no result (exit {child.returncode})"}
        for line in (stdout or "").splitlines():
            line = line.strip()
            if line.startswith("{") and line.endswith("}"):
                try:
                    record = json.loads(line)
                except json.JSONDecodeError:
                    pass
        if child.returncode != 0 and record.get("ok"):
            record["worker_exit_code"] = child.returncode
        if stderr and stderr.strip():
            record["stderr_tail"] = stderr.strip().splitlines()[-1][:200]
        results.append(record)
    start_file.unlink(missing_ok=True)
    return results


# --- optional HTTP supplement -------------------------------------------------


def run_http_supplement(args: argparse.Namespace, database) -> dict:
    """Small run through the ASGI app, using main.py's own concurrency mechanism.

    The real handler is ``await asyncio.to_thread(claim_one, ...)``; this does the
    same with httpx's ASGITransport (no server, no port, so nothing in the repo
    or on the network is touched).  ``wait_seconds=0`` makes an empty queue return
    204 immediately instead of long-polling for 30s.  Deliberately small (N=4):
    this covers the HTTP path, the process-level race above carries the load.

    Why the break has to be installed before ``import main``
    -------------------------------------------------------
    ``main.py`` does ``from storage import claim_one`` at import time, so simply
    loading a patched module here would leave the handler calling the *intact*
    function -- ``--break`` would look like it ran while the endpoint was testing
    unmodified code.  Registering the patched module as ``sys.modules['storage']``
    before ``main`` is first imported is what makes the handler bind the patched
    ``claim_one``.  What was done is reported in ``storage_module_used``.
    """

    import asyncio
    import httpx

    if args.break_kind != "none":
        patched_module, _, _ = load_storage_module(args.break_kind, args.sleep_ms, verbose=False)
        sys.modules["storage"] = patched_module
        storage_label = f"PATCHED in-memory module (break={args.break_kind}) bound into main"
    else:
        storage_label = "real storage.py (baseline: nothing patched)"

    import main
    from database import Attempt, Task
    from sqlalchemy import func, select
    import storage as storage_in_use

    database.init_db()  # seeding goes through storage, not the ASGI lifespan
    n, m = args.http_n, max(args.http_n, 2)

    # Seeding uses storage directly: httpx's sync Client cannot drive an
    # ASGITransport at all (``AttributeError: 'ASGITransport' object has no
    # attribute '__enter__'`` -- verified), so there is no sync HTTP path here.
    import storage as real_storage

    sender = storage_in_use.register_agent("http-sender", None)
    # One worker identity, N concurrent claims.  This is the only shape that is
    # both realistic and contended: ``claim_one`` filters on ``recipient_id``, so
    # N *different* claiming agents would each see only their own queue and could
    # never collide (an earlier version of this function did exactly that and
    # correctly returned 204 for every claim -- a probe measuring nothing).  One
    # agent running several pollers is a supported deployment, which is why
    # ``ClaimRequest.worker_id`` exists.
    worker = storage_in_use.register_agent("http-worker", None)
    for i in range(m):
        storage_in_use.create_task(sender["agent_id"], worker["agent_id"], f"http-{i}", None)
    worker_headers = {"Authorization": f"Bearer {worker['token']}"}
    recipient_id = worker["agent_id"]

    async def race() -> list[dict]:
        raise_app_exceptions = args.http_raise_app_exceptions != "false"
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=main.app, raise_app_exceptions=raise_app_exceptions),
            base_url="http://probe",
        ) as client:
            barrier = asyncio.Barrier(n)

            async def one(index: int) -> dict:
                await barrier.wait()
                started = time.perf_counter()
                try:
                    response = await client.post("/api/v1/tasks/claim", headers=worker_headers,
                                                 json={"worker_id": f"h{index}", "wait_seconds": 0})
                    payload = response.json() if response.status_code == 200 else {}
                    return {"worker": index, "status": response.status_code,
                            "task_id": payload.get("task_id"),
                            "body_head": (response.text or "")[:120] if response.status_code >= 400 else None,
                            "elapsed_ms": round((time.perf_counter() - started) * 1000, 2)}
                except Exception as exc:  # noqa: BLE001 - the handler's behaviour *is* the measurement
                    return {"worker": index, "status": f"client-exception {type(exc).__name__}",
                            "task_id": None,
                            "elapsed_ms": round((time.perf_counter() - started) * 1000, 2)}

            return list(await asyncio.gather(*(one(index) for index in range(n))))

    outcomes = asyncio.run(race())

    claimed = [o["task_id"] for o in outcomes if o.get("task_id")]
    duplicates = sum(c - 1 for c in Counter(claimed).values() if c > 1)
    with database.engine.connect() as connection:
        processing = int(connection.execute(
            select(func.count()).select_from(Task).where(Task.recipient_id == recipient_id,
                                                        Task.status == "processing")).scalar())
        attempt_rows = int(connection.execute(
            select(func.count()).select_from(Attempt).join(
                Task, Attempt.task_id == Task.id
            ).where(Task.recipient_id == recipient_id)).scalar())
    statuses = Counter(str(o["status"]) for o in outcomes)
    return {"n": n, "m": m, "outcomes": outcomes, "claimed": len(claimed),
            "status_counts": dict(statuses), "storage_module_used": storage_label,
            "raise_app_exceptions": args.http_raise_app_exceptions,
            "duplicate_task_claims": duplicates, "tasks_processing_for_recipient": processing,
            "attempt_rows_for_recipient": attempt_rows,
            "note": "single worker identity, N concurrent claims (worker_id differs); "
                    "this is the only HTTP shape that both matches the recipient filter and contends"}


# --- verdict ------------------------------------------------------------------


def evaluate(results: list[dict], *, n: int) -> dict:
    """Score one round.

    Two distinct signals are counted, and conflating them would be a mistake:

    ``duplicate_task_claims``
        the same task handed out successfully twice.  This is the invariant,
        and nothing should ever produce it.

    ``concurrent_claim_conflicts``
        a worker lost the race and PostgreSQL rejected its attempt row with
        ``uq_attempt_task_number``.  Two transactions had read the *same* task
        and both inserted ``(task_id, attempt_number)``.  The unique constraint
        turned a would-be double assignment into an error, so the invariant
        still holds while the safeguard is removed.  Counting only the
        successful claims would read "0 duplicates" and look like the break did
        nothing -- the constraint is what is being credited in that reading, not
        ``SKIP LOCKED``.

    The contended task id is taken from PostgreSQL's own ``DETAIL`` line rather
    than inferred, because the server is the only party that saw both writers.
    SQLite cannot produce this signal: it has no such constraint violation path
    under ``BEGIN IMMEDIATE`` (the second writer never reaches the SELECT).
    """

    successes = [r for r in results if r.get("ok") and r.get("task_id")]
    counts = Counter(r["task_id"] for r in successes)
    conflicts = [conflict_task_id(r["error"]) for r in results if is_attempt_collision(r.get("error"))]
    return {
        "workers": n,
        "returned": len(results),
        "successes": len(successes),
        "empty_claims": sum(1 for r in results if r.get("ok") and r.get("task_id") is None),
        "errors": [{"worker": r["worker"], "error": r.get("error")} for r in results if r.get("error")],
        "distinct_claimed_tasks": len(counts),
        "duplicate_task_claims": sum(c - 1 for c in counts.values() if c > 1),
        "duplicate_detail": {t: c for t, c in counts.items() if c > 1},
        "concurrent_claim_conflicts": len(conflicts),
        "conflicted_task_ids": sorted({t for t in conflicts if t}),
        "retry_total": sum(r.get("retries") or 0 for r in results),
        "workers_with_retries": sum(1 for r in results if (r.get("retries") or 0) > 0),
        "elapsed_ms_max": max((r.get("elapsed_ms") or 0 for r in results), default=None),
        "distinct_backend_pids": len({r["backend_pid"] for r in results if r.get("backend_pid")}) or None,
        "claim_start_spread_ms": claim_start_spread(results),
    }


COLLISION_TASK_PATTERN = re.compile(r"\(task_id, attempt_number\)=\((task_[0-9a-f]+), ")


def is_attempt_collision(error: str | None) -> bool:
    """Did this worker lose the race to the attempt-uniqueness constraint?"""

    return bool(error) and "uq_attempt_task_number" in error


def conflict_task_id(error: str | None) -> str | None:
    """The contended task id, read out of PostgreSQL's DETAIL line."""

    if not error:
        return None
    match = COLLISION_TASK_PATTERN.search(error)
    return match.group(1) if match else None


def claim_start_spread(results: list[dict]) -> float | None:
    """How tightly the workers actually entered the claim (ms).

    A wide spread means there was barely a race, which must be reported instead
    of quietly presented as "no duplicates, therefore safe".
    """

    stamps = []
    for row in results:
        raw = row.get("started_at")
        if raw:
            stamps.append(datetime.fromisoformat(raw))
    if len(stamps) < 2:
        return None
    return round((max(stamps) - min(stamps)).total_seconds() * 1000, 2)


def print_round(detail: dict) -> None:
    verdict = detail["verdict"]
    truth = detail["db_truth"]
    print(f"--- round {detail['round']}: {verdict['successes']}/{verdict['workers']} claimed, "
          f"duplicates={verdict['duplicate_task_claims']}, "
          f"conflicts={verdict['concurrent_claim_conflicts']}, retries={verdict['retry_total']} "
          f"({verdict['workers_with_retries']} workers), start_spread={verdict['claim_start_spread_ms']}ms")
    print(f"    db truth: tasks_processing={truth['tasks_processing']} attempt_rows={truth['attempt_rows']} "
          f"multi_attempt_tasks={truth['tasks_with_multiple_attempts']}")
    for row in detail["workers"]:
        print(f"    w{row['worker']}: ok={row.get('ok')} task={str(row.get('task_id'))[:14]} "
              f"attempt={row.get('attempt')} elapsed={row.get('elapsed_ms')}ms pid={row.get('backend_pid')} "
              f"retries={row.get('retries')} err={str(row.get('error'))[:70]}")


# --- entry point --------------------------------------------------------------


def main() -> int:
    args = parse_args()
    if args.verify_clean:
        return verify_clean()
    if args.worker_config:
        return run_worker(json.loads(Path(args.worker_config).read_text(encoding="utf-8")))

    if not args.variant:
        print("REFUSING: --variant is required (sqlite|postgres).", file=sys.stderr)
        return 5
    refusal = check_combination(args)
    if refusal:
        print("REFUSING: " + refusal, file=sys.stderr)
        return 5

    install_url(resolve_url(args))
    import database

    attest(args)
    baseline_clean = verify_clean()
    if args.break_kind != "none" and baseline_clean != 0:
        print("REFUSING: sources differ from HEAD; a broken variant must start clean.", file=sys.stderr)
        return 5
    print()

    storage, patched_source, baseline_hash = load_storage_module(args.break_kind, args.sleep_ms)
    result: dict = {
        "variant": args.variant, "break": args.break_kind, "transport": args.transport,
        "sleep_ms_requested": args.sleep_ms,
        "window_widened": bool(args.sleep_ms) or args.break_kind in SLEEPING_BREAKS,
        "n": args.n, "m": args.m, "rounds": args.rounds,
        "database_url": database.DATABASE_URL,
        "backend_scheme": database.DATABASE_URL.split("://", 1)[0],
        "storage_py_sha256_on_disk": baseline_hash,
        "patched_in_memory_only": args.break_kind != "none" or bool(args.sleep_ms),
        "started_at": datetime.now(timezone.utc).isoformat(timespec="seconds"),
        "rounds_detail": [], "peak_concurrent_backends": None,
    }
    if (args.break_kind != "none" or args.sleep_ms) and args.transport != "http":
        # For --transport http the effectiveness check happens inside the
        # supplement, since that path needs the patch installed before main is
        # imported and reports which storage module it bound.
        result["patched_claim_sql"] = assert_patch_effective(args, storage, database)
        print()

    if args.transport == "http":
        reset_schema(database)  # same clean starting point as the direct variants
        result["http"] = run_http_supplement(args, database)
        result["verify_clean_after"] = "CLEAN" if verify_clean() == 0 else "DIRTY"
        http = result["http"]
        print(json.dumps(result, indent=2, default=str))
        if http["duplicate_task_claims"]:
            print(f"VERDICT: {http['duplicate_task_claims']} duplicate claim(s) over HTTP -- invariant VIOLATED")
            return 4
        failures = [s for s in http["status_counts"] if not s.startswith("2")]
        if failures:
            print(f"VERDICT: HTTP failures observed under break={args.break_kind}: {failures}")
            return 4
        print("VERDICT: HTTP path held -- every claim 200/204, no duplicate task")
        return 0

    tmpdir = Path(tempfile.mkdtemp(prefix="relay-probe-", dir=tempfile.gettempdir()))
    samples: list[int] = []
    stop: threading.Event | None = None
    monitor: threading.Thread | None = None
    if database.DATABASE_URL.startswith("postgres"):
        stop = threading.Event()
        monitor = threading.Thread(target=backend_activity_monitor, args=(database, samples, stop), daemon=True)
        monitor.start()
    try:
        for round_index in range(args.rounds):
            reset_schema(database)
            seed = seed_round(m=args.m, claimers=args.n)
            seed["round"] = round_index + 1
            race_started = time.perf_counter()
            workers = spawn_workers(args, database, seed, tmpdir)
            verdict = evaluate(workers, n=args.n)
            truth = db_truth(database)
            detail = {"round": round_index + 1, "race_wall_ms": round((time.perf_counter() - race_started) * 1000, 2),
                      "workers": workers, "verdict": verdict, "db_truth": truth}
            result["rounds_detail"].append(detail)
            if not args.json_only:
                print_round(detail)
    finally:
        if stop is not None:
            stop.set()
            if monitor is not None:
                monitor.join(timeout=5)
            positives = [s for s in samples if s >= 0]
            result["peak_concurrent_backends"] = max(positives) if positives else None
            result["activity_samples"] = len(positives)
        shutil.rmtree(tmpdir, ignore_errors=True)

    totals = {
        "duplicate_task_claims": sum(d["verdict"]["duplicate_task_claims"] for d in result["rounds_detail"]),
        "concurrent_claim_conflicts": sum(
            d["verdict"]["concurrent_claim_conflicts"] for d in result["rounds_detail"]
        ),
        "conflicted_task_ids": sorted({
            task_id
            for detail in result["rounds_detail"]
            for task_id in detail["verdict"]["conflicted_task_ids"]
        }),
        "successes": sum(d["verdict"]["successes"] for d in result["rounds_detail"]),
        "retry_total": sum(d["verdict"]["retry_total"] for d in result["rounds_detail"]),
        "errors": sum(len(d["verdict"]["errors"]) for d in result["rounds_detail"]),
    }
    result["totals"] = totals
    result["verify_clean_after"] = "CLEAN" if verify_clean() == 0 else "DIRTY"
    calibrated = bool(args.sleep_ms) or args.break_kind in SLEEPING_BREAKS
    result["window_widened"] = calibrated
    print()
    print(json.dumps(result, indent=2, default=str))

    if totals["duplicate_task_claims"]:
        print(f"VERDICT: {totals['duplicate_task_claims']} duplicate claim(s) observed -- invariant VIOLATED")
        return 4
    if args.break_kind == "none" and not calibrated:
        print("VERDICT: invariant held -- no task was claimed twice")
        return 0
    # A broken or widened variant is only informative if *something* moved.  The
    # unique-constraint collisions count as that signal: they are the visible
    # trace of two writers having read the same task.
    if totals["concurrent_claim_conflicts"] or totals["duplicate_task_claims"]:
        print(
            f"VERDICT: SIGNAL OBSERVED -- {totals['concurrent_claim_conflicts']} concurrent "
            f"claim conflict(s) on {totals['conflicted_task_ids']} "
            f"(duplicates={totals['duplicate_task_claims']})"
        )
        return 0
    print("VERDICT: broken/calibrated variant showed no signal -- INCONCLUSIVE (conclusion case C)")
    return 3


if __name__ == "__main__":
    try:
        sys.exit(main())
    except SystemExit:
        raise
    except Exception:  # noqa: BLE001 - never leave the harness with a bare traceback
        traceback.print_exc()
        sys.exit(5)
