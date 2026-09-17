#!/usr/bin/env python3
"""HTTP smoke test for a *running* Agent Relay instance (Q3 container check).

Why this exists separately from tests/integration:

* tests/integration uses starlette's TestClient, which talks to the ASGI app
  in-process over a fake transport -- it never opens a socket, so it cannot
  prove anything about a container, a published port, or the volume-backed
  database.  This script speaks real HTTP to a real process.
* It is stdlib-only, so it runs inside the production image (built with
  ``uv sync --frozen --no-dev``, i.e. no pytest and no httpx) as well as on the
  host.

It deliberately does NOT live under tests/ with a test_*.py name: that would
make the normal ``pytest`` run hit a live server and break the isolated 9-test
baseline.

Usage:
    RELAY_BASE_URL=http://127.0.0.1:8081 python scripts/smoke_http.py
    docker exec agent-relay python scripts/smoke_http.py      # in-container
    docker exec agent-relay python scripts/smoke_http.py --self-base-url

Exit code 0 means every step passed.
"""

from __future__ import annotations

import json
import os
import sys
import urllib.error
import urllib.request

DEFAULT_BASE = os.getenv("RELAY_BASE_URL", "http://127.0.0.1:8000")

FAILURES: list[str] = []
CHECKS: list[str] = []


def check(condition: bool, label: str, detail: str = "") -> bool:
    if condition:
        CHECKS.append(f"  PASS  {label}")
    else:
        FAILURES.append(f"  FAIL  {label} :: {detail}")
    return bool(condition)


def call(
    base: str,
    method: str,
    path: str,
    payload: dict | None = None,
    token: str | None = None,
) -> tuple[int, dict | None, str]:
    headers = {"content-type": "application/json"}
    if token:
        headers["authorization"] = f"Bearer {token}"
    body = json.dumps(payload).encode() if payload is not None else None
    request = urllib.request.Request(base.rstrip("/") + path, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read().decode("utf-8")
            return response.status, (json.loads(raw) if raw else None), raw
    except urllib.error.HTTPError as exc:
        raw = exc.read().decode("utf-8", "replace")
        try:
            return exc.code, json.loads(raw) if raw else None, raw
        except json.JSONDecodeError:
            return exc.code, None, raw


def main() -> int:
    base = DEFAULT_BASE
    for arg in sys.argv[1:]:
        if arg.startswith("--base-url="):
            # Shell env vars are unreliable across the WSL -> Windows executable
            # boundary (verified), so accept a plain CLI override as well.
            base = arg.split("=", 1)[1]
    if "--self-base-url" in sys.argv:
        # Inside the container the published host port is not reachable; talk to
        # uvicorn on its own loopback instead.
        base = "http://127.0.0.1:8000"
    print(f"Agent Relay HTTP smoke test -> {base}")

    # 0) liveness/readiness: /ready probes the real tables, so it also proves
    #    the schema exists in whatever database this instance is using.
    status, body, raw = call(base, "GET", "/ready")
    check(status == 200 and body == {"status": "ready"}, "GET /ready returns ready", f"{status} {raw[:200]}")

    # 1) register two identities (the only unauthenticated endpoint).
    status, alice, raw = call(base, "POST", "/api/v1/agents", {"name": "smoke-alice"})
    check(status == 201 and alice and alice.get("token", "").startswith("agt_"), "register sender", f"{status} {raw[:200]}")
    status, worker, raw = call(base, "POST", "/api/v1/agents", {"name": "smoke-worker"})
    check(status == 201 and worker and worker.get("token", "").startswith("agt_"), "register recipient", f"{status} {raw[:200]}")
    if not alice or not worker:
        return report(base)

    alice_token, worker_token = alice["token"], worker["token"]

    # 2) sender submits a task for the recipient.
    status, task, raw = call(
        base, "POST", "/api/v1/tasks", {"to": worker["agent_id"], "input": "hello relay"}, alice_token
    )
    check(status == 201 and task and task.get("status") == "queued", "POST /tasks -> queued", f"{status} {raw[:200]}")
    if not task:
        return report(base)
    task_id = task["task_id"]

    # 3) recipient claims it.
    status, claim, raw = call(base, "POST", "/api/v1/tasks/claim", {"worker_id": "smoke-w", "wait_seconds": 0}, worker_token)
    check(
        status == 200 and claim and claim.get("claim_token", "").startswith("clm_") and claim.get("attempt") == 1,
        "POST /tasks/claim returns active claim",
        f"{status} {raw[:200]}",
    )
    if not claim:
        return report(base)
    claim_token = claim["claim_token"]

    # 4) recipient completes it.
    status, done, raw = call(
        base, "POST", f"/api/v1/tasks/{task_id}/complete", {"claim_token": claim_token, "output": "HELLO RELAY"}, worker_token
    )
    check(status == 200 and done and done.get("status") == "completed", "POST /tasks/{id}/complete", f"{status} {raw[:200]}")

    # 5) sender sees the terminal state.  There is no "delivered" status in this
    #    protocol: the terminal happy path is "completed".
    status, final, raw = call(base, "GET", f"/api/v1/tasks/{task_id}", None, alice_token)
    check(status == 200 and final and final.get("status") == "completed", "sender sees status=completed", f"{status} {raw[:200]}")
    check(final is not None and final.get("output") == "HELLO RELAY", "sender sees stored output", f"{raw[:200]}")

    # 6) delivery history: outcome recorded, claim token never exposed.
    status, attempts, raw = call(base, "GET", f"/api/v1/tasks/{task_id}/attempts", None, alice_token)
    items = (attempts or {}).get("items") or []
    check(status == 200 and items and items[0]["outcome"] == "completed", "attempts[0].outcome=completed", f"{status} {raw[:200]}")
    check("claim_token" not in raw and claim_token not in raw, "attempts body leaks no claim token", raw[:200])

    # 7) auth boundary: no token, and an unrelated agent.
    status, body, raw = call(base, "POST", "/api/v1/tasks/claim", {"wait_seconds": 0})
    check(status == 401 and body and body["error"]["code"] == "missing_credentials", "claim without token -> 401", f"{status} {raw[:200]}")
    status, stranger, raw = call(base, "POST", "/api/v1/agents", {"name": "smoke-stranger"})
    if stranger:
        status, body, raw = call(base, "GET", f"/api/v1/tasks/{task_id}", None, stranger["token"])
        check(status == 404, "unrelated agent cannot read task", f"{status} {raw[:200]}")

    # 8) the dashboard is served by this instance (token is pasted by a human).
    try:
        with urllib.request.urlopen(base.rstrip("/") + "/", timeout=30) as response:
            page = response.read().decode("utf-8", "replace")
        check(response.status == 200 and "sessionStorage" in page, "GET / serves dashboard html", f"{response.status}")
    except urllib.error.HTTPError as exc:
        check(False, "GET / serves dashboard html", str(exc.code))

    return report(base)


def report(base: str) -> int:
    print("\n".join(CHECKS))
    if FAILURES:
        print("\n".join(FAILURES))
    print(f"\n{len(CHECKS)} passed, {len(FAILURES)} failed  (base={base})")
    if FAILURES:
        return 1
    print("SMOKE OK")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
