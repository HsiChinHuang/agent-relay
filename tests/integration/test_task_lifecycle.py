"""Integration test for the Q1 manual flow, run against the real API and DB.

Nothing is mocked: the ``client`` fixture (``tests/integration/conftest.py``)
drives the real FastAPI app, which uses the real SQLAlchemy engine against the
real file behind ``RELAY_DATABASE_URL``.  Q3-Q6 reuse ``client``,
:func:`register`, :func:`submit_task` and :func:`claim_task` as the regression
check that "the same flow still works".

State vocabulary note: this system has exactly four task statuses -- ``queued``,
``processing``, ``completed``, ``failed`` (the set validated in ``main.py``).
There is no ``delivered`` status, so the terminal happy-path status asserted
here is ``completed``.
"""

from __future__ import annotations

from fastapi.testclient import TestClient

from database import DATABASE_URL


def test_uses_scratch_database() -> None:
    normalized = DATABASE_URL.replace("\\", "/")
    assert "agent-relay.db" not in normalized, (
        f"integration fixtures empty every table; refusing dev database {DATABASE_URL!r}"
    )
    assert "test" in normalized.lower(), (
        f"integration fixtures need a *test* database, got {DATABASE_URL!r}"
    )
    if normalized.startswith("postgres"):
        # psycopg3 only.  A bare ``postgresql://`` URL makes SQLAlchemy choose
        # psycopg2, which is not installed, so the driver name is load-bearing.
        assert normalized.startswith("postgresql+psycopg://"), (
            f"PostgreSQL targets must use the psycopg3 dialect, got {DATABASE_URL!r}"
        )


# ---------------------------------------------------------------- helpers


def register(client: TestClient, name: str) -> tuple[dict, dict[str, str]]:
    """Register an identity and return ``(record, bearer_headers)``."""

    response = client.post("/api/v1/agents", json={"name": name})
    assert response.status_code == 201, response.text
    data = response.json()
    assert data["token"].startswith("agt_"), data
    return data, {"Authorization": f"Bearer {data['token']}"}


def submit_task(
    client: TestClient,
    sender_headers: dict[str, str],
    recipient_id: str,
    *,
    text: str = "hello",
    idempotency_key: str | None = None,
) -> str:
    """Send a task as the authenticated sender and return its id."""

    headers = dict(sender_headers)
    if idempotency_key is not None:
        headers["Idempotency-Key"] = idempotency_key
    response = client.post("/api/v1/tasks", headers=headers, json={"to": recipient_id, "input": text})
    assert response.status_code == 201, response.text
    body = response.json()
    assert body["status"] == "queued", body
    return body["task_id"]


def claim_task(
    client: TestClient,
    worker_headers: dict[str, str],
    *,
    worker_id: str = "w1",
    wait_seconds: int = 0,
) -> dict:
    """Claim one queued task for the authenticated identity."""

    response = client.post(
        "/api/v1/tasks/claim",
        headers=worker_headers,
        json={"worker_id": worker_id, "wait_seconds": wait_seconds},
    )
    assert response.status_code == 200, response.text
    return response.json()


def complete_task(client: TestClient, headers: dict[str, str], task_id: str, claim_token: str, output: str):
    return client.post(
        f"/api/v1/tasks/{task_id}/complete",
        headers=headers,
        json={"claim_token": claim_token, "output": output},
    )


def register_submit_and_claim(client: TestClient, *, text: str = "hello", output: str = "HELLO") -> dict:
    """Register two identities, submit a task, and claim it.

    Stops *before* the terminal call on purpose: several tests need to assert on
    the completion response itself.  ``output`` is only the suggested payload.
    """

    sender, sender_headers = register(client, "sender")
    worker, worker_headers = register(client, "worker")
    task_id = submit_task(client, sender_headers, worker["agent_id"], text=text)
    claim = claim_task(client, worker_headers)
    return {
        "sender": sender,
        "sender_headers": sender_headers,
        "worker": worker,
        "worker_headers": worker_headers,
        "task_id": task_id,
        "claim": claim,
        "claim_token": claim["claim_token"],
        "output": output,
    }


# ------------------------------------------------------------------ tests


def test_uses_scratch_database() -> None:
    """Guard for requirement 7: this suite must never target the dev database."""

    normalized = DATABASE_URL.replace("\\", "/")
    assert "agent-relay.db" not in normalized, (
        f"integration fixtures call drop_all(); refusing dev database {DATABASE_URL!r}"
    )
    if normalized.startswith("sqlite"):
        assert "/tmp" in normalized or ":memory:" in normalized, normalized


def test_sender_sees_completed(client: TestClient):
    """Q1 flow end to end: register, submit, claim, complete, read back."""

    sender, sender_headers = register(client, "sender")
    worker, worker_headers = register(client, "worker")

    task_id = submit_task(client, sender_headers, worker["agent_id"], text="hello")

    claim = claim_task(client, worker_headers, worker_id="w1", wait_seconds=0)
    assert claim["task_id"] == task_id
    assert claim["from"] == sender["agent_id"]
    assert claim["input"] == "hello"
    assert claim["attempt"] == 1
    claim_token = claim["claim_token"]
    assert claim_token.startswith("clm_"), claim_token

    done = complete_task(client, worker_headers, task_id, claim_token, "HELLO")
    assert done.status_code == 200, done.text
    assert done.json() == {"task_id": task_id, "status": "completed"}

    # Requirement 4: the sender is an authorized participant and sees the
    # terminal status "completed" (there is no "delivered" status in this API).
    final = client.get(f"/api/v1/tasks/{task_id}", headers=sender_headers)
    assert final.status_code == 200, final.text
    body = final.json()
    assert body["status"] == "completed", body
    assert body["output"] == "HELLO"
    assert body["error"] is None
    assert body["attempt_count"] == 1
    assert body["from"] == sender["agent_id"]
    assert body["to"] == worker["agent_id"]
    assert body["finished_at"] is not None

    # Requirement 5: delivery history reports the accepted outcome.
    attempts = client.get(f"/api/v1/tasks/{task_id}/attempts", headers=sender_headers)
    assert attempts.status_code == 200, attempts.text
    items = attempts.json()["items"]
    assert len(items) == 1, items
    assert items[0]["outcome"] == "completed", items[0]
    assert items[0]["attempt"] == 1
    assert items[0]["worker_id"] == "w1"
    assert items[0]["finished_at"] is not None

    # Requirement 6: the claim token is never exposed -- checked per key and
    # against the raw body, so it cannot hide under a renamed field.
    assert "claim_token" not in items[0]
    assert "claim_token" not in attempts.text
    assert claim_token not in attempts.text


def test_terminal_retry_is_idempotent(client: TestClient):
    """Regression for the retry path Q4/Q5 will lean on: same payload twice."""

    ctx = register_submit_and_claim(client)
    first = complete_task(client, ctx["worker_headers"], ctx["task_id"], ctx["claim_token"], ctx["output"])
    assert first.status_code == 200, first.text
    retry = complete_task(client, ctx["worker_headers"], ctx["task_id"], ctx["claim_token"], ctx["output"])
    assert retry.status_code == 200, retry.text
    assert retry.json() == {"task_id": ctx["task_id"], "status": "completed"}
    assert client.get(f"/api/v1/tasks/{ctx['task_id']}", headers=ctx["sender_headers"]).json()["status"] == "completed"

    conflicting = complete_task(client, ctx["worker_headers"], ctx["task_id"], ctx["claim_token"], "DIFFERENT")
    assert conflicting.status_code == 409
    assert conflicting.json()["error"]["code"] == "conflicting_terminal"
    assert client.get(f"/api/v1/tasks/{ctx['task_id']}", headers=ctx["sender_headers"]).json()["output"] == "HELLO"


def test_flow_visible_to_both_participants(client: TestClient):
    """Dashboard regression: the sent/received lists agree with the detail view."""

    ctx = register_submit_and_claim(client, text="list me", output="LIST ME")
    completed = complete_task(client, ctx["worker_headers"], ctx["task_id"], ctx["claim_token"], ctx["output"])
    assert completed.status_code == 200, completed.text

    sent = client.get("/api/v1/tasks?direction=sent", headers=ctx["sender_headers"]).json()["items"]
    received = client.get("/api/v1/tasks?direction=received", headers=ctx["worker_headers"]).json()["items"]
    assert [item["task_id"] for item in sent] == [ctx["task_id"]]
    assert sent[0]["status"] == "completed", sent
    assert [item["task_id"] for item in received] == [ctx["task_id"]]
    assert received[0]["status"] == "completed"
    # The sender is not the recipient, so the sender's inbox stays empty.
    assert client.get("/api/v1/tasks?direction=received", headers=ctx["sender_headers"]).json()["items"] == []


def test_flow_requires_bearer_token(client: TestClient):
    """Auth boundary regression for the same flow."""

    ctx = register_submit_and_claim(client, text="authz")

    unauthenticated = client.post("/api/v1/tasks/claim", json={"wait_seconds": 0})
    assert unauthenticated.status_code == 401
    assert unauthenticated.json()["error"]["code"] == "missing_credentials"

    wrong = client.post(
        "/api/v1/tasks/claim",
        headers={"Authorization": "***"},
        json={"wait_seconds": 0},
    )
    assert wrong.status_code == 401
    assert wrong.json()["error"]["code"] == "invalid_credentials"

    stranger, stranger_headers = register(client, "stranger")
    forbidden = client.get(f"/api/v1/tasks/{ctx['task_id']}", headers=stranger_headers)
    assert forbidden.status_code == 404
    assert forbidden.json()["error"]["code"] == "not_found"
