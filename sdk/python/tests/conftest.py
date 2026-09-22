"""Shared fixtures.

Every test drives the real :class:`~taskforge.TaskForgeClient` through an
``httpx.MockTransport``. That is deliberate and matches how
``internal/cli``'s own exit-code tests run through ``Run()`` against a real
``httptest.Server``: the product surface is what is under test, never the
mapping tables in isolation.
"""

from __future__ import annotations

from collections.abc import Callable, Iterator
from dataclasses import dataclass, field
from typing import Any

import httpx
import pytest

from taskforge import TaskForgeClient

Handler = Callable[[httpx.Request], httpx.Response]


@dataclass
class Recorder:
    """Captures every request the client actually sent."""

    requests: list[httpx.Request] = field(default_factory=list)

    @property
    def last(self) -> httpx.Request:
        assert self.requests, "no request was sent"
        return self.requests[-1]

    @property
    def count(self) -> int:
        return len(self.requests)


@dataclass
class Harness:
    client: TaskForgeClient
    recorder: Recorder


def build_client(
    handler: Handler,
    *,
    api_key: str | None = "tfk_test.secret",
    base_url: str | None = "http://api.test",
) -> Harness:
    """Build a client whose transport is a recording mock."""
    recorder = Recorder()

    def recording(request: httpx.Request) -> httpx.Response:
        recorder.requests.append(request)
        return handler(request)

    http_client = httpx.Client(transport=httpx.MockTransport(recording))
    client = TaskForgeClient(
        base_url=base_url, api_key=api_key, http_client=http_client
    )
    return Harness(client=client, recorder=recorder)


def json_response(status: int, payload: Any) -> Handler:
    """A handler that always answers with one JSON body."""

    def handler(_: httpx.Request) -> httpx.Response:
        return httpx.Response(status, json=payload)

    return handler


def api_error(status: int, code: str, message: str = "boom") -> Handler:
    """A handler answering the API's standard error envelope."""
    return json_response(
        status, {"error": {"code": code, "message": message, "request_id": "req-1"}}
    )


# --- sample response bodies, field-for-field per api/openapi.yaml ---------


def job_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "id": "11111111-1111-4111-8111-111111111111",
        "queue": "default",
        "job_type": "demo.echo",
        "payload": {"message": "hello"},
        "status": "QUEUED",
        "priority": 50,
        "max_attempts": 3,
        "timeout_seconds": 300,
        "required_capabilities": [],
        "scheduled_at": None,
        "available_at": "2026-09-21T10:00:00Z",
        "cancel_requested_at": None,
        "replayed_from_job_id": None,
        "created_at": "2026-09-21T10:00:00Z",
        "updated_at": "2026-09-21T10:00:00Z",
    }
    body.update(overrides)
    return body


def dlq_entry_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "id": "22222222-2222-4222-8222-222222222222",
        "job_id": "11111111-1111-4111-8111-111111111111",
        "queue": "default",
        "job_type": "demo.echo",
        "priority": 50,
        "max_attempts": 3,
        "reason": "ATTEMPTS_EXHAUSTED",
        "created_at": "2026-09-21T10:00:00Z",
        "replay_count": 0,
    }
    body.update(overrides)
    return body


def job_summary_body(**overrides: Any) -> dict[str, Any]:
    """A JobSummary: every Job field EXCEPT payload."""
    body: dict[str, Any] = {
        "id": "11111111-1111-4111-8111-111111111111",
        "queue": "default",
        "job_type": "demo.echo",
        "status": "QUEUED",
        "priority": 50,
        "max_attempts": 3,
        "timeout_seconds": 300,
        "required_capabilities": [],
        "scheduled_at": None,
        "available_at": "2026-09-21T10:00:00Z",
        "cancel_requested_at": None,
        "replayed_from_job_id": None,
        "created_at": "2026-09-21T10:00:00Z",
        "updated_at": "2026-09-21T10:00:00Z",
    }
    body.update(overrides)
    return body


def attempt_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "id": "44444444-4444-4444-8444-444444444444",
        "attempt_number": 1,
        "status": "SUCCEEDED",
        "worker_id": "55555555-5555-4555-8555-555555555555",
        "worker_name": "local-worker",
        "created_at": "2026-09-21T10:00:00Z",
        "started_at": "2026-09-21T10:00:01Z",
        "finished_at": "2026-09-21T10:00:02Z",
        "timeout_at": "2026-09-21T10:05:01Z",
        "failure_class": None,
        "error_code": None,
        "error_message": None,
        "retry_delay_ms": None,
        "retry_at": None,
    }
    body.update(overrides)
    return body


def worker_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "id": "55555555-5555-4555-8555-555555555555",
        "name": "local-worker",
        "status": "HEALTHY",
        "worker_group": "default",
        "hostname": "local-worker.local",
        "concurrency_limit": 4,
        "capabilities": ["cpu"],
        "supported_job_types": ["demo.echo"],
        "registered_at": "2026-09-21T10:00:00Z",
        "last_heartbeat_at": "2026-09-21T10:00:05Z",
        "ended_at": None,
        "heartbeat_age_seconds": 1.25,
        "active_leases": 0,
    }
    body.update(overrides)
    return body


def queue_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "name": "default",
        "worker_group": "default",
        "max_concurrency": 100,
        "depth": {
            "PENDING": 0,
            "QUEUED": 2,
            "LEASED": 0,
            "RUNNING": 1,
            "RETRY_WAIT": 0,
            "CANCEL_REQUESTED": 0,
        },
    }
    body.update(overrides)
    return body


def key_body(**overrides: Any) -> dict[str, Any]:
    body: dict[str, Any] = {
        "id": "33333333-3333-4333-8333-333333333333",
        "scope": "local-dev",
        "name": "my-laptop",
        "prefix": "tfk_abc",
        "created_at": "2026-09-21T10:00:00Z",
        "key": "tfk_abc.secret",
    }
    body.update(overrides)
    return body


@pytest.fixture
def clean_env(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Remove every TaskForge variable so a test starts from nothing."""
    for name in (
        "TASKFORGE_SDK_API_URL",
        "TASKFORGE_SDK_API_KEY",
        "TASKFORGE_CLI_API_URL",
        "TASKFORGE_CLI_API_KEY",
        "TASKFORGE_API_ADDR",
        "TASKFORGE_WORKER_API_URL",
    ):
        monkeypatch.delenv(name, raising=False)
    yield
