"""The jobs namespace: routes, methods, bodies, and success statuses."""

from __future__ import annotations

import json

import httpx
import pytest

from conftest import build_client, job_body, json_response
from taskforge import (
    Cancellation,
    Job,
    JobStatus,
    Replay,
    UnexpectedResponseError,
)


def test_submit_builds_the_documented_request() -> None:
    harness = build_client(json_response(201, job_body()))
    job = harness.client.jobs.submit(
        queue="default",
        job_type="demo.echo",
        payload={"message": "hello"},
        priority=70,
        max_attempts=5,
        timeout_seconds=45,
        required_capabilities=["cpu", "gpu"],
    )

    sent = harness.recorder.last
    assert sent.method == "POST"
    assert str(sent.url) == "http://api.test/v1/jobs"
    assert json.loads(sent.content) == {
        "queue": "default",
        "job_type": "demo.echo",
        "payload": {"message": "hello"},
        "priority": 70,
        "max_attempts": 5,
        "timeout_seconds": 45,
        "required_capabilities": ["cpu", "gpu"],
    }
    assert isinstance(job, Job)
    assert job.status is JobStatus.QUEUED


def test_submit_applies_the_documented_defaults() -> None:
    harness = build_client(json_response(201, job_body()))
    harness.client.jobs.submit(queue="q", job_type="t", payload={})

    body = json.loads(harness.recorder.last.content)
    assert body["priority"] == 50
    assert body["max_attempts"] == 3
    assert body["timeout_seconds"] == 300
    assert body["required_capabilities"] == []


def test_submit_omits_scheduled_at_unless_given() -> None:
    harness = build_client(json_response(201, job_body()))
    harness.client.jobs.submit(queue="q", job_type="t", payload={})
    assert "scheduled_at" not in json.loads(harness.recorder.last.content)

    harness.client.jobs.submit(
        queue="q", job_type="t", payload={}, scheduled_at="2026-10-01T00:00:00Z"
    )
    body = json.loads(harness.recorder.last.content)
    assert body["scheduled_at"] == "2026-10-01T00:00:00Z"


@pytest.mark.parametrize("status", [200, 201])
def test_submit_accepts_both_success_statuses(status: int) -> None:
    """200 is an idempotent replay of an earlier identical submission."""
    harness = build_client(json_response(status, job_body()))
    assert harness.client.jobs.submit(queue="q", job_type="t", payload={}).id


def test_get_and_result_routes() -> None:
    harness = build_client(json_response(200, job_body()))
    harness.client.jobs.get("abc")
    assert str(harness.recorder.last.url) == "http://api.test/v1/jobs/abc"

    payload_harness = build_client(json_response(200, {"echo": "hello"}))
    assert payload_harness.client.jobs.result("abc") == {"echo": "hello"}
    assert str(payload_harness.recorder.last.url) == (
        "http://api.test/v1/jobs/abc/result"
    )


def test_result_returns_any_json_value() -> None:
    """api/openapi.yaml: the body is whatever JSON the job_type produces."""
    for payload in ({"a": 1}, [1, 2, 3], "text", 42, True):
        harness = build_client(json_response(200, payload))
        assert harness.client.jobs.result("abc") == payload


def test_result_of_literal_null_is_not_treated_as_a_decode_failure() -> None:
    """A handler may legitimately produce `null`.

    Built with an explicit body rather than `json=None`, which httpx reads
    as "no body at all" -- the two are genuinely different responses.
    """

    def null_body(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            200, content=b"null", headers={"content-type": "application/json"}
        )

    harness = build_client(null_body)
    assert harness.client.jobs.result("abc") is None


def test_an_empty_success_body_is_an_unexpected_response() -> None:
    """Distinct from literal `null`: nothing at all is not valid JSON."""

    def empty(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"")

    harness = build_client(empty)
    with pytest.raises(UnexpectedResponseError):
        harness.client.jobs.result("abc")


def test_cancel() -> None:
    harness = build_client(
        json_response(
            200,
            {
                "job_id": "abc",
                "status": "CANCEL_REQUESTED",
                "cancel_requested_at": "2026-09-21T10:00:00Z",
                "already_requested": True,
            },
        )
    )
    cancellation = harness.client.jobs.cancel("abc")

    assert harness.recorder.last.method == "POST"
    assert str(harness.recorder.last.url) == "http://api.test/v1/jobs/abc/cancel"
    assert not harness.recorder.last.content, "the route takes no body"
    assert isinstance(cancellation, Cancellation)
    assert cancellation.already_requested is True


def test_retry_returns_a_replay() -> None:
    harness = build_client(
        json_response(
            201,
            {
                "original_job_id": "dead-1",
                "replacement": job_body(replayed_from_job_id="dead-1"),
                "replayed": False,
            },
        )
    )
    replay = harness.client.jobs.retry("dead-1")

    assert str(harness.recorder.last.url) == "http://api.test/v1/jobs/dead-1/retry"
    assert isinstance(replay, Replay)
    assert replay.original_job_id == "dead-1"
    assert replay.replacement.replayed_from_job_id == "dead-1"
    assert replay.already_replayed is False


@pytest.mark.parametrize(
    "job_id",
    ["with space", "with?query=1", "with/slash", "with#hash", "with%percent"],
)
def test_ids_special_to_a_url_stay_one_path_segment(job_id: str) -> None:
    """An id must never silently become a different route or a query string."""
    harness = build_client(json_response(200, job_body()))
    harness.client.jobs.get(job_id)

    # Assert on the RAW url: httpx's .path property percent-decodes, which
    # would hide the very escaping under test.
    raw = str(harness.recorder.last.url)
    assert harness.recorder.last.url.query == b""
    assert raw.startswith("http://api.test/v1/jobs/")
    tail = raw[len("http://api.test/v1/jobs/") :]
    assert "/" not in tail, f"{job_id!r} escaped its segment: {raw}"
    assert "?" not in tail and "#" not in tail


def test_submit_sends_accept_and_content_type() -> None:
    harness = build_client(json_response(201, job_body()))
    harness.client.jobs.submit(queue="q", job_type="t", payload={})

    headers = harness.recorder.last.headers
    assert headers["accept"] == "application/json"
    assert headers["content-type"] == "application/json"


def test_the_operator_read_surface_is_wired() -> None:
    """M6A implemented the four routes this test used to assert were absent.

    Through M5E this asserted that ``jobs.list``, ``client.workers`` and
    ``client.queues`` did NOT exist, because a method with no route to call
    would be fabricated functionality (PROJECT_SPEC.md section 5). The routes
    exist now, so the assertion is inverted rather than deleted: the
    namespaces must be reachable from the client, which is what stops a
    future refactor from silently dropping one.

    What each method actually does is covered in tests/test_reads.py.
    """
    harness = build_client(json_response(200, job_body()))
    client = harness.client

    assert callable(client.jobs.list)
    assert callable(client.jobs.attempts)
    assert callable(client.workers.list)
    assert callable(client.queues.list)


def test_client_is_a_context_manager() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json=job_body())

    harness = build_client(handler)
    with harness.client as client:
        assert client.jobs.get("abc").id
