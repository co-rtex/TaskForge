"""The M6A operator read surface: jobs.list, jobs.attempts, workers, queues."""

from __future__ import annotations

from collections.abc import Callable

import pytest

from conftest import (
    api_error,
    attempt_body,
    build_client,
    job_summary_body,
    json_response,
    queue_body,
    worker_body,
)
from taskforge import (
    Attempt,
    AttemptList,
    AttemptStatus,
    FailureClass,
    JobPage,
    JobStatus,
    JobSummary,
    NotFoundError,
    QueueList,
    RequestRejectedError,
    SessionStatus,
    TaskForgeClient,
    UnexpectedResponseError,
    WorkerPage,
)

# --- jobs.list -------------------------------------------------------------


def test_jobs_list_with_no_arguments_sends_no_query_parameters() -> None:
    """Omitting every filter lets the server apply its documented defaults."""
    harness = build_client(json_response(200, {"jobs": []}))
    page = harness.client.jobs.list()

    assert str(harness.recorder.last.url) == "http://api.test/v1/jobs"
    assert isinstance(page, JobPage)
    assert page.jobs == ()
    assert page.next_cursor is None


def test_jobs_list_passes_every_filter() -> None:
    harness = build_client(json_response(200, {"jobs": []}))
    harness.client.jobs.list(
        status="QUEUED", queue="default", limit=50, cursor="opaque-cursor"
    )

    url = harness.recorder.last.url
    assert url.params["status"] == "QUEUED"
    assert url.params["queue"] == "default"
    assert url.params["limit"] == "50"
    assert url.params["cursor"] == "opaque-cursor"


def test_jobs_list_parses_summaries_and_cursor() -> None:
    harness = build_client(
        json_response(
            200,
            {
                "jobs": [job_summary_body(status="RUNNING", priority=90)],
                "next_cursor": "next-1",
            },
        )
    )
    page = harness.client.jobs.list()

    assert page.next_cursor == "next-1"
    assert len(page.jobs) == 1
    job = page.jobs[0]
    assert isinstance(job, JobSummary)
    assert job.status is JobStatus.RUNNING
    assert job.priority == 90
    assert job.queue == "default"


def test_job_summary_has_no_payload_attribute() -> None:
    """The omission is the contract, not an accident of this fixture.

    A caller who expected ``summary.payload`` must get an AttributeError
    rather than silently reading a field the server never sent.
    """
    harness = build_client(json_response(200, {"jobs": [job_summary_body()]}))
    job = harness.client.jobs.list().jobs[0]

    assert not hasattr(job, "payload")
    assert "payload" not in job.raw


def test_jobs_list_rejects_an_unknown_status_from_the_server() -> None:
    """An unrecognized enum is reported, never guessed at."""
    harness = build_client(
        json_response(200, {"jobs": [job_summary_body(status="ASCENDED")]})
    )
    with pytest.raises(UnexpectedResponseError):
        harness.client.jobs.list()


def test_jobs_list_surfaces_an_invalid_cursor_as_request_rejected() -> None:
    harness = build_client(api_error(422, "invalid_cursor"))
    with pytest.raises(RequestRejectedError) as excinfo:
        harness.client.jobs.list(cursor="nope")
    assert excinfo.value.code == "invalid_cursor"


def test_jobs_list_surfaces_a_bad_limit_as_request_rejected() -> None:
    harness = build_client(api_error(422, "validation_failed"))
    with pytest.raises(RequestRejectedError) as excinfo:
        harness.client.jobs.list(limit=9999)
    assert excinfo.value.code == "validation_failed"


# --- jobs.attempts ---------------------------------------------------------


def test_attempts_requests_the_documented_path() -> None:
    harness = build_client(json_response(200, {"attempts": []}))
    result = harness.client.jobs.attempts("job-1")

    assert str(harness.recorder.last.url) == "http://api.test/v1/jobs/job-1/attempts"
    assert isinstance(result, AttemptList)
    assert result.attempts == ()


def test_attempts_percent_encodes_the_job_id() -> None:
    """An id with URL-special characters stays one path segment."""
    harness = build_client(json_response(200, {"attempts": []}))
    harness.client.jobs.attempts("a b?c=d")

    url = harness.recorder.last.url
    assert url.path == "/v1/jobs/a b?c=d/attempts"
    assert not url.params


def test_attempts_parses_a_succeeded_attempt() -> None:
    harness = build_client(json_response(200, {"attempts": [attempt_body()]}))
    attempt = harness.client.jobs.attempts("job-1").attempts[0]

    assert isinstance(attempt, Attempt)
    assert attempt.status is AttemptStatus.SUCCEEDED
    assert attempt.attempt_number == 1
    assert attempt.worker_name == "local-worker"
    assert attempt.failure_class is None
    assert attempt.retry_delay_ms is None


def test_attempts_parses_the_failure_and_retry_fields() -> None:
    harness = build_client(
        json_response(
            200,
            {
                "attempts": [
                    attempt_body(
                        status="FAILED",
                        failure_class="RETRYABLE",
                        error_code="handler_error",
                        error_message="the trusted handler reported an error",
                        retry_delay_ms=1500,
                        retry_at="2026-09-21T10:00:10Z",
                    )
                ]
            },
        )
    )
    attempt = harness.client.jobs.attempts("job-1").attempts[0]

    assert attempt.status is AttemptStatus.FAILED
    assert attempt.failure_class is FailureClass.RETRYABLE
    assert attempt.error_code == "handler_error"
    assert attempt.retry_delay_ms == 1500
    assert attempt.retry_at == "2026-09-21T10:00:10Z"


def test_attempts_parses_an_abandoned_attempt_with_no_start() -> None:
    """A crashed worker's attempt, and one canceled before it ever started.

    These are the rows the timeline exists to show, so they must parse --
    not merely the happy SUCCEEDED case.
    """
    harness = build_client(
        json_response(
            200,
            {
                "attempts": [
                    attempt_body(
                        attempt_number=1,
                        status="ABANDONED",
                        started_at=None,
                        timeout_at=None,
                        failure_class="ABANDONED",
                    ),
                    attempt_body(
                        attempt_number=2, status="TIMED_OUT", failure_class="TIMED_OUT"
                    ),
                ]
            },
        )
    )
    attempts = harness.client.jobs.attempts("job-1").attempts

    assert [a.status for a in attempts] == [
        AttemptStatus.ABANDONED,
        AttemptStatus.TIMED_OUT,
    ]
    assert attempts[0].started_at is None
    assert attempts[0].failure_class is FailureClass.ABANDONED


def test_attempts_publishes_no_session_or_lease_identifier() -> None:
    """The model has no attribute for one, and the fixture carries none."""
    harness = build_client(json_response(200, {"attempts": [attempt_body()]}))
    attempt = harness.client.jobs.attempts("job-1").attempts[0]

    for forbidden in (
        "worker_session_id",
        "session_id",
        "lease_id",
        "outcome_request_id",
    ):
        assert not hasattr(attempt, forbidden)
        assert forbidden not in attempt.raw


def test_attempts_of_an_unknown_job_is_not_found() -> None:
    harness = build_client(api_error(404, "not_found"))
    with pytest.raises(NotFoundError):
        harness.client.jobs.attempts("job-1")


# --- workers.list ----------------------------------------------------------


def test_workers_list_with_no_arguments_sends_no_query_parameters() -> None:
    harness = build_client(json_response(200, {"workers": []}))
    page = harness.client.workers.list()

    assert str(harness.recorder.last.url) == "http://api.test/v1/workers"
    assert isinstance(page, WorkerPage)
    assert page.workers == ()


def test_workers_list_passes_limit_and_cursor() -> None:
    harness = build_client(json_response(200, {"workers": []}))
    harness.client.workers.list(limit=10, cursor="opaque")

    url = harness.recorder.last.url
    assert url.params["limit"] == "10"
    assert url.params["cursor"] == "opaque"


def test_workers_list_parses_a_healthy_worker() -> None:
    harness = build_client(json_response(200, {"workers": [worker_body()]}))
    worker = harness.client.workers.list().workers[0]

    assert worker.status is SessionStatus.HEALTHY
    assert worker.name == "local-worker"
    assert worker.concurrency_limit == 4
    assert worker.capabilities == ("cpu",)
    assert worker.supported_job_types == ("demo.echo",)
    assert worker.heartbeat_age_seconds == 1.25
    assert worker.active_leases == 0
    assert worker.ended_at is None


@pytest.mark.parametrize("status", ["UNHEALTHY", "OFFLINE"])
def test_workers_list_parses_a_crashed_or_replaced_worker(status: str) -> None:
    """These are exactly the workers the endpoint exists to surface.

    A crashed process's session becomes UNHEALTHY and a replaced one
    OFFLINE. An SDK that could not represent them would make the endpoint's
    whole design unusable from Python.
    """
    harness = build_client(
        json_response(
            200,
            {
                "workers": [
                    worker_body(
                        status=status,
                        ended_at="2026-09-21T10:01:00Z",
                        heartbeat_age_seconds=93.0,
                        active_leases=1,
                    )
                ]
            },
        )
    )
    worker = harness.client.workers.list().workers[0]

    assert worker.status is SessionStatus(status)
    assert worker.ended_at == "2026-09-21T10:01:00Z"
    assert worker.heartbeat_age_seconds == 93.0
    assert worker.active_leases == 1


def test_worker_heartbeat_age_accepts_an_integer_from_the_server() -> None:
    """JSON does not distinguish 0 from 0.0; the model must take either."""
    harness = build_client(
        json_response(200, {"workers": [worker_body(heartbeat_age_seconds=0)]})
    )
    worker = harness.client.workers.list().workers[0]
    assert worker.heartbeat_age_seconds == 0.0
    assert isinstance(worker.heartbeat_age_seconds, float)


def test_workers_list_publishes_no_session_identifier() -> None:
    harness = build_client(json_response(200, {"workers": [worker_body()]}))
    worker = harness.client.workers.list().workers[0]

    for forbidden in ("session_id", "worker_session_id", "lease_id", "scope"):
        assert not hasattr(worker, forbidden)
        assert forbidden not in worker.raw


def test_workers_list_surfaces_an_invalid_cursor() -> None:
    harness = build_client(api_error(422, "invalid_cursor"))
    with pytest.raises(RequestRejectedError) as excinfo:
        harness.client.workers.list(cursor="nope")
    assert excinfo.value.code == "invalid_cursor"


# --- queues.list -----------------------------------------------------------


def test_queues_list_is_unpaginated_and_sends_no_parameters() -> None:
    harness = build_client(json_response(200, {"queues": []}))
    result = harness.client.queues.list()

    assert str(harness.recorder.last.url) == "http://api.test/v1/queues"
    assert isinstance(result, QueueList)
    assert result.queues == ()


def test_queues_list_parses_depth_keyed_by_job_status() -> None:
    harness = build_client(json_response(200, {"queues": [queue_body()]}))
    queue = harness.client.queues.list().queues[0]

    assert queue.name == "default"
    assert queue.worker_group == "default"
    assert queue.max_concurrency == 100
    assert queue.depth[JobStatus.QUEUED] == 2
    assert queue.depth[JobStatus.RUNNING] == 1
    assert queue.depth[JobStatus.PENDING] == 0


def test_queue_depth_carries_every_non_terminal_status_and_no_terminal_one() -> None:
    harness = build_client(json_response(200, {"queues": [queue_body()]}))
    depth = harness.client.queues.list().queues[0].depth

    for status in (
        JobStatus.PENDING,
        JobStatus.QUEUED,
        JobStatus.LEASED,
        JobStatus.RUNNING,
        JobStatus.RETRY_WAIT,
        JobStatus.CANCEL_REQUESTED,
    ):
        assert status in depth, f"{status} must always be present, even at zero"
    for status in (JobStatus.SUCCEEDED, JobStatus.CANCELED, JobStatus.DEAD_LETTERED):
        assert status not in depth, f"{status} is terminal and is not depth"


def test_queue_rejects_a_depth_key_that_is_not_a_job_status() -> None:
    harness = build_client(
        json_response(200, {"queues": [queue_body(depth={"ASCENDED": 1})]})
    )
    with pytest.raises(UnexpectedResponseError):
        harness.client.queues.list()


def test_queue_publishes_no_cross_scope_in_flight_figure() -> None:
    """max_concurrency is queue-wide; depth is this scope's. No blend of the
    two is reported, because that would disclose another scope's load."""
    harness = build_client(json_response(200, {"queues": [queue_body()]}))
    queue = harness.client.queues.list().queues[0]

    for forbidden in ("in_flight", "active", "utilization", "available", "total"):
        assert not hasattr(queue, forbidden)
        assert forbidden not in queue.raw


# --- shared contract -------------------------------------------------------

#: Every M6A read, as a callable, so the shared-contract tests below cover all
#: four rather than whichever one was written first. One empty body satisfies
#: all four models at once, since each reads only its own key.
_EVERY_READ: dict[str, Callable[[TaskForgeClient], object]] = {
    "jobs.list": lambda c: c.jobs.list(),
    "jobs.attempts": lambda c: c.jobs.attempts("job-1"),
    "workers.list": lambda c: c.workers.list(),
    "queues.list": lambda c: c.queues.list(),
}

_EMPTY_BODY: dict[str, list[object]] = {
    "jobs": [],
    "attempts": [],
    "workers": [],
    "queues": [],
}


@pytest.mark.parametrize("name", sorted(_EVERY_READ))
def test_every_read_presents_the_api_key(name: str) -> None:
    """These are public /v1 routes; the credential goes on all of them."""
    harness = build_client(json_response(200, _EMPTY_BODY))
    _EVERY_READ[name](harness.client)
    assert harness.recorder.last.headers["authorization"] == "Bearer tfk_test.secret"


@pytest.mark.parametrize("name", sorted(_EVERY_READ))
def test_no_read_sends_an_idempotency_key(name: str) -> None:
    """A read has no idempotency identity; sending one would be meaningless."""
    harness = build_client(json_response(200, _EMPTY_BODY))
    _EVERY_READ[name](harness.client)
    assert "idempotency-key" not in harness.recorder.last.headers


@pytest.mark.parametrize("name", sorted(_EVERY_READ))
def test_every_read_uses_get(name: str) -> None:
    """A read must never be sent as a method that could be taken as a write."""
    harness = build_client(json_response(200, _EMPTY_BODY))
    _EVERY_READ[name](harness.client)
    assert harness.recorder.last.method == "GET"
