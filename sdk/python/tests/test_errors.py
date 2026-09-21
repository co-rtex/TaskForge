"""The exception taxonomy, and its correspondence to the CLI's exit codes.

Every class is asserted through a real public method against a mock
transport -- the product surface -- not by inspecting the mapping table.
Every multi-membership class has one test per member, so the grouping is
proved deliberate rather than "whichever codes happened to be handled".
"""

from __future__ import annotations

import httpx
import pytest

from conftest import api_error, build_client, job_body, json_response
from taskforge import (
    APIError,
    ConfigurationError,
    ConflictError,
    InternalServerError,
    NotFoundError,
    RequestRejectedError,
    ServiceUnavailableError,
    TaskForgeError,
    TransportError,
    UnauthorizedError,
    UnexpectedResponseError,
)
from taskforge.errors import API_ERROR_EXCEPTIONS

# --- the contract, pinned -------------------------------------------------

#: The ten exit codes internal/cli/exitcode.go documents, against the class
#: each corresponds to. 0 has no exception: success is a normal return.
CLI_EXIT_CODES: dict[type[TaskForgeError], int] = {
    ConfigurationError: 1,
    RequestRejectedError: 2,
    UnauthorizedError: 3,
    NotFoundError: 4,
    ConflictError: 5,
    InternalServerError: 6,
    ServiceUnavailableError: 7,
    TransportError: 8,
    UnexpectedResponseError: 9,
}


def test_exit_codes_match_the_cli_contract() -> None:
    """These numbers are taskforge-cli's stable, scripted contract.

    The two tables are hand-maintained copies of one mapping -- Python
    cannot import a Go constant -- and this test is what stops them
    drifting. If internal/cli/exitcode.go changes, this fails.
    """
    for exception_class, expected in CLI_EXIT_CODES.items():
        assert exception_class.exit_code == expected, exception_class.__name__


def test_exit_codes_are_distinct_and_never_zero() -> None:
    codes = [c.exit_code for c in CLI_EXIT_CODES]
    assert len(set(codes)) == len(codes), "two classes share an exit code"
    assert 0 not in codes, "0 means success and is never an exception"


def test_every_exception_is_a_taskforge_error() -> None:
    for exception_class in CLI_EXIT_CODES:
        assert issubclass(exception_class, TaskForgeError)


def test_api_error_map_covers_exactly_the_reachable_set() -> None:
    """Exhaustive for the Error.code values a route this SDK calls can
    actually return -- mirroring internal/cli/exitcode.go's apiErrorExitCodes
    entry for entry, so it cannot silently grow or shrink."""
    assert {
        "malformed_json": RequestRejectedError,
        "payload_too_large": RequestRejectedError,
        "validation_failed": RequestRejectedError,
        "invalid_cursor": RequestRejectedError,
        "unauthorized": UnauthorizedError,
        "not_found": NotFoundError,
        "idempotency_conflict": ConflictError,
        "job_not_cancelable": ConflictError,
        "job_not_dead_lettered": ConflictError,
        "internal_error": InternalServerError,
        "service_unavailable": ServiceUnavailableError,
    } == API_ERROR_EXCEPTIONS
    assert len(API_ERROR_EXCEPTIONS) == 11


# --- one test per class, through the public surface -----------------------


@pytest.mark.parametrize(
    ("status", "code"),
    [
        (400, "malformed_json"),
        (413, "payload_too_large"),
        (422, "validation_failed"),
        (422, "invalid_cursor"),
    ],
)
def test_request_rejected(status: int, code: str) -> None:
    harness = build_client(api_error(status, code))
    with pytest.raises(RequestRejectedError) as raised:
        harness.client.jobs.get("job-1")
    assert raised.value.code == code
    assert raised.value.exit_code == 2


def test_unauthorized() -> None:
    harness = build_client(api_error(401, "unauthorized"))
    with pytest.raises(UnauthorizedError) as raised:
        harness.client.jobs.get("job-1")
    assert raised.value.exit_code == 3


def test_not_found() -> None:
    harness = build_client(api_error(404, "not_found"))
    with pytest.raises(NotFoundError) as raised:
        harness.client.jobs.result("job-1")
    assert raised.value.exit_code == 4


@pytest.mark.parametrize(
    "code", ["idempotency_conflict", "job_not_cancelable", "job_not_dead_lettered"]
)
def test_conflict(code: str) -> None:
    harness = build_client(api_error(409, code))
    with pytest.raises(ConflictError) as raised:
        harness.client.jobs.cancel("job-1")
    assert raised.value.code == code, "branch on .code, not on the class"
    assert raised.value.exit_code == 5


def test_internal_error() -> None:
    harness = build_client(api_error(500, "internal_error"))
    with pytest.raises(InternalServerError) as raised:
        harness.client.jobs.get("job-1")
    assert raised.value.exit_code == 6
    assert raised.value.request_id == "req-1"


def test_service_unavailable() -> None:
    harness = build_client(api_error(503, "service_unavailable"))
    with pytest.raises(ServiceUnavailableError) as raised:
        harness.client.jobs.get("job-1")
    assert raised.value.exit_code == 7


def test_transport_error_on_connection_failure() -> None:
    def refuse(request: httpx.Request) -> httpx.Response:
        raise httpx.ConnectError("connection refused", request=request)

    harness = build_client(refuse)
    with pytest.raises(TransportError) as raised:
        harness.client.jobs.get("job-1")
    assert raised.value.exit_code == 8


def test_client_side_timeout_is_transport_not_service_unavailable() -> None:
    """The 8-versus-7 distinction internal/cli/exitcode.go keeps explicit.

    A client-side timeout never reached a durable outcome; a 503 means the
    API was reached and reported that ITS OWN deadline elapsed. Collapsing
    them sends an operator looking in the wrong place.
    """

    def timeout(request: httpx.Request) -> httpx.Response:
        raise httpx.ReadTimeout("timed out", request=request)

    harness = build_client(timeout)
    with pytest.raises(TransportError) as raised:
        harness.client.jobs.get("job-1")
    assert not isinstance(raised.value, ServiceUnavailableError)
    assert raised.value.exit_code == 8


@pytest.mark.parametrize(
    ("handler", "label"),
    [
        (api_error(409, "state_conflict"), "code outside the reachable set"),
        (json_response(418, {"nonsense": True}), "status this route never returns"),
    ],
)
def test_unexpected_response(handler: object, label: str) -> None:
    harness = build_client(handler)  # type: ignore[arg-type]
    with pytest.raises(UnexpectedResponseError) as raised:
        harness.client.jobs.get("job-1")
    assert raised.value.exit_code == 9, label


def test_unexpected_response_on_a_non_json_body() -> None:
    def html(request: httpx.Request) -> httpx.Response:
        return httpx.Response(502, text="<html>gateway</html>")

    harness = build_client(html)
    with pytest.raises(UnexpectedResponseError) as raised:
        harness.client.jobs.get("job-1")
    assert raised.value.exit_code == 9
    assert "gateway" in str(raised.value.raw)


def test_unexpected_response_on_a_success_body_that_is_not_json() -> None:
    def broken(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, text="not json")

    harness = build_client(broken)
    with pytest.raises(UnexpectedResponseError):
        harness.client.jobs.get("job-1")


def test_error_carries_the_servers_envelope_unmodified() -> None:
    harness = build_client(
        json_response(
            422,
            {
                "error": {
                    "code": "validation_failed",
                    "message": "the request was rejected by validation",
                    "details": [{"field": "priority", "message": "out of range"}],
                    "request_id": "req-42",
                    "a_field_this_sdk_does_not_know": "preserved",
                }
            },
        )
    )
    with pytest.raises(APIError) as raised:
        harness.client.jobs.get("job-1")

    error = raised.value
    assert error.code == "validation_failed"
    assert error.message == "the request was rejected by validation"
    assert error.http_status == 422
    assert error.request_id == "req-42"
    assert error.details == [{"field": "priority", "message": "out of range"}]
    assert error.raw["error"]["a_field_this_sdk_does_not_know"] == "preserved"


def test_catching_the_base_class_catches_everything() -> None:
    harness = build_client(api_error(404, "not_found"))
    with pytest.raises(TaskForgeError):
        harness.client.jobs.get("job-1")

    ok = build_client(json_response(200, job_body()))
    with pytest.raises(TaskForgeError):
        ok.client.jobs.submit(
            queue="q", job_type="t", payload={}, idempotency_key="k" * 256
        )
