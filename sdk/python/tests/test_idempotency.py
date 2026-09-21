"""The acceptance criterion, made checkable.

docs/ROADMAP.md's M5E criterion is "the SDK places an idempotency key in the
canonical header". These tests are what hold the implementation to it.
"""

from __future__ import annotations

import uuid
from urllib.parse import parse_qs, urlsplit

import pytest

from conftest import build_client, job_body, json_response, key_body
from taskforge import ConfigurationError

HEADER = "Idempotency-Key"

#: Exactly the routes api/openapi.yaml marks `Idempotency-Key: required: true`.
IDEMPOTENT_CALLS = ["submit", "retry", "replay"]


def _invoke(client: object, name: str, key: str | None = None) -> None:
    c = client
    if name == "submit":
        c.jobs.submit(  # type: ignore[attr-defined]
            queue="default",
            job_type="demo.echo",
            payload={"message": "hello"},
            idempotency_key=key,
        )
    elif name == "retry":
        c.jobs.retry("job-1", idempotency_key=key)  # type: ignore[attr-defined]
    elif name == "replay":
        c.dlq.replay("job-1", idempotency_key=key)  # type: ignore[attr-defined]
    else:  # pragma: no cover - guarded by the parametrize list
        raise AssertionError(name)


def _body_for(name: str) -> dict[str, object]:
    if name == "submit":
        return job_body()
    return {
        "original_job_id": "job-1",
        "replacement": job_body(),
        "replayed": False,
    }


@pytest.mark.parametrize("call", IDEMPOTENT_CALLS)
def test_generated_key_is_sent_in_the_canonical_header(call: str) -> None:
    harness = build_client(json_response(201, _body_for(call)))
    _invoke(harness.client, call)

    assert HEADER in harness.recorder.last.headers


@pytest.mark.parametrize("call", IDEMPOTENT_CALLS)
def test_generated_key_is_a_uuid4_matching_the_cli_default(call: str) -> None:
    """taskforge-cli generates uuid.NewString(); this must be indistinguishable."""
    harness = build_client(json_response(201, _body_for(call)))
    _invoke(harness.client, call)

    sent = harness.recorder.last.headers[HEADER]
    parsed = uuid.UUID(sent)
    assert parsed.version == 4
    assert str(parsed) == sent, "canonical lowercase hyphenated form"
    assert 1 <= len(sent) <= 255, "within the header's documented length"


@pytest.mark.parametrize("call", IDEMPOTENT_CALLS)
def test_caller_supplied_key_is_sent_verbatim(call: str) -> None:
    """Including characters that are awkward in a URL but legal in a header."""
    harness = build_client(json_response(200, _body_for(call)))
    _invoke(harness.client, call, key="my-own key/=?&#+%20")

    assert harness.recorder.last.headers[HEADER] == "my-own key/=?&#+%20"


@pytest.mark.parametrize("call", IDEMPOTENT_CALLS)
@pytest.mark.parametrize(
    "bad_key",
    [
        pytest.param("caf\u00e9-key", id="non-ascii"),
        pytest.param("k" * 256, id="too-long"),
        pytest.param("has\nnewline", id="control-character"),
    ],
)
def test_unusable_caller_key_is_a_configuration_error(call: str, bad_key: str) -> None:
    """The SDK rejects it itself, before any request, rather than letting a
    raw UnicodeEncodeError escape a caller's ``except TaskForgeError``."""
    harness = build_client(json_response(201, _body_for(call)))

    with pytest.raises(ConfigurationError):
        _invoke(harness.client, call, key=bad_key)

    assert harness.recorder.count == 0, "no request may be sent"


@pytest.mark.parametrize("call", IDEMPOTENT_CALLS)
def test_generated_keys_differ_between_calls(call: str) -> None:
    harness = build_client(json_response(201, _body_for(call)))
    _invoke(harness.client, call)
    _invoke(harness.client, call)

    first = harness.recorder.requests[0].headers[HEADER]
    second = harness.recorder.requests[1].headers[HEADER]
    assert first != second


@pytest.mark.parametrize("call", IDEMPOTENT_CALLS)
def test_key_is_never_in_the_body_or_the_query_string(call: str) -> None:
    """PROJECT_SPEC.md section 4: the SDK places it in THAT canonical header."""
    harness = build_client(json_response(201, _body_for(call)))
    _invoke(harness.client, call, key="sentinel-key-value")

    request = harness.recorder.last
    body = request.content.decode() if request.content else ""
    assert "sentinel-key-value" not in body
    assert "idempotency" not in body.lower()

    query = parse_qs(urlsplit(str(request.url)).query)
    assert query == {}


def test_no_other_method_sends_an_idempotency_key() -> None:
    """Only the three routes that require it may send it."""
    harness = build_client(json_response(200, job_body()))
    client = harness.client

    client.jobs.get("job-1")
    client.jobs.result("job-1")

    harness_cancel = build_client(
        json_response(
            200,
            {
                "job_id": "job-1",
                "status": "CANCELED",
                "cancel_requested_at": "2026-09-21T10:00:00Z",
                "already_requested": False,
            },
        )
    )
    harness_cancel.client.jobs.cancel("job-1")

    harness_dlq = build_client(json_response(200, {"entries": []}))
    harness_dlq.client.dlq.list()

    harness_keys = build_client(json_response(201, key_body()))
    harness_keys.client.api_keys.create(scope="local-dev", name="k")

    harness_keys_list = build_client(json_response(200, {"keys": []}))
    harness_keys_list.client.api_keys.list()
    harness_keys_list.client.worker_keys.list()

    harness_revoke = build_client(
        json_response(200, key_body(revoked_at=None, already_revoked=False))
    )
    harness_revoke.client.api_keys.revoke("key-1")

    for h in (
        harness,
        harness_cancel,
        harness_dlq,
        harness_keys,
        harness_keys_list,
        harness_revoke,
    ):
        for request in h.recorder.requests:
            assert HEADER not in request.headers, f"{request.method} {request.url}"


def test_cancel_sends_no_idempotency_key_specifically() -> None:
    """Cancellation's identity is scope plus job id and nothing else."""
    harness = build_client(
        json_response(
            200,
            {
                "job_id": "job-1",
                "status": "CANCEL_REQUESTED",
                "cancel_requested_at": "2026-09-21T10:00:00Z",
                "already_requested": False,
            },
        )
    )
    harness.client.jobs.cancel("job-1")

    assert HEADER not in harness.recorder.last.headers
