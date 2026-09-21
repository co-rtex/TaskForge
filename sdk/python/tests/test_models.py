"""Parsing: tolerant of unknown keys, strict about missing required ones."""

from __future__ import annotations

import pytest

from conftest import build_client, dlq_entry_body, job_body, json_response
from taskforge import Job, JobStatus, UnexpectedResponseError


def test_unknown_fields_are_preserved_in_raw() -> None:
    """A field a newer server adds must not be silently dropped.

    This is the property taskforge-cli gets for free by printing the
    server's bytes. A typed model has to keep it deliberately.
    """
    harness = build_client(
        json_response(200, job_body(a_future_field="preserved", another={"nested": 1}))
    )
    job = harness.client.jobs.get("abc")

    assert job.raw["a_future_field"] == "preserved"
    assert job.raw["another"] == {"nested": 1}
    assert job.id == "11111111-1111-4111-8111-111111111111"


@pytest.mark.parametrize(
    "missing",
    [
        "id",
        "queue",
        "job_type",
        "payload",
        "status",
        "priority",
        "max_attempts",
        "timeout_seconds",
        "required_capabilities",
        "scheduled_at",
        "available_at",
        "cancel_requested_at",
        "replayed_from_job_id",
        "created_at",
        "updated_at",
    ],
)
def test_a_missing_required_field_is_an_unexpected_response(missing: str) -> None:
    body = job_body()
    del body[missing]
    harness = build_client(json_response(200, body))

    with pytest.raises(UnexpectedResponseError) as raised:
        harness.client.jobs.get("abc")
    assert missing in str(raised.value)
    assert raised.value.exit_code == 9


def test_an_unknown_job_status_is_an_unexpected_response() -> None:
    """A server that adds a tenth state breaks an older SDK's parse.

    Deliberate: reporting version skew beats handing a caller a state their
    code has never heard of. Documented in README.md and models.py.
    """
    harness = build_client(json_response(200, job_body(status="HIBERNATING")))

    with pytest.raises(UnexpectedResponseError) as raised:
        harness.client.jobs.get("abc")
    assert "HIBERNATING" in str(raised.value)


def test_every_documented_job_status_parses() -> None:
    for status in JobStatus:
        harness = build_client(json_response(200, job_body(status=status.value)))
        assert harness.client.jobs.get("abc").status is status


def test_status_is_an_enum_not_a_bare_string() -> None:
    """AGENTS.md section 5: domain types are typed, not string."""
    harness = build_client(json_response(200, job_body(status="RUNNING")))
    job = harness.client.jobs.get("abc")

    assert job.status is JobStatus.RUNNING
    assert isinstance(job.status, JobStatus)
    # It is still str-comparable, so existing string handling keeps working.
    assert job.status == "RUNNING"


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("priority", "50"),
        ("priority", True),
        ("max_attempts", 1.5),
        ("id", 12345),
        ("required_capabilities", "cpu"),
        ("payload", "not-an-object"),
    ],
)
def test_a_wrongly_typed_field_is_an_unexpected_response(
    field: str, value: object
) -> None:
    harness = build_client(json_response(200, job_body(**{field: value})))
    with pytest.raises(UnexpectedResponseError):
        harness.client.jobs.get("abc")


def test_a_non_object_response_is_an_unexpected_response() -> None:
    harness = build_client(json_response(200, ["not", "a", "job"]))
    with pytest.raises(UnexpectedResponseError):
        harness.client.jobs.get("abc")


def test_nullable_job_fields_round_trip() -> None:
    harness = build_client(
        json_response(
            200,
            job_body(
                scheduled_at="2026-10-01T00:00:00Z",
                cancel_requested_at="2026-09-21T10:30:00Z",
                replayed_from_job_id="00000000-0000-4000-8000-000000000000",
            ),
        )
    )
    job = harness.client.jobs.get("abc")

    assert job.scheduled_at == "2026-10-01T00:00:00Z"
    assert job.cancel_requested_at == "2026-09-21T10:30:00Z"
    assert job.replayed_from_job_id == "00000000-0000-4000-8000-000000000000"


def test_models_are_frozen() -> None:
    harness = build_client(json_response(200, job_body()))
    job = harness.client.jobs.get("abc")

    with pytest.raises(Exception):  # noqa: B017 - FrozenInstanceError
        job.status = JobStatus.SUCCEEDED  # type: ignore[misc]


def test_dlq_entry_preserves_unknown_fields_too() -> None:
    harness = build_client(
        json_response(
            200, {"entries": [dlq_entry_body(future_field="kept")], "extra": 1}
        )
    )
    page = harness.client.dlq.list()

    assert page.entries[0].raw["future_field"] == "kept"
    assert page.raw["extra"] == 1


def test_a_replay_with_a_malformed_replacement_is_reported() -> None:
    bad = job_body()
    del bad["status"]
    harness = build_client(
        json_response(
            201, {"original_job_id": "d", "replacement": bad, "replayed": False}
        )
    )
    with pytest.raises(UnexpectedResponseError):
        harness.client.dlq.replay("d")


def test_job_repr_omits_raw() -> None:
    """repr is for humans; raw duplicates every other field."""
    harness = build_client(json_response(200, job_body()))
    job = harness.client.jobs.get("abc")

    assert isinstance(job, Job)
    assert "raw=" not in repr(job)
    assert "QUEUED" in repr(job)
