"""The DLQ namespace: listing, cursor iteration, and replay."""

from __future__ import annotations

import httpx
import pytest

from conftest import build_client, dlq_entry_body, job_body, json_response
from taskforge import DLQEntry, DLQPage, DLQReason, Replay


def test_list_with_no_arguments_sends_no_query_parameters() -> None:
    """Omitting both lets the server apply its own documented defaults."""
    harness = build_client(json_response(200, {"entries": []}))
    page = harness.client.dlq.list()

    assert str(harness.recorder.last.url) == "http://api.test/v1/dlq"
    assert isinstance(page, DLQPage)
    assert page.entries == ()
    assert page.next_cursor is None


def test_list_passes_limit_and_cursor() -> None:
    harness = build_client(json_response(200, {"entries": []}))
    harness.client.dlq.list(limit=50, cursor="opaque-cursor")

    url = harness.recorder.last.url
    assert url.params["limit"] == "50"
    assert url.params["cursor"] == "opaque-cursor"


def test_list_parses_entries() -> None:
    harness = build_client(
        json_response(
            200,
            {
                "entries": [
                    dlq_entry_body(
                        reason="PERMANENT_FAILURE",
                        error_code="handler_error",
                        error_message="boom",
                        attempt_number=3,
                    )
                ],
                "next_cursor": "next-1",
            },
        )
    )
    page = harness.client.dlq.list()

    assert page.next_cursor == "next-1"
    entry = page.entries[0]
    assert isinstance(entry, DLQEntry)
    assert entry.reason is DLQReason.PERMANENT_FAILURE
    assert entry.error_message == "boom"
    assert entry.attempt_number == 3


def test_nullable_entry_fields_absent_and_null_are_both_none() -> None:
    harness = build_client(
        json_response(
            200,
            {
                "entries": [
                    dlq_entry_body(),  # optional fields absent entirely
                    dlq_entry_body(
                        terminal_attempt_id=None,
                        attempt_number=None,
                        failure_class=None,
                    ),
                ]
            },
        )
    )
    absent, explicit_null = harness.client.dlq.list().entries

    for entry in (absent, explicit_null):
        assert entry.terminal_attempt_id is None
        assert entry.attempt_number is None
        assert entry.failure_class is None


def test_iter_entries_follows_the_cursor_to_the_end() -> None:
    pages = [
        {"entries": [dlq_entry_body(id="a")], "next_cursor": "c1"},
        {"entries": [dlq_entry_body(id="b")], "next_cursor": "c2"},
        {"entries": [dlq_entry_body(id="c")]},  # no cursor: the last page
    ]
    seen_cursors: list[str | None] = []

    def handler(request: httpx.Request) -> httpx.Response:
        seen_cursors.append(request.url.params.get("cursor"))
        return httpx.Response(200, json=pages[len(seen_cursors) - 1])

    harness = build_client(handler)
    ids = [entry.id for entry in harness.client.dlq.iter_entries()]

    assert ids == ["a", "b", "c"]
    assert seen_cursors == [None, "c1", "c2"]
    assert harness.recorder.count == 3, "stops when next_cursor is absent"


def test_iter_entries_stops_on_an_empty_last_page() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"entries": []})

    harness = build_client(handler)
    assert list(harness.client.dlq.iter_entries()) == []
    assert harness.recorder.count == 1


def test_iter_entries_passes_the_page_size_through() -> None:
    def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={"entries": []})

    harness = build_client(handler)
    list(harness.client.dlq.iter_entries(limit=10))

    assert harness.recorder.last.url.params["limit"] == "10"


@pytest.mark.parametrize("status", [200, 201])
def test_replay_accepts_both_success_statuses(status: int) -> None:
    harness = build_client(
        json_response(
            status,
            {
                "original_job_id": "dead-1",
                "replacement": job_body(replayed_from_job_id="dead-1"),
                "replayed": status == 200,
            },
        )
    )
    replay = harness.client.dlq.replay("dead-1")

    assert isinstance(replay, Replay)
    assert str(harness.recorder.last.url) == "http://api.test/v1/dlq/dead-1/replay"
    assert replay.already_replayed is (status == 200)
