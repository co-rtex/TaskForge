"""The ``client.dlq`` namespace."""

from __future__ import annotations

from collections.abc import Iterator
from typing import Any

from ._jobs import _key_or_new, _path
from ._transport import Transport
from .models import DLQEntry, DLQPage, Replay

__all__ = ["DLQ"]

_CREATED_OR_REPLAYED = (200, 201)


class DLQ:
    """TaskForge's own logical dead-letter queue.

    This is authoritative PostgreSQL state, not the broker's infrastructure
    DLQ for unprocessable notification messages (ADR-0012).
    """

    def __init__(self, transport: Transport) -> None:
        self._transport = transport

    def list(self, *, limit: int | None = None, cursor: str | None = None) -> DLQPage:
        """Read one bounded, scope-filtered page, newest first.

        ``GET /v1/dlq``. Pagination is keyset on ``(created_at DESC, id
        DESC)``, so a page boundary can neither duplicate nor omit an entry.

        ``limit`` is 1..100; omitting it applies the server's own default.
        ``cursor`` is the :attr:`~taskforge.models.DLQPage.next_cursor` from
        a previous page -- it is opaque, a position rather than an
        authorization.

        :raises RequestRejectedError: with code ``validation_failed`` for an
            out-of-range limit, or ``invalid_cursor`` for a cursor this
            endpoint did not issue.
        """
        params: dict[str, Any] = {}
        if limit is not None:
            params["limit"] = limit
        if cursor is not None:
            params["cursor"] = cursor
        response = self._transport.request("GET", "/v1/dlq", params=params or None)
        return DLQPage.from_api(response)

    def iter_entries(self, *, limit: int | None = None) -> Iterator[DLQEntry]:
        """Iterate every dead-letter entry, following ``next_cursor``.

        A convenience over :meth:`list` and nothing more: it is a plain
        client-side loop using only the documented cursor, and it promises
        nothing the API does not already keep. ``limit`` is the per-request
        page size, not a total.

        The DLQ is live state, so an entry dead-lettered while this
        iterator is running may or may not be seen. The keyset ordering
        guarantees no entry present throughout is duplicated or skipped.
        """
        cursor: str | None = None
        while True:
            page = self.list(limit=limit, cursor=cursor)
            yield from page.entries
            cursor = page.next_cursor
            if not cursor:
                return

    def replay(self, job_id: str, *, idempotency_key: str | None = None) -> Replay:
        """Replay a dead-lettered job as a new, linked job.

        ``POST /v1/dlq/{job_id}/replay``. Replay never resurrects a terminal
        job; it creates a replacement carrying ``replayed_from_job_id``
        (ADR-0012). Identical semantics to
        :meth:`taskforge.TaskForgeClient.jobs.retry`, with which it shares
        one identity namespace.

        :raises ConflictError: with code ``job_not_dead_lettered`` when the
            job is not in the DLQ.
        """
        response = self._transport.request(
            "POST",
            f"/v1/dlq/{_path(job_id)}/replay",
            idempotency_key=_key_or_new(idempotency_key),
            success=_CREATED_OR_REPLAYED,
        )
        return Replay.from_api(response)
