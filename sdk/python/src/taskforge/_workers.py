"""The ``client.workers`` namespace."""

from __future__ import annotations

from typing import Any

from ._transport import Transport
from .models import WorkerPage

__all__ = ["Workers"]


class Workers:
    """Read-only visibility into worker capacity and health.

    This is the public, authenticated view. The worker-control surface that
    workers themselves call lives under ``/internal/v1`` and is not part of
    this SDK.
    """

    def __init__(self, transport: Transport) -> None:
        self._transport = transport

    def list(
        self, *, limit: int | None = None, cursor: str | None = None
    ) -> WorkerPage:
        """Read one bounded, scope-filtered page of workers, by name.

        ``GET /v1/workers``. Each worker is joined to its **most recent
        process session, whatever that session's status** -- so a worker
        whose process crashed (``UNHEALTHY``) and one that a newer boot
        replaced (``OFFLINE``) both appear. That is deliberate: those are
        the workers an operator is usually looking for.

        Pagination is keyset on ``name``, which is unique per scope and
        therefore already a total order.

        This endpoint reports facts and does not judge them.
        :attr:`~taskforge.models.Worker.heartbeat_age_seconds` is measured by
        PostgreSQL, and nothing here says whether a worker is "stale": the
        threshold that decides that lives in the reconciler's configuration.

        :raises RequestRejectedError: with code ``validation_failed`` for an
            out-of-range limit, or ``invalid_cursor`` for a cursor this
            endpoint did not issue.
        """
        params: dict[str, Any] = {}
        if limit is not None:
            params["limit"] = limit
        if cursor is not None:
            params["cursor"] = cursor
        response = self._transport.request("GET", "/v1/workers", params=params or None)
        return WorkerPage.from_api(response)
