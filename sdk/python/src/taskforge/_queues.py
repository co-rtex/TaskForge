"""The ``client.queues`` namespace."""

from __future__ import annotations

from ._transport import Transport
from .models import QueueList

__all__ = ["Queues"]


class Queues:
    """Queue configuration and this scope's non-terminal depth."""

    def __init__(self, transport: Transport) -> None:
        self._transport = transport

    def list(self) -> QueueList:
        """Read every queue, with this credential's non-terminal depth in it.

        ``GET /v1/queues``. Unpaginated, because no API creates a queue: the
        table holds exactly the rows an operator provisioned.

        :attr:`~taskforge.models.Queue.depth` carries **every** non-terminal
        status as a key, always, zero-filled, so you never have to tell "no
        jobs in this status" apart from "the server did not report this
        status". Terminal statuses are absent by design: their counts grow
        without bound and answer a historical question rather than an
        operational one.

        .. warning::
           :attr:`~taskforge.models.Queue.max_concurrency` and
           :attr:`~taskforge.models.Queue.depth` are **not comparable**. The
           limit is queue-wide and shared by every scope; the depth counts
           only this credential's own jobs. No queue-wide in-flight figure is
           reported, because that would disclose another scope's load.
        """
        response = self._transport.request("GET", "/v1/queues")
        return QueueList.from_api(response)
