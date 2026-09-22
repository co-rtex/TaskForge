"""The entry point: :class:`TaskForgeClient`."""

from __future__ import annotations

from types import TracebackType
from typing import Self

import httpx

from ._dlq import DLQ
from ._jobs import Jobs
from ._keys import ApiKeys, WorkerKeys
from ._queues import Queues
from ._transport import DEFAULT_TIMEOUT_SECONDS, Transport
from ._workers import Workers
from .config import resolve_api_key, resolve_base_url

__all__ = ["TaskForgeClient"]


class TaskForgeClient:
    """A typed client for the TaskForge public API.

    ::

        from taskforge import TaskForgeClient

        with TaskForgeClient() as client:
            job = client.jobs.submit(
                queue="default",
                job_type="demo.echo",
                payload={"message": "hello"},
            )
            print(job.id, job.status)

    Configuration precedence is the constructor argument, then the
    environment variable, then the loopback default -- the same order
    ``internal/cli.ResolveBaseURL`` applies. The variables are
    ``TASKFORGE_SDK_API_URL`` and ``TASKFORGE_SDK_API_KEY``, which are this
    SDK's own: it never reads ``TASKFORGE_API_ADDR`` (the server's *bind*
    address) and never reads ``taskforge-cli``'s variables. See
    :mod:`taskforge.config` for why.

    The client holds a connection pool and should be closed when you are
    done with it, either by using it as a context manager or by calling
    :meth:`close`.

    There is deliberately no retry logic. Three operations carry an
    idempotency identity the caller owns, and a retry policy belongs to the
    caller who knows whether repeating a given call is safe -- the same
    decision ``taskforge-cli`` makes.

    As of M6A the operator read surface is available: ``jobs.list()``,
    ``jobs.attempts()``, ``workers.list()`` and ``queues.list()``, which
    complete the public ``/v1`` routes ``docs/PROJECT_SPEC.md`` section 4
    lists.

    There are deliberately no cursor-following iterator helpers on those
    listings beyond :meth:`taskforge.TaskForgeClient.dlq.iter_entries`. A
    helper that silently walked every page would turn one documented request
    into an arbitrary number of them; the cursor is documented and a caller
    who wants every page can loop on it.
    """

    def __init__(
        self,
        base_url: str | None = None,
        api_key: str | None = None,
        *,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        http_client: httpx.Client | None = None,
    ) -> None:
        """
        :param base_url: absolute http(s) URL of the API. Falls back to
            ``TASKFORGE_SDK_API_URL``, then ``http://127.0.0.1:8080``.
        :param api_key: credential presented as ``Authorization: Bearer`` on
            the public routes. Falls back to ``TASKFORGE_SDK_API_KEY``, then
            to none. Never sent to the ``/internal/v1`` key-management
            routes.
        :param timeout: seconds bounding a single HTTP call. Independent of
            the server's own per-request budget.
        :param http_client: an ``httpx.Client`` to use instead of one this
            client creates -- the seam the test suite drives a
            ``MockTransport`` through. A client passed here is *not* closed
            by :meth:`close`; its owner closes it.
        :raises ConfigurationError: if the resolved base URL is not an
            absolute http(s) URL. Raised here, before any request.
        """
        self._transport = Transport(
            resolve_base_url(base_url),
            resolve_api_key(api_key),
            timeout=timeout,
            http_client=http_client,
        )

        #: Job submission, reads, cancellation, and operator retry.
        self.jobs = Jobs(self._transport)
        #: The logical dead-letter queue and replay.
        self.dlq = DLQ(self._transport)
        #: Read-only worker capacity and health.
        self.workers = Workers(self._transport)
        #: Queue configuration and this scope's non-terminal depth.
        self.queues = Queues(self._transport)
        #: Credentials for the public routes (loopback-only surface).
        self.api_keys = ApiKeys(self._transport)
        #: Credentials that register worker sessions (loopback-only surface).
        self.worker_keys = WorkerKeys(self._transport)

    @property
    def base_url(self) -> str:
        """The resolved API address, with no trailing slash."""
        return self._transport.base_url

    def close(self) -> None:
        """Release the underlying connection pool.

        A no-op for an ``http_client`` the caller supplied.
        """
        self._transport.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc: BaseException | None,
        tb: TracebackType | None,
    ) -> None:
        self.close()
