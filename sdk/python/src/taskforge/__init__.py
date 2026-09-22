"""Typed Python client for the TaskForge job-processing API.

TaskForge is a distributed job-processing, scheduling, and reliability
platform. This package is a **pure consumer** of its public ``/v1`` surface
and its loopback-only ``/internal/v1`` credential-management routes, as
documented in ``api/openapi.yaml``. It contains no domain logic and no
credential logic: it obtains and presents API keys rather than
reimplementing a model that lives server-side (ADR-0013, ADR-0014).

::

    from taskforge import TaskForgeClient, NotFoundError

    with TaskForgeClient() as client:
        job = client.jobs.submit(
            queue="default",
            job_type="demo.echo",
            payload={"message": "hello"},
        )
        try:
            print(client.jobs.result(job.id))
        except NotFoundError:
            print("no result recorded yet")

See :mod:`taskforge.errors` for the exception taxonomy and how it maps onto
``taskforge-cli``'s exit codes.
"""

from __future__ import annotations

from ._dlq import DLQ
from ._jobs import Jobs
from ._keys import ApiKeys, WorkerKeys
from ._queues import Queues
from ._transport import DEFAULT_TIMEOUT_SECONDS, new_idempotency_key
from ._workers import Workers
from .client import TaskForgeClient
from .config import (
    API_KEY_ENV,
    API_URL_ENV,
    DEFAULT_BASE_URL,
    resolve_api_key,
    resolve_base_url,
)
from .errors import (
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
from .models import (
    ApiKeyCreated,
    ApiKeyList,
    ApiKeyRevoked,
    ApiKeySummary,
    Attempt,
    AttemptList,
    AttemptStatus,
    Cancellation,
    CancellationStatus,
    DLQEntry,
    DLQPage,
    DLQReason,
    FailureClass,
    Job,
    JobPage,
    JobStatus,
    JobSummary,
    Queue,
    QueueList,
    Replay,
    SessionStatus,
    Worker,
    WorkerKeyCreated,
    WorkerKeyList,
    WorkerKeyRevoked,
    WorkerKeySummary,
    WorkerPage,
)

__version__ = "0.1.0"

__all__ = [
    "API_KEY_ENV",
    "API_URL_ENV",
    "DEFAULT_BASE_URL",
    "DEFAULT_TIMEOUT_SECONDS",
    "DLQ",
    "APIError",
    "ApiKeyCreated",
    "ApiKeyList",
    "ApiKeyRevoked",
    "ApiKeySummary",
    "ApiKeys",
    "Attempt",
    "AttemptList",
    "AttemptStatus",
    "Cancellation",
    "CancellationStatus",
    "ConfigurationError",
    "ConflictError",
    "DLQEntry",
    "DLQPage",
    "DLQReason",
    "FailureClass",
    "InternalServerError",
    "Job",
    "JobPage",
    "JobStatus",
    "JobSummary",
    "Jobs",
    "NotFoundError",
    "Queue",
    "QueueList",
    "Queues",
    "Replay",
    "RequestRejectedError",
    "ServiceUnavailableError",
    "SessionStatus",
    "TaskForgeClient",
    "TaskForgeError",
    "TransportError",
    "UnauthorizedError",
    "UnexpectedResponseError",
    "Worker",
    "WorkerKeyCreated",
    "WorkerKeyList",
    "WorkerKeyRevoked",
    "WorkerKeySummary",
    "WorkerKeys",
    "WorkerPage",
    "Workers",
    "__version__",
    "new_idempotency_key",
    "resolve_api_key",
    "resolve_base_url",
]
