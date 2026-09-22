"""Typed views over the API's responses.

Every model is a frozen dataclass over the fields ``api/openapi.yaml`` marks
``required``, and every one carries :attr:`raw` -- the untouched decoded
response object.

``raw`` is the point. ``taskforge-cli`` deliberately never re-derives a
response; it prints the server's bytes, because the server's schema is the
only contract that needs to stay in sync. A dataclass on its own would break
that property by silently dropping any field a newer server added. Carrying
``raw`` alongside the typed fields keeps attribute access (``job.status``)
without losing anything the server sent.

Parsing is therefore **tolerant of unknown keys and strict about missing
required ones**: an unknown field lands in ``raw`` and is ignored, while a
missing required field or an unrecognized enum value raises
:class:`~taskforge.errors.UnexpectedResponseError`. That is the same trade
``ExitUnexpectedResponse`` makes in the CLI -- a response this version does
not recognize is reported, not guessed at. Its consequence is stated
plainly: a server that adds a tenth job status breaks an older SDK's parse
of a job in that status, and that is preferred over handing a caller a state
their code has never heard of.
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from enum import Enum, StrEnum
from typing import Any, ClassVar, Self, TypeVar

from .errors import UnexpectedResponseError

__all__ = [
    "ApiKeyCreated",
    "ApiKeyList",
    "ApiKeyRevoked",
    "ApiKeySummary",
    "Attempt",
    "AttemptList",
    "AttemptStatus",
    "Cancellation",
    "CancellationStatus",
    "DLQEntry",
    "DLQPage",
    "DLQReason",
    "FailureClass",
    "Job",
    "JobPage",
    "JobStatus",
    "JobSummary",
    "Queue",
    "QueueList",
    "Replay",
    "SessionStatus",
    "Worker",
    "WorkerKeyCreated",
    "WorkerKeyList",
    "WorkerKeyRevoked",
    "WorkerKeySummary",
    "WorkerPage",
]


class JobStatus(StrEnum):
    """The full V1 job state machine, every state reachable as of M4.

    There is deliberately no job-level ``FAILED``: one failed attempt is not
    a job outcome, so permanent and exhausted failure are ``DEAD_LETTERED``
    and individual failures stay in attempt history.
    """

    PENDING = "PENDING"
    QUEUED = "QUEUED"
    LEASED = "LEASED"
    RUNNING = "RUNNING"
    RETRY_WAIT = "RETRY_WAIT"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"
    SUCCEEDED = "SUCCEEDED"
    CANCELED = "CANCELED"
    DEAD_LETTERED = "DEAD_LETTERED"


class CancellationStatus(StrEnum):
    """``CANCELED`` means it is over; ``CANCEL_REQUESTED`` means an attempt
    still holds authority that is being withdrawn."""

    CANCELED = "CANCELED"
    CANCEL_REQUESTED = "CANCEL_REQUESTED"


class DLQReason(StrEnum):
    """Why a job was dead-lettered."""

    PERMANENT_FAILURE = "PERMANENT_FAILURE"
    ATTEMPTS_EXHAUSTED = "ATTEMPTS_EXHAUSTED"


class AttemptStatus(StrEnum):
    """One attempt's lifecycle, which is separate from the job's.

    ``ABANDONED`` is what a crashed worker's attempt becomes. ``TIMED_OUT``
    is server-authoritative and recorded by reconciliation -- a worker cannot
    declare its own timeout.
    """

    LEASED = "LEASED"
    RUNNING = "RUNNING"
    SUCCEEDED = "SUCCEEDED"
    FAILED = "FAILED"
    TIMED_OUT = "TIMED_OUT"
    CANCELED = "CANCELED"
    ABANDONED = "ABANDONED"


class FailureClass(StrEnum):
    """How an attempt ended, in retry-policy terms.

    ``TIMED_OUT``, ``CANCELED`` and ``ABANDONED`` are server-authoritative;
    ``RETRYABLE`` and ``PERMANENT`` come from a trusted handler.
    """

    RETRYABLE = "RETRYABLE"
    PERMANENT = "PERMANENT"
    TIMED_OUT = "TIMED_OUT"
    CANCELED = "CANCELED"
    ABANDONED = "ABANDONED"


class SessionStatus(StrEnum):
    """The server-owned health state of one worker process lifetime.

    ``UNHEALTHY`` is what reconciliation marks a session whose heartbeat went
    stale -- a crashed process. ``OFFLINE`` is a session a newer boot
    replaced. ``GET /v1/workers`` lists both.
    """

    STARTING = "STARTING"
    HEALTHY = "HEALTHY"
    DRAINING = "DRAINING"
    UNHEALTHY = "UNHEALTHY"
    OFFLINE = "OFFLINE"


# ---------------------------------------------------------------------------
# Parsing helpers. Each raises UnexpectedResponseError rather than KeyError or
# ValueError, so a caller only ever has to catch TaskForgeError.
# ---------------------------------------------------------------------------


_E = TypeVar("_E", bound=Enum)


def _unexpected(message: str, raw: Any) -> UnexpectedResponseError:
    return UnexpectedResponseError(
        code="unexpected_response",
        message=message,
        http_status=200,
        raw=raw,
    )


def _object(payload: Any, what: str) -> Mapping[str, Any]:
    if not isinstance(payload, Mapping):
        raise _unexpected(
            f"expected a JSON object for {what}, got {type(payload).__name__}",
            payload,
        )
    return payload


def _require(payload: Mapping[str, Any], key: str, what: str) -> Any:
    if key not in payload:
        raise _unexpected(f"{what} is missing the required field {key!r}", payload)
    return payload[key]


def _str(payload: Mapping[str, Any], key: str, what: str) -> str:
    value = _require(payload, key, what)
    if not isinstance(value, str):
        raise _unexpected(f"{what} field {key!r} must be a string", payload)
    return value


def _int(payload: Mapping[str, Any], key: str, what: str) -> int:
    value = _require(payload, key, what)
    # bool is a subclass of int; a boolean here is a contract violation.
    if not isinstance(value, int) or isinstance(value, bool):
        raise _unexpected(f"{what} field {key!r} must be an integer", payload)
    return value


def _bool(payload: Mapping[str, Any], key: str, what: str) -> bool:
    value = _require(payload, key, what)
    if not isinstance(value, bool):
        raise _unexpected(f"{what} field {key!r} must be a boolean", payload)
    return value


def _opt_str(payload: Mapping[str, Any], key: str, what: str) -> str | None:
    value = _require(payload, key, what)
    if value is None:
        return None
    if not isinstance(value, str):
        raise _unexpected(f"{what} field {key!r} must be a string or null", payload)
    return value


def _enum(enum_class: type[_E], payload: Mapping[str, Any], key: str, what: str) -> _E:
    value = _str(payload, key, what)
    try:
        return enum_class(value)
    except ValueError as exc:
        raise _unexpected(
            f"{what} field {key!r} carries {value!r}, which this SDK version "
            f"does not recognize",
            payload,
        ) from exc


# ---------------------------------------------------------------------------
# Models
# ---------------------------------------------------------------------------


@dataclass(frozen=True, slots=True)
class Job:
    """One job's durable lifecycle state.

    Attempt history is deliberately absent: ``GET /v1/jobs/{job_id}``
    returns lifecycle fields only. A result, once recorded, is retrieved
    separately through :meth:`taskforge.TaskForgeClient.jobs.result`, so
    reading a job's status never pulls a potentially large result body
    along with it.
    """

    id: str
    queue: str
    job_type: str
    payload: Mapping[str, Any]
    status: JobStatus
    priority: int
    max_attempts: int
    timeout_seconds: int
    required_capabilities: Sequence[str]
    scheduled_at: str | None
    available_at: str
    cancel_requested_at: str | None
    replayed_from_job_id: str | None
    created_at: str
    updated_at: str
    raw: Mapping[str, Any] = field(repr=False)

    @classmethod
    def from_api(cls, payload: Any) -> Self:
        obj = _object(payload, "a job")
        capabilities = _require(obj, "required_capabilities", "a job")
        if not isinstance(capabilities, list):
            raise _unexpected("a job field 'required_capabilities' must be a list", obj)
        job_payload = _require(obj, "payload", "a job")
        return cls(
            id=_str(obj, "id", "a job"),
            queue=_str(obj, "queue", "a job"),
            job_type=_str(obj, "job_type", "a job"),
            payload=_object(job_payload, "a job payload"),
            status=_enum(JobStatus, obj, "status", "a job"),
            priority=_int(obj, "priority", "a job"),
            max_attempts=_int(obj, "max_attempts", "a job"),
            timeout_seconds=_int(obj, "timeout_seconds", "a job"),
            required_capabilities=tuple(str(c) for c in capabilities),
            scheduled_at=_opt_str(obj, "scheduled_at", "a job"),
            available_at=_str(obj, "available_at", "a job"),
            cancel_requested_at=_opt_str(obj, "cancel_requested_at", "a job"),
            replayed_from_job_id=_opt_str(obj, "replayed_from_job_id", "a job"),
            created_at=_str(obj, "created_at", "a job"),
            updated_at=_str(obj, "updated_at", "a job"),
            raw=obj,
        )


@dataclass(frozen=True, slots=True)
class Cancellation:
    """The outcome of a cancellation request."""

    job_id: str
    status: CancellationStatus
    cancel_requested_at: str
    already_requested: bool
    raw: Mapping[str, Any] = field(repr=False)

    @classmethod
    def from_api(cls, payload: Any) -> Self:
        obj = _object(payload, "a cancellation")
        return cls(
            job_id=_str(obj, "job_id", "a cancellation"),
            status=_enum(CancellationStatus, obj, "status", "a cancellation"),
            cancel_requested_at=_str(obj, "cancel_requested_at", "a cancellation"),
            already_requested=_bool(obj, "already_requested", "a cancellation"),
            raw=obj,
        )


@dataclass(frozen=True, slots=True)
class Replay:
    """A replacement job created from a terminal one.

    Replay never resurrects a terminal job; it creates a new one linked back
    to it (ADR-0012). ``already_replayed`` is true when this replay identity
    had already created the replacement.
    """

    original_job_id: str
    replacement: Job
    already_replayed: bool
    raw: Mapping[str, Any] = field(repr=False)

    @classmethod
    def from_api(cls, payload: Any) -> Self:
        obj = _object(payload, "a replay")
        return cls(
            original_job_id=_str(obj, "original_job_id", "a replay"),
            replacement=Job.from_api(_require(obj, "replacement", "a replay")),
            already_replayed=_bool(obj, "replayed", "a replay"),
            raw=obj,
        )


@dataclass(frozen=True, slots=True)
class DLQEntry:
    """One dead-letter entry.

    Entries deliberately carry no job payload: a list endpoint that returned
    payloads would let one request pull an unbounded amount of user data. The
    job itself is still readable through
    :meth:`taskforge.TaskForgeClient.jobs.get`.
    """

    id: str
    job_id: str
    queue: str
    job_type: str
    priority: int
    max_attempts: int
    reason: DLQReason
    created_at: str
    replay_count: int
    terminal_attempt_id: str | None
    attempt_number: int | None
    attempt_status: str | None
    failure_class: str | None
    error_code: str | None
    error_message: str | None
    raw: Mapping[str, Any] = field(repr=False)

    @classmethod
    def from_api(cls, payload: Any) -> Self:
        obj = _object(payload, "a DLQ entry")
        attempt_number = obj.get("attempt_number")
        if attempt_number is not None and (
            not isinstance(attempt_number, int) or isinstance(attempt_number, bool)
        ):
            raise _unexpected(
                "a DLQ entry field 'attempt_number' must be an integer or null", obj
            )
        return cls(
            id=_str(obj, "id", "a DLQ entry"),
            job_id=_str(obj, "job_id", "a DLQ entry"),
            queue=_str(obj, "queue", "a DLQ entry"),
            job_type=_str(obj, "job_type", "a DLQ entry"),
            priority=_int(obj, "priority", "a DLQ entry"),
            max_attempts=_int(obj, "max_attempts", "a DLQ entry"),
            reason=_enum(DLQReason, obj, "reason", "a DLQ entry"),
            created_at=_str(obj, "created_at", "a DLQ entry"),
            replay_count=_int(obj, "replay_count", "a DLQ entry"),
            terminal_attempt_id=_optional_str(obj, "terminal_attempt_id"),
            attempt_number=attempt_number,
            attempt_status=_optional_str(obj, "attempt_status"),
            failure_class=_optional_str(obj, "failure_class"),
            error_code=_optional_str(obj, "error_code"),
            error_message=_optional_str(obj, "error_message"),
            raw=obj,
        )


def _optional_str(payload: Mapping[str, Any], key: str) -> str | None:
    """Read a nullable, non-required field. Absent and null are the same."""
    value = payload.get(key)
    return value if isinstance(value, str) else None


@dataclass(frozen=True, slots=True)
class DLQPage:
    """One bounded, scope-filtered page of the logical DLQ, newest first.

    A full page is not proof more exist; the presence of
    :attr:`next_cursor` is. The cursor is opaque -- it is a position, not an
    authorization.
    """

    entries: Sequence[DLQEntry]
    next_cursor: str | None
    raw: Mapping[str, Any] = field(repr=False)

    @classmethod
    def from_api(cls, payload: Any) -> Self:
        obj = _object(payload, "a DLQ page")
        entries = _require(obj, "entries", "a DLQ page")
        if not isinstance(entries, list):
            raise _unexpected("a DLQ page field 'entries' must be a list", obj)
        return cls(
            entries=tuple(DLQEntry.from_api(entry) for entry in entries),
            next_cursor=_optional_str(obj, "next_cursor"),
            raw=obj,
        )


@dataclass(frozen=True, slots=True)
class JobSummary:
    """One row of a job listing: every :class:`Job` field except ``payload``.

    The omission is deliberate, not an oversight. A list endpoint that
    returned payloads would let one request pull an unbounded amount of user
    data, so the server never reads the payload for this route at all. Fetch
    a single job with :meth:`taskforge.TaskForgeClient.jobs.get` when you
    need its payload.
    """

    id: str
    queue: str
    job_type: str
    status: JobStatus
    priority: int
    max_attempts: int
    timeout_seconds: int
    required_capabilities: Sequence[str]
    scheduled_at: str | None
    available_at: str
    cancel_requested_at: str | None
    replayed_from_job_id: str | None
    created_at: str
    updated_at: str
    raw: Mapping[str, Any] = field(repr=False)

    @classmethod
    def from_api(cls, payload: Any) -> Self:
        obj = _object(payload, "a job summary")
        capabilities = _require(obj, "required_capabilities", "a job summary")
        if not isinstance(capabilities, list):
            raise _unexpected(
                "a job summary field 'required_capabilities' must be a list", obj
            )
        return cls(
            id=_str(obj, "id", "a job summary"),
            queue=_str(obj, "queue", "a job summary"),
            job_type=_str(obj, "job_type", "a job summary"),
            status=_enum(JobStatus, obj, "status", "a job summary"),
            priority=_int(obj, "priority", "a job summary"),
            max_attempts=_int(obj, "max_attempts", "a job summary"),
            timeout_seconds=_int(obj, "timeout_seconds", "a job summary"),
            required_capabilities=tuple(str(c) for c in capabilities),
            scheduled_at=_opt_str(obj, "scheduled_at", "a job summary"),
            available_at=_str(obj, "available_at", "a job summary"),
            cancel_requested_at=_opt_str(obj, "cancel_requested_at", "a job summary"),
            replayed_from_job_id=_opt_str(obj, "replayed_from_job_id", "a job summary"),
            created_at=_str(obj, "created_at", "a job summary"),
            updated_at=_str(obj, "updated_at", "a job summary"),
            raw=obj,
        )


@dataclass(frozen=True, slots=True)
class JobPage:
    """One bounded, scope-filtered page of jobs, newest first.

    A full page is not proof more exist; the presence of :attr:`next_cursor`
    is. The cursor is opaque -- a position, not an authorization.

    **A walk is not a snapshot.** A job's ``created_at`` is its transaction's
    start time, so a submission that committed after a page was read can
    carry a timestamp already behind the cursor and will not appear in that
    walk. Keyset ordering guarantees no job present throughout a walk is
    duplicated or skipped; it does not guarantee a walk observes a job that
    became visible during it.
    """

    jobs: Sequence[JobSummary]
    next_cursor: str | None
    raw: Mapping[str, Any] = field(repr=False)

    @classmethod
    def from_api(cls, payload: Any) -> Self:
        obj = _object(payload, "a job page")
        jobs = _require(obj, "jobs", "a job page")
        if not isinstance(jobs, list):
            raise _unexpected("a job page field 'jobs' must be a list", obj)
        return cls(
            jobs=tuple(JobSummary.from_api(job) for job in jobs),
            next_cursor=_optional_str(obj, "next_cursor"),
            raw=obj,
        )


@dataclass(frozen=True, slots=True)
class Attempt:
    """One execution attempt of a job.

    Session identifiers, lease identifiers, and outcome identities are
    deliberately absent from this endpoint: every worker-control route except
    registration trusts a session id as authority, so publishing one on a
    public read would hand out an identifier that surface treats as a
    credential. :attr:`worker_id` names a logical worker and authorizes
    nothing.
    """

    id: str
    attempt_number: int
    status: AttemptStatus
    worker_id: str
    worker_name: str
    created_at: str
    started_at: str | None
    finished_at: str | None
    timeout_at: str | None
    failure_class: FailureClass | None
    error_code: str | None
    error_message: str | None
    retry_delay_ms: int | None
    retry_at: str | None
    raw: Mapping[str, Any] = field(repr=False)

    @classmethod
    def from_api(cls, payload: Any) -> Self:
        obj = _object(payload, "an attempt")
        failure_class = obj.get("failure_class")
        parsed_class: FailureClass | None = None
        if failure_class is not None:
            parsed_class = _enum(FailureClass, obj, "failure_class", "an attempt")
        retry_delay = obj.get("retry_delay_ms")
        if retry_delay is not None and (
            not isinstance(retry_delay, int) or isinstance(retry_delay, bool)
        ):
            raise _unexpected(
                "an attempt field 'retry_delay_ms' must be an integer or null", obj
            )
        return cls(
            id=_str(obj, "id", "an attempt"),
            attempt_number=_int(obj, "attempt_number", "an attempt"),
            status=_enum(AttemptStatus, obj, "status", "an attempt"),
            worker_id=_str(obj, "worker_id", "an attempt"),
            worker_name=_str(obj, "worker_name", "an attempt"),
            created_at=_str(obj, "created_at", "an attempt"),
            started_at=_optional_str(obj, "started_at"),
            finished_at=_optional_str(obj, "finished_at"),
            timeout_at=_optional_str(obj, "timeout_at"),
            failure_class=parsed_class,
            error_code=_optional_str(obj, "error_code"),
            error_message=_optional_str(obj, "error_message"),
            retry_delay_ms=retry_delay,
            retry_at=_optional_str(obj, "retry_at"),
            raw=obj,
        )


@dataclass(frozen=True, slots=True)
class AttemptList:
    """A job's full attempt timeline, oldest first.

    Unpaginated: ``max_attempts`` is capped at 100 by the schema, so a
    timeline is bounded by the job itself rather than by a page size. An
    empty list is a real job that has never been attempted.
    """

    attempts: Sequence[Attempt]
    raw: Mapping[str, Any] = field(repr=False)

    @classmethod
    def from_api(cls, payload: Any) -> Self:
        obj = _object(payload, "an attempt list")
        attempts = _require(obj, "attempts", "an attempt list")
        if not isinstance(attempts, list):
            raise _unexpected("an attempt list field 'attempts' must be a list", obj)
        return cls(
            attempts=tuple(Attempt.from_api(attempt) for attempt in attempts),
            raw=obj,
        )


@dataclass(frozen=True, slots=True)
class Worker:
    """A logical worker joined to its most recent process session.

    The session reported is the latest one **whatever its status**, so a
    crashed worker (``UNHEALTHY``) and a replaced one (``OFFLINE``) both
    appear.
    """

    id: str
    name: str
    status: SessionStatus
    worker_group: str
    hostname: str
    concurrency_limit: int
    capabilities: Sequence[str]
    supported_job_types: Sequence[str]
    registered_at: str
    last_heartbeat_at: str
    ended_at: str | None
    heartbeat_age_seconds: float
    active_leases: int
    raw: Mapping[str, Any] = field(repr=False)

    @classmethod
    def from_api(cls, payload: Any) -> Self:
        obj = _object(payload, "a worker")
        capabilities = _require(obj, "capabilities", "a worker")
        if not isinstance(capabilities, list):
            raise _unexpected("a worker field 'capabilities' must be a list", obj)
        job_types = _require(obj, "supported_job_types", "a worker")
        if not isinstance(job_types, list):
            raise _unexpected(
                "a worker field 'supported_job_types' must be a list", obj
            )
        age = _require(obj, "heartbeat_age_seconds", "a worker")
        if isinstance(age, bool) or not isinstance(age, (int, float)):
            raise _unexpected(
                "a worker field 'heartbeat_age_seconds' must be a number", obj
            )
        return cls(
            id=_str(obj, "id", "a worker"),
            name=_str(obj, "name", "a worker"),
            status=_enum(SessionStatus, obj, "status", "a worker"),
            worker_group=_str(obj, "worker_group", "a worker"),
            hostname=_str(obj, "hostname", "a worker"),
            concurrency_limit=_int(obj, "concurrency_limit", "a worker"),
            capabilities=tuple(str(c) for c in capabilities),
            supported_job_types=tuple(str(j) for j in job_types),
            registered_at=_str(obj, "registered_at", "a worker"),
            last_heartbeat_at=_str(obj, "last_heartbeat_at", "a worker"),
            ended_at=_optional_str(obj, "ended_at"),
            heartbeat_age_seconds=float(age),
            active_leases=_int(obj, "active_leases", "a worker"),
            raw=obj,
        )


@dataclass(frozen=True, slots=True)
class WorkerPage:
    """One bounded, scope-filtered page of workers, by name."""

    workers: Sequence[Worker]
    next_cursor: str | None
    raw: Mapping[str, Any] = field(repr=False)

    @classmethod
    def from_api(cls, payload: Any) -> Self:
        obj = _object(payload, "a worker page")
        workers = _require(obj, "workers", "a worker page")
        if not isinstance(workers, list):
            raise _unexpected("a worker page field 'workers' must be a list", obj)
        return cls(
            workers=tuple(Worker.from_api(worker) for worker in workers),
            next_cursor=_optional_str(obj, "next_cursor"),
            raw=obj,
        )


@dataclass(frozen=True, slots=True)
class Queue:
    """One queue and this scope's non-terminal depth in it.

    :attr:`depth` is keyed by :class:`JobStatus` and carries **every**
    non-terminal status, always, zero-filled. Terminal statuses are absent by
    design.

    :attr:`max_concurrency` is **queue-wide and shared across scopes**, while
    :attr:`depth` counts only this credential's jobs. The two are not
    comparable, and no queue-wide in-flight figure is reported, because that
    would disclose another scope's load.
    """

    name: str
    worker_group: str
    max_concurrency: int
    depth: Mapping[JobStatus, int]
    raw: Mapping[str, Any] = field(repr=False)

    @classmethod
    def from_api(cls, payload: Any) -> Self:
        obj = _object(payload, "a queue")
        raw_depth = _object(_require(obj, "depth", "a queue"), "a queue depth")
        depth: dict[JobStatus, int] = {}
        for key, value in raw_depth.items():
            try:
                status = JobStatus(key)
            except ValueError as exc:
                raise _unexpected(
                    f"a queue depth names {key!r}, which this SDK version does "
                    f"not recognize as a job status",
                    obj,
                ) from exc
            if not isinstance(value, int) or isinstance(value, bool):
                raise _unexpected(f"a queue depth for {key!r} must be an integer", obj)
            depth[status] = value
        return cls(
            name=_str(obj, "name", "a queue"),
            worker_group=_str(obj, "worker_group", "a queue"),
            max_concurrency=_int(obj, "max_concurrency", "a queue"),
            depth=depth,
            raw=obj,
        )


@dataclass(frozen=True, slots=True)
class QueueList:
    """Every queue, by name. Unpaginated: no API creates a queue."""

    queues: Sequence[Queue]
    raw: Mapping[str, Any] = field(repr=False)

    @classmethod
    def from_api(cls, payload: Any) -> Self:
        obj = _object(payload, "a queue list")
        queues = _require(obj, "queues", "a queue list")
        if not isinstance(queues, list):
            raise _unexpected("a queue list field 'queues' must be a list", obj)
        return cls(
            queues=tuple(Queue.from_api(queue) for queue in queues),
            raw=obj,
        )


@dataclass(frozen=True, slots=True)
class ApiKeySummary:
    """A key as a listing shows it. Never contains the secret."""

    id: str
    scope: str
    name: str
    prefix: str
    created_at: str
    revoked_at: str | None
    raw: Mapping[str, Any] = field(repr=False)

    @classmethod
    def from_api(cls, payload: Any) -> Self:
        obj = _object(payload, "a key summary")
        return cls(
            id=_str(obj, "id", "a key summary"),
            scope=_str(obj, "scope", "a key summary"),
            name=_str(obj, "name", "a key summary"),
            prefix=_str(obj, "prefix", "a key summary"),
            created_at=_str(obj, "created_at", "a key summary"),
            revoked_at=_opt_str(obj, "revoked_at", "a key summary"),
            raw=obj,
        )


@dataclass(frozen=True, slots=True)
class ApiKeyCreated:
    """A freshly minted key, including its secret.

    :attr:`key` is the complete credential, ``tfk_<lookup>.<secret>``. This
    is the only response in TaskForge that ever contains it and it is
    returned exactly once -- it is not stored server-side and cannot be
    recovered. **This SDK does not persist it anywhere either**; what the
    caller does with it is the caller's decision.
    """

    id: str
    scope: str
    name: str
    prefix: str
    created_at: str
    key: str
    raw: Mapping[str, Any] = field(repr=False)

    @classmethod
    def from_api(cls, payload: Any) -> Self:
        obj = _object(payload, "a created key")
        return cls(
            id=_str(obj, "id", "a created key"),
            scope=_str(obj, "scope", "a created key"),
            name=_str(obj, "name", "a created key"),
            prefix=_str(obj, "prefix", "a created key"),
            created_at=_str(obj, "created_at", "a created key"),
            key=_str(obj, "key", "a created key"),
            raw=obj,
        )


@dataclass(frozen=True, slots=True)
class ApiKeyRevoked:
    """A revoked key.

    :attr:`already_revoked` is true when the key was already revoked before
    this call; the reported :attr:`revoked_at` is then the original instant,
    not this one.
    """

    id: str
    scope: str
    name: str
    prefix: str
    created_at: str
    revoked_at: str | None
    already_revoked: bool
    raw: Mapping[str, Any] = field(repr=False)

    @classmethod
    def from_api(cls, payload: Any) -> Self:
        obj = _object(payload, "a revoked key")
        summary = ApiKeySummary.from_api(obj)
        return cls(
            id=summary.id,
            scope=summary.scope,
            name=summary.name,
            prefix=summary.prefix,
            created_at=summary.created_at,
            revoked_at=summary.revoked_at,
            already_revoked=_bool(obj, "already_revoked", "a revoked key"),
            raw=obj,
        )


@dataclass(frozen=True, slots=True)
class ApiKeyList:
    """A page of key summaries."""

    #: The summary type this list parses its entries into. Subclasses
    #: override it so a worker-key listing yields WorkerKeySummary rather
    #: than ApiKeySummary at runtime, not merely in the annotation.
    _summary_type: ClassVar[type[ApiKeySummary]]

    keys: Sequence[ApiKeySummary]
    raw: Mapping[str, Any] = field(repr=False)

    @classmethod
    def from_api(cls, payload: Any) -> Self:
        obj = _object(payload, "a key list")
        keys = _require(obj, "keys", "a key list")
        if not isinstance(keys, list):
            raise _unexpected("a key list field 'keys' must be a list", obj)
        return cls(
            keys=tuple(cls._summary_type.from_api(k) for k in keys),
            raw=obj,
        )


ApiKeyList._summary_type = ApiKeySummary


# Worker keys share the API keys' wire shape exactly: api/openapi.yaml's
# WorkerKeySummary / WorkerKeyCreated / WorkerKeyRevoked / WorkerKeyList are
# field-for-field identical to their ApiKey* counterparts, so the parsing is
# inherited rather than duplicated.
#
# They are nonetheless distinct TYPES rather than aliases, because they are
# distinct credentials authenticating distinct surfaces (ADR-0014): a worker
# key registers a worker session, an API key reaches the public routes, and
# neither works in the other's place. An alias would let a caller pass one
# where the other is meant and have it type-check.


@dataclass(frozen=True, slots=True)
class WorkerKeySummary(ApiKeySummary):
    """A worker key as a listing shows it. Never contains the secret."""


@dataclass(frozen=True, slots=True)
class WorkerKeyCreated(ApiKeyCreated):
    """A freshly minted worker key, including its one-time secret."""


@dataclass(frozen=True, slots=True)
class WorkerKeyRevoked(ApiKeyRevoked):
    """A revoked worker key."""


@dataclass(frozen=True, slots=True)
class WorkerKeyList(ApiKeyList):
    """A page of worker-key summaries."""


WorkerKeyList._summary_type = WorkerKeySummary
