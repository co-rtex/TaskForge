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
    "Cancellation",
    "CancellationStatus",
    "DLQEntry",
    "DLQPage",
    "DLQReason",
    "Job",
    "JobStatus",
    "Replay",
    "WorkerKeyCreated",
    "WorkerKeyList",
    "WorkerKeyRevoked",
    "WorkerKeySummary",
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
