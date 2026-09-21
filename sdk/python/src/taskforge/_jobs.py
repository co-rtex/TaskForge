"""The ``client.jobs`` namespace."""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

from ._transport import Transport, new_idempotency_key
from .errors import ConfigurationError
from .models import Cancellation, Job, Replay

__all__ = ["Jobs"]

#: Submission and replay both document 200 (an idempotent replay of an
#: earlier identical request) and 201 (created) as success, identically.
_CREATED_OR_REPLAYED = (200, 201)


class Jobs:
    """Job submission, reads, cancellation, and operator retry."""

    def __init__(self, transport: Transport) -> None:
        self._transport = transport

    def submit(
        self,
        *,
        queue: str,
        job_type: str,
        payload: dict[str, Any],
        priority: int = 50,
        max_attempts: int = 3,
        timeout_seconds: int = 300,
        scheduled_at: str | None = None,
        required_capabilities: list[str] | tuple[str, ...] = (),
        idempotency_key: str | None = None,
    ) -> Job:
        """Submit a job, durably and idempotently.

        ``POST /v1/jobs``. A success response means the job is durable: the
        job, its idempotency record, and the broker notification are written
        in one PostgreSQL transaction.

        ``max_attempts`` counts **total** attempts, including the first.
        ``scheduled_at`` is an RFC 3339 instant; absent, null, or already-due
        means immediate submission. Whether a schedule is still in the future
        is decided by PostgreSQL, not by your clock.

        ``idempotency_key`` is placed in the canonical ``Idempotency-Key``
        request header. When omitted, a random UUIDv4 is generated for you.

        .. warning::
           An auto-generated key makes this submission *unique*, not
           *repeatable*. Calling ``submit()`` twice with identical arguments
           and no key creates two jobs. If you need a network-level retry of
           this call to be safe, pass your own ``idempotency_key`` and reuse
           it on the retry.

        :raises ConflictError: with code ``idempotency_conflict`` when this
            key was already used for a materially different request.
        :raises RequestRejectedError: for an unknown queue, an out-of-range
            field, a malformed ``scheduled_at``, or an oversized payload.
        """
        body: dict[str, Any] = {
            "queue": queue,
            "job_type": job_type,
            "payload": payload,
            "priority": priority,
            "max_attempts": max_attempts,
            "timeout_seconds": timeout_seconds,
            "required_capabilities": list(required_capabilities),
        }
        if scheduled_at is not None:
            body["scheduled_at"] = scheduled_at

        response = self._transport.request(
            "POST",
            "/v1/jobs",
            json_body=body,
            idempotency_key=_key_or_new(idempotency_key),
            success=_CREATED_OR_REPLAYED,
        )
        return Job.from_api(response)

    def get(self, job_id: str) -> Job:
        """Read a job's durable lifecycle state.

        ``GET /v1/jobs/{job_id}``. Attempt history is not included.

        :raises NotFoundError: when no such job exists *in this credential's
            scope*. A malformed id and a job in another scope are
            indistinguishable by design.
        """
        response = self._transport.request("GET", f"/v1/jobs/{_path(job_id)}")
        return Job.from_api(response)

    def result(self, job_id: str) -> Any:
        """Read the exact JSON a handler produced for this job.

        ``GET /v1/jobs/{job_id}/result``. Inline and object-backed results
        are served identically (ADR-0015), so this returns the handler's
        output either way.

        There is deliberately no model here: ``api/openapi.yaml`` documents
        the body as "whatever JSON value the job's ``job_type`` produces",
        so the decoded value is returned as-is rather than forced into a
        shape the contract does not promise. That value may legitimately be
        ``None`` if a handler produced a literal ``null``.

        :raises NotFoundError: when the job has not succeeded, succeeded
            with nothing to record, or does not exist in this scope -- all
            three answer identically, for the same anti-oracle reason
            :meth:`get` does.
        """
        return self._transport.request("GET", f"/v1/jobs/{_path(job_id)}/result")

    def cancel(self, job_id: str) -> Cancellation:
        """Cancel a job, or begin withdrawing authority from its attempt.

        ``POST /v1/jobs/{job_id}/cancel``. This needs no idempotency key:
        identity is scope plus job id and nothing else, so cancelling twice
        is not two cancellations, it is one decision observed twice.

        A ``PENDING``, ``QUEUED`` or ``RETRY_WAIT`` job becomes terminal
        ``CANCELED`` immediately. A ``LEASED`` or ``RUNNING`` job becomes
        ``CANCEL_REQUESTED``: the attempt is finalized either by the worker
        acknowledging cooperatively or by reconciliation once the lease
        lapses.

        :raises ConflictError: with code ``job_not_cancelable`` when the job
            is already terminal.
        """
        response = self._transport.request("POST", f"/v1/jobs/{_path(job_id)}/cancel")
        return Cancellation.from_api(response)

    def retry(self, job_id: str, *, idempotency_key: str | None = None) -> Replay:
        """Operator-retry a dead-lettered job.

        ``POST /v1/jobs/{job_id}/retry``. This is the same operation as
        :meth:`taskforge.TaskForgeClient.dlq.replay` and shares one identity
        namespace with it -- scope, original job id, and key -- so the same
        key presented through either route returns the same replacement job
        rather than creating a second one (ADR-0012). Different keys
        deliberately create different replacements.

        The terminal job is never mutated; a new, linked job is created.

        :raises ConflictError: with code ``job_not_dead_lettered`` when the
            job is not in the DLQ.
        """
        response = self._transport.request(
            "POST",
            f"/v1/jobs/{_path(job_id)}/retry",
            idempotency_key=_key_or_new(idempotency_key),
            success=_CREATED_OR_REPLAYED,
        )
        return Replay.from_api(response)


#: api/openapi.yaml bounds the Idempotency-Key header at 1..255 characters.
_MAX_IDEMPOTENCY_KEY_LENGTH = 255


def _key_or_new(key: str | None) -> str:
    """Return the caller's key, validated, or generate one.

    This is the SDK's only idempotency-key generator, and the only place a
    caller-supplied one is checked.

    A key must survive being written into an HTTP header, and it must fit
    the 1..255 bound ``api/openapi.yaml`` documents. A key that cannot be
    encoded as ASCII would otherwise surface as a raw ``UnicodeEncodeError``
    from deep inside the HTTP library -- an exception that is not a
    :class:`~taskforge.errors.TaskForgeError`, escaping a caller's
    ``except TaskForgeError``. Rejecting it here keeps that promise, and
    :class:`~taskforge.errors.ConfigurationError` is exactly the right
    class: the SDK rejected the invocation itself and no request was made.
    """
    if not key:
        return new_idempotency_key()
    if len(key) > _MAX_IDEMPOTENCY_KEY_LENGTH:
        raise ConfigurationError(
            f"idempotency_key must be at most {_MAX_IDEMPOTENCY_KEY_LENGTH} "
            f"characters; got {len(key)}"
        )
    try:
        key.encode("ascii")
    except UnicodeEncodeError as exc:
        raise ConfigurationError(
            "idempotency_key must be ASCII so it can be sent as an HTTP "
            f"header; got {key!r}"
        ) from exc
    if any(ord(c) < 0x20 or ord(c) == 0x7F for c in key):
        raise ConfigurationError(
            f"idempotency_key must not contain control characters; got {key!r}"
        )
    return key


def _path(segment: str) -> str:
    """Percent-encode one path segment.

    An id containing ``?``, ``=``, ``/`` or a space must stay one segment
    rather than silently becoming a different route with a query string.
    """
    return quote(segment, safe="")
