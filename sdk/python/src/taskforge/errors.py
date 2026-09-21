"""The SDK's exception taxonomy.

This is the Python expression of ``internal/cli/exitcode.go``'s exit-code
contract. That file groups failures by **remediation** -- what a caller's
code should actually do differently -- rather than by HTTP status or by
``api/openapi.yaml``'s ``Error.code`` enum one-for-one. This module keeps
exactly the same grouping, so one table reviews both clients:

============================  =========  ====================================
Exception                     exit_code  ``Error.code`` values it covers
============================  =========  ====================================
(no exception; a return)      0          --
:class:`ConfigurationError`   1          none -- raised before any request
:class:`RequestRejectedError` 2          ``malformed_json``,
                                         ``payload_too_large``,
                                         ``validation_failed``,
                                         ``invalid_cursor``
:class:`UnauthorizedError`    3          ``unauthorized``
:class:`NotFoundError`        4          ``not_found``
:class:`ConflictError`        5          ``idempotency_conflict``,
                                         ``job_not_cancelable``,
                                         ``job_not_dead_lettered``
:class:`InternalServerError`  6          ``internal_error``
:class:`ServiceUnavailableError` 7       ``service_unavailable``
:class:`TransportError`       8          none -- the API was never reached
:class:`UnexpectedResponseError` 9       anything outside the set above
============================  =========  ====================================

``exit_code`` exists so a script wrapping this SDK can ``sys.exit(exc.exit_code)``
and produce output a ``taskforge-cli`` consumer already understands. The
numbers are pinned to ``internal/cli/exitcode.go`` by
``tests/test_errors.py::test_exit_codes_match_the_cli_contract``; the two
tables are hand-maintained copies of one mapping, and that test is what
stops them drifting.

:class:`ConflictError` deliberately has no per-code subclasses. The CLI
groups those three codes because "retrying the identical request will not
help" is what they share, while the remediation differs; a caller branches
on :attr:`APIError.code` for that, which carries strictly more information
than a class split would.
"""

from __future__ import annotations

from typing import Any

__all__ = [
    "APIError",
    "ConfigurationError",
    "ConflictError",
    "InternalServerError",
    "NotFoundError",
    "RequestRejectedError",
    "ServiceUnavailableError",
    "TaskForgeError",
    "TransportError",
    "UnauthorizedError",
    "UnexpectedResponseError",
    "exception_for_api_error",
]


class TaskForgeError(Exception):
    """Base class for every error this SDK raises.

    ``except TaskForgeError`` catches everything the SDK can raise on its
    own behalf, and nothing else.
    """

    #: The ``taskforge-cli`` exit code this failure class corresponds to.
    #: ``0`` is never used: success is a normal return, never an exception.
    exit_code = 1


class ConfigurationError(TaskForgeError):
    """The SDK rejected its own configuration; no request was made.

    The analogue of ``ExitUsageError``. Raised for a ``base_url`` that is
    not an absolute http(s) URL, and for a method argument this SDK can
    reject without asking the server.
    """

    exit_code = 1


class TransportError(TaskForgeError):
    """The request never produced an HTTP response.

    DNS failure, connection refused, TLS failure, or a client-side timeout.
    The analogue of ``ExitTransportError``.

    This is deliberately distinct from :class:`ServiceUnavailableError`,
    which means the API *was* reached and itself reported that its own
    deadline elapsed. ``internal/cli/exitcode.go`` keeps 8 and 7 apart for
    the same reason: the remediations are different, and collapsing them
    sends an operator looking in the wrong place.
    """

    exit_code = 8

    def __init__(self, message: str, *, cause: BaseException | None = None) -> None:
        super().__init__(message)
        #: The underlying ``httpx`` exception, kept for diagnosis.
        self.cause = cause


class APIError(TaskForgeError):
    """The API answered, and the answer was a failure.

    Every subclass carries the server's own error envelope, unmodified.
    Branch on :attr:`code`, not on :attr:`message` -- ``api/openapi.yaml``
    says so explicitly, and the message is prose the server may reword.
    """

    exit_code = 6

    def __init__(
        self,
        *,
        code: str,
        message: str,
        http_status: int,
        request_id: str | None = None,
        details: list[dict[str, Any]] | None = None,
        raw: Any = None,
    ) -> None:
        super().__init__(f"{code}: {message}")
        #: The server's stable machine-readable ``Error.code``.
        self.code = code
        #: The server's human-readable message, passed through verbatim.
        self.message = message
        #: The HTTP status that carried this error.
        self.http_status = http_status
        #: The server's ``request_id``, when it sent one. It locates the
        #: cause in the server's own logs.
        self.request_id = request_id
        #: Field-level problems, when the server reported any.
        self.details = details or []
        #: The untouched decoded response body. Nothing the server sent is
        #: lost, including fields this SDK version does not know about.
        self.raw = raw


class RequestRejectedError(APIError):
    """The API rejected this specific request as malformed or invalid.

    Covers ``malformed_json``, ``payload_too_large``, ``validation_failed``
    and ``invalid_cursor``. All four share one remediation -- send a
    *different* request -- which is what makes them one class; retrying the
    identical request will fail identically every time.
    """

    exit_code = 2


class UnauthorizedError(APIError):
    """The presented credential, or its absence, was refused.

    A missing header, a malformed credential, an unknown key and a revoked
    key are indistinguishable by design (ADR-0013): telling them apart would
    make a lookup prefix an oracle. This SDK preserves that -- there is no
    richer sub-taxonomy here, because the server deliberately does not
    provide one.
    """

    exit_code = 3


class NotFoundError(APIError):
    """The named resource does not exist within the credential's scope.

    A malformed id, a resource in another scope, and a resource with nothing
    recorded are deliberately indistinguishable, so this class alone does
    not tell a caller which it was.
    """

    exit_code = 4


class ConflictError(APIError):
    """The operation cannot be applied given the resource's current state.

    Covers ``idempotency_conflict``, ``job_not_cancelable`` and
    ``job_not_dead_lettered``. The remediation differs per code -- present a
    fresh idempotency key, or inspect the job's current status -- so branch
    on :attr:`APIError.code`. What all three share, and what makes them one
    class, is that retrying the identical request will not help.
    """

    exit_code = 5


class InternalServerError(APIError):
    """The API's own request failed for a reason it sanitizes.

    Use :attr:`APIError.request_id` to locate the cause in the server's
    logs. This is a server-side problem, not a caller mistake, and is not
    necessarily safe to retry.
    """

    exit_code = 6


class ServiceUnavailableError(APIError):
    """The request's own server-side deadline elapsed before its outcome was known.

    Most endpoints document this as unconditionally safe to repeat, but
    ``POST /internal/v1/api-keys`` and ``POST /internal/v1/worker-keys``
    explicitly are not: key creation carries no idempotency identity, so a
    blind retry mints a second credential. The exception class alone does
    not say which; :attr:`APIError.message`, passed through verbatim from
    the server, does.
    """

    exit_code = 7


class UnexpectedResponseError(APIError):
    """The API returned something this SDK version does not recognize.

    An HTTP status this operation does not document, a body that is not the
    JSON it should be, an ``Error.code`` outside the reachable set, a
    response missing a field ``api/openapi.yaml`` marks required, or a
    ``status`` value outside :class:`~taskforge.models.JobStatus`.

    It signals a version mismatch between this SDK and the server, and is
    deliberately never a fallback for a recognized failure that simply was
    not handled: every ``Error.code`` a route this SDK calls can actually
    return is enumerated in :data:`API_ERROR_EXCEPTIONS`.
    """

    exit_code = 9


#: Every ``api/openapi.yaml`` ``Error.code`` a route this SDK calls can
#: actually return, mapped to the exception it raises. Exhaustive for that
#: reachable set -- not for the API's full 23-value enum, most of which
#: belongs to the worker-control surface this SDK never calls
#: (``worker_session_conflict``, ``worker_session_unavailable``,
#: ``claim_conflict``, ``fence_rejected``, ``lease_expired``,
#: ``state_conflict``, ``renewal_conflict``, ``attempt_timed_out``,
#: ``outcome_conflict``, ``cancellation_requested``) or to cases no
#: SDK-invoked route documents (``unknown_queue``, ``method_not_allowed``)
#: -- twelve unreachable values against the eleven reachable ones below.
#:
#: This mirrors ``internal/cli/exitcode.go``'s ``apiErrorExitCodes`` exactly,
#: and ``tests/test_errors.py`` pins it so it cannot silently drift.
API_ERROR_EXCEPTIONS: dict[str, type[APIError]] = {
    "malformed_json": RequestRejectedError,
    "payload_too_large": RequestRejectedError,
    "validation_failed": RequestRejectedError,
    "invalid_cursor": RequestRejectedError,
    "unauthorized": UnauthorizedError,
    "not_found": NotFoundError,
    "idempotency_conflict": ConflictError,
    "job_not_cancelable": ConflictError,
    "job_not_dead_lettered": ConflictError,
    "internal_error": InternalServerError,
    "service_unavailable": ServiceUnavailableError,
}


def exception_for_api_error(code: str) -> type[APIError]:
    """Resolve a server-reported ``Error.code`` to its exception class.

    A code outside :data:`API_ERROR_EXCEPTIONS` resolves to
    :class:`UnexpectedResponseError` rather than to a guess at a meaning
    this SDK was not built to recognize.
    """
    return API_ERROR_EXCEPTIONS.get(code, UnexpectedResponseError)
