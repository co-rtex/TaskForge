"""The ``client.api_keys`` and ``client.worker_keys`` namespaces.

Every route here is on the **loopback-only** ``/internal/v1`` surface and is
itself unauthenticated by design (ADR-0013, ADR-0014) -- that is how the
first credential of either kind comes into existence. They are reached
through :meth:`Transport.request_unauthenticated`, so a credential
configured for the public routes is never presented to them.

This SDK obtains and presents credentials. It generates none, parses none,
verifies none, and persists none: the credential model is implemented
server-side in ``internal/auth`` and ``internal/workerauth``.
"""

from __future__ import annotations

from typing import Any
from urllib.parse import quote

from ._transport import Transport
from .models import (
    ApiKeyCreated,
    ApiKeyList,
    ApiKeyRevoked,
    WorkerKeyCreated,
    WorkerKeyList,
    WorkerKeyRevoked,
)

__all__ = ["ApiKeys", "WorkerKeys"]


class _KeyNamespace:
    """Shared implementation. The two routes differ only in their path."""

    _base_path: str

    def __init__(self, transport: Transport) -> None:
        self._transport = transport

    def _create(self, scope: str, name: str) -> Any:
        return self._transport.request_unauthenticated(
            "POST",
            self._base_path,
            json_body={"scope": scope, "name": name},
            success=(201,),
        )

    def _list(self, limit: int | None) -> Any:
        params: dict[str, Any] = {}
        if limit is not None:
            params["limit"] = limit
        return self._transport.request_unauthenticated(
            "GET", self._base_path, params=params or None
        )

    def _revoke(self, key_id: str) -> Any:
        return self._transport.request_unauthenticated(
            "POST", f"{self._base_path}/{quote(key_id, safe='')}/revoke"
        )


class ApiKeys(_KeyNamespace):
    """Mint, list, and revoke the credentials for the public ``/v1`` routes."""

    _base_path = "/internal/v1/api-keys"

    def create(self, *, scope: str, name: str) -> ApiKeyCreated:
        """Mint an API key.

        ``POST /internal/v1/api-keys``. ``scope`` is the tenancy boundary
        the key acts within -- every job and dead-letter entry it can reach
        is filtered by it. ``name`` is an operator-facing label only;
        nothing authenticates against it.

        The returned :attr:`~taskforge.models.ApiKeyCreated.key` is the
        complete credential and is returned **exactly once**. It is not
        stored server-side, cannot be recovered, and this SDK does not
        persist it for you.

        .. warning::
           Key creation carries no idempotency identity. If this call fails
           with :class:`~taskforge.errors.ServiceUnavailableError`, a blind
           retry may mint a *second* credential. Check
           :meth:`list` before retrying.
        """
        return ApiKeyCreated.from_api(self._create(scope, name))

    def list(self, *, limit: int | None = None) -> ApiKeyList:
        """List key summaries. Never returns secrets.

        ``GET /internal/v1/api-keys``. ``limit`` is 1..200; omitting it
        applies the server's own default.
        """
        return ApiKeyList.from_api(self._list(limit))

    def revoke(self, key_id: str) -> ApiKeyRevoked:
        """Revoke a key by its id.

        ``POST /internal/v1/api-keys/{key_id}/revoke``. Revocation takes
        effect on the next request. Revoking an already-revoked key is not
        an error: :attr:`~taskforge.models.ApiKeyRevoked.already_revoked`
        reports it, and ``revoked_at`` stays the original instant.
        """
        return ApiKeyRevoked.from_api(self._revoke(key_id))


class WorkerKeys(_KeyNamespace):
    """Mint, list, and revoke the credentials that register worker sessions.

    A worker key authenticates ``taskforge-worker``'s registration only;
    every later worker-control call trusts the registered session plus a
    cheap revocation check (ADR-0014). Only a worker whose key names a scope
    can claim jobs submitted under an API key of that same scope.
    """

    _base_path = "/internal/v1/worker-keys"

    def create(self, *, scope: str, name: str) -> WorkerKeyCreated:
        """Mint a worker key.

        ``POST /internal/v1/worker-keys``. Carries the same one-time-secret
        and no-idempotency-identity caveats as
        :meth:`ApiKeys.create`.
        """
        return WorkerKeyCreated.from_api(self._create(scope, name))

    def list(self, *, limit: int | None = None) -> WorkerKeyList:
        """List worker-key summaries. Never returns secrets."""
        return WorkerKeyList.from_api(self._list(limit))

    def revoke(self, key_id: str) -> WorkerKeyRevoked:
        """Revoke a worker key by its id.

        Once revoked, every worker-control route is refused for sessions
        registered with it, on their next call.
        """
        return WorkerKeyRevoked.from_api(self._revoke(key_id))
