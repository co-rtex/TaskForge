"""HTTP plumbing: the one place a request is built, sent, and classified.

This is the Python counterpart of ``internal/cli/client.go``. It keeps that
file's two load-bearing properties:

* **Two request paths, not one.** :meth:`Transport.request` presents the
  configured credential; :meth:`Transport.request_unauthenticated` never
  does. The loopback-only ``/internal/v1/api-keys`` and
  ``/internal/v1/worker-keys`` routes are unauthenticated by design
  (ADR-0013, ADR-0014), and a client presenting a secret to a route that
  does not check it is doing the wrong thing even though the server would
  silently ignore it.
* **No retry logic and no credential handling beyond carrying a token.**
  TaskForge's credential model is implemented server-side in
  ``internal/auth`` and ``internal/workerauth``. This SDK never generates,
  parses, verifies, or persists key material -- it only ever presents a
  token it was handed.

It is also the single place the ``Idempotency-Key`` header is set. No
resource module assembles that header itself.
"""

from __future__ import annotations

import json
import uuid
from typing import Any

import httpx

from .errors import (
    APIError,
    TransportError,
    UnexpectedResponseError,
    exception_for_api_error,
)

__all__ = ["DEFAULT_TIMEOUT_SECONDS", "Transport", "new_idempotency_key"]

#: Bounds a single SDK HTTP call. Deliberately independent of
#: ``TASKFORGE_API_REQUEST_TIMEOUT``, the server's own per-request budget:
#: a client outside the server process must bound its own wait regardless of
#: how the server it happens to be talking to is configured. Matches
#: ``internal/cli.DefaultRequestTimeout``.
DEFAULT_TIMEOUT_SECONDS = 30.0


def new_idempotency_key() -> str:
    """Generate an idempotency key.

    A random RFC 4122 version 4 UUID in the canonical lowercase hyphenated
    form -- 36 characters, comfortably inside the header's documented 1..255
    length -- matching ``taskforge-cli``'s own ``uuid.NewString()`` default
    so the two clients generate indistinguishable keys.
    """
    return str(uuid.uuid4())


class Transport:
    """Builds, sends, and classifies one HTTP request at a time."""

    def __init__(
        self,
        base_url: str,
        api_key: str | None,
        *,
        timeout: float = DEFAULT_TIMEOUT_SECONDS,
        http_client: httpx.Client | None = None,
    ) -> None:
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self._owns_client = http_client is None
        self._client = http_client or httpx.Client(timeout=timeout)

    def close(self) -> None:
        """Close the underlying HTTP client, if this transport owns it."""
        if self._owns_client:
            self._client.close()

    # -- request paths -----------------------------------------------------

    def request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: Any = None,
        idempotency_key: str | None = None,
        success: tuple[int, ...] = (200,),
    ) -> Any:
        """Issue an authenticated request against a public ``/v1`` route."""
        return self._send(
            method,
            path,
            params=params,
            json_body=json_body,
            idempotency_key=idempotency_key,
            success=success,
            authenticate=True,
        )

    def request_unauthenticated(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        json_body: Any = None,
        success: tuple[int, ...] = (200,),
    ) -> Any:
        """Issue a request against a loopback-only ``/internal/v1`` route.

        The configured credential is never attached, even when one is set
        for the public routes in the same client.
        """
        return self._send(
            method,
            path,
            params=params,
            json_body=json_body,
            idempotency_key=None,
            success=success,
            authenticate=False,
        )

    # -- the single send path ---------------------------------------------

    def _send(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None,
        json_body: Any,
        idempotency_key: str | None,
        success: tuple[int, ...],
        authenticate: bool,
    ) -> Any:
        headers: dict[str, str] = {"Accept": "application/json"}
        if authenticate and self.api_key:
            headers["Authorization"] = f"Bearer {self.api_key}"
        # The canonical header, set here and nowhere else. It is never
        # placed in the body and never in the query string -- see
        # docs/PROJECT_SPEC.md section 4: "If an SDK also exposes the key as
        # a method argument, the SDK places it in that canonical header."
        if idempotency_key is not None:
            headers["Idempotency-Key"] = idempotency_key

        try:
            response = self._client.request(
                method,
                self.base_url + path,
                params=params,
                json=json_body,
                headers=headers,
            )
        except httpx.TimeoutException as exc:
            # A client-side timeout never reached a durable outcome. It is
            # NOT ServiceUnavailableError, which means the API was reached
            # and reported that ITS OWN deadline elapsed.
            raise TransportError(
                f"the request to {self.base_url}{path} timed out: {exc}", cause=exc
            ) from exc
        except httpx.HTTPError as exc:
            raise TransportError(
                f"could not reach the API at {self.base_url}: {exc}", cause=exc
            ) from exc

        return self._classify(response, success)

    def _classify(self, response: httpx.Response, success: tuple[int, ...]) -> Any:
        # `decoded is None` is NOT a decode failure: GET /v1/jobs/{id}/result
        # returns whatever JSON value a handler produced, and a literal
        # `null` decodes to None legitimately. Decode success is tracked
        # separately rather than inferred from the value.
        decoded: Any = None
        decoded_ok = True
        try:
            decoded = response.json()
        except (json.JSONDecodeError, ValueError):
            decoded_ok = False

        if response.status_code in success:
            if not decoded_ok:
                raise UnexpectedResponseError(
                    code="unexpected_response",
                    message=(
                        "the API returned a success status with a body that is "
                        "not valid JSON"
                    ),
                    http_status=response.status_code,
                    raw=response.text,
                )
            return decoded

        raise self._error_for(response, decoded if decoded_ok else None)

    def _error_for(self, response: httpx.Response, decoded: Any) -> APIError:
        envelope = decoded.get("error") if isinstance(decoded, dict) else None
        if not isinstance(envelope, dict) or not envelope.get("code"):
            return UnexpectedResponseError(
                code="unexpected_response",
                message=(
                    "the API returned a response this SDK version does not recognize"
                ),
                http_status=response.status_code,
                raw=decoded if decoded is not None else response.text,
            )

        code = str(envelope["code"])
        message = str(envelope.get("message", ""))
        request_id = envelope.get("request_id")
        details = envelope.get("details")
        exception_class = exception_for_api_error(code)
        return exception_class(
            code=code,
            message=message,
            http_status=response.status_code,
            request_id=str(request_id) if request_id is not None else None,
            details=details if isinstance(details, list) else None,
            raw=decoded,
        )
