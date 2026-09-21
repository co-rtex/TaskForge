"""Where the SDK talks, and which credential it presents.

This module is the Python counterpart of ``internal/cli/config.go``, and it
makes the same decision that file makes, for the same reasons -- with one
deliberate difference, recorded in
``docs/adr/0016-python-sdk-toolchain-and-client-configuration.md``.

**The SDK reads its own environment variables.** It never reads
``TASKFORGE_API_ADDR``, which is ``taskforge-api``'s own *bind* address: a
bare ``host:port`` with no scheme, validated by ``internal/config`` to be a
loopback bind. A bind address and a reachable client target are different
shapes -- a server can bind a wildcard no client can dial, and a client
needs a scheme -- so one name carrying both meanings would make each
reader's interpretation of a shared ``.env`` line depend on which reader it
is.

It also does not read ``taskforge-cli``'s ``TASKFORGE_CLI_API_URL`` /
``TASKFORGE_CLI_API_KEY``. Those are documented in ``internal/cli/config.go``
as "the environment variable **taskforge-cli** reads"; a second,
different-language consumer reading them would falsify that sentence. The
credential half matters more than the address half: a CLI is a process a
developer invokes deliberately, but this is a *library inside someone
else's process*. A developer who exported ``TASKFORGE_CLI_API_KEY`` for
their shell would otherwise have every Python process in that shell
silently acquire that credential -- including a ``TaskForgeClient()`` whose
author believed it had none. Ambient credential pickup is defensible for a
CLI and is not defensible for a library.

The repository's established pattern is already one client-target variable
per consumer: ``TASKFORGE_WORKER_API_URL`` for ``taskforge-worker``,
``TASKFORGE_CLI_API_URL`` for ``taskforge-cli``. The SDK is the third
consumer and takes the third pair of names.
"""

from __future__ import annotations

import os
from urllib.parse import urlsplit

from .errors import ConfigurationError

__all__ = [
    "API_KEY_ENV",
    "API_URL_ENV",
    "DEFAULT_BASE_URL",
    "resolve_api_key",
    "resolve_base_url",
]

#: The address ``taskforge-api`` binds to out of the box. Only the VALUE is
#: shared with ``internal/config``'s default for ``TASKFORGE_API_ADDR``; the
#: variable name deliberately is not (see the module docstring).
DEFAULT_BASE_URL = "http://127.0.0.1:8080"

#: The API address this SDK talks to.
API_URL_ENV = "TASKFORGE_SDK_API_URL"

#: The API key this SDK presents as ``Authorization: Bearer`` on the public
#: ``/v1`` routes. Never sent to the loopback-only ``/internal/v1``
#: key-management routes, which are unauthenticated by design (ADR-0013,
#: ADR-0014).
API_KEY_ENV = "TASKFORGE_SDK_API_KEY"


def resolve_base_url(
    explicit: str | None = None,
    *,
    env: dict[str, str] | None = None,
) -> str:
    """Decide which API address the SDK talks to.

    Precedence is ``explicit`` (the ``base_url`` constructor argument), then
    :data:`API_URL_ENV`, then :data:`DEFAULT_BASE_URL` -- the same order
    ``internal/cli.ResolveBaseURL`` applies to ``--api-url`` and
    ``TASKFORGE_CLI_API_URL``.

    Whichever wins must be an absolute http(s) URL with a host. Anything
    else raises :class:`~taskforge.errors.ConfigurationError` here, before a
    client is ever constructed and before any request is made.

    Loopback is the *default*, not a constraint: ``base_url`` exists so that
    a later non-local deployment changes where the SDK points, not how it
    talks to the API. The server enforces its own loopback posture.
    """
    environ = os.environ if env is None else env

    value = explicit
    source = "base_url"
    if value is None or value == "":
        value = environ.get(API_URL_ENV, "")
        source = API_URL_ENV
    if value == "":
        return DEFAULT_BASE_URL

    parts = urlsplit(value)
    if parts.scheme not in ("http", "https") or not parts.netloc:
        raise ConfigurationError(
            f"{source} must be an absolute http(s) URL, "
            f"for example {DEFAULT_BASE_URL}; got {value!r}"
        )
    return value.rstrip("/")


def resolve_api_key(
    explicit: str | None = None,
    *,
    env: dict[str, str] | None = None,
) -> str | None:
    """Decide which credential the SDK presents on the public routes.

    Precedence is ``explicit`` (the ``api_key`` constructor argument), then
    :data:`API_KEY_ENV`, then ``None``.

    ``None`` is a legitimate configuration: the loopback-only
    ``/internal/v1`` key-management routes need no credential, so an
    unauthenticated client is exactly what mints the first one. A public
    ``/v1`` call made without a credential is not rejected here -- the
    server answers ``401`` and the SDK raises
    :class:`~taskforge.errors.UnauthorizedError`, which keeps "this
    credential was refused" a single, server-owned decision rather than two
    subtly different ones.
    """
    environ = os.environ if env is None else env

    if explicit:
        return explicit
    from_env = environ.get(API_KEY_ENV, "")
    return from_env or None
