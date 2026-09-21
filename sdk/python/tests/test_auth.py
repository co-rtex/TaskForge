"""The credential boundary.

`internal/cli/client.go` splits its request paths in two -- `do` presents the
credential, `doUnauthenticated` never does -- because the loopback-only
key-management routes are unauthenticated by design (ADR-0013, ADR-0014) and
a client presenting a secret to a route that does not check it is wrong even
though the server would ignore it. These tests pin the same split here.
"""

from __future__ import annotations

import pytest

from conftest import build_client, job_body, json_response, key_body

PUBLIC_ROUTES_SEND_AUTH = "Authorization"


def test_bearer_is_sent_on_public_routes_when_a_key_is_configured() -> None:
    harness = build_client(json_response(200, job_body()), api_key="tfk_abc.secret")
    harness.client.jobs.get("job-1")

    assert harness.recorder.last.headers["Authorization"] == "Bearer tfk_abc.secret"


def test_bearer_is_withheld_when_no_key_is_configured() -> None:
    harness = build_client(json_response(200, job_body()), api_key=None)
    harness.client.jobs.get("job-1")

    assert "Authorization" not in harness.recorder.last.headers


@pytest.mark.parametrize(
    "call",
    [
        "api_keys.create",
        "api_keys.list",
        "api_keys.revoke",
        "worker_keys.create",
        "worker_keys.list",
        "worker_keys.revoke",
    ],
)
def test_key_management_routes_never_receive_the_credential(call: str) -> None:
    """Even when one IS configured for the public routes in the same client.

    This is the test that caught the equivalent real bug in taskforge-cli's
    own review cycle.
    """
    namespace, verb = call.split(".")
    if verb == "create":
        handler = json_response(201, key_body())
    elif verb == "list":
        handler = json_response(200, {"keys": []})
    else:
        handler = json_response(
            200, key_body(revoked_at="2026-09-21T11:00:00Z", already_revoked=False)
        )

    harness = build_client(handler, api_key="tfk_configured.secret")
    target = getattr(getattr(harness.client, namespace), verb)
    if verb == "create":
        target(scope="local-dev", name="k")
    elif verb == "list":
        target()
    else:
        target("key-1")

    sent = harness.recorder.last
    assert "Authorization" not in sent.headers, f"{call} leaked a credential"
    assert "/internal/v1/" in str(sent.url)


def test_public_and_internal_paths_coexist_in_one_client() -> None:
    """One client, one configured key: public gets it, internal does not."""

    def handler(request):  # type: ignore[no-untyped-def]
        import httpx

        if "/internal/v1/" in str(request.url):
            return httpx.Response(200, json={"keys": []})
        return httpx.Response(200, json=job_body())

    harness = build_client(handler, api_key="tfk_abc.secret")
    harness.client.jobs.get("job-1")
    harness.client.api_keys.list()

    public, internal = harness.recorder.requests
    assert public.headers["Authorization"] == "Bearer tfk_abc.secret"
    assert "Authorization" not in internal.headers
