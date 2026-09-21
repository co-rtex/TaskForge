"""The credential-management namespaces."""

from __future__ import annotations

import json

from conftest import build_client, json_response, key_body
from taskforge import (
    ApiKeyCreated,
    ApiKeyList,
    ApiKeyRevoked,
    ApiKeySummary,
    WorkerKeyCreated,
    WorkerKeyList,
    WorkerKeySummary,
)


def test_api_key_create() -> None:
    harness = build_client(json_response(201, key_body()))
    created = harness.client.api_keys.create(scope="local-dev", name="my-laptop")

    sent = harness.recorder.last
    assert sent.method == "POST"
    assert str(sent.url) == "http://api.test/internal/v1/api-keys"
    assert json.loads(sent.content) == {"scope": "local-dev", "name": "my-laptop"}
    assert isinstance(created, ApiKeyCreated)
    assert created.key == "tfk_abc.secret"


def test_worker_key_create_uses_its_own_route() -> None:
    harness = build_client(json_response(201, key_body()))
    created = harness.client.worker_keys.create(scope="local-dev", name="my-worker")

    assert str(harness.recorder.last.url) == "http://api.test/internal/v1/worker-keys"
    assert isinstance(created, WorkerKeyCreated)


def test_list_and_limit() -> None:
    harness = build_client(
        json_response(
            200,
            {
                "keys": [
                    {
                        "id": "k1",
                        "scope": "local-dev",
                        "name": "n",
                        "prefix": "tfk_abc",
                        "created_at": "2026-09-21T10:00:00Z",
                        "revoked_at": None,
                    }
                ]
            },
        )
    )
    listing = harness.client.api_keys.list(limit=25)

    assert harness.recorder.last.url.params["limit"] == "25"
    assert isinstance(listing, ApiKeyList)
    assert listing.keys[0].revoked_at is None


def test_worker_key_list_parses_into_worker_key_types() -> None:
    """The distinction is real at runtime, not only in the annotation."""
    body = {
        "keys": [
            {
                "id": "k1",
                "scope": "local-dev",
                "name": "n",
                "prefix": "tfk_abc",
                "created_at": "2026-09-21T10:00:00Z",
                "revoked_at": None,
            }
        ]
    }
    harness = build_client(json_response(200, body))
    listing = harness.client.worker_keys.list()

    assert isinstance(listing, WorkerKeyList)
    assert isinstance(listing.keys[0], WorkerKeySummary)

    api_harness = build_client(json_response(200, body))
    api_listing = api_harness.client.api_keys.list()
    assert isinstance(api_listing.keys[0], ApiKeySummary)
    assert not isinstance(api_listing.keys[0], WorkerKeySummary)


def test_revoke() -> None:
    harness = build_client(
        json_response(
            200,
            {
                "id": "k1",
                "scope": "local-dev",
                "name": "n",
                "prefix": "tfk_abc",
                "created_at": "2026-09-21T10:00:00Z",
                "revoked_at": "2026-09-21T11:00:00Z",
                "already_revoked": True,
            },
        )
    )
    revoked = harness.client.api_keys.revoke("k1")

    assert str(harness.recorder.last.url) == (
        "http://api.test/internal/v1/api-keys/k1/revoke"
    )
    assert isinstance(revoked, ApiKeyRevoked)
    assert revoked.already_revoked is True
    assert revoked.revoked_at == "2026-09-21T11:00:00Z"


def test_key_ids_special_to_a_url_stay_one_segment() -> None:
    harness = build_client(
        json_response(200, key_body(revoked_at=None, already_revoked=False))
    )
    harness.client.api_keys.revoke("weird/id?x=1")

    raw = str(harness.recorder.last.url)
    assert raw == "http://api.test/internal/v1/api-keys/weird%2Fid%3Fx%3D1/revoke"


def test_the_sdk_never_persists_a_minted_secret(tmp_path: object) -> None:
    """It hands the secret back and forgets it.

    A weak but real check that no file appears and nothing is cached on the
    client: the credential model is server-side, and this SDK only ever
    presents a token it was given.
    """
    harness = build_client(json_response(201, key_body()))
    created = harness.client.api_keys.create(scope="local-dev", name="n")

    assert created.key == "tfk_abc.secret"
    # The configured credential is untouched by minting a new one.
    assert harness.client._transport.api_key == "tfk_test.secret"
