"""Address and credential resolution, and the environment-variable boundary."""

from __future__ import annotations

import pytest

from taskforge import (
    API_KEY_ENV,
    API_URL_ENV,
    DEFAULT_BASE_URL,
    ConfigurationError,
    TaskForgeClient,
    resolve_api_key,
    resolve_base_url,
)


def test_explicit_argument_wins() -> None:
    env = {API_URL_ENV: "http://from-env:9000"}
    assert resolve_base_url("http://explicit:8000", env=env) == "http://explicit:8000"


def test_environment_is_the_fallback() -> None:
    env = {API_URL_ENV: "http://from-env:9000"}
    assert resolve_base_url(None, env=env) == "http://from-env:9000"


def test_loopback_default_when_nothing_is_set() -> None:
    assert resolve_base_url(None, env={}) == DEFAULT_BASE_URL


def test_trailing_slash_is_trimmed() -> None:
    assert resolve_base_url("http://api.test/", env={}) == "http://api.test"


@pytest.mark.parametrize(
    "bad",
    ["127.0.0.1:8080", "ftp://api.test", "http://", "not a url", "/v1/jobs"],
)
def test_a_non_absolute_http_url_is_a_configuration_error(bad: str) -> None:
    with pytest.raises(ConfigurationError):
        resolve_base_url(bad, env={})


def test_the_error_names_the_source_it_came_from() -> None:
    with pytest.raises(ConfigurationError) as explicit:
        resolve_base_url("nope", env={})
    assert "base_url" in str(explicit.value)

    with pytest.raises(ConfigurationError) as from_env:
        resolve_base_url(None, env={API_URL_ENV: "nope"})
    assert API_URL_ENV in str(from_env.value)


def test_api_key_resolution() -> None:
    assert resolve_api_key("explicit", env={API_KEY_ENV: "from-env"}) == "explicit"
    assert resolve_api_key(None, env={API_KEY_ENV: "from-env"}) == "from-env"
    assert resolve_api_key(None, env={}) is None
    assert resolve_api_key(None, env={API_KEY_ENV: ""}) is None


# --- the boundary this milestone's review turned on ----------------------


def test_the_sdk_reads_none_of_the_other_binaries_variables(
    monkeypatch: pytest.MonkeyPatch, clean_env: None
) -> None:
    """Not TASKFORGE_API_ADDR (a bind address), and not the CLI's variables.

    All three are set to poison values. A client that read any of them would
    resolve to something other than the loopback default.
    """
    monkeypatch.setenv("TASKFORGE_API_ADDR", "0.0.0.0:9999")
    monkeypatch.setenv("TASKFORGE_CLI_API_URL", "http://cli-poison:9999")
    monkeypatch.setenv("TASKFORGE_CLI_API_KEY", "tfk_cli_poison.secret")
    monkeypatch.setenv("TASKFORGE_WORKER_API_URL", "http://worker-poison:9999")

    client = TaskForgeClient()
    try:
        assert client.base_url == DEFAULT_BASE_URL
        assert resolve_api_key(None) is None
    finally:
        client.close()


def test_the_sdks_own_variables_are_read(
    monkeypatch: pytest.MonkeyPatch, clean_env: None
) -> None:
    monkeypatch.setenv(API_URL_ENV, "http://sdk-target:8100")
    monkeypatch.setenv(API_KEY_ENV, "tfk_sdk.secret")

    client = TaskForgeClient()
    try:
        assert client.base_url == "http://sdk-target:8100"
        assert resolve_api_key(None) == "tfk_sdk.secret"
    finally:
        client.close()


def test_env_var_names_are_sdk_specific() -> None:
    assert API_URL_ENV == "TASKFORGE_SDK_API_URL"
    assert API_KEY_ENV == "TASKFORGE_SDK_API_KEY"


def test_a_bad_url_raises_before_the_client_exists(clean_env: None) -> None:
    with pytest.raises(ConfigurationError):
        TaskForgeClient(base_url="127.0.0.1:8080")
