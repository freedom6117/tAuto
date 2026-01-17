from __future__ import annotations

import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from tauto.proxy import ProxyConfig, as_requests_proxies, load_proxy_config  # noqa: E402


def test_load_proxy_config_from_settings() -> None:
    config = load_proxy_config(
        {
            "enabled": True,
            "http": "http://proxy.local:8080",
            "https": "https://proxy.local:8443",
            "no_proxy": "localhost, 127.0.0.1",
        }
    )

    assert config == ProxyConfig(
        enabled=True,
        http="http://proxy.local:8080",
        https="https://proxy.local:8443",
        no_proxy=("localhost", "127.0.0.1"),
    )


def test_load_proxy_config_falls_back_to_env() -> None:
    env = {
        "HTTP_PROXY": "http://env.proxy:8080",
        "HTTPS_PROXY": "https://env.proxy:8443",
        "NO_PROXY": "internal.local",
    }

    config = load_proxy_config({}, env=env)

    assert config.enabled is True
    assert config.http == "http://env.proxy:8080"
    assert config.https == "https://env.proxy:8443"
    assert config.no_proxy == ("internal.local",)


def test_load_proxy_config_disabled() -> None:
    config = load_proxy_config({"enabled": False, "http": "http://proxy.local"})

    assert config.enabled is False
    assert config.http is None
    assert config.https is None
    assert config.no_proxy == tuple()


def test_as_requests_proxies() -> None:
    config = ProxyConfig(
        enabled=True,
        http="http://proxy.local:8080",
        https=None,
        no_proxy=tuple(),
    )

    assert as_requests_proxies(config) == {"http": "http://proxy.local:8080"}


@pytest.mark.parametrize(
    "value, expected",
    [
        ("host1, host2", ("host1", "host2")),
        (["host1", "host2"], ("host1", "host2")),
        ("", tuple()),
    ],
)
def test_no_proxy_normalization(value, expected) -> None:
    env = {"HTTP_PROXY": "http://env.proxy:8080", "NO_PROXY": ""}

    config = load_proxy_config({"no_proxy": value}, env=env)

    assert config.no_proxy == expected
