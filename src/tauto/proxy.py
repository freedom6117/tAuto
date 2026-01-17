"""Proxy configuration helpers."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Mapping, MutableMapping, Sequence
import os


@dataclass(frozen=True)
class ProxyConfig:
    enabled: bool
    http: str | None
    https: str | None
    no_proxy: tuple[str, ...]


def _normalize_no_proxy(value: str | Sequence[str] | None) -> tuple[str, ...]:
    if value is None:
        return tuple()
    if isinstance(value, str):
        parts = [segment.strip() for segment in value.split(",")]
    else:
        parts = [str(segment).strip() for segment in value]
    cleaned = [segment for segment in parts if segment]
    return tuple(cleaned)


def load_proxy_config(
    settings: Mapping[str, object] | None = None,
    env: Mapping[str, str] | None = None,
) -> ProxyConfig:
    """Load proxy configuration from settings with environment fallbacks."""

    settings = settings or {}
    env = env or os.environ

    http = _coalesce_setting(settings, env, "http")
    https = _coalesce_setting(settings, env, "https")
    no_proxy = settings.get("no_proxy")
    if no_proxy is None:
        no_proxy = env.get("NO_PROXY") or env.get("no_proxy")

    enabled_setting = settings.get("enabled")
    if enabled_setting is None:
        enabled = bool(http or https)
    else:
        enabled = bool(enabled_setting)

    if not enabled:
        return ProxyConfig(enabled=False, http=None, https=None, no_proxy=tuple())

    return ProxyConfig(
        enabled=True,
        http=http,
        https=https,
        no_proxy=_normalize_no_proxy(no_proxy),
    )


def _coalesce_setting(
    settings: Mapping[str, object],
    env: Mapping[str, str],
    key: str,
) -> str | None:
    value = settings.get(key)
    if isinstance(value, str) and value:
        return value

    env_value = env.get(key.upper() + "_PROXY") or env.get(key + "_proxy")
    return env_value or None


def as_requests_proxies(config: ProxyConfig) -> MutableMapping[str, str]:
    """Convert a proxy config to the requests-compatible proxies mapping."""

    if not config.enabled:
        return {}

    proxies: dict[str, str] = {}
    if config.http:
        proxies["http"] = config.http
    if config.https:
        proxies["https"] = config.https
    return proxies
