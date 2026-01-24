"""Binance public REST API client for spot market data."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class BinanceApiError(RuntimeError):
    """Raised when Binance API returns an error response."""


@dataclass
class BinanceClient:
    """Binance public REST API client (spot)."""

    base_url: str = "https://api.binance.com"
    timeout: float = 10.0
    max_retries: int = 3
    retry_backoff: float = 0.5
    session: requests.Session = field(default_factory=requests.Session, init=False, repr=False)

    def __post_init__(self) -> None:
        retry = Retry(
            total=max(self.max_retries - 1, 0),
            connect=max(self.max_retries - 1, 0),
            read=max(self.max_retries - 1, 0),
            status=max(self.max_retries - 1, 0),
            backoff_factor=self.retry_backoff,
            status_forcelist=(429, 500, 502, 503, 504),
            allowed_methods=("GET",),
            raise_on_status=False,
        )
        adapter = HTTPAdapter(max_retries=retry)
        self.session.mount("https://", adapter)
        self.session.mount("http://", adapter)

    def _request(self, path: str, params: Optional[Dict[str, Any]] = None) -> Any:
        url = f"{self.base_url}{path}"
        response = self.session.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        payload = response.json()
        if isinstance(payload, dict) and payload.get("code") is not None:
            raise BinanceApiError(f"Binance API error: {payload}")
        return payload

    def get_klines(
        self,
        symbol: str,
        interval: str,
        limit: int = 500,
        start_time: Optional[int] = None,
        end_time: Optional[int] = None,
    ) -> List[List[Any]]:
        params: Dict[str, Any] = {"symbol": symbol, "interval": interval, "limit": str(limit)}
        if start_time is not None:
            params["startTime"] = str(start_time)
        if end_time is not None:
            params["endTime"] = str(end_time)
        payload = self._request("/api/v3/klines", params)
        return payload if isinstance(payload, list) else []

    def get_ticker(self, symbol: str) -> Dict[str, Any]:
        payload = self._request("/api/v3/ticker/price", {"symbol": symbol})
        return payload if isinstance(payload, dict) else {}

    def get_order_book(self, symbol: str, limit: int = 20) -> Dict[str, Any]:
        payload = self._request("/api/v3/depth", {"symbol": symbol, "limit": str(limit)})
        return payload if isinstance(payload, dict) else {}


__all__ = ["BinanceApiError", "BinanceClient"]
