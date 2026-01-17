"""Lightweight OKX REST client for public endpoints."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Iterable, List, Optional

import requests


@dataclass(frozen=True)
class OkxClient:
    """Client for OKX public REST endpoints."""

    base_url: str = "https://www.okx.com"
    timeout: float = 10.0

    def _request(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        response = requests.get(url, params=params, timeout=self.timeout)
        response.raise_for_status()
        payload = response.json()
        if payload.get("code") != "0":
            raise RuntimeError(f"OKX API error: {payload}")
        return payload

    def list_instruments(self, inst_type: str = "SPOT") -> List[Dict[str, Any]]:
        """Return all instruments for a given type (SPOT, SWAP, FUTURES, OPTION)."""
        payload = self._request("/api/v5/public/instruments", {"instType": inst_type})
        return payload.get("data", [])

    def get_order_book(self, inst_id: str, depth: int = 5) -> Dict[str, Any]:
        """Return order book data for an instrument."""
        payload = self._request(
            "/api/v5/market/books",
            {"instId": inst_id, "sz": str(depth)},
        )
        data = payload.get("data", [])
        return data[0] if data else {}

    def get_trades(self, inst_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """Return recent trades for an instrument."""
        payload = self._request(
            "/api/v5/market/trades",
            {"instId": inst_id, "limit": str(limit)},
        )
        return payload.get("data", [])


def summarize_instruments(instruments: Iterable[Dict[str, Any]]) -> List[str]:
    """Create a compact human-friendly list of instrument identifiers."""
    return [instrument.get("instId", "") for instrument in instruments]


__all__ = ["OkxClient", "summarize_instruments"]
