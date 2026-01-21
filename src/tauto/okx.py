"""OKX 公共 REST 接口客户端。"""

from __future__ import annotations

from dataclasses import dataclass, field
import random
import time
from typing import Any, Dict, Iterable, List, Optional

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


class OkxApiError(RuntimeError):
    """当 OKX 接口返回非 0 code 时抛出。"""


@dataclass
class OkxClient:
    """OKX 公共 REST 接口客户端。"""

    base_url: str = "https://www.okx.com"
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

    def _compute_backoff(self, attempt: int) -> float:
        base = self.retry_backoff * (2 ** (attempt - 1))
        jitter = random.uniform(0, self.retry_backoff)
        return base + jitter

    def _request(self, path: str, params: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        url = f"{self.base_url}{path}"
        last_error: Exception | None = None
        for attempt in range(1, self.max_retries + 1):
            try:
                response = self.session.get(url, params=params, timeout=self.timeout)
                response.raise_for_status()
                payload = response.json()
                if payload.get("code") != "0":
                    raise OkxApiError(f"OKX API error: {payload}")
                return payload
            except (requests.RequestException, ValueError, OkxApiError) as exc:
                last_error = exc
                if attempt >= self.max_retries:
                    raise
                time.sleep(self._compute_backoff(attempt))
        if last_error:
            raise last_error
        raise RuntimeError("Unexpected request failure without exception.")

    def list_instruments(self, inst_type: str = "SPOT") -> List[Dict[str, Any]]:
        """获取指定类型的交易产品列表（SPOT、SWAP、FUTURES、OPTION）。"""
        payload = self._request("/api/v5/public/instruments", {"instType": inst_type})
        return payload.get("data", [])

    def get_order_book(self, inst_id: str, depth: int = 5) -> Dict[str, Any]:
        """获取指定交易对的盘口数据。"""
        payload = self._request(
            "/api/v5/market/books",
            {"instId": inst_id, "sz": str(depth)},
        )
        data = payload.get("data", [])
        return data[0] if data else {}

    def get_trades(self, inst_id: str, limit: int = 100) -> List[Dict[str, Any]]:
        """获取指定交易对的最新成交数据（用于分时图/成交明细）。"""
        payload = self._request(
            "/api/v5/market/trades",
            {"instId": inst_id, "limit": str(limit)},
        )
        return payload.get("data", [])

    def get_ticker(self, inst_id: str) -> Dict[str, Any]:
        """获取指定交易对的最新行情。"""
        payload = self._request(
            "/api/v5/market/ticker",
            {"instId": inst_id},
        )
        data = payload.get("data", [])
        return data[0] if data else {}

    def get_candlesticks(
        self,
        inst_id: str,
        bar: str = "1m",
        limit: int = 100,
        after: Optional[str] = None,
        before: Optional[str] = None,
        use_history: bool = False,
    ) -> List[List[str]]:
        """获取指定交易对的 K 线数据，支持不同周期，用于绘制 K 线。"""
        params: Dict[str, Any] = {"instId": inst_id, "bar": bar, "limit": str(limit)}
        if after:
            params["after"] = after
        if before:
            params["before"] = before
        path = "/api/v5/market/history-candles" if use_history else "/api/v5/market/candles"
        payload = self._request(path, params)
        return payload.get("data", [])


def summarize_instruments(instruments: Iterable[Dict[str, Any]]) -> List[str]:
    """将交易产品列表转换为可读的 instId 列表。"""
    return [instrument.get("instId", "") for instrument in instruments]


__all__ = ["OkxApiError", "OkxClient", "summarize_instruments"]
