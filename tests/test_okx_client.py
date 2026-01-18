from __future__ import annotations

import sys
from pathlib import Path
from typing import Any
from unittest.mock import MagicMock, patch

import pytest
import requests

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "src"))

from tauto.okx import OkxApiError, OkxClient  # noqa: E402


class DummyResponse:
    def __init__(self, payload: dict[str, Any], status_code: int = 200) -> None:
        self._payload = payload
        self.status_code = status_code

    def raise_for_status(self) -> None:
        if self.status_code >= 400:
            raise requests.HTTPError("bad response")

    def json(self) -> dict[str, Any]:
        return self._payload


def test_list_instruments() -> None:
    response = DummyResponse({"code": "0", "data": [{"instId": "BTC-USDT"}]})

    with patch("requests.get", return_value=response) as mock_get:
        client = OkxClient()
        instruments = client.list_instruments("SPOT")

    assert instruments == [{"instId": "BTC-USDT"}]
    mock_get.assert_called_once_with(
        "https://www.okx.com/api/v5/public/instruments",
        params={"instType": "SPOT"},
        timeout=client.timeout,
    )


def test_get_order_book() -> None:
    response = DummyResponse({"code": "0", "data": [{"bids": [["1", "2"]]}]})

    with patch("requests.get", return_value=response):
        client = OkxClient()
        order_book = client.get_order_book("BTC-USDT", depth=10)

    assert order_book == {"bids": [["1", "2"]]}


def test_get_trades() -> None:
    response = DummyResponse({"code": "0", "data": [{"tradeId": "1"}]})

    with patch("requests.get", return_value=response):
        client = OkxClient()
        trades = client.get_trades("BTC-USDT", limit=50)

    assert trades == [{"tradeId": "1"}]


def test_retries_on_request_failure() -> None:
    good_response = DummyResponse({"code": "0", "data": []})
    mock_get = MagicMock(side_effect=[requests.RequestException("boom"), good_response])

    with patch("requests.get", mock_get), patch("time.sleep") as mock_sleep:
        client = OkxClient(max_retries=2, retry_backoff=0.1)
        result = client.list_instruments()

    assert result == []
    assert mock_get.call_count == 2
    mock_sleep.assert_called_once_with(0.1)


def test_retries_on_api_error() -> None:
    error_response = DummyResponse({"code": "500", "msg": "fail"})
    good_response = DummyResponse({"code": "0", "data": []})
    mock_get = MagicMock(side_effect=[error_response, good_response])

    with patch("requests.get", mock_get), patch("time.sleep") as mock_sleep:
        client = OkxClient(max_retries=2, retry_backoff=0.2)
        result = client.list_instruments()

    assert result == []
    mock_sleep.assert_called_once_with(0.2)


def test_api_error_raised_after_retries() -> None:
    error_response = DummyResponse({"code": "500", "msg": "fail"})

    with patch("requests.get", return_value=error_response):
        client = OkxClient(max_retries=1)
        with pytest.raises(OkxApiError):
            client.list_instruments()
