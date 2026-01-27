"""FastAPI service for K-line chart data and static UI."""

from __future__ import annotations

import logging
import os
from pathlib import Path
import time
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from .binance import BinanceClient
from .okx import OKX_ORDERBOOK_MAX_DEPTH, OkxClient
from .storage import CandleStick, SqliteCandleStore

BASE_DIR = Path(__file__).resolve().parent
WEB_DIR = BASE_DIR / "web"

SUPPORTED_BARS = {
    "1m": "1m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1H",
    "1H": "1H",
    "2h": "2H",
    "2H": "2H",
    "4h": "4H",
    "4H": "4H",
    "6h": "6H",
    "6H": "6H",
    "12h": "12H",
    "12H": "12H",
    "1d": "1D",
    "1D": "1D",
    "2d": "2D",
    "2D": "2D",
    "3d": "3D",
    "3D": "3D",
    "1w": "1W",
    "1W": "1W",
    "1M": "1M",
    "3m": "3M",
    "3M": "3M",
}
BINANCE_BARS = {
    "1m": "1m",
    "3m": "3m",
    "5m": "5m",
    "15m": "15m",
    "30m": "30m",
    "1h": "1h",
    "2h": "2h",
    "4h": "4h",
    "6h": "6h",
    "12h": "12h",
    "1d": "1d",
    "3d": "3d",
    "1w": "1w",
    "1M": "1M",
}
BINANCE_SPOT_SOURCE = "binance"
BINANCE_FUTURES_SOURCE = "binance_futures"

VALID_SOURCES = {"okx", BINANCE_SPOT_SOURCE, BINANCE_FUTURES_SOURCE}

DEFAULT_INST_ID = os.getenv("TAUTO_INST_ID", "BTC-USDT")
DEFAULT_DB_PATH = os.getenv("TAUTO_DB_PATH", "candles.db")

app = FastAPI(title="TAuto K-Line Service")
store = SqliteCandleStore(DEFAULT_DB_PATH)
okx_client = OkxClient()
binance_client = BinanceClient()
binance_futures_client = BinanceClient(base_url="https://fapi.binance.com")
BINANCE_CLIENTS = {
    BINANCE_SPOT_SOURCE: binance_client,
    BINANCE_FUTURES_SOURCE: binance_futures_client,
}


@app.on_event("startup")
def _startup() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    store.initialize()


app.mount("/static", StaticFiles(directory=WEB_DIR), name="static")


@app.get("/", response_class=HTMLResponse)
def index() -> HTMLResponse:
    html_path = WEB_DIR / "index.html"
    return HTMLResponse(html_path.read_text(encoding="utf-8"))


@app.get("/api/candles")
def get_candles(
    inst_id: str = Query(DEFAULT_INST_ID, description="Instrument ID"),
    bar: str = Query("1m", description="Candlestick bar"),
    limit: Optional[int] = Query(300, ge=1, le=2000),
    source: str = Query("okx", description="Data source (okx/binance/binance_futures)"),
    all_data: bool = Query(
        False, description="Return all candles for the selected bar when true."
    ),
    since_ts: Optional[int] = Query(
        None, description="Return candles newer than the provided timestamp."
    ),
    end_ts: Optional[int] = Query(
        None, description="Return candles older than or equal to the provided timestamp."
    ),
) -> dict:
    if source not in VALID_SOURCES:
        raise HTTPException(status_code=400, detail="Unsupported data source")
    if source in BINANCE_CLIENTS:
        normalized = BINANCE_BARS.get(bar)
        if normalized is None:
            raise HTTPException(status_code=400, detail="Unsupported bar interval")
        resolved_limit = min(limit or 500, 1000)
        client = BINANCE_CLIENTS[source]
        if all_data:
            _backfill_binance_history(inst_id, normalized, client=client, source=source)
            candles = store.fetch_candles(source, inst_id, normalized, limit=None)
            payload = [_to_kline_payload(candle) for candle in candles]
        else:
            start_time = since_ts + 1 if since_ts is not None else None
            _store_binance_klines(
                inst_id=inst_id,
                bar=normalized,
                limit=resolved_limit,
                client=client,
                source=source,
                start_time=start_time,
                end_time=end_ts,
            )
            candles = store.fetch_candles(
                source,
                inst_id,
                normalized,
                limit=resolved_limit,
                start_ts=start_time,
                end_ts=end_ts,
            )
            payload = [_to_kline_payload(candle) for candle in candles]
        return {
            "instId": inst_id,
            "bar": bar,
            "count": len(payload),
            "data": payload,
            "source": source,
        }
    normalized = SUPPORTED_BARS.get(bar)
    if normalized is None:
        raise HTTPException(status_code=400, detail="Unsupported bar interval")
    resolved_limit = None if all_data else limit
    start_ts = None
    if since_ts is not None and not all_data:
        start_ts = since_ts + 1
    candles = store.fetch_candles(
        "okx", inst_id, normalized, limit=resolved_limit, start_ts=start_ts, end_ts=end_ts
    )
    payload = [_to_kline_payload(candle) for candle in candles]
    return {
        "instId": inst_id,
        "bar": bar,
        "count": len(payload),
        "data": payload,
        "source": source,
    }


@app.get("/api/ticker")
def get_ticker(
    inst_id: str = Query(DEFAULT_INST_ID, description="Instrument ID"),
    source: str = Query("okx", description="Data source (okx/binance/binance_futures)"),
) -> dict:
    if source not in VALID_SOURCES:
        raise HTTPException(status_code=400, detail="Unsupported data source")
    try:
        if source in BINANCE_CLIENTS:
            ticker = BINANCE_CLIENTS[source].get_ticker(inst_id)
            if not ticker:
                raise HTTPException(status_code=502, detail="Ticker data unavailable")
            ts_ms = int(time.time() * 1000)
            return {
                "instId": inst_id,
                "last": ticker.get("price"),
                "ts": ts_ms,
                "source": source,
            }
        ticker = okx_client.get_ticker(inst_id)
        if not ticker:
            raise HTTPException(status_code=502, detail="Ticker data unavailable")
        return {
            "instId": inst_id,
            "last": ticker.get("last"),
            "ts": ticker.get("ts"),
            "source": source,
        }
    except HTTPException:
        raise
    except Exception as exc:  # noqa: BLE001 - map upstream errors to gateway error
        logging.getLogger(__name__).warning("Ticker fetch failed: %s", exc)
        raise HTTPException(status_code=502, detail="Ticker data unavailable") from exc


@app.get("/api/orderbook")
def get_orderbook(
    inst_id: str = Query(DEFAULT_INST_ID, description="Instrument ID"),
    depth: int = Query(1000, ge=1, le=1000),
    source: str = Query("okx", description="Data source (okx/binance/binance_futures)"),
) -> dict:
    if source not in VALID_SOURCES:
        raise HTTPException(status_code=400, detail="Unsupported data source")
    resolved_depth = depth
    if source in BINANCE_CLIENTS:
        book = BINANCE_CLIENTS[source].get_order_book(inst_id, limit=depth)
    else:
        resolved_depth = min(depth, OKX_ORDERBOOK_MAX_DEPTH)
        book = okx_client.get_order_book(inst_id, depth=resolved_depth)
    if not book:
        raise HTTPException(status_code=502, detail="Order book data unavailable")
    ts_value = book.get("ts")
    ts_ms = int(ts_value) if ts_value is not None else int(time.time() * 1000)
    try:
        store.upsert_orderbook_snapshot(
            inst_id=inst_id,
            ts_ms=ts_ms,
            bids=book.get("bids", []),
            asks=book.get("asks", []),
            depth=resolved_depth,
        )
    except Exception:  # noqa: BLE001 - avoid breaking API on persistence errors
        logging.getLogger(__name__).exception("Failed to store order book snapshot")
    return {
        "instId": inst_id,
        "ts": ts_ms,
        "bids": book.get("bids", []),
        "asks": book.get("asks", []),
        "source": source,
    }


def _store_binance_klines(
    inst_id: str,
    bar: str,
    limit: int,
    client: BinanceClient,
    source: str,
    start_time: Optional[int] = None,
    end_time: Optional[int] = None,
) -> int:
    klines = client.get_klines(
        symbol=inst_id,
        interval=bar,
        limit=limit,
        start_time=start_time,
        end_time=end_time,
    )
    if not klines:
        return 0
    candles = [
        CandleStick(
            source=source,
            inst_id=inst_id,
            bar=bar,
            ts=int(row[0]),
            open=float(row[1]),
            high=float(row[2]),
            low=float(row[3]),
            close=float(row[4]),
            volume=float(row[5]),
            volume_ccy=float(row[7]) if len(row) > 7 else 0.0,
            volume_quote=float(row[7]) if len(row) > 7 else 0.0,
            confirm=True,
        )
        for row in klines
    ]
    store.upsert_candles(candles)
    return len(candles)


def _backfill_binance_history(inst_id: str, bar: str, client: BinanceClient, source: str) -> None:
    interval_ms = _binance_interval_ms(bar)
    now_ms = int(time.time() * 1000)
    cutoff_ms = now_ms - (90 * 24 * 60 * 60 * 1000)
    end_time = now_ms
    while end_time >= cutoff_ms:
        fetched = _store_binance_klines(
            inst_id=inst_id,
            bar=bar,
            limit=1000,
            client=client,
            source=source,
            end_time=end_time,
        )
        if fetched == 0:
            break
        end_time -= fetched * interval_ms


def _binance_interval_ms(interval: str) -> int:
    unit = interval[-1]
    value = int(interval[:-1])
    if unit == "m":
        return value * 60 * 1000
    if unit == "h":
        return value * 60 * 60 * 1000
    if unit == "d":
        return value * 24 * 60 * 60 * 1000
    if unit == "w":
        return value * 7 * 24 * 60 * 60 * 1000
    if unit == "M":
        return value * 30 * 24 * 60 * 60 * 1000
    raise ValueError(f"Unsupported interval: {interval}")


def _to_kline_payload(candle: CandleStick) -> dict:
    return {
        "timestamp": candle.ts,
        "open": candle.open,
        "high": candle.high,
        "low": candle.low,
        "close": candle.close,
        "volume": candle.volume,
    }
