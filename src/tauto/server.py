"""FastAPI service for K-line chart data and static UI."""

from __future__ import annotations

import logging
import os
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

from .candles import CandlestickService
from .okx import OkxClient
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
}

DEFAULT_INST_ID = os.getenv("TAUTO_INST_ID", "BTC-USDT")
DEFAULT_DB_PATH = os.getenv("TAUTO_DB_PATH", "candles.db")

app = FastAPI(title="TAuto K-Line Service")
store = SqliteCandleStore(DEFAULT_DB_PATH)
client = OkxClient()


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
    limit: int = Query(300, ge=10, le=2000),
    refresh: bool = Query(True, description="Refresh from OKX before reading DB"),
) -> dict:
    normalized = SUPPORTED_BARS.get(bar)
    if normalized is None:
        raise HTTPException(status_code=400, detail="Unsupported bar interval")
    if refresh:
        _refresh_candles(inst_id, normalized, limit)
    candles = store.fetch_candles(inst_id, normalized, limit=limit)
    payload = [_to_kline_payload(candle) for candle in candles]
    return {"instId": inst_id, "bar": bar, "count": len(payload), "data": payload}


def _refresh_candles(inst_id: str, bar: str, limit: int) -> None:
    service = CandlestickService(client=client, store=store, bar=bar)
    service.initialize()
    latest = store.latest_timestamp(inst_id, bar)
    if latest is None:
        now_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
        interval_ms = _bar_to_milliseconds(bar)
        start_ts = max(_thirty_days_ago(now_ts), now_ts - (limit * interval_ms))
        fetched = service.fetch_history(inst_id, start_ts, now_ts)
        logging.getLogger(__name__).info(
            "Fetched %s historical candles for %s (%s)",
            len(fetched),
            inst_id,
            bar,
        )
        return
    now_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
    realtime = service.fetch_realtime(inst_id, limit=1)
    logging.getLogger(__name__).info(
        "Fetched %s realtime candles for %s (%s)",
        len(realtime),
        inst_id,
        bar,
    )
    previous_latest = service.fill_since_latest(inst_id)
    if previous_latest is not None:
        logging.getLogger(__name__).info(
            "Backfilled candles since %s for %s (%s)",
            previous_latest,
            inst_id,
            bar,
        )
    start_ts = max(latest, _thirty_days_ago(now_ts))
    if start_ts < now_ts:
        fetched = service.fetch_history(inst_id, start_ts, now_ts)
        logging.getLogger(__name__).info(
            "Filled candles from %s to %s for %s (%s)",
            start_ts,
            now_ts,
            inst_id,
            bar,
        )


def _to_kline_payload(candle: CandleStick) -> dict:
    return {
        "timestamp": candle.ts,
        "open": candle.open,
        "high": candle.high,
        "low": candle.low,
        "close": candle.close,
        "volume": candle.volume,
    }


def _bar_to_milliseconds(bar: str) -> int:
    if bar.endswith("s"):
        return int(bar[:-1]) * 1000
    if bar.endswith("m"):
        return int(bar[:-1]) * 60 * 1000
    if bar.endswith("H"):
        return int(bar[:-1]) * 60 * 60 * 1000
    if bar.endswith("D"):
        return int(bar[:-1]) * 24 * 60 * 60 * 1000
    raise ValueError(f"Unsupported bar format: {bar}")


def _thirty_days_ago(now_ts: int) -> int:
    return now_ts - (30 * 24 * 60 * 60 * 1000)
