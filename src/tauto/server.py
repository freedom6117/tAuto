"""FastAPI service for K-line chart data and static UI."""

from __future__ import annotations

import logging
import os
from pathlib import Path
from typing import Optional

from fastapi import FastAPI, HTTPException, Query
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles

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

DEFAULT_INST_ID = os.getenv("TAUTO_INST_ID", "BTC-USDT")
DEFAULT_DB_PATH = os.getenv("TAUTO_DB_PATH", "candles.db")

app = FastAPI(title="TAuto K-Line Service")
store = SqliteCandleStore(DEFAULT_DB_PATH)


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
) -> dict:
    normalized = SUPPORTED_BARS.get(bar)
    if normalized is None:
        raise HTTPException(status_code=400, detail="Unsupported bar interval")
    candles = store.fetch_candles(inst_id, normalized, limit=limit)
    payload = [_to_kline_payload(candle) for candle in candles]
    return {"instId": inst_id, "bar": bar, "count": len(payload), "data": payload}


def _to_kline_payload(candle: CandleStick) -> dict:
    return {
        "timestamp": candle.ts,
        "open": candle.open,
        "high": candle.high,
        "low": candle.low,
        "close": candle.close,
        "volume": candle.volume,
    }
