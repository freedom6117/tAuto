"""Background service for keeping candlestick data up to date."""

from __future__ import annotations

import logging
import os
import time
from collections import deque
from datetime import datetime, timezone
from typing import Deque, Iterable, Tuple

from .binance import BinanceClient
from .candles import CandlestickService
from .okx import OkxClient
from .storage import CandleStick, SqliteCandleStore

DEFAULT_INST_IDS = [
    inst.strip()
    for inst in os.getenv("TAUTO_INST_IDS", "BTC-USDT,BTC-USDT-SWAP,ETH-USDT,ETH-USDT-SWAP").split(",")
    if inst.strip()
]
DEFAULT_BINANCE_INST_IDS = [
    inst.strip()
    for inst in os.getenv("TAUTO_BINANCE_INST_IDS", "BTCUSDT,ETHUSDT").split(",")
    if inst.strip()
]
DEFAULT_DB_PATH = os.getenv("TAUTO_DB_PATH", "candles.db")
DEFAULT_LIMIT = int(os.getenv("TAUTO_FETCH_LIMIT", "300"))
DEFAULT_INTERVAL = float(os.getenv("TAUTO_FETCH_INTERVAL", "15"))
DEFAULT_QPS = float(os.getenv("TAUTO_FETCH_QPS", "10"))
DEFAULT_BACKFILL_DAYS = int(os.getenv("TAUTO_BACKFILL_DAYS_PER_CYCLE", "3"))
DEFAULT_BARS = [
    "1m",
    "5m",
    "15m",
    "30m",
    "1H",
    "2H",
    "4H",
    "6H",
    "12H",
    "1D",
    "2D",
    "3D",
    "1W",
    "1M",
    "3M",
]
DEFAULT_BINANCE_BARS = [
    "1m",
    "3m",
    "5m",
    "15m",
    "30m",
    "1h",
    "2h",
    "4h",
    "6h",
    "12h",
    "1d",
    "3d",
    "1w",
    "1M",
]


class BinanceBackfillService:
    """Binance candlestick backfill helper."""

    source: str = "binance"

    def __init__(self, client: BinanceClient, store: SqliteCandleStore, bar: str) -> None:
        self.client = client
        self.store = store
        self.bar = bar

    def fetch_history(self, inst_id: str, start_ts: int, end_ts: int) -> list[CandleStick]:
        interval_ms = _binance_interval_ms(self.bar)
        all_candles: list[CandleStick] = []
        cursor = end_ts
        while cursor >= start_ts:
            klines = self.client.get_klines(
                symbol=inst_id,
                interval=self.bar,
                limit=1000,
                start_time=start_ts,
                end_time=cursor,
            )
            if not klines:
                break
            candles = [_parse_binance_kline(inst_id, self.bar, row) for row in klines]
            filtered = [
                candle
                for candle in candles
                if start_ts <= candle.ts <= end_ts
            ]
            if filtered:
                self.store.upsert_candles(filtered)
                all_candles.extend(filtered)
            oldest = min(candle.ts for candle in candles)
            cursor = oldest - interval_ms
        return all_candles


def run_fetcher() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    store = SqliteCandleStore(DEFAULT_DB_PATH)
    store.initialize()
    okx_client = OkxClient()
    binance_client = BinanceClient()
    okx_services = {}
    for bar in DEFAULT_BARS:
        service = CandlestickService(client=okx_client, store=store, bar=bar)
        service.initialize()
        okx_services[bar] = service
    binance_services = {}
    for bar in DEFAULT_BINANCE_BARS:
        binance_services[bar] = BinanceBackfillService(
            client=binance_client, store=store, bar=bar
        )
    min_interval = 1 / max(DEFAULT_QPS, 1)
    day_queue = _build_missing_day_queue_multi(
        store,
        {
            "okx": (DEFAULT_INST_IDS, okx_services),
            "binance": (DEFAULT_BINANCE_INST_IDS, binance_services),
        },
        int(datetime.now(timezone.utc).timestamp() * 1000),
    )

    while True:
        cycle_start = time.time()
        for inst_id in DEFAULT_INST_IDS:
            for bar, service in okx_services.items():
                try:
                    _refresh_candles(service, store, inst_id, bar, DEFAULT_LIMIT)
                except Exception:  # noqa: BLE001 - keep fetcher alive on transient failures
                    logging.getLogger(__name__).exception(
                        "Failed to refresh candles for %s (%s)", inst_id, bar
                    )
                time.sleep(min_interval)
        try:
            _process_backfill_queue_multi(
                day_queue,
                {
                    "okx": (DEFAULT_INST_IDS, okx_services),
                    "binance": (DEFAULT_BINANCE_INST_IDS, binance_services),
                },
                store,
                DEFAULT_BACKFILL_DAYS,
            )
        except Exception:  # noqa: BLE001 - keep fetcher alive on transient failures
            logging.getLogger(__name__).exception("Failed to process backfill queue")
        elapsed = time.time() - cycle_start
        time.sleep(max(0, DEFAULT_INTERVAL - elapsed))


def _refresh_candles(
    service: CandlestickService,
    store: SqliteCandleStore,
    inst_id: str,
    bar: str,
    limit: int,
) -> None:
    latest = store.latest_timestamp(service.client.source, inst_id, bar)
    now_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
    if latest is None:
        interval_ms = _bar_to_milliseconds(bar)
        start_ts = max(_three_months_ago(now_ts), now_ts - (limit * interval_ms))
        fetched = service.fetch_history(inst_id, start_ts, now_ts)
        logging.getLogger(__name__).info(
            "Fetched %s historical candles for %s (%s)",
            len(fetched),
            inst_id,
            bar,
        )
        return

    realtime = service.fetch_realtime(inst_id, limit=1, latest_ts=latest)
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
    start_ts = max(latest, _three_months_ago(now_ts))
    if start_ts < now_ts:
        fetched = service.fetch_history(inst_id, start_ts, now_ts)
        logging.getLogger(__name__).info(
            "Filled candles from %s to %s for %s (%s)",
            start_ts,
            now_ts,
            inst_id,
            bar,
        )


def _bar_to_milliseconds(bar: str) -> int:
    if bar.endswith("s"):
        return int(bar[:-1]) * 1000
    if bar.endswith("m"):
        return int(bar[:-1]) * 60 * 1000
    if bar.endswith("H"):
        return int(bar[:-1]) * 60 * 60 * 1000
    if bar.endswith("D"):
        return int(bar[:-1]) * 24 * 60 * 60 * 1000
    if bar.endswith("W"):
        return int(bar[:-1]) * 7 * 24 * 60 * 60 * 1000
    if bar.endswith("M"):
        return int(bar[:-1]) * 30 * 24 * 60 * 60 * 1000
    raise ValueError(f"Unsupported bar format: {bar}")


def _three_months_ago(now_ts: int) -> int:
    return now_ts - (90 * 24 * 60 * 60 * 1000)


def _build_day_queue(now_ts: int) -> Deque[Tuple[int, int]]:
    day_ms = 24 * 60 * 60 * 1000
    start_ts = _three_months_ago(now_ts)
    end_day_start = _day_start_ts(now_ts)
    start_day_start = _day_start_ts(start_ts)
    days = deque()
    current = end_day_start
    while current >= start_day_start:
        day_end = min(current + day_ms - 1, now_ts)
        days.append((current, day_end))
        current -= day_ms
    return days


def _build_missing_day_queue_multi(
    store: SqliteCandleStore,
    sources: dict[str, tuple[Iterable[str], dict[str, object]]],
    now_ts: int,
) -> Deque[Tuple[str, int, int]]:
    logger = logging.getLogger(__name__)
    day_queue: Deque[Tuple[str, int, int]] = deque()
    for day_start, day_end in _build_day_queue(now_ts):
        for source, (inst_ids, services) in sources.items():
            if _day_has_missing(store, source, inst_ids, services, day_start, day_end):
                day_queue.append((source, day_start, day_end))
    if not day_queue:
        logger.info("No missing candles detected in the last 3 months window.")
    return day_queue


def _day_start_ts(ts: int) -> int:
    dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    day_start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(day_start.timestamp() * 1000)


def _process_backfill_queue_multi(
    day_queue: Deque[Tuple[str, int, int]],
    sources: dict[str, tuple[Iterable[str], dict[str, object]]],
    store: SqliteCandleStore,
    days_per_cycle: int,
) -> None:
    if not day_queue:
        day_queue.extend(
            _build_missing_day_queue_multi(
                store,
                sources,
                int(datetime.now(timezone.utc).timestamp() * 1000),
            )
        )
    if days_per_cycle <= 0:
        return
    logger = logging.getLogger(__name__)
    for _ in range(min(days_per_cycle, len(day_queue))):
        source, day_start, day_end = day_queue.popleft()
        inst_ids, services = sources[source]
        for inst_id in inst_ids:
            for bar, service in services.items():
                missing = _find_missing_in_day(
                    store, source, inst_id, bar, day_start, day_end
                )
                if not missing:
                    continue
                service.fetch_history(inst_id, day_start, day_end)
                first_missing = min(missing)
                last_missing = max(missing)
                logger.info(
                    "Backfilled %s missing candles for %s:%s (%s) on %s (missing %s - %s)",
                    len(missing),
                    source,
                    inst_id,
                    bar,
                    datetime.fromtimestamp(day_start / 1000, tz=timezone.utc).date(),
                    _format_ts(first_missing),
                    _format_ts(last_missing),
                )


def _find_missing_in_day(
    store: SqliteCandleStore,
    source: str,
    inst_id: str,
    bar: str,
    day_start: int,
    day_end: int,
) -> list[int]:
    interval_ms = (
        _binance_interval_ms(bar) if source == "binance" else _bar_to_milliseconds(bar)
    )
    aligned_start = day_start - (day_start % interval_ms)
    aligned_end = day_end - (day_end % interval_ms)
    expected = list(range(aligned_start, aligned_end + interval_ms, interval_ms))
    existing = set(
        store.fetch_existing_timestamps(
            source, inst_id, bar, aligned_start, aligned_end
        )
    )
    return [ts for ts in expected if ts not in existing]


def _day_has_missing(
    store: SqliteCandleStore,
    source: str,
    inst_ids: Iterable[str],
    services: dict[str, object],
    day_start: int,
    day_end: int,
) -> bool:
    for inst_id in inst_ids:
        for bar in services:
            if _find_missing_in_day(store, source, inst_id, bar, day_start, day_end):
                return True
    return False


def _parse_binance_kline(inst_id: str, bar: str, row: list) -> CandleStick:
    return CandleStick(
        source="binance",
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


def _format_ts(ts: int) -> str:
    return datetime.fromtimestamp(ts / 1000, tz=timezone.utc).isoformat()


if __name__ == "__main__":
    run_fetcher()
