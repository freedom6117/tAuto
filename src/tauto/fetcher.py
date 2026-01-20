"""Background service for keeping candlestick data up to date."""

from __future__ import annotations

import logging
import os
import time
from collections import deque
from datetime import datetime, timezone
from typing import Deque, Iterable, Tuple

from .candles import CandlestickService
from .okx import OkxClient
from .storage import SqliteCandleStore

DEFAULT_INST_IDS = [
    inst.strip()
    for inst in os.getenv("TAUTO_INST_IDS", "BTC-USDT,BTC-USDT-SWAP").split(",")
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
    "8H",
    "12H",
    "1D",
]


def run_fetcher() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s - %(message)s",
    )
    store = SqliteCandleStore(DEFAULT_DB_PATH)
    store.initialize()
    client = OkxClient()
    services = {}
    for bar in DEFAULT_BARS:
        service = CandlestickService(client=client, store=store, bar=bar)
        service.initialize()
        services[bar] = service
    min_interval = 1 / max(DEFAULT_QPS, 1)
    day_queue = _build_day_queue(int(datetime.now(timezone.utc).timestamp() * 1000))

    while True:
        cycle_start = time.time()
        for inst_id in DEFAULT_INST_IDS:
            for bar, service in services.items():
                _refresh_candles(service, store, inst_id, bar, DEFAULT_LIMIT)
                time.sleep(min_interval)
        _process_backfill_queue(
            day_queue,
            services,
            store,
            DEFAULT_INST_IDS,
            DEFAULT_BACKFILL_DAYS,
        )
        elapsed = time.time() - cycle_start
        time.sleep(max(0, DEFAULT_INTERVAL - elapsed))


def _refresh_candles(
    service: CandlestickService,
    store: SqliteCandleStore,
    inst_id: str,
    bar: str,
    limit: int,
) -> None:
    latest = store.latest_timestamp(inst_id, bar)
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


def _day_start_ts(ts: int) -> int:
    dt = datetime.fromtimestamp(ts / 1000, tz=timezone.utc)
    day_start = dt.replace(hour=0, minute=0, second=0, microsecond=0)
    return int(day_start.timestamp() * 1000)


def _process_backfill_queue(
    day_queue: Deque[Tuple[int, int]],
    services: dict[str, CandlestickService],
    store: SqliteCandleStore,
    inst_ids: Iterable[str],
    days_per_cycle: int,
) -> None:
    if not day_queue:
        day_queue.extend(_build_day_queue(int(datetime.now(timezone.utc).timestamp() * 1000)))
    if days_per_cycle <= 0:
        return
    logger = logging.getLogger(__name__)
    for _ in range(min(days_per_cycle, len(day_queue))):
        day_start, day_end = day_queue.popleft()
        for inst_id in inst_ids:
            for bar, service in services.items():
                missing = _find_missing_in_day(store, inst_id, bar, day_start, day_end)
                if not missing:
                    continue
                service.fetch_history(inst_id, day_start, day_end)
                logger.info(
                    "Backfilled %s missing candles for %s (%s) on %s",
                    len(missing),
                    inst_id,
                    bar,
                    datetime.fromtimestamp(day_start / 1000, tz=timezone.utc).date(),
                )


def _find_missing_in_day(
    store: SqliteCandleStore,
    inst_id: str,
    bar: str,
    day_start: int,
    day_end: int,
) -> list[int]:
    interval_ms = _bar_to_milliseconds(bar)
    aligned_start = day_start - (day_start % interval_ms)
    aligned_end = day_end - (day_end % interval_ms)
    expected = list(range(aligned_start, aligned_end + interval_ms, interval_ms))
    existing = set(store.fetch_existing_timestamps(inst_id, bar, aligned_start, aligned_end))
    return [ts for ts in expected if ts not in existing]


if __name__ == "__main__":
    run_fetcher()
