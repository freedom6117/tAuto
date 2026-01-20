"""Candlestick data service with persistence and rate limiting."""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
import time
from typing import Iterable, List, Optional

from .okx import OkxClient
from .storage import CandleStick, DatabaseBackend, compute_retention_cutoff


class RateLimiter:
    """Simple token bucket rate limiter."""

    def __init__(self, rate_per_second: float) -> None:
        self._rate = max(rate_per_second, 0.0)
        self._capacity = max(rate_per_second, 1.0)
        self._tokens = self._capacity
        self._last_refill = time.monotonic()

    def acquire(self) -> None:
        if self._rate <= 0:
            return
        while True:
            now = time.monotonic()
            elapsed = now - self._last_refill
            self._tokens = min(self._capacity, self._tokens + elapsed * self._rate)
            self._last_refill = now
            if self._tokens >= 1:
                self._tokens -= 1
                return
            sleep_time = max((1 - self._tokens) / self._rate, 0.01)
            time.sleep(sleep_time)


@dataclass
class CandlestickService:
    """Candlestick data fetcher with persistence and maintenance."""

    client: OkxClient
    store: DatabaseBackend
    bar: str = "1m"
    history_qps: float = 10.0
    realtime_qps: float = 1.0
    retention_months: int = 1
    history_limit: int = 300
    history_limiter: RateLimiter = field(init=False)
    realtime_limiter: RateLimiter = field(init=False)

    def __post_init__(self) -> None:
        self.history_limiter = RateLimiter(self.history_qps)
        self.realtime_limiter = RateLimiter(self.realtime_qps)

    def initialize(self) -> None:
        self.store.initialize()

    def fetch_realtime(self, inst_id: str, limit: int = 1) -> List[CandleStick]:
        """Fetch latest candles with realtime rate limiting."""

        self.realtime_limiter.acquire()
        data = self.client.get_candlesticks(inst_id=inst_id, bar=self.bar, limit=limit)
        candles = [self._parse_candle(inst_id, row) for row in data]
        self.store.upsert_candles(candles)
        return candles

    def fetch_history(
        self,
        inst_id: str,
        start_ts: int,
        end_ts: int,
    ) -> List[CandleStick]:
        """Fetch historical candles between timestamps."""

        all_candles: List[CandleStick] = []
        cursor: Optional[int] = None
        while True:
            self.history_limiter.acquire()
            before = str(cursor) if cursor else None
            data = self.client.get_candlesticks(
                inst_id=inst_id,
                bar=self.bar,
                limit=self.history_limit,
                before=before,
                use_history=True,
            )
            if not data:
                break
            candles = [self._parse_candle(inst_id, row) for row in data]
            filtered = [
                candle
                for candle in candles
                if start_ts <= candle.ts <= end_ts
            ]
            all_candles.extend(filtered)
            self.store.upsert_candles(filtered)
            oldest = min(candle.ts for candle in candles)
            if oldest <= start_ts:
                break
            cursor = oldest
        return all_candles

    def backfill_missing(
        self,
        inst_id: str,
        start_ts: int,
        end_ts: int,
    ) -> List[int]:
        """Detect and backfill missing timestamps."""

        expected = self._expected_timestamps(start_ts, end_ts)
        existing = set(
            self.store.fetch_existing_timestamps(inst_id, self.bar, start_ts, end_ts)
        )
        missing = [ts for ts in expected if ts not in existing]
        for ts in missing:
            self.fetch_history(inst_id, ts, ts)
        return missing

    def fill_since_latest(self, inst_id: str) -> Optional[int]:
        """Backfill from the latest stored candle to now."""

        latest = self.store.latest_timestamp(inst_id, self.bar)
        if latest is None:
            return None
        now_ts = int(datetime.now(timezone.utc).timestamp() * 1000)
        if latest >= now_ts:
            return latest
        self.fetch_history(inst_id, latest, now_ts)
        return latest

    def cleanup_old_data(self) -> int:
        cutoff_ts = compute_retention_cutoff(self.retention_months)
        return self.store.delete_older_than(cutoff_ts)

    def _expected_timestamps(self, start_ts: int, end_ts: int) -> List[int]:
        interval_ms = _bar_to_milliseconds(self.bar)
        aligned_start = start_ts - (start_ts % interval_ms)
        aligned_end = end_ts - (end_ts % interval_ms)
        return list(range(aligned_start, aligned_end + interval_ms, interval_ms))

    def _parse_candle(self, inst_id: str, row: Iterable[str]) -> CandleStick:
        values = list(row)
        return CandleStick(
            inst_id=inst_id,
            bar=self.bar,
            ts=int(values[0]),
            open=float(values[1]),
            high=float(values[2]),
            low=float(values[3]),
            close=float(values[4]),
            volume=float(values[5]),
            volume_ccy=float(values[6]),
            volume_quote=float(values[7]),
            confirm=values[8] == "1",
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


__all__ = ["CandlestickService", "RateLimiter"]
