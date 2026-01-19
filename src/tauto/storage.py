"""Database abstractions and SQLite implementation."""

from __future__ import annotations

from dataclasses import dataclass
import logging
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional, Protocol, Sequence
import sqlite3


@dataclass(frozen=True)
class CandleStick:
    """Represents a single candlestick data point."""

    inst_id: str
    bar: str
    ts: int
    open: float
    high: float
    low: float
    close: float
    volume: float
    volume_ccy: float
    volume_quote: float
    confirm: bool


class DatabaseBackend(Protocol):
    """Database interface for candle storage."""

    def initialize(self) -> None:
        """Initialize the database schema."""

    def upsert_candles(self, candles: Sequence[CandleStick]) -> None:
        """Upsert candlestick data into storage."""

    def fetch_existing_timestamps(
        self,
        inst_id: str,
        bar: str,
        start_ts: int,
        end_ts: int,
    ) -> List[int]:
        """Fetch timestamps already present for the given range."""

    def latest_timestamp(self, inst_id: str, bar: str) -> Optional[int]:
        """Fetch the latest timestamp stored for the given instrument."""

    def fetch_candles(
        self,
        inst_id: str,
        bar: str,
        limit: int = 300,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
    ) -> List[CandleStick]:
        """Fetch candlestick data for the given range."""

    def delete_older_than(self, cutoff_ts: int) -> int:
        """Delete data older than the given timestamp. Returns deleted rows."""


class CacheBackend(Protocol):
    """Optional cache abstraction (e.g. Redis) for future use."""

    def get(self, key: str) -> Optional[str]:
        """Retrieve a cached value."""

    def set(self, key: str, value: str, ttl_seconds: int) -> None:
        """Set a cached value with TTL."""


@dataclass
class SqliteCandleStore:
    """SQLite-backed candle storage implementation."""

    db_path: str = "candles.db"
    logger: logging.Logger = logging.getLogger(__name__)

    def _connect(self) -> sqlite3.Connection:
        connection = sqlite3.connect(self.db_path)
        connection.execute("PRAGMA journal_mode=WAL")
        connection.execute("PRAGMA synchronous=NORMAL")
        return connection

    def initialize(self) -> None:
        with self._connect() as connection:
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS candles (
                    inst_id TEXT NOT NULL,
                    bar TEXT NOT NULL,
                    ts INTEGER NOT NULL,
                    open REAL NOT NULL,
                    high REAL NOT NULL,
                    low REAL NOT NULL,
                    close REAL NOT NULL,
                    volume REAL NOT NULL,
                    volume_ccy REAL NOT NULL,
                    volume_quote REAL NOT NULL,
                    confirm INTEGER NOT NULL,
                    PRIMARY KEY (inst_id, bar, ts)
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_candles_ts ON candles(ts)"
            )

    def upsert_candles(self, candles: Sequence[CandleStick]) -> None:
        if not candles:
            return
        inst_id = candles[0].inst_id
        bar = candles[0].bar
        latest_ts = candles[-1].ts
        rows = [
            (
                candle.inst_id,
                candle.bar,
                candle.ts,
                candle.open,
                candle.high,
                candle.low,
                candle.close,
                candle.volume,
                candle.volume_ccy,
                candle.volume_quote,
                1 if candle.confirm else 0,
            )
            for candle in candles
        ]
        with self._connect() as connection:
            connection.executemany(
                """
                INSERT INTO candles (
                    inst_id, bar, ts, open, high, low, close,
                    volume, volume_ccy, volume_quote, confirm
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(inst_id, bar, ts) DO UPDATE SET
                    open=excluded.open,
                    high=excluded.high,
                    low=excluded.low,
                    close=excluded.close,
                    volume=excluded.volume,
                    volume_ccy=excluded.volume_ccy,
                    volume_quote=excluded.volume_quote,
                    confirm=excluded.confirm
                """,
                rows,
            )
        self.logger.info(
            "Upserted %s candles for %s (%s), latest ts=%s",
            len(candles),
            inst_id,
            bar,
            latest_ts,
        )

    def fetch_existing_timestamps(
        self,
        inst_id: str,
        bar: str,
        start_ts: int,
        end_ts: int,
    ) -> List[int]:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                SELECT ts FROM candles
                WHERE inst_id = ? AND bar = ? AND ts BETWEEN ? AND ?
                ORDER BY ts ASC
                """,
                (inst_id, bar, start_ts, end_ts),
            )
            return [row[0] for row in cursor.fetchall()]

    def latest_timestamp(self, inst_id: str, bar: str) -> Optional[int]:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                SELECT MAX(ts) FROM candles WHERE inst_id = ? AND bar = ?
                """,
                (inst_id, bar),
            )
            value = cursor.fetchone()[0]
            return int(value) if value is not None else None

    def fetch_candles(
        self,
        inst_id: str,
        bar: str,
        limit: int = 300,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
    ) -> List[CandleStick]:
        where_clauses = ["inst_id = ?", "bar = ?"]
        params: list[object] = [inst_id, bar]
        if start_ts is not None:
            where_clauses.append("ts >= ?")
            params.append(start_ts)
        if end_ts is not None:
            where_clauses.append("ts <= ?")
            params.append(end_ts)
        where_sql = " AND ".join(where_clauses)
        query = (
            "SELECT inst_id, bar, ts, open, high, low, close, volume, "
            "volume_ccy, volume_quote, confirm "
            "FROM candles WHERE "
            f"{where_sql} ORDER BY ts DESC LIMIT ?"
        )
        params.append(limit)
        with self._connect() as connection:
            cursor = connection.execute(query, params)
            rows = cursor.fetchall()
        candles = [
            CandleStick(
                inst_id=row[0],
                bar=row[1],
                ts=row[2],
                open=row[3],
                high=row[4],
                low=row[5],
                close=row[6],
                volume=row[7],
                volume_ccy=row[8],
                volume_quote=row[9],
                confirm=bool(row[10]),
            )
            for row in rows
        ]
        return list(reversed(candles))

    def delete_older_than(self, cutoff_ts: int) -> int:
        with self._connect() as connection:
            cursor = connection.execute(
                "DELETE FROM candles WHERE ts < ?",
                (cutoff_ts,),
            )
            return cursor.rowcount


def subtract_months(reference: datetime, months: int) -> datetime:
    """Subtract a number of calendar months from a datetime."""

    year = reference.year
    month = reference.month - months
    while month <= 0:
        month += 12
        year -= 1

    day = min(reference.day, _days_in_month(year, month))
    return reference.replace(year=year, month=month, day=day)


def _days_in_month(year: int, month: int) -> int:
    next_month = datetime(year, month, 28, tzinfo=timezone.utc) + timedelta(days=4)
    last_day = next_month.replace(day=1) - timedelta(days=1)
    return last_day.day


def compute_retention_cutoff(months: int, now: Optional[datetime] = None) -> int:
    """Compute retention cutoff timestamp in milliseconds."""

    now = now or datetime.now(timezone.utc)
    cutoff = subtract_months(now, months)
    return int(cutoff.timestamp() * 1000)


__all__ = [
    "CacheBackend",
    "CandleStick",
    "DatabaseBackend",
    "SqliteCandleStore",
    "compute_retention_cutoff",
]
