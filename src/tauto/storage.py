"""Database abstractions and SQLite implementation."""

from __future__ import annotations

from dataclasses import dataclass
import json
import logging
from datetime import datetime, timedelta, timezone
from typing import Iterable, List, Optional, Protocol, Sequence
import sqlite3


@dataclass(frozen=True)
class CandleStick:
    """Represents a single candlestick data point."""

    source: str
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
        source: str,
        inst_id: str,
        bar: str,
        start_ts: int,
        end_ts: int,
    ) -> List[int]:
        """Fetch timestamps already present for the given range."""

    def latest_timestamp(self, source: str, inst_id: str, bar: str) -> Optional[int]:
        """Fetch the latest timestamp stored for the given instrument."""

    def fetch_candles(
        self,
        source: str,
        inst_id: str,
        bar: str,
        limit: Optional[int] = 300,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
    ) -> List[CandleStick]:
        """Fetch candlestick data for the given range."""

    def delete_older_than(self, cutoff_ts: int) -> int:
        """Delete data older than the given timestamp. Returns deleted rows."""

    def upsert_orderbook_snapshot(
        self,
        inst_id: str,
        ts_ms: int,
        bids: Sequence[Sequence[object]],
        asks: Sequence[Sequence[object]],
        depth: Optional[int] = None,
    ) -> None:
        """Upsert order book snapshot data into storage."""


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
            self._migrate_schema(connection)
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS candles (
                    source TEXT NOT NULL,
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
                    PRIMARY KEY (source, inst_id, bar, ts)
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_candles_ts ON candles(source, ts)"
            )
            connection.execute(
                """
                CREATE TABLE IF NOT EXISTS orderbook_snapshots (
                    inst_id TEXT NOT NULL,
                    ts_sec INTEGER NOT NULL,
                    ts_ms INTEGER NOT NULL,
                    depth INTEGER,
                    bids TEXT NOT NULL,
                    asks TEXT NOT NULL,
                    PRIMARY KEY (inst_id, ts_sec)
                )
                """
            )
            connection.execute(
                "CREATE INDEX IF NOT EXISTS idx_orderbook_ts ON orderbook_snapshots(ts_sec)"
            )

    def upsert_candles(self, candles: Sequence[CandleStick]) -> None:
        if not candles:
            return
        source = candles[0].source
        inst_id = candles[0].inst_id
        bar = candles[0].bar
        latest_ts = candles[-1].ts
        rows = [
            (
                candle.source,
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
                    source, inst_id, bar, ts, open, high, low, close,
                    volume, volume_ccy, volume_quote, confirm
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(source, inst_id, bar, ts) DO UPDATE SET
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
            "Upserted %s candles for %s:%s (%s), latest ts=%s",
            len(candles),
            source,
            inst_id,
            bar,
            latest_ts,
        )

    def fetch_existing_timestamps(
        self,
        source: str,
        inst_id: str,
        bar: str,
        start_ts: int,
        end_ts: int,
    ) -> List[int]:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                SELECT ts FROM candles
                WHERE source = ? AND inst_id = ? AND bar = ? AND ts BETWEEN ? AND ?
                ORDER BY ts ASC
                """,
                (source, inst_id, bar, start_ts, end_ts),
            )
            return [row[0] for row in cursor.fetchall()]

    def latest_timestamp(self, source: str, inst_id: str, bar: str) -> Optional[int]:
        with self._connect() as connection:
            cursor = connection.execute(
                """
                SELECT MAX(ts) FROM candles WHERE source = ? AND inst_id = ? AND bar = ?
                """,
                (source, inst_id, bar),
            )
            value = cursor.fetchone()[0]
            return int(value) if value is not None else None

    def fetch_candles(
        self,
        source: str,
        inst_id: str,
        bar: str,
        limit: Optional[int] = 300,
        start_ts: Optional[int] = None,
        end_ts: Optional[int] = None,
    ) -> List[CandleStick]:
        where_clauses = ["source = ?", "inst_id = ?", "bar = ?"]
        params: list[object] = [source, inst_id, bar]
        if start_ts is not None:
            where_clauses.append("ts >= ?")
            params.append(start_ts)
        if end_ts is not None:
            where_clauses.append("ts <= ?")
            params.append(end_ts)
        where_sql = " AND ".join(where_clauses)
        base_query = (
            "SELECT source, inst_id, bar, ts, open, high, low, close, volume, "
            "volume_ccy, volume_quote, confirm "
            "FROM candles WHERE "
            f"{where_sql} ORDER BY ts DESC"
        )
        if limit is not None:
            base_query = f"{base_query} LIMIT ?"
            params.append(limit)
        with self._connect() as connection:
            cursor = connection.execute(base_query, params)
            rows = cursor.fetchall()
        candles = [
            CandleStick(
                source=row[0],
                inst_id=row[1],
                bar=row[2],
                ts=row[3],
                open=row[4],
                high=row[5],
                low=row[6],
                close=row[7],
                volume=row[8],
                volume_ccy=row[9],
                volume_quote=row[10],
                confirm=bool(row[11]),
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

    def upsert_orderbook_snapshot(
        self,
        inst_id: str,
        ts_ms: int,
        bids: Sequence[Sequence[object]],
        asks: Sequence[Sequence[object]],
        depth: Optional[int] = None,
    ) -> None:
        ts_sec = int(ts_ms) // 1000
        payload = (
            inst_id,
            ts_sec,
            int(ts_ms),
            depth,
            json.dumps(bids, separators=(",", ":"), ensure_ascii=False),
            json.dumps(asks, separators=(",", ":"), ensure_ascii=False),
        )
        with self._connect() as connection:
            connection.execute(
                """
                INSERT INTO orderbook_snapshots (
                    inst_id, ts_sec, ts_ms, depth, bids, asks
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(inst_id, ts_sec) DO UPDATE SET
                    ts_ms=excluded.ts_ms,
                    depth=excluded.depth,
                    bids=excluded.bids,
                    asks=excluded.asks
                """,
                payload,
            )

    def _migrate_schema(self, connection: sqlite3.Connection) -> None:
        cursor = connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='candles'"
        )
        exists = cursor.fetchone() is not None
        if not exists:
            return
        columns = [row[1] for row in connection.execute("PRAGMA table_info(candles)")]
        if "source" in columns:
            return
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS candles_v2 (
                source TEXT NOT NULL,
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
                PRIMARY KEY (source, inst_id, bar, ts)
            )
            """
        )
        connection.execute(
            """
            INSERT INTO candles_v2 (
                source, inst_id, bar, ts, open, high, low, close,
                volume, volume_ccy, volume_quote, confirm
            )
            SELECT 'okx', inst_id, bar, ts, open, high, low, close,
                   volume, volume_ccy, volume_quote, confirm
            FROM candles
            """
        )
        connection.execute("DROP TABLE candles")
        connection.execute("ALTER TABLE candles_v2 RENAME TO candles")


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
