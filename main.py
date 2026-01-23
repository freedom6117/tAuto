"""OKX Demo 命令行入口。"""

from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from tauto.candles import CandlestickService  # noqa: E402
from tauto.okx import OkxClient, summarize_instruments  # noqa: E402
from tauto.storage import SqliteCandleStore  # noqa: E402


def _pretty_print(data: Any) -> None:
    print(json.dumps(data, ensure_ascii=False, indent=2))


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="OKX public API demo")
    subparsers = parser.add_subparsers(dest="command", required=True)

    list_parser = subparsers.add_parser("list", help="List instruments")
    list_parser.add_argument(
        "--type",
        dest="inst_type",
        default="SPOT",
        help="Instrument type (SPOT, SWAP, FUTURES, OPTION)",
    )

    book_parser = subparsers.add_parser("book", help="Fetch order book")
    book_parser.add_argument("inst_id", help="Instrument ID, e.g. BTC-USDT")
    book_parser.add_argument("--depth", type=int, default=5, help="Order book depth")

    trades_parser = subparsers.add_parser("trades", help="Fetch recent trades")
    trades_parser.add_argument("inst_id", help="Instrument ID, e.g. BTC-USDT")
    trades_parser.add_argument("--limit", type=int, default=100, help="Trade limit")

    candles_parser = subparsers.add_parser("candles", help="Store candlestick data")
    candles_parser.add_argument("inst_id", help="Instrument ID, e.g. BTC-USDT")
    candles_parser.add_argument("--bar", default="1m", help="Candlestick bar")
    candles_parser.add_argument("--db", default="candles.db", help="SQLite DB path")
    candles_parser.add_argument(
        "--start",
        type=int,
        default=None,
        help="Historical start timestamp in milliseconds",
    )
    candles_parser.add_argument(
        "--end",
        type=int,
        default=None,
        help="Historical end timestamp in milliseconds",
    )
    candles_parser.add_argument(
        "--retention-months",
        type=int,
        default=1,
        help="Retention months for cleanup",
    )
    candles_parser.add_argument(
        "--history-qps",
        type=float,
        default=10.0,
        help="Historical fetch QPS",
    )
    candles_parser.add_argument(
        "--realtime-qps",
        type=float,
        default=1.0,
        help="Realtime fetch QPS",
    )

    candles_monitor_parser = subparsers.add_parser(
        "candles-monitor", help="Monitor candlestick storage for 1 minute"
    )
    candles_monitor_parser.add_argument("inst_id", help="Instrument ID, e.g. BTC-USDT")
    candles_monitor_parser.add_argument("--bar", default="1s", help="Candlestick bar")
    candles_monitor_parser.add_argument(
        "--db", default="candles.db", help="SQLite DB path"
    )
    candles_monitor_parser.add_argument(
        "--duration",
        type=int,
        default=60,
        help="Monitor duration in seconds",
    )
    candles_monitor_parser.add_argument(
        "--history-qps",
        type=float,
        default=10.0,
        help="Historical fetch QPS",
    )
    candles_monitor_parser.add_argument(
        "--realtime-qps",
        type=float,
        default=1.0,
        help="Realtime fetch QPS",
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()
    client = OkxClient()

    if args.command == "list":
        instruments = client.list_instruments(args.inst_type)
        _pretty_print(
            {
                "count": len(instruments),
                "instIds": summarize_instruments(instruments),
            }
        )
        return

    if args.command == "book":
        order_book = client.get_order_book(args.inst_id, args.depth)
        _pretty_print(order_book)
        return

    if args.command == "trades":
        trades = client.get_trades(args.inst_id, args.limit)
        _pretty_print(trades)
        return

    if args.command == "candles":
        store = SqliteCandleStore(args.db)
        service = CandlestickService(
            client=client,
            store=store,
            bar=args.bar,
            history_qps=args.history_qps,
            realtime_qps=args.realtime_qps,
            retention_months=args.retention_months,
        )
        service.initialize()
        if args.start is not None and args.end is not None:
            service.fetch_history(args.inst_id, args.start, args.end)
            service.backfill_missing(args.inst_id, args.start, args.end)
        else:
            service.fetch_realtime(args.inst_id)
            service.fill_since_latest(args.inst_id)
        deleted = service.cleanup_old_data()
        _pretty_print({"deleted": deleted})
        return

    if args.command == "candles-monitor":
        store = SqliteCandleStore(args.db)
        service = CandlestickService(
            client=client,
            store=store,
            bar=args.bar,
            history_qps=args.history_qps,
            realtime_qps=args.realtime_qps,
        )
        service.initialize()
        start = time.monotonic()
        for tick in range(args.duration):
            service.fetch_realtime(args.inst_id)
            latest = store.latest_timestamp(service.client.source, args.inst_id, args.bar)
            now = time.strftime("%Y-%m-%d %H:%M:%S")
            print(
                f"[{now}] tick={tick + 1}/{args.duration} "
                f"latest_ts={latest or 'n/a'}"
            )
            next_tick = start + tick + 1
            time.sleep(max(0.0, next_tick - time.monotonic()))
        return


if __name__ == "__main__":
    main()
