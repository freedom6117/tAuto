"""CLI entrypoint for the OKX demo client."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(ROOT / "src"))

from tauto.okx import OkxClient, summarize_instruments  # noqa: E402


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


if __name__ == "__main__":
    main()
