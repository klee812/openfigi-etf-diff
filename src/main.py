"""CLI entry point: python -m src.main <command>"""
from __future__ import annotations

import argparse
import logging
import sys

from . import config
from .client import OpenFigiClient
from .scanner import full_scan, incremental_diff
from .storage import export_diff_csv, export_diff_json, load_snapshot


def cmd_exchanges(args: argparse.Namespace) -> None:
    """Handle the ``exchanges`` subcommand.

    Fetches all valid exchange codes from the OpenFIGI API and prints them
    in sorted order.

    Args:
        args: Parsed CLI arguments (unused beyond command dispatch).
    """
    client = OpenFigiClient()
    codes = client.get_exchange_codes()
    print(f"Found {len(codes)} exchange codes:\n")
    for code in sorted(codes):
        print(f"  {code}")


def cmd_scan(args: argparse.Namespace) -> None:
    """Handle the ``scan`` subcommand.

    Runs a full scan of all exchanges and saves the resulting snapshot to
    ``data/snapshot.json``. Prints a brief summary on completion.

    Args:
        args: Parsed CLI arguments (unused beyond command dispatch).
    """
    client = OpenFigiClient()
    snapshot = full_scan(client)
    print(f"Scan complete.")
    print(f"  Exchanges: {len(snapshot.exchanges)}")
    print(f"  Unique composite FIGIs: {len(snapshot.all_composite_figis)}")
    print(f"  Saved to: {config.SNAPSHOT_PATH}")


def cmd_diff(args: argparse.Namespace) -> None:
    """Handle the ``diff`` subcommand.

    Loads the existing snapshot and runs an incremental diff against the
    current API state. If new ETFs are found, exports them in the requested
    format(s). Exits with code 1 if no prior snapshot exists.

    Args:
        args: Parsed CLI arguments. Reads ``args.format`` (``"csv"``,
            ``"json"``, or ``"both"``).
    """
    previous = load_snapshot()
    if previous is None:
        print("No existing snapshot found. Run 'scan' first.")
        sys.exit(1)

    client = OpenFigiClient()
    diff = incremental_diff(client, previous)

    print(diff.summary())

    fmt = args.format
    if diff.new_count > 0:
        if fmt in ("csv", "both"):
            path = export_diff_csv(diff)
            print(f"CSV exported to: {path}")
        if fmt in ("json", "both"):
            path = export_diff_json(diff)
            print(f"JSON exported to: {path}")
    else:
        print("No new ETFs found.")


def cmd_info(args: argparse.Namespace) -> None:
    """Handle the ``info`` subcommand.

    Loads the existing snapshot and prints summary statistics: timestamp,
    exchange count, total composite FIGIs, and the top 20 exchanges by total.
    Exits with code 1 if no prior snapshot exists.

    Args:
        args: Parsed CLI arguments (unused beyond command dispatch).
    """
    snapshot = load_snapshot()
    if snapshot is None:
        print("No snapshot found. Run 'scan' first.")
        sys.exit(1)

    print(f"Snapshot timestamp: {snapshot.timestamp}")
    print(f"Exchanges: {len(snapshot.exchanges)}")
    print(f"Unique composite FIGIs: {len(snapshot.all_composite_figis)}")
    print()

    # Top exchanges by count
    sorted_exchanges = sorted(
        snapshot.exchanges.values(), key=lambda e: e.total, reverse=True
    )
    print("Top 20 exchanges by total:")
    for snap in sorted_exchanges[:20]:
        print(f"  {snap.exch_code:10s}  {snap.total:>6d} total  {len(snap.figis):>6d} FIGIs  {snap.last_scanned}")


def main() -> None:
    """Parse CLI arguments and dispatch to the appropriate subcommand handler.

    Available subcommands:
    - ``exchanges``: List all valid exchange codes.
    - ``scan``: Run a full scan and save a snapshot.
    - ``diff``: Incrementally diff against the last snapshot and export new ETFs.
    - ``info``: Print statistics about the current snapshot.

    The ``--verbose`` / ``-v`` flag enables DEBUG-level logging.
    """
    parser = argparse.ArgumentParser(
        prog="openfigi-etf-diff",
        description="Monitor global ETF/ETP listings using the OpenFIGI API",
    )
    parser.add_argument(
        "--verbose", "-v", action="store_true", help="Enable debug logging"
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subparsers.add_parser("exchanges", help="List available exchange codes")
    subparsers.add_parser("scan", help="Full scan of all exchanges")

    diff_parser = subparsers.add_parser("diff", help="Incremental diff against last snapshot")
    diff_parser.add_argument(
        "--format", choices=["csv", "json", "both"], default="both",
        help="Output format for new ETFs (default: both)",
    )

    subparsers.add_parser("info", help="Show snapshot statistics")

    args = parser.parse_args()

    log_level = logging.DEBUG if args.verbose else logging.INFO
    logging.basicConfig(
        level=log_level,
        format="%(asctime)s %(levelname)-8s %(name)s: %(message)s",
        datefmt="%H:%M:%S",
    )

    commands = {
        "exchanges": cmd_exchanges,
        "scan": cmd_scan,
        "diff": cmd_diff,
        "info": cmd_info,
    }
    commands[args.command](args)


if __name__ == "__main__":
    main()
