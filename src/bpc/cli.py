"""Command-line interface for beatport-continuity."""
from __future__ import annotations

import argparse
import sys
from datetime import date
from typing import Callable, Sequence

from .config import load_paths
from .db import get_conn, init_db
from .logging_utils import get_logger
from .time_utils import today_bucket

LOG = get_logger(__name__)


def handle_init_db(_args: argparse.Namespace) -> None:
    paths = load_paths()
    paths.data.mkdir(parents=True, exist_ok=True)
    db_path = paths.db

    LOG.info("Initializing database at %s", db_path)
    conn = get_conn(str(db_path))
    init_db(conn)
    conn.close()
    print(f"Database initialized at {db_path}")


def _resolve_snapshot_date(args: argparse.Namespace)  -> date:
    return args.snapshot_date or today_bucket()


def handle_ingest(args: argparse.Namespace) -> None:
    snap_date = _resolve_snapshot_date(args)
    LOG.info("Ingesting weekly charts for snapshot %s (placeholder)", snap_date)
    print(f"TODO: ingest (snapshot_date={snap_date})")


def handle_compute(_args: argparse.Namespace) -> None:
    LOG.info("Computing durability metrics (placeholder)")
    print("TODO: compute")


def handle_report(_args: argparse.Namespace) -> None:
    LOG.info("Rendering report (placeholder)")
    print("TODO: report")


def handle_run_all(args: argparse.Namespace) -> None:
    LOG.info("Running full pipeline (placeholder)")
    handlers: Sequence[Callable[[argparse.Namespace], None]] = (
        handle_init_db,
        handle_ingest,
        handle_compute,
        handle_report,
    )
    for handler in handlers:
        handler(args)


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="bpc",
        description="Beatport continuity CLI",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)

    subcommands = [
        ("init-db", handle_init_db, "Initialize local storage or tables"),
        ("ingest", handle_ingest, "Pull weekly charts and cache them"),
        ("compute", handle_compute, "Compute continuity/durability metrics"),
        ("report", handle_report, "Render HTML report"),
        ("run-all", handle_run_all, "Run init, ingest, compute, and report"),
    ]

    for name, handler, help_text in subcommands:
        subparser = subparsers.add_parser(name, help=help_text)
        subparser.set_defaults(func=handler)

        if name in {"ingest", "run-all"}:
            subparser.add_argument(
                "--snapshot-date",
                type=date.fromisoformat,
                help="ISO date (YYYY-MM-DD) for the weekly snapshot; defaults to current week bucket",
            )

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
