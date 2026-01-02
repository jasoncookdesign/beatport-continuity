"""Command-line interface for beatport-continuity."""
from __future__ import annotations

import argparse
import sys
from typing import Callable, Sequence

from .logging_utils import get_logger

LOG = get_logger(__name__)


def handle_init_db(_args: argparse.Namespace) -> None:
    LOG.info("Initializing database scaffold (placeholder)")
    print("TODO: init-db")


def handle_ingest(_args: argparse.Namespace) -> None:
    LOG.info("Ingesting weekly charts (placeholder)")
    print("TODO: ingest")


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

    return parser


def main(argv: Sequence[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    args.func(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
